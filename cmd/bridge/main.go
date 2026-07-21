// Command bridge runs the S3-compatible HTTP server that translates
// Pterodactyl Panel/Wings backup traffic into proxmox-backup-client
// invocations against a Proxmox Backup Server.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/backend"
	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/config"
	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/logging"
	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/pbs"
	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/s3api"
	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/sigv4"
	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/stage"
	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/store"
)

func main() {
	healthcheck := flag.Bool("healthcheck", false, "check that a locally running bridge is healthy, then exit (used by the Docker HEALTHCHECK)")
	flag.Parse()
	if *healthcheck {
		os.Exit(runHealthcheck())
	}

	cfg, err := config.Load()
	if err != nil {
		// Logger isn't set up yet; config errors go straight to stderr.
		os.Stderr.WriteString("bridge: " + err.Error() + "\n")
		os.Exit(1)
	}

	log := logging.New(cfg.LogLevel, cfg.LogFormat)
	slog.SetDefault(log)

	if err := run(cfg, log); err != nil {
		log.Error("bridge exited with error", "error", err)
		os.Exit(1)
	}
}

func run(cfg config.Config, log *slog.Logger) error {
	db, err := store.Open(cfg.DataDir + "/bridge.db")
	if err != nil {
		return err
	}
	defer db.Close()

	stg, err := stage.New(cfg.ScratchDir)
	if err != nil {
		return err
	}

	pbsClient := &pbs.Client{
		Repository:  cfg.PBSRepository,
		Password:    pbsPassword(cfg),
		Fingerprint: cfg.PBSFingerprint,
		BinPath:     cfg.PBSBinPath,
		Timeout:     cfg.PBSCommandTimeout,
	}

	be := backend.New(db, stg, pbsClient, cfg.PBSBackupType, log)

	handler := &s3api.Handler{
		Verifier: &sigv4.Verifier{
			Creds:     sigv4.Credentials{AccessKey: cfg.AccessKey, SecretKey: cfg.SecretKey},
			ClockSkew: cfg.ClockSkew,
		},
		Backend: be,
		Log:     log,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	gc := &stage.GC{Store: db, Stage: stg, TTL: cfg.MultipartTTL, Interval: cfg.GCInterval, Log: log}
	go gc.Run(ctx)

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 30 * time.Second,
		// No overall read/write timeout: multipart CompleteMultipartUpload
		// invokes a synchronous proxmox-backup-client backup, which for a
		// large backup can legitimately take a long time. See docs/LIMITATIONS.md.
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("bridge listening", "addr", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Info("shutting down")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

// runHealthcheck performs a minimal, dependency-free liveness check against
// this same process's own /healthz endpoint, reading only the listen address
// from the environment (not full config.Load, which would also require PBS
// credentials to be valid) so it stays fast and side-effect-free.
func runHealthcheck() int {
	addr := os.Getenv("BRIDGE_LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	url := "http://localhost" + addr + "/healthz"
	if len(addr) > 0 && addr[0] != ':' {
		url = "http://" + addr + "/healthz"
	}

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

func pbsPassword(cfg config.Config) string {
	if cfg.PBSAPIToken != "" {
		return cfg.PBSAPIToken
	}
	return cfg.PBSPassword
}
