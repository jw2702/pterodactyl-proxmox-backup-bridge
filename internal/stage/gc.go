package stage

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/store"
)

// GC periodically sweeps abandoned multipart uploads (no activity within
// ttl) and reconciles the scratch directory tree against bbolt state.
type GC struct {
	Store    *store.DB
	Stage    *Manager
	TTL      time.Duration
	Interval time.Duration
	Log      *slog.Logger
}

func (g *GC) log() *slog.Logger {
	if g.Log != nil {
		return g.Log
	}
	return slog.Default()
}

// Run blocks, sweeping on g.Interval until ctx is cancelled. Call it in its
// own goroutine.
func (g *GC) Run(ctx context.Context) {
	g.reconcileOnStartup()

	ticker := time.NewTicker(g.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.sweep()
		}
	}
}

func (g *GC) sweep() {
	cutoff := time.Now().Add(-g.TTL)
	uploads, err := g.Store.ListUploadsOlderThan(cutoff)
	if err != nil {
		g.log().Error("gc: listing stale uploads failed", "error", err)
		return
	}
	for _, u := range uploads {
		if err := g.Stage.RemoveUploadDir(u.UploadID); err != nil {
			g.log().Error("gc: removing scratch dir failed", "upload_id", u.UploadID, "error", err)
		}
		if err := g.Store.DeleteUpload(u.UploadID); err != nil {
			g.log().Error("gc: deleting upload record failed", "upload_id", u.UploadID, "error", err)
			continue
		}
		g.log().Info("gc: removed abandoned multipart upload", "upload_id", u.UploadID, "bucket", u.Bucket, "key", u.Key)
	}
}

// reconcileOnStartup removes scratch directories left behind by a crash
// (present on disk but with no corresponding bbolt upload record).
func (g *GC) reconcileOnStartup() {
	multipartRoot := filepath.Join(g.Stage.Root, "multipart")
	entries, err := os.ReadDir(multipartRoot)
	if err != nil {
		if !os.IsNotExist(err) {
			g.log().Error("gc: reading scratch multipart dir failed", "error", err)
		}
		return
	}

	knownIDs, err := g.Store.ListAllUploadIDs()
	if err != nil {
		g.log().Error("gc: listing known upload IDs failed", "error", err)
		return
	}
	known := make(map[string]bool, len(knownIDs))
	for _, id := range knownIDs {
		known[id] = true
	}

	for _, entry := range entries {
		if !entry.IsDir() || known[entry.Name()] {
			continue
		}
		path := filepath.Join(multipartRoot, entry.Name())
		if err := os.RemoveAll(path); err != nil {
			g.log().Error("gc: removing orphaned scratch dir failed", "path", path, "error", err)
			continue
		}
		g.log().Info("gc: removed orphaned scratch dir with no bbolt record", "path", path)
	}
}
