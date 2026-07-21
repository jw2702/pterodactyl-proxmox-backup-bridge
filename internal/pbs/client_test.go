package pbs

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func stubBinPath(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not determine test file location")
	}
	// internal/pbs/client_test.go -> repo root -> scripts/stub-proxmox-backup-client
	path := filepath.Join(filepath.Dir(thisFile), "..", "..", "scripts", "stub-proxmox-backup-client")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stub script not found at %s: %v", path, err)
	}
	return path
}

func newTestClient(t *testing.T) (*Client, string) {
	t.Helper()
	stateDir := t.TempDir()
	t.Setenv("STUB_PBS_STATE_DIR", stateDir)
	return &Client{
		Repository: "test@pbs:store1",
		Password:   "testpass",
		BinPath:    stubBinPath(t),
		Timeout:    10 * time.Second,
	}, stateDir
}

func TestBackupRestoreForget_RoundTrip(t *testing.T) {
	c, _ := newTestClient(t)
	ctx := context.Background()

	src := filepath.Join(t.TempDir(), "src.img")
	if err := os.WriteFile(src, []byte("backup archive contents"), 0o644); err != nil {
		t.Fatal(err)
	}

	backupTime := time.Unix(1700000000, 0).UTC()
	usedTime, err := c.Backup(ctx, src, "host", "mybackup", backupTime, "mybucket")
	if err != nil {
		t.Fatalf("Backup: %v", err)
	}
	if !usedTime.Equal(backupTime) {
		t.Fatalf("expected no collision, usedTime=%v want %v", usedTime, backupTime)
	}

	out := filepath.Join(t.TempDir(), "restored.img")
	if err := c.Restore(ctx, "host", "mybackup", usedTime, "mybucket", out); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "backup archive contents" {
		t.Fatalf("restored data = %q", data)
	}

	if err := c.UpdateNotes(ctx, "host", "mybackup", usedTime, "mybucket", "mybucket/mykey"); err != nil {
		t.Fatalf("UpdateNotes: %v", err)
	}

	if err := c.Forget(ctx, "host", "mybackup", usedTime, "mybucket"); err != nil {
		t.Fatalf("Forget: %v", err)
	}

	err = c.Restore(ctx, "host", "mybackup", usedTime, "mybucket", out)
	if err == nil {
		t.Fatal("expected restore of forgotten snapshot to fail")
	}
}

func TestRestoreStream_StreamsContent(t *testing.T) {
	c, _ := newTestClient(t)
	ctx := context.Background()

	src := filepath.Join(t.TempDir(), "src.img")
	want := "streamed backup archive contents"
	if err := os.WriteFile(src, []byte(want), 0o644); err != nil {
		t.Fatal(err)
	}

	backupTime := time.Unix(1700000000, 0).UTC()
	usedTime, err := c.Backup(ctx, src, "host", "streamtest", backupTime, "mybucket")
	if err != nil {
		t.Fatalf("Backup: %v", err)
	}

	rc, err := c.RestoreStream(ctx, "host", "streamtest", usedTime, "mybucket")
	if err != nil {
		t.Fatalf("RestoreStream: %v", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading stream: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if string(got) != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestRestoreStream_MissingSnapshotErrorsOnClose(t *testing.T) {
	c, _ := newTestClient(t)
	ctx := context.Background()

	rc, err := c.RestoreStream(ctx, "host", "does-not-exist", time.Unix(1700000000, 0), "mybucket")
	if err != nil {
		// Some implementations might fail synchronously at Start(); either
		// is acceptable as long as the caller gets an error.
		return
	}
	_, readErr := io.ReadAll(rc)
	closeErr := rc.Close()
	if readErr == nil && closeErr == nil {
		t.Fatal("expected an error reading or closing a stream for a nonexistent snapshot")
	}
}

func TestForget_IdempotentOnMissingSnapshot(t *testing.T) {
	c, _ := newTestClient(t)
	ctx := context.Background()
	// Never backed up; Forget should succeed (treated as idempotent).
	if err := c.Forget(ctx, "host", "nonexistent", time.Unix(1700000000, 0), "mybucket"); err != nil {
		t.Fatalf("expected idempotent success, got %v", err)
	}
}

func TestEnsureNamespace_Idempotent(t *testing.T) {
	c, _ := newTestClient(t)
	ctx := context.Background()
	if err := c.EnsureNamespace(ctx, "mybucket"); err != nil {
		t.Fatalf("first EnsureNamespace: %v", err)
	}
	if err := c.EnsureNamespace(ctx, "mybucket"); err != nil {
		t.Fatalf("second (idempotent) EnsureNamespace: %v", err)
	}
}

func TestBackup_CollisionRetry(t *testing.T) {
	c, _ := newTestClient(t)
	t.Setenv("STUB_FORCE_COLLISION_COUNT", "2")
	ctx := context.Background()

	src := filepath.Join(t.TempDir(), "src.img")
	if err := os.WriteFile(src, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	backupTime := time.Unix(1700000000, 0).UTC()
	usedTime, err := c.Backup(ctx, src, "host", "collide", backupTime, "mybucket")
	if err != nil {
		t.Fatalf("Backup with retries: %v", err)
	}
	wantTime := backupTime.Add(2 * time.Second)
	if !usedTime.Equal(wantTime) {
		t.Fatalf("usedTime = %v, want %v (2 retries)", usedTime, wantTime)
	}
}

func TestArgvAndEnvLogged(t *testing.T) {
	c, _ := newTestClient(t)
	logFile := filepath.Join(t.TempDir(), "invocations.log")
	t.Setenv("STUB_LOG_FILE", logFile)
	ctx := context.Background()

	src := filepath.Join(t.TempDir(), "src.img")
	os.WriteFile(src, []byte("x"), 0o644)

	if _, err := c.Backup(ctx, src, "host", "logtest", time.Unix(1700000000, 0), "logns"); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	got := string(data)
	for _, want := range []string{"--backup-type host", "--backup-id logtest", "--backup-time 1700000000", "--ns logns", "PBS_REPOSITORY", "PBS_PASSWORD"} {
		if !strings.Contains(got, want) {
			t.Errorf("log missing %q, log was: %s", want, got)
		}
	}
}
