package pbs

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
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

// TestBackup_TransientRetry mirrors a single flaky-connection blip during
// backup: fewer failures than MaxTransientRetries must be absorbed
// transparently.
func TestBackup_TransientRetry(t *testing.T) {
	c, _ := newTestClient(t)
	t.Setenv("STUB_FORCE_BACKUP_FAIL_COUNT", "1")
	ctx := context.Background()

	src := filepath.Join(t.TempDir(), "src.img")
	if err := os.WriteFile(src, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	backupTime := time.Unix(1700000000, 0).UTC()
	if _, err := c.Backup(ctx, src, "host", "transient-backup", backupTime, "mybucket"); err != nil {
		t.Fatalf("expected transient failure to be retried transparently, got: %v", err)
	}
}

// TestBackup_TransientRetryExhausted verifies that persistent transient
// failures are eventually surfaced rather than retried forever.
func TestBackup_TransientRetryExhausted(t *testing.T) {
	c, _ := newTestClient(t)
	t.Setenv("STUB_FORCE_BACKUP_FAIL_COUNT", strconv.Itoa(MaxTransientRetries+2))
	ctx := context.Background()

	src := filepath.Join(t.TempDir(), "src.img")
	if err := os.WriteFile(src, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	backupTime := time.Unix(1700000000, 0).UTC()
	if _, err := c.Backup(ctx, src, "host", "exhausted-backup", backupTime, "mybucket"); err == nil {
		t.Fatal("expected Backup to fail after exhausting transient retries")
	}
}

// TestRestore_TransientRetry mirrors a flaky connection during a ranged
// restore (which writes to a local file rather than streaming).
func TestRestore_TransientRetry(t *testing.T) {
	c, _ := newTestClient(t)
	ctx := context.Background()

	src := filepath.Join(t.TempDir(), "src.img")
	want := "restore retry contents"
	if err := os.WriteFile(src, []byte(want), 0o644); err != nil {
		t.Fatal(err)
	}
	backupTime := time.Unix(1700000000, 0).UTC()
	usedTime, err := c.Backup(ctx, src, "host", "restore-retry", backupTime, "mybucket")
	if err != nil {
		t.Fatalf("Backup: %v", err)
	}

	t.Setenv("STUB_FORCE_RESTORE_FAIL_COUNT", "1")
	out := filepath.Join(t.TempDir(), "restored.img")
	if err := c.Restore(ctx, "host", "restore-retry", usedTime, "mybucket", out); err != nil {
		t.Fatalf("expected transient restore failure to be retried transparently, got: %v", err)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// TestForget_TransientRetry mirrors a flaky connection during DeleteObject's
// underlying Forget call.
func TestForget_TransientRetry(t *testing.T) {
	c, _ := newTestClient(t)
	ctx := context.Background()

	src := filepath.Join(t.TempDir(), "src.img")
	if err := os.WriteFile(src, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	backupTime := time.Unix(1700000000, 0).UTC()
	usedTime, err := c.Backup(ctx, src, "host", "forget-retry", backupTime, "mybucket")
	if err != nil {
		t.Fatalf("Backup: %v", err)
	}

	t.Setenv("STUB_FORCE_FORGET_FAIL_COUNT", "1")
	if err := c.Forget(ctx, "host", "forget-retry", usedTime, "mybucket"); err != nil {
		t.Fatalf("expected transient forget failure to be retried transparently, got: %v", err)
	}
}

// TestRestoreStream_TransientRetry mirrors a flaky connection that fails
// before producing any output: the stream must still come back intact and
// readable, since nothing had been handed to the caller yet at the point of
// failure.
func TestRestoreStream_TransientRetry(t *testing.T) {
	c, _ := newTestClient(t)
	ctx := context.Background()

	src := filepath.Join(t.TempDir(), "src.img")
	want := "streamed retry contents"
	if err := os.WriteFile(src, []byte(want), 0o644); err != nil {
		t.Fatal(err)
	}
	backupTime := time.Unix(1700000000, 0).UTC()
	usedTime, err := c.Backup(ctx, src, "host", "stream-retry", backupTime, "mybucket")
	if err != nil {
		t.Fatalf("Backup: %v", err)
	}

	t.Setenv("STUB_FORCE_RESTORE_FAIL_COUNT", "1")
	rc, err := c.RestoreStream(ctx, "host", "stream-retry", usedTime, "mybucket")
	if err != nil {
		t.Fatalf("expected transient stream-start failure to be retried transparently, got: %v", err)
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

// TestRestoreStream_TransientRetryExhausted verifies persistent failures
// starting the restore stream are surfaced as an error from RestoreStream
// itself (not merely on Close), since nothing was ever returned to the
// caller.
func TestRestoreStream_TransientRetryExhausted(t *testing.T) {
	c, _ := newTestClient(t)
	ctx := context.Background()

	src := filepath.Join(t.TempDir(), "src.img")
	if err := os.WriteFile(src, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	backupTime := time.Unix(1700000000, 0).UTC()
	usedTime, err := c.Backup(ctx, src, "host", "stream-exhausted", backupTime, "mybucket")
	if err != nil {
		t.Fatalf("Backup: %v", err)
	}

	t.Setenv("STUB_FORCE_RESTORE_FAIL_COUNT", strconv.Itoa(MaxTransientRetries+2))
	if _, err := c.RestoreStream(ctx, "host", "stream-exhausted", usedTime, "mybucket"); err == nil {
		t.Fatal("expected RestoreStream to fail after exhausting transient retries")
	}
}

// TestBackup_TransientRetrySharesTimeoutBudget verifies c.Timeout bounds the
// whole retry sequence (every attempt plus every backoff wait, combined)
// rather than being handed out fresh to each attempt. Without that shared
// budget, persistent failures would take roughly
// transientRetryBaseDelay+transientRetryMaxDelay (~1s) of backoff alone,
// regardless of how small c.Timeout is set.
func TestBackup_TransientRetrySharesTimeoutBudget(t *testing.T) {
	c, _ := newTestClient(t)
	c.Timeout = 300 * time.Millisecond
	t.Setenv("STUB_FORCE_BACKUP_FAIL_COUNT", strconv.Itoa(MaxTransientRetries+2))
	ctx := context.Background()

	src := filepath.Join(t.TempDir(), "src.img")
	if err := os.WriteFile(src, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	_, err := c.Backup(ctx, src, "host", "budget-bound", time.Unix(1700000000, 0), "mybucket")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected Backup to fail against persistent transient failures")
	}
	if elapsed > 600*time.Millisecond {
		t.Fatalf("expected the shared %v timeout budget to cut retries short, but took %v", c.Timeout, elapsed)
	}
}

// TestRestoreStream_SuccessfulStreamSurvivesRetryBudget verifies that once
// RestoreStream hands back a stream, it is no longer bound by c.Timeout: the
// retry budget only governs how long starting the stream is allowed to take,
// not how long the caller may then spend reading from it (which can
// legitimately be much longer for a large restore).
func TestRestoreStream_SuccessfulStreamSurvivesRetryBudget(t *testing.T) {
	c, _ := newTestClient(t)
	c.Timeout = 100 * time.Millisecond
	ctx := context.Background()

	src := filepath.Join(t.TempDir(), "src.img")
	want := "read happens well after the retry budget expired"
	if err := os.WriteFile(src, []byte(want), 0o644); err != nil {
		t.Fatal(err)
	}
	backupTime := time.Unix(1700000000, 0).UTC()
	usedTime, err := c.Backup(ctx, src, "host", "budget-survive", backupTime, "mybucket")
	if err != nil {
		t.Fatalf("Backup: %v", err)
	}

	rc, err := c.RestoreStream(ctx, "host", "budget-survive", usedTime, "mybucket")
	if err != nil {
		t.Fatalf("RestoreStream: %v", err)
	}

	// Let c.Timeout expire well before reading anything from the stream.
	time.Sleep(3 * c.Timeout)

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading stream after the retry budget expired: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if string(got) != want {
		t.Fatalf("got %q, want %q", got, want)
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
