package stage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/store"
)

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	m, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return m
}

func TestWriteToTemp(t *testing.T) {
	m := newTestManager(t)
	res, err := m.WriteToTemp("puts", "put-*.tmp", bytes.NewReader([]byte("hello world")))
	if err != nil {
		t.Fatalf("WriteToTemp: %v", err)
	}
	if res.Size != 11 {
		t.Fatalf("size = %d, want 11", res.Size)
	}
	data, err := os.ReadFile(res.Path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello world" {
		t.Fatalf("got %q", data)
	}
}

func TestWritePartAndConcat(t *testing.T) {
	m := newTestManager(t)
	uploadID := "up-1"

	parts := [][]byte{
		bytes.Repeat([]byte("A"), 5),
		bytes.Repeat([]byte("B"), 5),
		bytes.Repeat([]byte("C"), 3),
	}
	var paths []string
	for i, pd := range parts {
		res, err := m.WritePart(uploadID, i+1, bytes.NewReader(pd))
		if err != nil {
			t.Fatalf("WritePart %d: %v", i+1, err)
		}
		paths = append(paths, res.Path)
	}

	final, err := m.ConcatParts(uploadID, paths)
	if err != nil {
		t.Fatalf("ConcatParts: %v", err)
	}
	data, err := os.ReadFile(final.Path)
	if err != nil {
		t.Fatal(err)
	}
	want := "AAAAABBBBBCCC"
	if string(data) != want {
		t.Fatalf("concatenated = %q, want %q", data, want)
	}
	if final.Size != int64(len(want)) {
		t.Fatalf("size = %d, want %d", final.Size, len(want))
	}
}

func TestWritePart_RetryOverwrites(t *testing.T) {
	m := newTestManager(t)
	uploadID := "up-retry"

	if _, err := m.WritePart(uploadID, 1, bytes.NewReader([]byte("first"))); err != nil {
		t.Fatal(err)
	}
	res2, err := m.WritePart(uploadID, 1, bytes.NewReader([]byte("second-attempt")))
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(res2.Path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "second-attempt" {
		t.Fatalf("expected retry to overwrite, got %q", data)
	}
}

func TestRemoveUploadDir(t *testing.T) {
	m := newTestManager(t)
	uploadID := "up-remove"
	if _, err := m.WritePart(uploadID, 1, bytes.NewReader([]byte("data"))); err != nil {
		t.Fatal(err)
	}
	if err := m.RemoveUploadDir(uploadID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(m.UploadDir(uploadID)); !os.IsNotExist(err) {
		t.Fatalf("expected upload dir to be gone, stat err = %v", err)
	}
}

func TestGC_SweepsAbandonedUploads(t *testing.T) {
	m := newTestManager(t)
	db, err := store.Open(filepath.Join(t.TempDir(), "bridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	uploadID := "abandoned-1"
	if err := db.CreateUpload(store.MultipartUpload{
		UploadID: uploadID, Bucket: "b", Key: "k",
		InitiatedAt: time.Now().Add(-48 * time.Hour), LastActivityAt: time.Now().Add(-48 * time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.WritePart(uploadID, 1, bytes.NewReader([]byte("orphaned data"))); err != nil {
		t.Fatal(err)
	}

	gc := &GC{Store: db, Stage: m, TTL: 24 * time.Hour, Interval: time.Hour}
	gc.sweep()

	if _, err := db.GetUpload(uploadID); err != store.ErrNotFound {
		t.Fatalf("expected upload record removed, got err=%v", err)
	}
	if _, err := os.Stat(m.UploadDir(uploadID)); !os.IsNotExist(err) {
		t.Fatalf("expected scratch dir removed, stat err = %v", err)
	}
}

func TestGC_ReconcileOnStartup_RemovesOrphanedScratchDir(t *testing.T) {
	m := newTestManager(t)
	db, err := store.Open(filepath.Join(t.TempDir(), "bridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Simulate a crash: scratch dir exists with no bbolt record.
	if _, err := m.WritePart("crash-orphan", 1, bytes.NewReader([]byte("x"))); err != nil {
		t.Fatal(err)
	}
	// And a legitimate upload that IS tracked, which must survive.
	if err := db.CreateUpload(store.MultipartUpload{UploadID: "legit", LastActivityAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.WritePart("legit", 1, bytes.NewReader([]byte("y"))); err != nil {
		t.Fatal(err)
	}

	gc := &GC{Store: db, Stage: m, TTL: 24 * time.Hour, Interval: time.Hour}
	gc.reconcileOnStartup()

	if _, err := os.Stat(m.UploadDir("crash-orphan")); !os.IsNotExist(err) {
		t.Fatalf("expected orphaned scratch dir removed, stat err = %v", err)
	}
	if _, err := os.Stat(m.UploadDir("legit")); err != nil {
		t.Fatalf("expected legitimate upload dir to survive: %v", err)
	}
}
