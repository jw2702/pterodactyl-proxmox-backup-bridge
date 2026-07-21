package backend

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/pbs"
	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/s3api"
	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/stage"
	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/store"
)

func stubBinPath(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not determine test file location")
	}
	path := filepath.Join(filepath.Dir(thisFile), "..", "..", "scripts", "stub-proxmox-backup-client")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stub script not found at %s: %v", path, err)
	}
	return path
}

func newTestBackend(t *testing.T) *Backend {
	t.Helper()
	t.Setenv("STUB_PBS_STATE_DIR", filepath.Join(t.TempDir(), "pbs-state"))

	db, err := store.Open(filepath.Join(t.TempDir(), "bridge.db"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	stg, err := stage.New(filepath.Join(t.TempDir(), "scratch"))
	if err != nil {
		t.Fatalf("stage.New: %v", err)
	}

	client := &pbs.Client{
		Repository: "test@pbs:store1",
		Password:   "testpass",
		BinPath:    stubBinPath(t),
		Timeout:    10 * time.Second,
	}

	return New(db, stg, client, "host", nil)
}

func TestPutGetHeadDeleteObject(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()

	body := []byte("real backend put/get round trip")
	info, err := b.PutObject(ctx, "mybucket", "mykey.tar.gz", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if info.Size != int64(len(body)) {
		t.Fatalf("size = %d, want %d", info.Size, len(body))
	}

	head, err := b.HeadObject(ctx, "mybucket", "mykey.tar.gz")
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if head.ETag != info.ETag {
		t.Fatalf("HeadObject ETag mismatch: %s vs %s", head.ETag, info.ETag)
	}

	rc, gotInfo, err := b.GetObject(ctx, "mybucket", "mykey.tar.gz")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(body) {
		t.Fatalf("GetObject body = %q, want %q", got, body)
	}
	if gotInfo.Size != int64(len(body)) {
		t.Fatalf("GetObject size = %d, want %d", gotInfo.Size, len(body))
	}

	if err := b.DeleteObject(ctx, "mybucket", "mykey.tar.gz"); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if _, err := b.HeadObject(ctx, "mybucket", "mykey.tar.gz"); err != s3api.ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestOverwrite_CreatesNewBeforeForgettingOld(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()

	if _, err := b.PutObject(ctx, "mybucket", "samekey", bytes.NewReader([]byte("version one"))); err != nil {
		t.Fatalf("first PutObject: %v", err)
	}
	firstMapping, err := b.Store.GetObjectMapping("mybucket", "samekey")
	if err != nil {
		t.Fatal(err)
	}

	// Ensure the second backup gets a distinct backup-time from the first
	// (the stub keys snapshots by unix-second granularity).
	time.Sleep(1100 * time.Millisecond)

	if _, err := b.PutObject(ctx, "mybucket", "samekey", bytes.NewReader([]byte("version two, longer"))); err != nil {
		t.Fatalf("second PutObject: %v", err)
	}
	secondMapping, err := b.Store.GetObjectMapping("mybucket", "samekey")
	if err != nil {
		t.Fatal(err)
	}

	if secondMapping.PBSBackupTime == firstMapping.PBSBackupTime {
		t.Fatalf("expected distinct backup times, got same: %d", secondMapping.PBSBackupTime)
	}

	// Old snapshot must have been forgotten: restoring it directly via PBS
	// (bypassing the store, which now points at the new one) should fail.
	err = b.PBS.Restore(ctx, firstMapping.PBSBackupType, firstMapping.PBSBackupID, time.Unix(firstMapping.PBSBackupTime, 0), firstMapping.Namespace, filepath.Join(t.TempDir(), "out"))
	if err == nil {
		t.Fatal("expected old snapshot to have been forgotten after overwrite")
	}

	rc, _, err := b.GetObject(ctx, "mybucket", "samekey")
	if err != nil {
		t.Fatalf("GetObject after overwrite: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != "version two, longer" {
		t.Fatalf("got %q, want new version", got)
	}
}

func TestMultipartUploadLifecycle(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()

	uploadID, err := b.CreateMultipartUpload(ctx, "mybucket", "bigfile.tar.gz")
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	partData := [][]byte{
		bytes.Repeat([]byte("1"), 5),
		bytes.Repeat([]byte("2"), 5),
		bytes.Repeat([]byte("3"), 2),
	}
	var parts []s3api.Part
	for i, pd := range partData {
		etag, err := b.UploadPart(ctx, "mybucket", "bigfile.tar.gz", uploadID, i+1, bytes.NewReader(pd))
		if err != nil {
			t.Fatalf("UploadPart %d: %v", i+1, err)
		}
		parts = append(parts, s3api.Part{PartNumber: i + 1, ETag: etag})
	}

	info, err := b.CompleteMultipartUpload(ctx, "mybucket", "bigfile.tar.gz", uploadID, parts)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	want := "1111122222" + "33"
	if info.Size != int64(len(want)) {
		t.Fatalf("size = %d, want %d", info.Size, len(want))
	}

	rc, _, err := b.GetObject(ctx, "mybucket", "bigfile.tar.gz")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != want {
		t.Fatalf("got %q, want %q", got, want)
	}

	if _, err := b.Store.GetUpload(uploadID); err == nil {
		t.Fatal("expected upload record to be removed after complete")
	}
}

func TestAbortMultipartUpload_CleansUpScratch(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()

	uploadID, err := b.CreateMultipartUpload(ctx, "mybucket", "abandoned.tar.gz")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.UploadPart(ctx, "mybucket", "abandoned.tar.gz", uploadID, 1, bytes.NewReader([]byte("data"))); err != nil {
		t.Fatal(err)
	}

	if err := b.AbortMultipartUpload(ctx, "mybucket", "abandoned.tar.gz", uploadID); err != nil {
		t.Fatalf("AbortMultipartUpload: %v", err)
	}

	if _, err := os.Stat(b.Stage.UploadDir(uploadID)); !os.IsNotExist(err) {
		t.Fatalf("expected scratch dir removed, stat err = %v", err)
	}
	if _, err := b.Store.GetUpload(uploadID); err == nil {
		t.Fatal("expected upload record removed")
	}
}

func TestListObjects_PrefixAndDelimiter(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()

	keys := []string{"servers/s1/backup1.tar.gz", "servers/s1/backup2.tar.gz", "servers/s2/backup1.tar.gz", "other/backup.tar.gz"}
	for _, k := range keys {
		if _, err := b.PutObject(ctx, "mybucket", k, bytes.NewReader([]byte(k))); err != nil {
			t.Fatalf("PutObject %s: %v", k, err)
		}
	}

	objects, prefixes, truncated, err := b.ListObjects(ctx, "mybucket", "servers/", "/", "", 100)
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if truncated {
		t.Fatal("did not expect truncation")
	}
	if len(objects) != 0 {
		t.Fatalf("expected no direct objects under servers/ with delimiter, got %+v", objects)
	}
	wantPrefixes := map[string]bool{"servers/s1/": true, "servers/s2/": true}
	if len(prefixes) != 2 {
		t.Fatalf("expected 2 common prefixes, got %+v", prefixes)
	}
	for _, p := range prefixes {
		if !wantPrefixes[p] {
			t.Fatalf("unexpected common prefix %q", p)
		}
	}

	flat, _, _, err := b.ListObjects(ctx, "mybucket", "servers/s1/", "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(flat) != 2 {
		t.Fatalf("expected 2 objects under servers/s1/ without delimiter, got %+v", flat)
	}
}

func TestNamespaceDerivedFromBucket(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()

	if _, err := b.PutObject(ctx, "my.dotted.bucket", "key1", bytes.NewReader([]byte("x"))); err != nil {
		t.Fatal(err)
	}
	mapping, err := b.Store.GetObjectMapping("my.dotted.bucket", "key1")
	if err != nil {
		t.Fatal(err)
	}
	if mapping.Namespace != "my-dotted-bucket" {
		t.Fatalf("namespace = %q, want dots replaced with dashes", mapping.Namespace)
	}
}
