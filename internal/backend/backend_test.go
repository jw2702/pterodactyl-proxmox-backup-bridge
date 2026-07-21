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

// TestGetObject_FullReadDoesNotMaterializeLocalFile confirms the fix for
// Wings' restore-request timeout: a full (non-Range) GetObject must stream
// directly from proxmox-backup-client rather than first restoring the whole
// object to a local scratch file (which was what made Wings wait long
// enough for Panel's own HTTP client to time out).
func TestGetObject_FullReadDoesNotMaterializeLocalFile(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()

	body := []byte("streamed, not staged")
	if _, err := b.PutObject(ctx, "mybucket", "streamkey", bytes.NewReader(body)); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	getsDir := filepath.Join(b.Stage.Root, "gets")

	rc, _, err := b.GetObject(ctx, "mybucket", "streamkey", nil)
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if string(got) != string(body) {
		t.Fatalf("got %q, want %q", got, body)
	}

	entries, err := os.ReadDir(getsDir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected no files staged under %s for a full GetObject, found %d", getsDir, len(entries))
	}
}

func TestGetObject_RangeStillUsesLocalFile(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()

	body := []byte("0123456789")
	if _, err := b.PutObject(ctx, "mybucket", "rangekey", bytes.NewReader(body)); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	rc, _, err := b.GetObject(ctx, "mybucket", "rangekey", &s3api.RangeSpec{Start: 2, End: 5})
	if err != nil {
		t.Fatalf("GetObject with range: %v", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	want := body[2:6]
	if string(got) != string(want) {
		t.Fatalf("got %q, want %q", got, want)
	}
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

	rc, gotInfo, err := b.GetObject(ctx, "mybucket", "mykey.tar.gz", nil)
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

	rc, _, err := b.GetObject(ctx, "mybucket", "samekey", nil)
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

	rc, _, err := b.GetObject(ctx, "mybucket", "bigfile.tar.gz", nil)
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

// TestListParts_MatchesUploadedPartsForPanelFallback mirrors Panel's own
// fallback path: if Wings reports a completed backup without its own parts
// list (officially allowed per Panel's request validation, "parts" is
// nullable), Panel calls S3 ListParts itself to build the part list before
// calling CompleteMultipartUpload.
func TestListParts_MatchesUploadedPartsForPanelFallback(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()

	uploadID, err := b.CreateMultipartUpload(ctx, "mybucket", "listparts-test.tar.gz")
	if err != nil {
		t.Fatal(err)
	}

	partData := [][]byte{
		bytes.Repeat([]byte("A"), 5),
		bytes.Repeat([]byte("B"), 3),
	}
	var uploadedETags []string
	for i, pd := range partData {
		etag, err := b.UploadPart(ctx, "mybucket", "listparts-test.tar.gz", uploadID, i+1, bytes.NewReader(pd))
		if err != nil {
			t.Fatalf("UploadPart %d: %v", i+1, err)
		}
		uploadedETags = append(uploadedETags, etag)
	}

	listed, err := b.ListParts(ctx, "mybucket", "listparts-test.tar.gz", uploadID)
	if err != nil {
		t.Fatalf("ListParts: %v", err)
	}
	if len(listed) != 2 {
		t.Fatalf("expected 2 parts, got %d", len(listed))
	}
	for i, p := range listed {
		if p.PartNumber != i+1 {
			t.Fatalf("part %d out of order: %+v", i, listed)
		}
		if p.ETag != uploadedETags[i] {
			t.Fatalf("part %d ETag = %q, want %q", i+1, p.ETag, uploadedETags[i])
		}
		if p.Size != int64(len(partData[i])) {
			t.Fatalf("part %d size = %d, want %d", i+1, p.Size, len(partData[i]))
		}
	}

	// Exactly what Panel does: use ListParts' output to drive
	// CompleteMultipartUpload.
	var completeParts []s3api.Part
	for _, p := range listed {
		completeParts = append(completeParts, s3api.Part{PartNumber: p.PartNumber, ETag: p.ETag})
	}
	info, err := b.CompleteMultipartUpload(ctx, "mybucket", "listparts-test.tar.gz", uploadID, completeParts)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload using ListParts output: %v", err)
	}
	if info.Size != int64(len(partData[0])+len(partData[1])) {
		t.Fatalf("size = %d", info.Size)
	}
}

func TestListParts_NonexistentUpload(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	_, err := b.ListParts(ctx, "mybucket", "key", "no-such-upload")
	if err != s3api.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

// TestCompleteMultipartUpload_RetryAfterTransientPBSFailure mirrors what
// actually happens in production: Panel's AWS SDK client automatically
// retries a failed CompleteMultipartUpload call with the same upload ID (no
// re-upload of parts). The backend must be able to satisfy that retry using
// the already-concatenated final file, since the original parts are deleted
// as soon as they're consumed during concatenation.
func TestCompleteMultipartUpload_RetryAfterTransientPBSFailure(t *testing.T) {
	b := newTestBackend(t)
	t.Setenv("STUB_FORCE_BACKUP_FAIL_COUNT", "1")
	ctx := context.Background()

	uploadID, err := b.CreateMultipartUpload(ctx, "mybucket", "retry-test.tar.gz")
	if err != nil {
		t.Fatal(err)
	}

	partData := [][]byte{
		bytes.Repeat([]byte("X"), 5),
		bytes.Repeat([]byte("Y"), 5),
	}
	var parts []s3api.Part
	for i, pd := range partData {
		etag, err := b.UploadPart(ctx, "mybucket", "retry-test.tar.gz", uploadID, i+1, bytes.NewReader(pd))
		if err != nil {
			t.Fatalf("UploadPart %d: %v", i+1, err)
		}
		parts = append(parts, s3api.Part{PartNumber: i + 1, ETag: etag})
	}

	// Sanity check: parts are on disk as separate files at this point.
	partPath := b.Stage.PartPath(uploadID, 1)
	if _, err := os.Stat(partPath); err != nil {
		t.Fatalf("expected part file to exist before Complete: %v", err)
	}

	// First attempt: the stub PBS client is configured to fail once here.
	_, err = b.CompleteMultipartUpload(ctx, "mybucket", "retry-test.tar.gz", uploadID, parts)
	if err == nil {
		t.Fatal("expected first CompleteMultipartUpload attempt to fail (simulated transient PBS error)")
	}

	// The concatenated final file must have survived the failure (it's what
	// the retry needs), while the original part file must already be gone
	// (freed as soon as it was consumed during concatenation).
	if _, err := os.Stat(partPath); !os.IsNotExist(err) {
		t.Fatalf("expected part file to already be removed after concatenation, stat err = %v", err)
	}
	finalPath := filepath.Join(b.Stage.UploadDir(uploadID), "final.img")
	if _, err := os.Stat(finalPath); err != nil {
		t.Fatalf("expected concatenated final.img to survive the failed attempt: %v", err)
	}

	// Second attempt: same uploadID, same parts list, exactly what Panel's
	// AWS SDK automatic retry sends. Must succeed without needing the
	// already-deleted part files.
	info, err := b.CompleteMultipartUpload(ctx, "mybucket", "retry-test.tar.gz", uploadID, parts)
	if err != nil {
		t.Fatalf("expected retry to succeed: %v", err)
	}
	want := "XXXXXYYYYY"
	if info.Size != int64(len(want)) {
		t.Fatalf("size = %d, want %d", info.Size, len(want))
	}

	rc, _, err := b.GetObject(ctx, "mybucket", "retry-test.tar.gz", nil)
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != want {
		t.Fatalf("got %q, want %q", got, want)
	}

	// And now everything should be cleaned up.
	if _, err := b.Store.GetUpload(uploadID); err == nil {
		t.Fatal("expected upload record to be removed after successful retry")
	}
	if _, err := os.Stat(b.Stage.UploadDir(uploadID)); !os.IsNotExist(err) {
		t.Fatalf("expected upload scratch dir removed after successful retry, stat err = %v", err)
	}
}

func TestDeleteObject_AbortsMatchingInProgressUpload(t *testing.T) {
	// Mirrors Pterodactyl Panel's behavior: deleting a backup that never
	// finished uploading issues a plain DeleteObject for the eventual key,
	// not AbortMultipartUpload. The backend must still clean up the
	// abandoned multipart scratch data in that case.
	b := newTestBackend(t)
	ctx := context.Background()

	uploadID, err := b.CreateMultipartUpload(ctx, "mybucket", "stuck-backup.tar.gz")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.UploadPart(ctx, "mybucket", "stuck-backup.tar.gz", uploadID, 1, bytes.NewReader([]byte("partial data"))); err != nil {
		t.Fatal(err)
	}

	err = b.DeleteObject(ctx, "mybucket", "stuck-backup.tar.gz")
	if err != s3api.ErrNotFound {
		t.Fatalf("expected ErrNotFound (idempotent) from DeleteObject, got %v", err)
	}

	if _, err := b.Store.GetUpload(uploadID); err == nil {
		t.Fatal("expected orphaned upload record to be removed")
	}
	if _, err := os.Stat(b.Stage.UploadDir(uploadID)); !os.IsNotExist(err) {
		t.Fatalf("expected scratch dir removed, stat err = %v", err)
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
