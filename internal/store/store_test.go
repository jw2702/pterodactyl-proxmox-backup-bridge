package store

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func newTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "bridge.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestObjectMapping_CRUD(t *testing.T) {
	db := newTestDB(t)

	m := ObjectMapping{
		Bucket: "b1", Key: "k1", Namespace: "b1",
		PBSBackupType: "host", PBSBackupID: "abc123", PBSBackupTime: 1000,
		Size: 42, ETag: "deadbeef", UpdatedAt: time.Now(),
	}
	if err := db.PutObjectMapping(m); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := db.GetObjectMapping("b1", "k1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.PBSBackupID != m.PBSBackupID || got.Size != m.Size {
		t.Fatalf("got %+v, want %+v", got, m)
	}

	if _, err := db.GetObjectMapping("b1", "nope"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	if err := db.DeleteObjectMapping("b1", "k1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := db.GetObjectMapping("b1", "k1"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
	if err := db.DeleteObjectMapping("b1", "k1"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound deleting twice, got %v", err)
	}
}

func TestObjectMapping_KeysWithSlashesDontCollideAcrossBuckets(t *testing.T) {
	db := newTestDB(t)
	if err := db.PutObjectMapping(ObjectMapping{Bucket: "bucket-a", Key: "shared/key"}); err != nil {
		t.Fatal(err)
	}
	if err := db.PutObjectMapping(ObjectMapping{Bucket: "bucket-a-x", Key: "y/shared/key"}); err != nil {
		t.Fatal(err)
	}
	a, err := db.GetObjectMapping("bucket-a", "shared/key")
	if err != nil {
		t.Fatal(err)
	}
	if a.Bucket != "bucket-a" {
		t.Fatalf("cross-bucket collision: got %+v", a)
	}
}

func TestListObjects_PrefixAndPagination(t *testing.T) {
	db := newTestDB(t)
	keys := []string{"a/1", "a/2", "a/3", "b/1"}
	for _, k := range keys {
		if err := db.PutObjectMapping(ObjectMapping{Bucket: "bucket", Key: k}); err != nil {
			t.Fatal(err)
		}
	}

	page1, truncated, err := db.ListObjects("bucket", "a/", "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 2 || !truncated {
		t.Fatalf("page1 = %+v, truncated=%v", page1, truncated)
	}
	if page1[0].Key != "a/1" || page1[1].Key != "a/2" {
		t.Fatalf("unexpected order: %+v", page1)
	}

	page2, truncated2, err := db.ListObjects("bucket", "a/", page1[len(page1)-1].Key, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 1 || truncated2 {
		t.Fatalf("page2 = %+v, truncated=%v", page2, truncated2)
	}
	if page2[0].Key != "a/3" {
		t.Fatalf("unexpected page2 key: %+v", page2)
	}

	bOnly, _, err := db.ListObjects("bucket", "b/", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(bOnly) != 1 || bOnly[0].Key != "b/1" {
		t.Fatalf("unexpected b-prefix results: %+v", bOnly)
	}
}

func TestMultipartUpload_CRUDAndParts(t *testing.T) {
	db := newTestDB(t)

	u := MultipartUpload{UploadID: "up1", Bucket: "b", Key: "k", Namespace: "b", InitiatedAt: time.Now(), LastActivityAt: time.Now()}
	if err := db.CreateUpload(u); err != nil {
		t.Fatal(err)
	}

	got, err := db.GetUpload("up1")
	if err != nil || got.UploadID != "up1" {
		t.Fatalf("GetUpload: %+v, %v", got, err)
	}

	for i := 1; i <= 3; i++ {
		p := PartInfo{PartNumber: i, ETag: fmt.Sprintf("etag%d", i), Size: int64(i), TempPath: fmt.Sprintf("/tmp/part%d", i)}
		if err := db.PutPart("up1", p); err != nil {
			t.Fatal(err)
		}
	}

	parts, err := db.ListParts("up1")
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(parts))
	}
	for i, p := range parts {
		if p.PartNumber != i+1 {
			t.Fatalf("parts out of order: %+v", parts)
		}
	}

	if err := db.DeleteUpload("up1"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.GetUpload("up1"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	remainingParts, err := db.ListParts("up1")
	if err != nil {
		t.Fatal(err)
	}
	if len(remainingParts) != 0 {
		t.Fatalf("expected parts to cascade-delete, got %+v", remainingParts)
	}
}

func TestListUploadsOlderThan(t *testing.T) {
	db := newTestDB(t)
	old := MultipartUpload{UploadID: "old", LastActivityAt: time.Now().Add(-2 * time.Hour)}
	fresh := MultipartUpload{UploadID: "fresh", LastActivityAt: time.Now()}
	db.CreateUpload(old)
	db.CreateUpload(fresh)

	stale, err := db.ListUploadsOlderThan(time.Now().Add(-1 * time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(stale) != 1 || stale[0].UploadID != "old" {
		t.Fatalf("expected only 'old', got %+v", stale)
	}
}

func TestConcurrentWrites_NoCorruption(t *testing.T) {
	db := newTestDB(t)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i)
			if err := db.PutObjectMapping(ObjectMapping{Bucket: "b", Key: key, PBSBackupID: key}); err != nil {
				t.Errorf("Put %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	results, _, err := db.ListObjects("b", "", "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 50 {
		t.Fatalf("expected 50 objects, got %d", len(results))
	}
}

func TestKeyedMutex_SerializesSameKey(t *testing.T) {
	km := NewKeyedMutex()
	var counter int
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock := km.Lock("same-key")
			defer unlock()
			tmp := counter
			time.Sleep(time.Millisecond)
			counter = tmp + 1
		}()
	}
	wg.Wait()
	if counter != 20 {
		t.Fatalf("expected 20 (serialized), got %d (race)", counter)
	}
}

func TestKeyedMutex_DoesNotSerializeDifferentKeys(t *testing.T) {
	km := NewKeyedMutex()
	done := make(chan struct{}, 2)
	start := time.Now()
	for _, k := range []string{"key-a", "key-b"} {
		go func(k string) {
			unlock := km.Lock(k)
			time.Sleep(50 * time.Millisecond)
			unlock()
			done <- struct{}{}
		}(k)
	}
	<-done
	<-done
	if elapsed := time.Since(start); elapsed > 90*time.Millisecond {
		t.Fatalf("expected concurrent execution across different keys, took %v", elapsed)
	}
}
