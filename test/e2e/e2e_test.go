// Package e2e drives the full bridge stack (SigV4 verification, S3 HTTP
// router, the real backend combining store+stage+pbs+idmap) over a real
// net/http server, using the stub proxmox-backup-client script in place of a
// real PBS instance. This is the strongest available offline validation of
// the request lifecycle Panel/Wings actually exercise: presigned
// multipart uploads, header-signed calls, restores with Range requests, and
// deletes.
package e2e

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/backend"
	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/pbs"
	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/s3api"
	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/sigv4"
	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/stage"
	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/store"
	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/testsign"
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

func newTestServer(t *testing.T) (*httptest.Server, *testsign.Signer) {
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
		Timeout:    30 * time.Second,
	}

	be := backend.New(db, stg, client, "host", nil)

	creds := sigv4.Credentials{AccessKey: "bridgekey", SecretKey: "bridgesecret"}
	h := &s3api.Handler{
		Verifier: &sigv4.Verifier{Creds: creds, ClockSkew: 15 * time.Minute},
		Backend:  be,
	}

	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	signer := &testsign.Signer{Creds: testsign.Credentials{AccessKey: creds.AccessKey, SecretKey: creds.SecretKey}}
	return srv, signer
}

// TestFullBackupRestoreDeleteLifecycle mimics the real Pterodactyl flow end
// to end: Panel-style multipart upload (Create -> presigned-equivalent
// UploadPart x3 -> Complete), a Wings-style presigned GET restore (including
// a Range request, since Wings/aws-sdk-go may request partial content), a
// HeadObject size check, and a Panel-style direct DeleteObject call.
func TestFullBackupRestoreDeleteLifecycle(t *testing.T) {
	srv, signer := newTestServer(t)
	client := &http.Client{}

	bucket, key := "pterodactyl-backups", "550e8400-e29b-41d4-a716-446655440000.tar.gz"

	// --- Panel initiates multipart upload (header-signed, as the AWS SDK
	// for PHP would do) ---
	createReq, _ := http.NewRequest(http.MethodPost, srv.URL+"/"+bucket+"/"+key+"?uploads", nil)
	if err := signer.SignHeader(createReq); err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(createReq)
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	var createResult struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&createResult); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if createResult.UploadID == "" {
		t.Fatal("expected non-empty upload ID")
	}

	// --- Wings uploads parts directly to presigned PUT URLs ---
	fullBackup := bytes.Repeat([]byte("PTERODACTYL-BACKUP-CHUNK-"), 4096) // ~100KB, split into 3 parts
	partSize := len(fullBackup) / 3
	var partsXML []struct {
		PartNumber int    `xml:"PartNumber"`
		ETag       string `xml:"ETag"`
	}
	for i := 0; i < 3; i++ {
		start := i * partSize
		end := start + partSize
		if i == 2 {
			end = len(fullBackup)
		}
		partData := fullBackup[start:end]

		partURL := srv.URL + "/" + bucket + "/" + key
		putReq, _ := http.NewRequest(http.MethodPut, partURL, bytes.NewReader(partData))
		putReq.ContentLength = int64(len(partData))
		q := putReq.URL.Query()
		q.Set("partNumber", itoa(i+1))
		q.Set("uploadId", createResult.UploadID)
		putReq.URL.RawQuery = q.Encode()
		signer.PresignURL(putReq, 15*time.Minute)

		resp, err := client.Do(putReq)
		if err != nil {
			t.Fatalf("UploadPart %d: %v", i+1, err)
		}
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			t.Fatalf("UploadPart %d status=%d body=%s", i+1, resp.StatusCode, b)
		}
		etag := resp.Header.Get("ETag")
		resp.Body.Close()
		partsXML = append(partsXML, struct {
			PartNumber int    `xml:"PartNumber"`
			ETag       string `xml:"ETag"`
		}{i + 1, etag})
	}

	// --- Panel completes the upload (header-signed) ---
	type completePart struct {
		PartNumber int    `xml:"PartNumber"`
		ETag       string `xml:"ETag"`
	}
	type completeBody struct {
		XMLName xml.Name       `xml:"CompleteMultipartUpload"`
		Parts   []completePart `xml:"Part"`
	}
	var cb completeBody
	for _, p := range partsXML {
		cb.Parts = append(cb.Parts, completePart{PartNumber: p.PartNumber, ETag: p.ETag})
	}
	bodyXML, err := xml.Marshal(cb)
	if err != nil {
		t.Fatal(err)
	}
	completeReq, _ := http.NewRequest(http.MethodPost, srv.URL+"/"+bucket+"/"+key+"?uploadId="+createResult.UploadID, bytes.NewReader(bodyXML))
	if err := signer.SignHeader(completeReq); err != nil {
		t.Fatal(err)
	}
	resp, err = client.Do(completeReq)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("Complete status=%d body=%s", resp.StatusCode, b)
	}
	resp.Body.Close()

	// --- Panel checks size via HeadObject (header-signed) ---
	headReq, _ := http.NewRequest(http.MethodHead, srv.URL+"/"+bucket+"/"+key, nil)
	if err := signer.SignHeader(headReq); err != nil {
		t.Fatal(err)
	}
	resp, err = client.Do(headReq)
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HeadObject status = %d", resp.StatusCode)
	}
	if cl := resp.Header.Get("Content-Length"); cl != itoa(len(fullBackup)) {
		t.Fatalf("Content-Length = %s, want %d", cl, len(fullBackup))
	}
	resp.Body.Close()

	// --- Wings restores via a presigned GET (full download) ---
	getReq, _ := http.NewRequest(http.MethodGet, srv.URL+"/"+bucket+"/"+key, nil)
	signer.PresignURL(getReq, 15*time.Minute)
	resp, err = client.Do(getReq)
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	got, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, fullBackup) {
		t.Fatalf("restored content mismatch: got %d bytes, want %d bytes", len(got), len(fullBackup))
	}

	// --- Wings restores a Range (partial content) ---
	rangeReq, _ := http.NewRequest(http.MethodGet, srv.URL+"/"+bucket+"/"+key, nil)
	rangeReq.Header.Set("Range", "bytes=10-19")
	signer.PresignURL(rangeReq, 15*time.Minute)
	resp, err = client.Do(rangeReq)
	if err != nil {
		t.Fatalf("Range GetObject: %v", err)
	}
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("Range status = %d, want 206", resp.StatusCode)
	}
	gotRange, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Equal(gotRange, fullBackup[10:20]) {
		t.Fatalf("range content mismatch: got %q, want %q", gotRange, fullBackup[10:20])
	}

	// --- Panel deletes the backup directly (header-signed) ---
	delReq, _ := http.NewRequest(http.MethodDelete, srv.URL+"/"+bucket+"/"+key, nil)
	if err := signer.SignHeader(delReq); err != nil {
		t.Fatal(err)
	}
	resp, err = client.Do(delReq)
	if err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("Delete status = %d, want 204", resp.StatusCode)
	}
	resp.Body.Close()

	// Confirm it's actually gone.
	headReq2, _ := http.NewRequest(http.MethodHead, srv.URL+"/"+bucket+"/"+key, nil)
	if err := signer.SignHeader(headReq2); err != nil {
		t.Fatal(err)
	}
	resp, err = client.Do(headReq2)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
