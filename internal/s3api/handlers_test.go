package s3api

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/sigv4"
	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/testsign"
)

func newTestServer(t *testing.T) (*httptest.Server, *testsign.Signer, *memBackend) {
	t.Helper()
	creds := sigv4.Credentials{AccessKey: "bridgekey", SecretKey: "bridgesecret"}
	backend := newMemBackend()
	h := &Handler{
		Verifier: &sigv4.Verifier{Creds: creds, ClockSkew: 15 * time.Minute},
		Backend:  backend,
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	signer := &testsign.Signer{Creds: testsign.Credentials{AccessKey: creds.AccessKey, SecretKey: creds.SecretKey}}
	return srv, signer, backend
}

func TestPutGetHeadDeleteObject_HeaderSigned(t *testing.T) {
	srv, signer, _ := newTestServer(t)

	body := []byte("hello pterodactyl backup")
	putReq, _ := http.NewRequest(http.MethodPut, srv.URL+"/mybucket/mykey", bytes.NewReader(body))
	if err := signer.SignHeader(putReq); err != nil {
		t.Fatalf("sign PUT: %v", err)
	}
	resp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatalf("PUT request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("PUT status = %d, body = %s", resp.StatusCode, b)
	}
	resp.Body.Close()

	headReq, _ := http.NewRequest(http.MethodHead, srv.URL+"/mybucket/mykey", nil)
	if err := signer.SignHeader(headReq); err != nil {
		t.Fatalf("sign HEAD: %v", err)
	}
	resp, err = http.DefaultClient.Do(headReq)
	if err != nil {
		t.Fatalf("HEAD request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HEAD status = %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Length") != "24" {
		t.Fatalf("HEAD Content-Length = %s, want 24", resp.Header.Get("Content-Length"))
	}
	resp.Body.Close()

	getReq, _ := http.NewRequest(http.MethodGet, srv.URL+"/mybucket/mykey", nil)
	if err := signer.SignHeader(getReq); err != nil {
		t.Fatalf("sign GET: %v", err)
	}
	resp, err = http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("GET request failed: %v", err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(got) != string(body) {
		t.Fatalf("GET body = %q, want %q", got, body)
	}

	// Range request
	rangeReq, _ := http.NewRequest(http.MethodGet, srv.URL+"/mybucket/mykey", nil)
	rangeReq.Header.Set("Range", "bytes=0-4")
	if err := signer.SignHeader(rangeReq); err != nil {
		t.Fatalf("sign Range GET: %v", err)
	}
	resp, err = http.DefaultClient.Do(rangeReq)
	if err != nil {
		t.Fatalf("Range GET failed: %v", err)
	}
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("Range GET status = %d, want 206", resp.StatusCode)
	}
	got, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(got) != "hello" {
		t.Fatalf("Range GET body = %q, want %q", got, "hello")
	}

	delReq, _ := http.NewRequest(http.MethodDelete, srv.URL+"/mybucket/mykey", nil)
	if err := signer.SignHeader(delReq); err != nil {
		t.Fatalf("sign DELETE: %v", err)
	}
	resp, err = http.DefaultClient.Do(delReq)
	if err != nil {
		t.Fatalf("DELETE request failed: %v", err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want 204", resp.StatusCode)
	}
	resp.Body.Close()

	getReq2, _ := http.NewRequest(http.MethodGet, srv.URL+"/mybucket/mykey", nil)
	if err := signer.SignHeader(getReq2); err != nil {
		t.Fatalf("sign GET2: %v", err)
	}
	resp, err = http.DefaultClient.Do(getReq2)
	if err != nil {
		t.Fatalf("GET2 request failed: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after delete status = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestGetObject_PresignedURL(t *testing.T) {
	srv, signer, backend := newTestServer(t)
	backend.objects["restorebucket/restorekey"] = memObject{data: []byte("restored data"), etag: "abc", mod: time.Now()}

	getReq, _ := http.NewRequest(http.MethodGet, srv.URL+"/restorebucket/restorekey", nil)
	signer.PresignURL(getReq, 15*time.Minute)

	resp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("presigned GET failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("presigned GET status = %d, body=%s", resp.StatusCode, b)
	}
	got, _ := io.ReadAll(resp.Body)
	if string(got) != "restored data" {
		t.Fatalf("got %q", got)
	}
}

func TestPutObject_PresignedURL(t *testing.T) {
	srv, signer, _ := newTestServer(t)

	putReq, _ := http.NewRequest(http.MethodPut, srv.URL+"/wingsbucket/wingskey", bytes.NewReader([]byte("wings upload")))
	putReq.ContentLength = int64(len("wings upload"))
	signer.PresignURL(putReq, 15*time.Minute)

	resp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatalf("presigned PUT failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("presigned PUT status = %d, body=%s", resp.StatusCode, b)
	}
}

func TestMultipartUploadLifecycle(t *testing.T) {
	srv, signer, _ := newTestServer(t)

	createReq, _ := http.NewRequest(http.MethodPost, srv.URL+"/mybucket/bigbackup.tar.gz?uploads", nil)
	if err := signer.SignHeader(createReq); err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		t.Fatalf("create multipart failed: %v", err)
	}
	var createResult initiateMultipartUploadResult
	dec := xml.NewDecoder(resp.Body)
	if err := dec.Decode(&createResult); err != nil {
		t.Fatalf("decode create result: %v", err)
	}
	resp.Body.Close()
	if createResult.UploadID == "" {
		t.Fatal("expected non-empty upload ID")
	}

	partsData := [][]byte{
		bytes.Repeat([]byte("A"), 5), // part 1
		bytes.Repeat([]byte("B"), 5), // part 2
		bytes.Repeat([]byte("C"), 3), // part 3
	}
	var completeParts []completedPart
	for i, pd := range partsData {
		partNum := i + 1
		url := srv.URL + "/mybucket/bigbackup.tar.gz?partNumber=" + itoaTest(partNum) + "&uploadId=" + createResult.UploadID
		req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(pd))
		if err := signer.SignHeader(req); err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("upload part %d failed: %v", partNum, err)
		}
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			t.Fatalf("upload part %d status = %d, body=%s", partNum, resp.StatusCode, b)
		}
		etag := resp.Header.Get("ETag")
		resp.Body.Close()
		completeParts = append(completeParts, completedPart{PartNumber: partNum, ETag: etag})
	}

	completeXML, err := xml.Marshal(completeMultipartUploadRequest{Parts: completeParts})
	if err != nil {
		t.Fatal(err)
	}
	completeURL := srv.URL + "/mybucket/bigbackup.tar.gz?uploadId=" + createResult.UploadID
	completeReq, _ := http.NewRequest(http.MethodPost, completeURL, bytes.NewReader(completeXML))
	if err := signer.SignHeader(completeReq); err != nil {
		t.Fatal(err)
	}
	resp, err = http.DefaultClient.Do(completeReq)
	if err != nil {
		t.Fatalf("complete multipart failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("complete status = %d, body=%s", resp.StatusCode, b)
	}
	resp.Body.Close()

	getReq, _ := http.NewRequest(http.MethodGet, srv.URL+"/mybucket/bigbackup.tar.gz", nil)
	if err := signer.SignHeader(getReq); err != nil {
		t.Fatal(err)
	}
	resp, err = http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("final GET failed: %v", err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	want := "AAAAABBBBBCCC"
	if string(got) != want {
		t.Fatalf("reassembled object = %q, want %q", got, want)
	}
}

func TestAbortMultipartUpload(t *testing.T) {
	srv, signer, backend := newTestServer(t)

	createReq, _ := http.NewRequest(http.MethodPost, srv.URL+"/mybucket/abandoned.tar.gz?uploads", nil)
	if err := signer.SignHeader(createReq); err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(createReq)
	if err != nil {
		t.Fatal(err)
	}
	var createResult initiateMultipartUploadResult
	xml.NewDecoder(resp.Body).Decode(&createResult)
	resp.Body.Close()

	abortURL := srv.URL + "/mybucket/abandoned.tar.gz?uploadId=" + createResult.UploadID
	abortReq, _ := http.NewRequest(http.MethodDelete, abortURL, nil)
	if err := signer.SignHeader(abortReq); err != nil {
		t.Fatal(err)
	}
	resp, err = http.DefaultClient.Do(abortReq)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("abort status = %d, want 204", resp.StatusCode)
	}
	resp.Body.Close()

	if _, ok := backend.uploads[createResult.UploadID]; ok {
		t.Fatal("expected upload to be removed from backend after abort")
	}
}

func TestNegativeAuth_MissingSignature(t *testing.T) {
	srv, _, _ := newTestServer(t)
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/mybucket/mykey", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
	var xerr s3Error
	xml.NewDecoder(resp.Body).Decode(&xerr)
	if xerr.Code != string(sigv4.ErrMissingAuth) {
		t.Fatalf("error code = %s, want %s", xerr.Code, sigv4.ErrMissingAuth)
	}
}

func TestNegativeAuth_WrongSecret(t *testing.T) {
	srv, _, _ := newTestServer(t)
	badSigner := &testsign.Signer{Creds: testsign.Credentials{AccessKey: "bridgekey", SecretKey: "wrongsecret"}}
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/mybucket/mykey", nil)
	if err := badSigner.SignHeader(req); err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
	var xerr s3Error
	xml.NewDecoder(resp.Body).Decode(&xerr)
	if xerr.Code != string(sigv4.ErrSignatureDoesNotMatch) {
		t.Fatalf("error code = %s, want %s", xerr.Code, sigv4.ErrSignatureDoesNotMatch)
	}
}
