package sigv4

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

// chunkedWriter builds a valid aws-chunked body for testing, signing each
// chunk exactly as the real ChunkedReader expects.
type chunkedWriter struct {
	secretKey     string
	scope         credentialScope
	amzDate       string
	prevSignature string
	buf           bytes.Buffer
}

func newChunkedWriter(secretKey string, scope credentialScope, amzDate, seedSignature string) *chunkedWriter {
	return &chunkedWriter{secretKey: secretKey, scope: scope, amzDate: amzDate, prevSignature: seedSignature}
}

func (w *chunkedWriter) writeChunk(data []byte) {
	sts := stringsJoin([]string{
		"AWS4-HMAC-SHA256-PAYLOAD",
		w.amzDate,
		w.scope.scopeString(),
		w.prevSignature,
		emptyPayloadHash,
		hashHex(string(data)),
	})
	sig := sign(w.secretKey, w.scope, sts)
	w.prevSignature = sig
	fmt.Fprintf(&w.buf, "%x;chunk-signature=%s\r\n", len(data), sig)
	w.buf.Write(data)
	w.buf.WriteString("\r\n")
}

func (w *chunkedWriter) finish() []byte {
	w.writeChunk(nil)
	return w.buf.Bytes()
}

func stringsJoin(parts []string) string {
	out := parts[0]
	for _, p := range parts[1:] {
		out += "\n" + p
	}
	return out
}

func TestChunkedReader_RoundTrip(t *testing.T) {
	secretKey := "bridgesecret"
	scope := credentialScope{AccessKey: "bridgekey", Date: "20260721", Region: "us-east-1", Service: "s3"}
	amzDate := "20260721T120000Z"
	seedSig := "seedsignatureplaceholder0000000000000000000000000000000000000"

	w := newChunkedWriter(secretKey, scope, amzDate, seedSig)
	w.writeChunk([]byte("hello "))
	w.writeChunk([]byte("world"))
	body := w.finish()

	cr := NewChunkedReader(bytes.NewReader(body), secretKey, scope, amzDate, seedSig)
	got, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("unexpected error reading chunked body: %v", err)
	}
	if string(got) != "hello world" {
		t.Fatalf("got %q, want %q", got, "hello world")
	}
}

func TestChunkedReader_TamperedChunkRejected(t *testing.T) {
	secretKey := "bridgesecret"
	scope := credentialScope{AccessKey: "bridgekey", Date: "20260721", Region: "us-east-1", Service: "s3"}
	amzDate := "20260721T120000Z"
	seedSig := "seedsignatureplaceholder0000000000000000000000000000000000000"

	w := newChunkedWriter(secretKey, scope, amzDate, seedSig)
	w.writeChunk([]byte("hello "))
	w.writeChunk([]byte("world"))
	body := w.finish()

	// Flip a byte inside the first chunk's data without recomputing its signature.
	tampered := bytes.Replace(body, []byte("hello "), []byte("HELLO "), 1)

	cr := NewChunkedReader(bytes.NewReader(tampered), secretKey, scope, amzDate, seedSig)
	_, err := io.ReadAll(cr)
	if err == nil {
		t.Fatal("expected error reading tampered chunked body")
	}
	ae, ok := err.(*AuthError)
	if !ok || ae.Code != ErrSignatureDoesNotMatch {
		t.Fatalf("expected AuthError SignatureDoesNotMatch, got %v (%T)", err, err)
	}
}

func TestChunkedReader_EmptyBody(t *testing.T) {
	secretKey := "bridgesecret"
	scope := credentialScope{AccessKey: "bridgekey", Date: "20260721", Region: "us-east-1", Service: "s3"}
	amzDate := "20260721T120000Z"
	seedSig := "seedsignatureplaceholder0000000000000000000000000000000000000"

	w := newChunkedWriter(secretKey, scope, amzDate, seedSig)
	body := w.finish()

	cr := NewChunkedReader(bytes.NewReader(body), secretKey, scope, amzDate, seedSig)
	got, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty body, got %q", got)
	}
}
