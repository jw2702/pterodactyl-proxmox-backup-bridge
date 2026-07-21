package sigv4

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"
)

func newPresignedRequest(t *testing.T, creds Credentials, method, rawURL string, signedAt time.Time, expiresSeconds int) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, rawURL, nil)

	scope := credentialScope{
		AccessKey: creds.AccessKey,
		Date:      signedAt.Format(amzDateOnly),
		Region:    "us-east-1",
		Service:   "s3",
	}

	q := req.URL.Query()
	q.Set("X-Amz-Algorithm", Algorithm)
	q.Set("X-Amz-Credential", creds.AccessKey+"/"+scope.scopeString())
	q.Set("X-Amz-Date", signedAt.Format(amzDateLayout))
	q.Set("X-Amz-Expires", strconv.Itoa(expiresSeconds))
	q.Set("X-Amz-SignedHeaders", "host")
	req.URL.RawQuery = q.Encode()

	canonHeaders, signedHeaders, err := canonicalHeaders(req, []string{"host"})
	if err != nil {
		t.Fatalf("canonicalHeaders: %v", err)
	}
	canonURI := canonicalURI(req.URL.Path)
	canonQuery := canonicalQueryString(url.Values(req.URL.Query()), map[string]bool{"X-Amz-Signature": true})
	canonicalRequest := buildCanonicalRequest(req.Method, canonURI, canonQuery, canonHeaders, signedHeaders, UnsignedPayload)
	sts := stringToSign(signedAt.Format(amzDateLayout), scope, hashHex(canonicalRequest))
	signature := sign(creds.SecretKey, scope, sts)

	q = req.URL.Query()
	q.Set("X-Amz-Signature", signature)
	req.URL.RawQuery = q.Encode()

	return req
}

func TestVerifyPresigned_RoundTrip(t *testing.T) {
	creds := Credentials{AccessKey: "bridgekey", SecretKey: "bridgesecret"}
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	req := newPresignedRequest(t, creds, http.MethodGet, "http://bridge.local/mybucket/mykey", now, 900)

	v := &Verifier{Creds: creds, ClockSkew: 15 * time.Minute, Now: func() time.Time { return now.Add(5 * time.Minute) }}
	if err := v.VerifyPresigned(req); err != nil {
		t.Fatalf("expected valid presigned request: %v", err)
	}
}

func TestVerify_DispatchesToPresigned(t *testing.T) {
	creds := Credentials{AccessKey: "bridgekey", SecretKey: "bridgesecret"}
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	req := newPresignedRequest(t, creds, http.MethodGet, "http://bridge.local/mybucket/mykey", now, 900)

	if !IsPresigned(req) {
		t.Fatal("expected IsPresigned to be true")
	}

	v := &Verifier{Creds: creds, ClockSkew: 15 * time.Minute, Now: func() time.Time { return now }}
	if err := v.Verify(req); err != nil {
		t.Fatalf("expected valid presigned request via Verify: %v", err)
	}
}

func TestVerifyPresigned_Expired(t *testing.T) {
	creds := Credentials{AccessKey: "bridgekey", SecretKey: "bridgesecret"}
	signedAt := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	req := newPresignedRequest(t, creds, http.MethodGet, "http://bridge.local/mybucket/mykey", signedAt, 60)

	afterExpiry := signedAt.Add(5 * time.Minute)
	v := &Verifier{Creds: creds, ClockSkew: 15 * time.Second, Now: func() time.Time { return afterExpiry }}
	err := v.VerifyPresigned(req)
	if err == nil {
		t.Fatal("expected expired presigned URL to be rejected")
	}
	ae, ok := err.(*AuthError)
	if !ok || ae.Code != ErrExpiredRequest {
		t.Fatalf("expected ErrExpiredRequest, got %v", err)
	}
}

func TestVerifyPresigned_TamperedSignatureExcluded(t *testing.T) {
	creds := Credentials{AccessKey: "bridgekey", SecretKey: "bridgesecret"}
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	req := newPresignedRequest(t, creds, http.MethodGet, "http://bridge.local/mybucket/mykey", now, 900)

	// Tamper with an unrelated (unsigned-by-host) query param after signing;
	// since it participates in the canonical query string it must invalidate
	// the signature.
	q := req.URL.Query()
	q.Set("response-content-type", "application/x-evil")
	req.URL.RawQuery = q.Encode()

	v := &Verifier{Creds: creds, ClockSkew: 15 * time.Minute, Now: func() time.Time { return now }}
	if err := v.VerifyPresigned(req); err == nil {
		t.Fatal("expected signature mismatch after query tampering")
	}
}

func TestVerifyPresigned_WrongAccessKey(t *testing.T) {
	creds := Credentials{AccessKey: "bridgekey", SecretKey: "bridgesecret"}
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	req := newPresignedRequest(t, creds, http.MethodGet, "http://bridge.local/mybucket/mykey", now, 900)

	v := &Verifier{Creds: Credentials{AccessKey: "otherkey", SecretKey: "bridgesecret"}, ClockSkew: 15 * time.Minute, Now: func() time.Time { return now }}
	err := v.VerifyPresigned(req)
	if err == nil {
		t.Fatal("expected InvalidAccessKeyId error")
	}
	ae, ok := err.(*AuthError)
	if !ok || ae.Code != ErrInvalidAccessKey {
		t.Fatalf("expected ErrInvalidAccessKey, got %v", err)
	}
}
