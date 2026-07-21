package sigv4

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestVerifyHeader_AWSSampleVector reproduces AWS's published SigV4 example
// ("GET Object", service "s3", bucket "examplebucket", key "test.txt",
// dated 20130524) to validate the canonical-request construction against a
// known-good, independently documented intermediate value: AWS's docs
// publish the canonical request's SHA-256 hash for this exact request as
// 7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972. This
// test asserts our canonical request hashes to that same published value
// (the strongest available offline check on canonicalization correctness),
// and then checks that VerifyHeader accepts a signature independently
// derived (via the same signing-key/HMAC chain) from that canonical request.
func TestVerifyHeader_AWSSampleVector(t *testing.T) {
	const wantCanonicalRequestHash = "7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972"

	accessKey := "AKIAIOSFODNN7EXAMPLE"
	secretKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

	req := httptest.NewRequest(http.MethodGet, "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	req.URL.Path = "/test.txt"
	req.Host = "examplebucket.s3.amazonaws.com"
	req.Header.Set("x-amz-date", "20130524T000000Z")
	req.Header.Set("x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	req.Header.Set("Range", "bytes=0-9")

	signedHeaderNames := []string{"host", "range", "x-amz-content-sha256", "x-amz-date"}
	canonHeaders, signedHeaders, err := canonicalHeaders(req, signedHeaderNames)
	if err != nil {
		t.Fatalf("canonicalHeaders: %v", err)
	}
	canonURI := canonicalURI(req.URL.Path)
	canonQuery := canonicalQueryString(req.URL.Query(), nil)
	canonicalRequest := buildCanonicalRequest(req.Method, canonURI, canonQuery, canonHeaders, signedHeaders, req.Header.Get("x-amz-content-sha256"))
	if got := hashHex(canonicalRequest); got != wantCanonicalRequestHash {
		t.Fatalf("canonical request hash mismatch:\n got:  %s\n want: %s\ncanonical request was:\n%q", got, wantCanonicalRequestHash, canonicalRequest)
	}

	scope := credentialScope{AccessKey: accessKey, Date: "20130524", Region: "us-east-1", Service: "s3"}
	sts := stringToSign("20130524T000000Z", scope, wantCanonicalRequestHash)
	signature := sign(secretKey, scope, sts)

	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential="+accessKey+"/20130524/us-east-1/s3/aws4_request, "+
			"SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, "+
			"Signature="+signature)

	fixedNow, _ := time.Parse(amzDateLayout, "20130524T000000Z")
	v := &Verifier{
		Creds:     Credentials{AccessKey: accessKey, SecretKey: secretKey},
		ClockSkew: 15 * time.Minute,
		Now:       func() time.Time { return fixedNow },
	}

	if err := v.VerifyHeader(req); err != nil {
		t.Fatalf("expected valid signature, got error: %v", err)
	}
}

func newSignedRequest(t *testing.T, creds Credentials, method, rawURL string, headers map[string]string, signedAt time.Time) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, rawURL, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	if req.Header.Get("x-amz-content-sha256") == "" {
		req.Header.Set("x-amz-content-sha256", emptyPayloadHash)
	}
	amzDate := signedAt.Format(amzDateLayout)
	req.Header.Set("x-amz-date", amzDate)

	scope := credentialScope{
		AccessKey: creds.AccessKey,
		Date:      signedAt.Format(amzDateOnly),
		Region:    "us-east-1",
		Service:   "s3",
	}

	signedHeaderNames := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	canonHeaders, signedHeaders, err := canonicalHeaders(req, signedHeaderNames)
	if err != nil {
		t.Fatalf("canonicalHeaders: %v", err)
	}
	canonURI := canonicalURI(req.URL.Path)
	canonQuery := canonicalQueryString(req.URL.Query(), nil)
	canonicalRequest := buildCanonicalRequest(req.Method, canonURI, canonQuery, canonHeaders, signedHeaders, req.Header.Get("x-amz-content-sha256"))
	sts := stringToSign(amzDate, scope, hashHex(canonicalRequest))
	signature := sign(creds.SecretKey, scope, sts)

	req.Header.Set("Authorization", Algorithm+" Credential="+creds.AccessKey+"/"+scope.scopeString()+
		", SignedHeaders="+signedHeaders+", Signature="+signature)
	return req
}

func TestVerifyHeader_RoundTrip(t *testing.T) {
	creds := Credentials{AccessKey: "bridgekey", SecretKey: "bridgesecret"}
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	req := newSignedRequest(t, creds, http.MethodPut, "http://bridge.local/mybucket/mykey", nil, now)

	v := &Verifier{Creds: creds, ClockSkew: 15 * time.Minute, Now: func() time.Time { return now }}
	if err := v.VerifyHeader(req); err != nil {
		t.Fatalf("expected valid signature: %v", err)
	}
}

func TestVerifyHeader_WrongSecret(t *testing.T) {
	creds := Credentials{AccessKey: "bridgekey", SecretKey: "bridgesecret"}
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	req := newSignedRequest(t, creds, http.MethodPut, "http://bridge.local/mybucket/mykey", nil, now)

	v := &Verifier{Creds: Credentials{AccessKey: "bridgekey", SecretKey: "wrong"}, ClockSkew: 15 * time.Minute, Now: func() time.Time { return now }}
	err := v.VerifyHeader(req)
	if err == nil {
		t.Fatal("expected signature mismatch error")
	}
	ae, ok := err.(*AuthError)
	if !ok || ae.Code != ErrSignatureDoesNotMatch {
		t.Fatalf("expected SignatureDoesNotMatch, got %v", err)
	}
}

func TestVerifyHeader_TamperedQuery(t *testing.T) {
	creds := Credentials{AccessKey: "bridgekey", SecretKey: "bridgesecret"}
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	req := newSignedRequest(t, creds, http.MethodGet, "http://bridge.local/mybucket/mykey?foo=bar", nil, now)
	// tamper after signing
	req.URL.RawQuery = "foo=baz"

	v := &Verifier{Creds: creds, ClockSkew: 15 * time.Minute, Now: func() time.Time { return now }}
	err := v.VerifyHeader(req)
	if err == nil {
		t.Fatal("expected signature mismatch after query tampering")
	}
}

func TestVerifyHeader_TamperedHeader(t *testing.T) {
	creds := Credentials{AccessKey: "bridgekey", SecretKey: "bridgesecret"}
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	req := newSignedRequest(t, creds, http.MethodPut, "http://bridge.local/mybucket/mykey", nil, now)
	req.Host = "attacker.local"

	v := &Verifier{Creds: creds, ClockSkew: 15 * time.Minute, Now: func() time.Time { return now }}
	if err := v.VerifyHeader(req); err == nil {
		t.Fatal("expected signature mismatch after host tampering")
	}
}

func TestVerifyHeader_ExpiredClockSkew(t *testing.T) {
	creds := Credentials{AccessKey: "bridgekey", SecretKey: "bridgesecret"}
	signedAt := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	req := newSignedRequest(t, creds, http.MethodPut, "http://bridge.local/mybucket/mykey", nil, signedAt)

	tooLate := signedAt.Add(20 * time.Minute)
	v := &Verifier{Creds: creds, ClockSkew: 15 * time.Minute, Now: func() time.Time { return tooLate }}
	err := v.VerifyHeader(req)
	if err == nil {
		t.Fatal("expected clock skew error")
	}
	ae, ok := err.(*AuthError)
	if !ok || ae.Code != ErrRequestTimeTooSkewed {
		t.Fatalf("expected RequestTimeTooSkewed, got %v", err)
	}
}

func TestVerifyHeader_MissingSignedHeader(t *testing.T) {
	creds := Credentials{AccessKey: "bridgekey", SecretKey: "bridgesecret"}
	now := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	req := newSignedRequest(t, creds, http.MethodPut, "http://bridge.local/mybucket/mykey", nil, now)

	// Rewrite the Authorization header to claim an extra signed header that
	// isn't actually present on the request.
	req.Header.Set("Authorization",
		Algorithm+" Credential="+creds.AccessKey+"/20260721/us-east-1/s3/aws4_request, "+
			"SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-nonexistent, Signature=deadbeef")

	v := &Verifier{Creds: creds, ClockSkew: 15 * time.Minute, Now: func() time.Time { return now }}
	err := v.VerifyHeader(req)
	if err == nil {
		t.Fatal("expected error for missing signed header")
	}
}
