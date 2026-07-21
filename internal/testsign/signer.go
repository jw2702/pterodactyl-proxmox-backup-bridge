// Package testsign is an independent, from-scratch AWS SigV4 request signer
// used only by tests. It deliberately does NOT share code with
// internal/sigv4 (the verifier under test) so that HTTP-level integration
// tests provide genuine external validation of the verifier, the way a real
// AWS SDK client would, rather than exercising the same code on both sides
// of the wire.
package testsign

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Credentials struct {
	AccessKey string
	SecretKey string
}

type Signer struct {
	Creds   Credentials
	Region  string
	Service string
	Now     func() time.Time
}

func (s *Signer) region() string {
	if s.Region != "" {
		return s.Region
	}
	return "us-east-1"
}

func (s *Signer) service() string {
	if s.Service != "" {
		return s.Service
	}
	return "s3"
}

func (s *Signer) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now().UTC()
}

func isUnreserved(b byte) bool {
	switch {
	case b >= 'A' && b <= 'Z', b >= 'a' && b <= 'z', b >= '0' && b <= '9':
		return true
	case b == '-' || b == '_' || b == '.' || b == '~':
		return true
	}
	return false
}

func uriEncode(s string, encodeSlash bool) string {
	var buf strings.Builder
	for i := 0; i < len(s); i++ {
		b := s[i]
		switch {
		case isUnreserved(b):
			buf.WriteByte(b)
		case b == '/' && !encodeSlash:
			buf.WriteByte(b)
		default:
			fmt.Fprintf(&buf, "%%%02X", b)
		}
	}
	return buf.String()
}

func hashHex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return mac.Sum(nil)
}

func signingKey(secret, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), []byte(date))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	return hmacSHA256(kService, []byte("aws4_request"))
}

func canonicalQuery(values url.Values, exclude map[string]bool) string {
	type kv struct{ k, v string }
	var pairs []kv
	for k, vs := range values {
		if exclude[k] {
			continue
		}
		for _, v := range vs {
			pairs = append(pairs, kv{uriEncode(k, true), uriEncode(v, true)})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].k != pairs[j].k {
			return pairs[i].k < pairs[j].k
		}
		return pairs[i].v < pairs[j].v
	})
	parts := make([]string, len(pairs))
	for i, p := range pairs {
		parts[i] = p.k + "=" + p.v
	}
	return strings.Join(parts, "&")
}

// SignHeader signs req using the Authorization-header form, reading and
// restoring req.Body (if present) to compute its SHA-256 payload hash.
func (s *Signer) SignHeader(req *http.Request) error {
	now := s.now()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")

	var payloadHash string
	if req.Body == nil {
		payloadHash = hashHex(nil)
	} else {
		data, err := io.ReadAll(req.Body)
		if err != nil {
			return err
		}
		req.Body = io.NopCloser(bytes.NewReader(data))
		req.ContentLength = int64(len(data))
		payloadHash = hashHex(data)
	}

	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	if req.Host == "" {
		req.Host = req.URL.Host
	}

	signedHeaderNames := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	if req.Header.Get("Content-Type") != "" {
		signedHeaderNames = append(signedHeaderNames, "content-type")
	}
	sort.Strings(signedHeaderNames)

	var lines []string
	for _, name := range signedHeaderNames {
		v := s.headerValue(req, name)
		lines = append(lines, name+":"+v)
	}
	canonHeaders := strings.Join(lines, "\n") + "\n"
	signedHeaders := strings.Join(signedHeaderNames, ";")

	canonURI := uriEncode(req.URL.Path, false)
	if canonURI == "" {
		canonURI = "/"
	}
	canonQuery := canonicalQuery(req.URL.Query(), nil)

	canonicalRequest := strings.Join([]string{
		req.Method, canonURI, canonQuery, canonHeaders, signedHeaders, payloadHash,
	}, "\n")

	scope := dateStamp + "/" + s.region() + "/" + s.service() + "/aws4_request"
	sts := strings.Join([]string{
		"AWS4-HMAC-SHA256", amzDate, scope, hashHex([]byte(canonicalRequest)),
	}, "\n")

	key := signingKey(s.Creds.SecretKey, dateStamp, s.region(), s.service())
	signature := hex.EncodeToString(hmacSHA256(key, []byte(sts)))

	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+s.Creds.AccessKey+"/"+scope+
		", SignedHeaders="+signedHeaders+", Signature="+signature)
	return nil
}

func (s *Signer) headerValue(req *http.Request, name string) string {
	if name == "host" {
		return req.Host
	}
	return strings.TrimSpace(req.Header.Get(name))
}

// PresignURL adds SigV4 presigned-URL query parameters to req.URL in place.
func (s *Signer) PresignURL(req *http.Request, expires time.Duration) {
	now := s.now()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	scope := dateStamp + "/" + s.region() + "/" + s.service() + "/aws4_request"

	if req.Host == "" {
		req.Host = req.URL.Host
	}

	q := req.URL.Query()
	q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	q.Set("X-Amz-Credential", s.Creds.AccessKey+"/"+scope)
	q.Set("X-Amz-Date", amzDate)
	q.Set("X-Amz-Expires", strconv.Itoa(int(expires.Seconds())))
	q.Set("X-Amz-SignedHeaders", "host")
	req.URL.RawQuery = q.Encode()

	canonHeaders := "host:" + req.Host + "\n"
	signedHeaders := "host"
	canonURI := uriEncode(req.URL.Path, false)
	if canonURI == "" {
		canonURI = "/"
	}
	canonQuery := canonicalQuery(req.URL.Query(), map[string]bool{"X-Amz-Signature": true})

	canonicalRequest := strings.Join([]string{
		req.Method, canonURI, canonQuery, canonHeaders, signedHeaders, "UNSIGNED-PAYLOAD",
	}, "\n")
	sts := strings.Join([]string{
		"AWS4-HMAC-SHA256", amzDate, scope, hashHex([]byte(canonicalRequest)),
	}, "\n")
	key := signingKey(s.Creds.SecretKey, dateStamp, s.region(), s.service())
	signature := hex.EncodeToString(hmacSHA256(key, []byte(sts)))

	q = req.URL.Query()
	q.Set("X-Amz-Signature", signature)
	req.URL.RawQuery = q.Encode()
}
