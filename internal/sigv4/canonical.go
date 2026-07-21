// Package sigv4 implements AWS Signature Version 4 request verification
// (both header-based and presigned-query-string based), scoped to what an
// S3-compatible server needs to validate requests from the AWS SDK for PHP
// (Pterodactyl Panel) and aws-sdk-go (Wings).
package sigv4

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/textproto"
	"sort"
	"strings"
)

const (
	Algorithm      = "AWS4-HMAC-SHA256"
	terminationStr = "aws4_request"
	amzDateLayout  = "20060102T150405Z"
	amzDateOnly    = "20060102"

	// UnsignedPayload is the special payload-hash token used by presigned S3 URLs.
	UnsignedPayload = "UNSIGNED-PAYLOAD"
	// StreamingPayload is the payload-hash token used for aws-chunked signed uploads.
	StreamingPayload = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
)

// Credentials is the single static access/secret key pair the bridge accepts.
type Credentials struct {
	AccessKey string
	SecretKey string
}

// credentialScope is the parsed form of an AWS4 credential scope:
// "<access>/<date>/<region>/<service>/aws4_request".
type credentialScope struct {
	AccessKey string
	Date      string // YYYYMMDD
	Region    string
	Service   string
}

func parseCredential(cred string) (credentialScope, error) {
	parts := strings.Split(cred, "/")
	if len(parts) != 5 {
		return credentialScope{}, fmt.Errorf("sigv4: malformed credential %q", cred)
	}
	if parts[4] != terminationStr {
		return credentialScope{}, fmt.Errorf("sigv4: unexpected credential termination %q", parts[4])
	}
	return credentialScope{
		AccessKey: parts[0],
		Date:      parts[1],
		Region:    parts[2],
		Service:   parts[3],
	}, nil
}

func (c credentialScope) scopeString() string {
	return strings.Join([]string{c.Date, c.Region, c.Service, terminationStr}, "/")
}

// isUnreserved reports whether b is one of the RFC 3986 unreserved characters
// that AWS's URI-encoding algorithm leaves untouched.
func isUnreserved(b byte) bool {
	switch {
	case b >= 'A' && b <= 'Z':
		return true
	case b >= 'a' && b <= 'z':
		return true
	case b >= '0' && b <= '9':
		return true
	case b == '-' || b == '_' || b == '.' || b == '~':
		return true
	default:
		return false
	}
}

// uriEncode implements AWS's URI-encoding algorithm. When encodeSlash is
// false, '/' is passed through unescaped (used for path segments).
func uriEncode(s string, encodeSlash bool) string {
	var buf strings.Builder
	buf.Grow(len(s) + 8)
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

// canonicalURI builds the CanonicalURI component from the request's decoded
// path. S3-style signing single-encodes the path rather than the general
// SigV4 double-encoding, so callers must pass the already-decoded path.
func canonicalURI(path string) string {
	if path == "" {
		return "/"
	}
	return uriEncode(path, false)
}

// canonicalQueryString builds the CanonicalQueryString component from a set
// of query parameters, excluding any parameters listed in exclude.
func canonicalQueryString(query map[string][]string, exclude map[string]bool) string {
	type kv struct{ k, v string }
	var pairs []kv
	for k, values := range query {
		if exclude[k] {
			continue
		}
		ek := uriEncode(k, true)
		for _, v := range values {
			pairs = append(pairs, kv{ek, uriEncode(v, true)})
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

// headerValue resolves the effective value for a signed header name,
// special-casing "host" since Go's net/http strips it out of r.Header.
func headerValue(r *http.Request, name string) (string, bool) {
	if strings.EqualFold(name, "host") {
		host := r.Host
		if host == "" {
			host = r.URL.Host
		}
		return host, host != ""
	}
	canon := textproto.CanonicalMIMEHeaderKey(name)
	values := r.Header.Values(canon)
	if len(values) == 0 {
		return "", false
	}
	trimmed := make([]string, len(values))
	for i, v := range values {
		trimmed[i] = collapseSpaces(strings.TrimSpace(v))
	}
	return strings.Join(trimmed, ","), true
}

func collapseSpaces(s string) string {
	var buf strings.Builder
	buf.Grow(len(s))
	prevSpace := false
	for _, r := range s {
		if r == ' ' || r == '\t' {
			if !prevSpace {
				buf.WriteByte(' ')
			}
			prevSpace = true
			continue
		}
		prevSpace = false
		buf.WriteRune(r)
	}
	return buf.String()
}

// canonicalHeaders builds the CanonicalHeaders and SignedHeaders components
// for the given (already lowercase) signed header names, which must be
// sorted by the caller's chosen order (typically as parsed from the request).
func canonicalHeaders(r *http.Request, signedHeaderNames []string) (canonical, signedHeaders string, err error) {
	names := append([]string(nil), signedHeaderNames...)
	sort.Strings(names)

	var lines []string
	for _, name := range names {
		v, ok := headerValue(r, name)
		if !ok {
			return "", "", fmt.Errorf("sigv4: signed header %q not present on request", name)
		}
		lines = append(lines, name+":"+v)
	}
	canonical = strings.Join(lines, "\n") + "\n"
	signedHeaders = strings.Join(names, ";")
	return canonical, signedHeaders, nil
}

// buildCanonicalRequest assembles the full canonical request string per the
// SigV4 spec.
func buildCanonicalRequest(method, canonURI, canonQuery, canonHeaders, signedHeaders, payloadHash string) string {
	return strings.Join([]string{
		method,
		canonURI,
		canonQuery,
		canonHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")
}

func hashHex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// stringToSign builds the SigV4 string-to-sign given the request timestamp,
// credential scope, and hashed canonical request.
func stringToSign(amzDate string, scope credentialScope, canonicalRequestHash string) string {
	return strings.Join([]string{
		Algorithm,
		amzDate,
		scope.scopeString(),
		canonicalRequestHash,
	}, "\n")
}

func hmacSHA256(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return mac.Sum(nil)
}

// signingKey derives the SigV4 signing key from the secret key and scope.
func signingKey(secret string, scope credentialScope) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), []byte(scope.Date))
	kRegion := hmacSHA256(kDate, []byte(scope.Region))
	kService := hmacSHA256(kRegion, []byte(scope.Service))
	kSigning := hmacSHA256(kService, []byte(terminationStr))
	return kSigning
}

// sign computes the final hex-encoded SigV4 signature.
func sign(secret string, scope credentialScope, strToSign string) string {
	key := signingKey(secret, scope)
	return hex.EncodeToString(hmacSHA256(key, []byte(strToSign)))
}

// secureCompare does a constant-time comparison of two hex signature strings.
func secureCompare(a, b string) bool {
	return hmac.Equal([]byte(a), []byte(b))
}
