package sigv4

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"
)

// Verifier validates AWS SigV4-signed requests (both header-based and
// presigned/query-string based) against a single static credential pair.
type Verifier struct {
	Creds Credentials

	// Service is the SigV4 "service" scope component; always "s3" for us.
	Service string

	// ClockSkew is the maximum allowed difference between the request's
	// signing time and server time.
	ClockSkew time.Duration

	// Now returns the current time; overridable in tests. Defaults to
	// time.Now().UTC() when nil.
	Now Clock
}

func (v *Verifier) now() time.Time {
	if v.Now != nil {
		return v.Now()
	}
	return defaultClock()
}

func (v *Verifier) service() string {
	if v.Service != "" {
		return v.Service
	}
	return "s3"
}

// authHeaderRe parses:
// AWS4-HMAC-SHA256 Credential=<cred>, SignedHeaders=<h1;h2>, Signature=<hex>
// Component order is not guaranteed by the spec, so we parse key=value pairs
// rather than relying on positional matches.
var authHeaderRe = regexp.MustCompile(`^AWS4-HMAC-SHA256\s+(.+)$`)

type parsedAuthHeader struct {
	Credential    string
	SignedHeaders string
	Signature     string
}

func parseAuthorizationHeader(h string) (parsedAuthHeader, error) {
	m := authHeaderRe.FindStringSubmatch(h)
	if m == nil {
		return parsedAuthHeader{}, fmt.Errorf("sigv4: unsupported Authorization scheme")
	}
	var out parsedAuthHeader
	for _, field := range strings.Split(m[1], ",") {
		field = strings.TrimSpace(field)
		kv := strings.SplitN(field, "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "Credential":
			out.Credential = kv[1]
		case "SignedHeaders":
			out.SignedHeaders = kv[1]
		case "Signature":
			out.Signature = kv[1]
		}
	}
	if out.Credential == "" || out.SignedHeaders == "" || out.Signature == "" {
		return parsedAuthHeader{}, fmt.Errorf("sigv4: incomplete Authorization header")
	}
	return out, nil
}

// IsPresigned reports whether the request carries presigned-URL SigV4
// parameters in its query string, as opposed to an Authorization header.
func IsPresigned(r *http.Request) bool {
	return r.URL.Query().Get("X-Amz-Signature") != ""
}

// Verify dispatches to the header- or presigned-verification path based on
// which form of SigV4 the request uses.
func (v *Verifier) Verify(r *http.Request) error {
	if IsPresigned(r) {
		return v.VerifyPresigned(r)
	}
	return v.VerifyHeader(r)
}

// VerifyHeader validates a request signed via the Authorization header.
func (v *Verifier) VerifyHeader(r *http.Request) error {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return authErr(ErrMissingAuth, "missing Authorization header")
	}
	parsed, err := parseAuthorizationHeader(authHeader)
	if err != nil {
		return authErr(ErrInvalidArgument, "%s", err.Error())
	}

	scope, err := parseCredential(parsed.Credential)
	if err != nil {
		return authErr(ErrInvalidArgument, "%s", err.Error())
	}
	if scope.AccessKey != v.Creds.AccessKey {
		return authErr(ErrInvalidAccessKey, "unknown access key %q", scope.AccessKey)
	}
	if scope.Service != v.service() {
		return authErr(ErrInvalidArgument, "unexpected service %q in credential scope", scope.Service)
	}

	amzDateStr := r.Header.Get("X-Amz-Date")
	if amzDateStr == "" {
		amzDateStr = r.Header.Get("Date")
	}
	if amzDateStr == "" {
		return authErr(ErrMissingAuth, "missing X-Amz-Date header")
	}
	signedAt, err := parseAmzDate(amzDateStr)
	if err != nil {
		return authErr(ErrInvalidArgument, "%s", err.Error())
	}
	if err := checkSkew(v.now(), signedAt, v.ClockSkew); err != nil {
		return authErr(ErrRequestTimeTooSkewed, "%s", err.Error())
	}

	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		return authErr(ErrMissingAuth, "missing X-Amz-Content-Sha256 header")
	}

	signedHeaderNames := strings.Split(parsed.SignedHeaders, ";")
	canonHeaders, signedHeaders, err := canonicalHeaders(r, signedHeaderNames)
	if err != nil {
		return authErr(ErrInvalidArgument, "%s", err.Error())
	}

	canonURI := canonicalURI(r.URL.Path)
	canonQuery := canonicalQueryString(r.URL.Query(), nil)
	canonicalRequest := buildCanonicalRequest(r.Method, canonURI, canonQuery, canonHeaders, signedHeaders, payloadHash)

	sts := stringToSign(amzDateStr, scope, hashHex(canonicalRequest))
	expected := sign(v.Creds.SecretKey, scope, sts)

	if !secureCompare(expected, parsed.Signature) {
		return authErr(ErrSignatureDoesNotMatch, "computed signature does not match provided signature")
	}
	return nil
}
