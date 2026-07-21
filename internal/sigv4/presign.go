package sigv4

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// VerifyPresigned validates a presigned-URL request (SigV4 parameters carried
// in the query string rather than an Authorization header).
func (v *Verifier) VerifyPresigned(r *http.Request) error {
	q := r.URL.Query()

	algorithm := q.Get("X-Amz-Algorithm")
	if algorithm != Algorithm {
		return authErr(ErrInvalidArgument, "unsupported X-Amz-Algorithm %q", algorithm)
	}

	credential := q.Get("X-Amz-Credential")
	if credential == "" {
		return authErr(ErrMissingAuth, "missing X-Amz-Credential")
	}
	// AWS SDKs send the credential URL-decoded in r.URL.Query() already
	// (Go's url.Values decodes '/' etc.), so no further unescape is needed.
	scope, err := parseCredential(credential)
	if err != nil {
		return authErr(ErrInvalidArgument, "%s", err.Error())
	}
	if scope.AccessKey != v.Creds.AccessKey {
		return authErr(ErrInvalidAccessKey, "unknown access key %q", scope.AccessKey)
	}
	if scope.Service != v.service() {
		return authErr(ErrInvalidArgument, "unexpected service %q in credential scope", scope.Service)
	}

	amzDateStr := q.Get("X-Amz-Date")
	if amzDateStr == "" {
		return authErr(ErrMissingAuth, "missing X-Amz-Date")
	}
	signedAt, err := parseAmzDate(amzDateStr)
	if err != nil {
		return authErr(ErrInvalidArgument, "%s", err.Error())
	}

	expiresStr := q.Get("X-Amz-Expires")
	expires, err := strconv.Atoi(expiresStr)
	if err != nil {
		return authErr(ErrInvalidArgument, "invalid X-Amz-Expires %q", expiresStr)
	}
	if err := checkExpiry(v.now(), signedAt, expires, v.ClockSkew); err != nil {
		return authErr(ErrExpiredRequest, "%s", err.Error())
	}

	signedHeadersParam := q.Get("X-Amz-SignedHeaders")
	if signedHeadersParam == "" {
		return authErr(ErrMissingAuth, "missing X-Amz-SignedHeaders")
	}
	signedHeaderNames := strings.Split(signedHeadersParam, ";")
	canonHeaders, signedHeaders, err := canonicalHeaders(r, signedHeaderNames)
	if err != nil {
		return authErr(ErrInvalidArgument, "%s", err.Error())
	}

	providedSignature := q.Get("X-Amz-Signature")
	if providedSignature == "" {
		return authErr(ErrMissingAuth, "missing X-Amz-Signature")
	}

	canonURI := canonicalURI(r.URL.Path)
	canonQuery := canonicalQueryString(url.Values(q), map[string]bool{"X-Amz-Signature": true})
	canonicalRequest := buildCanonicalRequest(r.Method, canonURI, canonQuery, canonHeaders, signedHeaders, UnsignedPayload)

	sts := stringToSign(amzDateStr, scope, hashHex(canonicalRequest))
	expected := sign(v.Creds.SecretKey, scope, sts)

	if !secureCompare(expected, providedSignature) {
		return authErr(ErrSignatureDoesNotMatch, "computed signature does not match provided signature")
	}
	return nil
}
