package sigv4

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

var emptyPayloadHash = hashHex("")

// ChunkedReader decodes an aws-chunked (STREAMING-AWS4-HMAC-SHA256-PAYLOAD)
// request body, verifying each chunk's signature against the chain seeded by
// the request's own Authorization-header signature, and yields the decoded
// payload bytes to callers. A signature mismatch anywhere in the stream
// aborts the read with an *AuthError.
type ChunkedReader struct {
	br            *bufio.Reader
	secretKey     string
	scope         credentialScope
	amzDate       string
	prevSignature string
	current       []byte
	done          bool
	err           error
}

// NewChunkedReader constructs a ChunkedReader. seedSignature is the
// Signature value from the request's Authorization header (the first chunk's
// signature chains from it).
func NewChunkedReader(body io.Reader, secretKey string, scope credentialScope, amzDate, seedSignature string) *ChunkedReader {
	return &ChunkedReader{
		br:            bufio.NewReader(body),
		secretKey:     secretKey,
		scope:         scope,
		amzDate:       amzDate,
		prevSignature: seedSignature,
	}
}

// NewChunkedReaderFromRequest builds a ChunkedReader for a request that has
// already passed VerifyHeader with X-Amz-Content-Sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD.
func NewChunkedReaderFromRequest(r *http.Request, secretKey string) (*ChunkedReader, error) {
	authHeader := r.Header.Get("Authorization")
	parsed, err := parseAuthorizationHeader(authHeader)
	if err != nil {
		return nil, err
	}
	scope, err := parseCredential(parsed.Credential)
	if err != nil {
		return nil, err
	}
	amzDateStr := r.Header.Get("X-Amz-Date")
	if amzDateStr == "" {
		return nil, fmt.Errorf("sigv4: chunked: missing X-Amz-Date")
	}
	return NewChunkedReader(r.Body, secretKey, scope, amzDateStr, parsed.Signature), nil
}

func (c *ChunkedReader) Read(p []byte) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	for len(c.current) == 0 {
		if c.done {
			return 0, io.EOF
		}
		if err := c.nextChunk(); err != nil {
			c.err = err
			return 0, err
		}
	}
	n := copy(p, c.current)
	c.current = c.current[n:]
	return n, nil
}

func (c *ChunkedReader) nextChunk() error {
	line, err := c.br.ReadString('\n')
	if err != nil {
		return fmt.Errorf("sigv4: chunked: reading chunk header: %w", err)
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		// Tolerate a stray blank line (e.g. trailing CRLF after the final chunk).
		line, err = c.br.ReadString('\n')
		if err != nil {
			return fmt.Errorf("sigv4: chunked: reading chunk header: %w", err)
		}
		line = strings.TrimRight(line, "\r\n")
	}

	head := strings.SplitN(line, ";", 2)
	size, err := strconv.ParseInt(head[0], 16, 64)
	if err != nil {
		return fmt.Errorf("sigv4: chunked: invalid chunk size %q: %w", head[0], err)
	}

	var chunkSig string
	if len(head) == 2 {
		kv := strings.SplitN(head[1], "=", 2)
		if len(kv) == 2 && kv[0] == "chunk-signature" {
			chunkSig = kv[1]
		}
	}
	if chunkSig == "" {
		return fmt.Errorf("sigv4: chunked: missing chunk-signature in %q", line)
	}

	data := make([]byte, size)
	if size > 0 {
		if _, err := io.ReadFull(c.br, data); err != nil {
			return fmt.Errorf("sigv4: chunked: reading chunk data: %w", err)
		}
	}
	trailer := make([]byte, 2)
	if _, err := io.ReadFull(c.br, trailer); err != nil {
		return fmt.Errorf("sigv4: chunked: reading chunk trailer: %w", err)
	}

	expected := c.computeChunkSignature(data)
	if !secureCompare(expected, chunkSig) {
		return &AuthError{Code: ErrSignatureDoesNotMatch, Message: "chunk signature mismatch in aws-chunked body"}
	}
	c.prevSignature = chunkSig

	if size == 0 {
		c.done = true
		return nil
	}
	c.current = data
	return nil
}

func (c *ChunkedReader) computeChunkSignature(data []byte) string {
	sts := strings.Join([]string{
		"AWS4-HMAC-SHA256-PAYLOAD",
		c.amzDate,
		c.scope.scopeString(),
		c.prevSignature,
		emptyPayloadHash,
		hashHex(string(data)),
	}, "\n")
	return sign(c.secretKey, c.scope, sts)
}
