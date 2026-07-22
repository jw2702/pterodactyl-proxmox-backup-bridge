package logging

import (
	"context"
	"crypto/rand"
	"encoding/hex"
)

type ctxKey int

const requestIDKey ctxKey = iota

// NewRequestID generates a short random hex request ID.
func NewRequestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// WithRequestID returns a copy of ctx carrying id, retrievable later via
// RequestIDFromContext. This lives here (rather than in internal/s3api,
// which originates the ID per incoming request) so that packages with no
// notion of HTTP requests of their own — like internal/pbs, which logs
// retries of PBS CLI invocations — can still tag their log lines with the
// request that triggered them, without importing internal/s3api (which
// itself depends on internal/backend, which depends on internal/pbs — a
// cycle if pbs imported s3api directly).
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey, id)
}

// RequestIDFromContext returns the request ID stored by WithRequestID, or ""
// if ctx doesn't carry one (e.g. in unit tests that call into internal/pbs
// or internal/backend directly, bypassing the HTTP layer).
func RequestIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(requestIDKey).(string)
	return v
}
