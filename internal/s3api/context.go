package s3api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
)

type ctxKey int

const requestIDKey ctxKey = iota

func newRequestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func withRequestID(r *http.Request) (*http.Request, string) {
	id := newRequestID()
	ctx := context.WithValue(r.Context(), requestIDKey, id)
	return r.WithContext(ctx), id
}
