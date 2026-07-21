package s3api

import (
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/sigv4"
)

// Handler is the top-level http.Handler implementing the S3-compatible API
// surface the bridge exposes to Pterodactyl Panel and Wings.
type Handler struct {
	Verifier *sigv4.Verifier
	Backend  Backend
	Log      *slog.Logger
}

func (h *Handler) log() *slog.Logger {
	if h.Log != nil {
		return h.Log
	}
	return slog.Default()
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r, reqID := withRequestID(r)
	w.Header().Set("x-amz-request-id", reqID)

	if r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
		return
	}

	if err := h.authenticate(r); err != nil {
		if ae, ok := err.(*sigv4.AuthError); ok {
			h.log().Warn("auth failed", "path", r.URL.Path, "code", ae.Code, "msg", ae.Message, "request_id", reqID)
			writeAuthError(w, r, ae)
			return
		}
		writeInternalError(w, r, err)
		return
	}

	bucket, key := splitPath(r.URL.Path)
	if bucket == "" {
		writeErrorCode(w, r, "InvalidArgument", "request path must include a bucket name")
		return
	}

	q := r.URL.Query()
	_, hasUploads := q["uploads"]
	_, hasUploadID := q["uploadId"]
	_, hasPartNumber := q["partNumber"]
	_, hasListType := q["list-type"]

	switch {
	case r.Method == http.MethodPost && hasUploads && key != "":
		h.handleCreateMultipartUpload(w, r, bucket, key)
	case r.Method == http.MethodPut && hasUploadID && hasPartNumber && key != "":
		h.handleUploadPart(w, r, bucket, key)
	case r.Method == http.MethodPost && hasUploadID && key != "":
		h.handleCompleteMultipartUpload(w, r, bucket, key)
	case r.Method == http.MethodDelete && hasUploadID && key != "":
		h.handleAbortMultipartUpload(w, r, bucket, key)
	case r.Method == http.MethodGet && key == "" && hasListType:
		h.handleListObjectsV2(w, r, bucket)
	case r.Method == http.MethodPut && key != "":
		h.handlePutObject(w, r, bucket, key)
	case r.Method == http.MethodGet && key != "":
		h.handleGetObject(w, r, bucket, key)
	case r.Method == http.MethodHead && key != "":
		h.handleHeadObject(w, r, bucket, key)
	case r.Method == http.MethodDelete && key != "":
		h.handleDeleteObject(w, r, bucket, key)
	default:
		writeErrorCode(w, r, "InvalidArgument", "unsupported operation for "+r.Method+" "+r.URL.Path)
	}
}

// authenticate verifies SigV4 and, for chunked header-signed uploads,
// transparently swaps r.Body for a sigv4.ChunkedReader so downstream
// handlers only ever see decoded payload bytes.
func (h *Handler) authenticate(r *http.Request) error {
	if err := h.Verifier.Verify(r); err != nil {
		return err
	}
	if !sigv4.IsPresigned(r) && r.Header.Get("X-Amz-Content-Sha256") == sigv4.StreamingPayload {
		cr, err := sigv4.NewChunkedReaderFromRequest(r, h.Verifier.Creds.SecretKey)
		if err != nil {
			return &sigv4.AuthError{Code: sigv4.ErrInvalidArgument, Message: err.Error()}
		}
		r.Body = &chunkedReaderCloser{cr, r.Body}
	}
	return nil
}

type chunkedReaderCloser struct {
	*sigv4.ChunkedReader
	underlying interface{ Close() error }
}

func (c *chunkedReaderCloser) Close() error { return c.underlying.Close() }

// splitPath extracts bucket and key from a path-style request path
// "/{bucket}/{key...}".
func splitPath(path string) (bucket, key string) {
	trimmed := strings.TrimPrefix(path, "/")
	idx := strings.IndexByte(trimmed, '/')
	if idx < 0 {
		return trimmed, ""
	}
	return trimmed[:idx], trimmed[idx+1:]
}

func queryInt(q map[string][]string, key string, def int) int {
	vs, ok := q[key]
	if !ok || len(vs) == 0 {
		return def
	}
	n, err := strconv.Atoi(vs[0])
	if err != nil {
		return def
	}
	return n
}
