package s3api

import (
	"net/http"
)

func (h *Handler) handlePutObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	info, err := h.Backend.PutObject(r.Context(), bucket, key, r.Body)
	if err != nil {
		writeInternalError(w, r, err)
		return
	}
	w.Header().Set("ETag", quoteETag(info.ETag))
	w.WriteHeader(http.StatusOK)
}

func quoteETag(etag string) string {
	if len(etag) >= 2 && etag[0] == '"' && etag[len(etag)-1] == '"' {
		return etag
	}
	return `"` + etag + `"`
}
