package s3api

import (
	"errors"
	"net/http"
)

func (h *Handler) handleAbortMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string) {
	uploadID := r.URL.Query().Get("uploadId")
	err := h.Backend.AbortMultipartUpload(r.Context(), bucket, key, uploadID)
	if err != nil && !errors.Is(err, ErrNotFound) {
		writeInternalError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
