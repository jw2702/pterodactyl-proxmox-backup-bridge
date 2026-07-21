package s3api

import (
	"errors"
	"net/http"
)

func (h *Handler) handleDeleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	err := h.Backend.DeleteObject(r.Context(), bucket, key)
	// S3 DeleteObject is idempotent: deleting an already-absent key is a
	// success (204), not an error.
	if err != nil && !errors.Is(err, ErrNotFound) {
		writeInternalError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
