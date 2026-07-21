package s3api

import (
	"errors"
	"net/http"
	"strconv"
)

func (h *Handler) handleHeadObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	info, err := h.Backend.HeadObject(r.Context(), bucket, key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("ETag", quoteETag(info.ETag))
	w.Header().Set("Last-Modified", info.LastModified.UTC().Format(http.TimeFormat))
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
	w.WriteHeader(http.StatusOK)
}
