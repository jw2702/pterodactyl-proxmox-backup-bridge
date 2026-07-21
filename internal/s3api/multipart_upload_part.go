package s3api

import (
	"errors"
	"net/http"
	"strconv"
)

func (h *Handler) handleUploadPart(w http.ResponseWriter, r *http.Request, bucket, key string) {
	q := r.URL.Query()
	uploadID := q.Get("uploadId")
	partNumber, err := strconv.Atoi(q.Get("partNumber"))
	if err != nil || partNumber < 1 || partNumber > 10000 {
		writeErrorCode(w, r, "InvalidArgument", "partNumber must be between 1 and 10000")
		return
	}

	etag, err := h.Backend.UploadPart(r.Context(), bucket, key, uploadID, partNumber, r.Body)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeErrorCode(w, r, "NoSuchUpload", "the specified upload does not exist")
			return
		}
		writeInternalError(w, r, err)
		return
	}
	w.Header().Set("ETag", quoteETag(etag))
	w.WriteHeader(http.StatusOK)
}
