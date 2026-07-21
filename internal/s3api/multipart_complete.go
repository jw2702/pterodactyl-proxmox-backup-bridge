package s3api

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
)

func (h *Handler) handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string) {
	uploadID := r.URL.Query().Get("uploadId")

	body, err := io.ReadAll(io.LimitReader(r.Body, 10<<20))
	if err != nil {
		writeErrorCode(w, r, "InvalidArgument", "failed to read request body")
		return
	}

	var reqXML completeMultipartUploadRequest
	if len(body) > 0 {
		if err := xml.Unmarshal(body, &reqXML); err != nil {
			writeErrorCode(w, r, "MalformedXML", "could not parse CompleteMultipartUpload body")
			return
		}
	}

	parts := make([]Part, len(reqXML.Parts))
	for i, p := range reqXML.Parts {
		parts[i] = Part{PartNumber: p.PartNumber, ETag: unquoteETag(p.ETag)}
	}

	info, err := h.Backend.CompleteMultipartUpload(r.Context(), bucket, key, uploadID, parts)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeErrorCode(w, r, "NoSuchUpload", "the specified upload does not exist")
			return
		}
		writeInternalError(w, r, err)
		return
	}

	writeXML(w, http.StatusOK, completeMultipartUploadResult{
		Bucket: bucket,
		Key:    key,
		ETag:   quoteETag(info.ETag),
	})
}

func unquoteETag(etag string) string {
	if len(etag) >= 2 && etag[0] == '"' && etag[len(etag)-1] == '"' {
		return etag[1 : len(etag)-1]
	}
	return etag
}
