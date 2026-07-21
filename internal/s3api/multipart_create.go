package s3api

import "net/http"

func (h *Handler) handleCreateMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string) {
	uploadID, err := h.Backend.CreateMultipartUpload(r.Context(), bucket, key)
	if err != nil {
		writeInternalError(w, r, err)
		return
	}
	writeXML(w, http.StatusOK, initiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
	})
}
