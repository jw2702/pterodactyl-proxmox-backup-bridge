package s3api

import "net/http"

func (h *Handler) handleListObjectsV2(w http.ResponseWriter, r *http.Request, bucket string) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	startAfter := q.Get("start-after")
	if startAfter == "" {
		startAfter = q.Get("continuation-token")
	}
	maxKeys := queryInt(q, "max-keys", 1000)
	if maxKeys <= 0 || maxKeys > 1000 {
		maxKeys = 1000
	}

	objects, commonPrefixes, truncated, err := h.Backend.ListObjects(r.Context(), bucket, prefix, delimiter, startAfter, maxKeys)
	if err != nil {
		writeInternalError(w, r, err)
		return
	}

	result := listBucketResult{
		Name:        bucket,
		Prefix:      prefix,
		MaxKeys:     maxKeys,
		IsTruncated: truncated,
		KeyCount:    len(objects),
	}
	for _, o := range objects {
		result.Contents = append(result.Contents, listBucketContent{
			Key:          o.Key,
			LastModified: o.LastModified.UTC().Format("2006-01-02T15:04:05.000Z"),
			ETag:         quoteETag(o.ETag),
			Size:         o.Size,
			StorageClass: "STANDARD",
		})
	}
	for _, p := range commonPrefixes {
		result.CommonPrefixes = append(result.CommonPrefixes, commonPrefixEntry{Prefix: p})
	}

	writeXML(w, http.StatusOK, result)
}
