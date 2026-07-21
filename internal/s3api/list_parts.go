package s3api

import (
	"encoding/xml"
	"errors"
	"net/http"
)

type listPartsPart struct {
	PartNumber   int    `xml:"PartNumber"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	LastModified string `xml:"LastModified"`
}

type listPartsResult struct {
	XMLName     xml.Name        `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListPartsResult"`
	Bucket      string          `xml:"Bucket"`
	Key         string          `xml:"Key"`
	UploadID    string          `xml:"UploadId"`
	IsTruncated bool            `xml:"IsTruncated"`
	Parts       []listPartsPart `xml:"Part"`
}

// handleListParts serves the S3 ListParts operation: Pterodactyl Panel calls
// this when Wings reports a completed backup without including its own
// parts list, to look them up itself before calling CompleteMultipartUpload.
func (h *Handler) handleListParts(w http.ResponseWriter, r *http.Request, bucket, key string) {
	uploadID := r.URL.Query().Get("uploadId")

	parts, err := h.Backend.ListParts(r.Context(), bucket, key, uploadID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeErrorCode(w, r, "NoSuchUpload", "the specified upload does not exist")
			return
		}
		writeInternalError(w, r, err)
		return
	}

	result := listPartsResult{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
	}
	for _, p := range parts {
		result.Parts = append(result.Parts, listPartsPart{
			PartNumber:   p.PartNumber,
			ETag:         quoteETag(p.ETag),
			Size:         p.Size,
			LastModified: p.LastModified.UTC().Format("2006-01-02T15:04:05.000Z"),
		})
	}

	writeXML(w, http.StatusOK, result)
}
