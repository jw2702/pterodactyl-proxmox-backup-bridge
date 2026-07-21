package s3api

import (
	"encoding/xml"
	"net/http"
)

const s3Namespace = "http://s3.amazonaws.com/doc/2006-03-01/"

type initiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

type completedPart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type completeMultipartUploadRequest struct {
	XMLName xml.Name        `xml:"CompleteMultipartUpload"`
	Parts   []completedPart `xml:"Part"`
}

type completeMultipartUploadResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CompleteMultipartUploadResult"`
	Bucket  string   `xml:"Bucket"`
	Key     string   `xml:"Key"`
	ETag    string   `xml:"ETag"`
}

type listBucketContent struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type listBucketResult struct {
	XMLName        xml.Name            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`
	Name           string              `xml:"Name"`
	Prefix         string              `xml:"Prefix"`
	KeyCount       int                 `xml:"KeyCount"`
	MaxKeys        int                 `xml:"MaxKeys"`
	IsTruncated    bool                `xml:"IsTruncated"`
	NextMarker     string              `xml:"NextContinuationToken,omitempty"`
	Contents       []listBucketContent `xml:"Contents"`
	CommonPrefixes []commonPrefixEntry `xml:"CommonPrefixes,omitempty"`
}

type commonPrefixEntry struct {
	Prefix string `xml:"Prefix"`
}

func writeXML(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(v)
}
