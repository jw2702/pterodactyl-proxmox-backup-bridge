package s3api

import (
	"context"
	"io"
	"time"
)

// ObjectInfo describes a stored object for HeadObject/GetObject/ListObjects
// responses.
type ObjectInfo struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
}

// Part is a single part reported by a client in CompleteMultipartUpload.
type Part struct {
	PartNumber int
	ETag       string
}

// ReadSeekCloser is the minimal interface GetObject results must satisfy.
type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

// ErrNotFound is returned by Backend methods when a bucket/key or upload ID
// doesn't exist.
var ErrNotFound = notFoundError{}

type notFoundError struct{}

func (notFoundError) Error() string { return "not found" }

// Backend is the storage abstraction that all S3 operation handlers depend
// on. In production it's implemented by internal/backend (which wires
// together internal/store, internal/stage, internal/pbs and internal/idmap);
// tests can substitute a simple in-memory implementation to exercise the
// HTTP/SigV4 layer in isolation.
type Backend interface {
	PutObject(ctx context.Context, bucket, key string, body io.Reader) (ObjectInfo, error)
	// GetObject returns an io.ReadSeekCloser (backed by a real file, since PBS
	// restores full files) so the handler can serve HTTP Range requests by
	// seeking rather than requiring true partial-restore support from PBS.
	GetObject(ctx context.Context, bucket, key string) (ReadSeekCloser, ObjectInfo, error)
	HeadObject(ctx context.Context, bucket, key string) (ObjectInfo, error)
	DeleteObject(ctx context.Context, bucket, key string) error
	ListObjects(ctx context.Context, bucket, prefix, delimiter, startAfter string, maxKeys int) (objects []ObjectInfo, commonPrefixes []string, isTruncated bool, err error)

	CreateMultipartUpload(ctx context.Context, bucket, key string) (uploadID string, err error)
	UploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, body io.Reader) (etag string, err error)
	CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []Part) (ObjectInfo, error)
	AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error
}
