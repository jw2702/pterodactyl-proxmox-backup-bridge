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

// Part is a single multipart-upload part: as reported by a client in
// CompleteMultipartUpload (PartNumber+ETag only), or as returned by
// ListParts (all fields populated).
type Part struct {
	PartNumber   int
	ETag         string
	Size         int64
	LastModified time.Time
}

// RangeSpec is an inclusive byte range requested via an HTTP Range header.
// A nil *RangeSpec passed to Backend.GetObject means "the whole object".
type RangeSpec struct {
	Start, End int64
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
	// GetObject returns the object's bytes. When rangeSpec is nil the
	// implementation should stream directly from the backing store without
	// buffering the whole object first (important for time-to-first-byte:
	// Wings blocks its own response to Panel on receiving HTTP response
	// headers from this call before backgrounding the actual restore, so
	// any buffering delay here directly causes Panel-side request
	// timeouts). When rangeSpec is non-nil, the implementation may need to
	// materialize the object locally first in order to serve an arbitrary
	// byte range; slicing to exactly [Start, End] is the implementation's
	// responsibility, not the caller's.
	GetObject(ctx context.Context, bucket, key string, rangeSpec *RangeSpec) (io.ReadCloser, ObjectInfo, error)
	HeadObject(ctx context.Context, bucket, key string) (ObjectInfo, error)
	DeleteObject(ctx context.Context, bucket, key string) error
	ListObjects(ctx context.Context, bucket, prefix, delimiter, startAfter string, maxKeys int) (objects []ObjectInfo, commonPrefixes []string, isTruncated bool, err error)

	CreateMultipartUpload(ctx context.Context, bucket, key string) (uploadID string, err error)
	UploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, body io.Reader) (etag string, err error)
	// ListParts returns the parts already uploaded for an in-progress
	// multipart upload. Pterodactyl Panel calls this when Wings reports a
	// completed backup without including its own parts list (an officially
	// supported, if uncommon, path per Panel's request validation).
	ListParts(ctx context.Context, bucket, key, uploadID string) ([]Part, error)
	CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []Part) (ObjectInfo, error)
	AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error
}
