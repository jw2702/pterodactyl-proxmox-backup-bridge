package s3api

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// memBackend is a trivial in-memory Backend used to exercise the HTTP/SigV4
// layer in isolation from the real PBS-backed implementation.
type memBackend struct {
	mu      sync.Mutex
	objects map[string]memObject
	uploads map[string]*memUpload
	nextID  int
}

type memObject struct {
	data []byte
	etag string
	mod  time.Time
}

type memUpload struct {
	bucket, key string
	parts       map[int][]byte
}

func newMemBackend() *memBackend {
	return &memBackend{
		objects: map[string]memObject{},
		uploads: map[string]*memUpload{},
	}
}

func objKey(bucket, key string) string { return bucket + "/" + key }

func md5Hex(b []byte) string {
	sum := md5.Sum(b)
	return hex.EncodeToString(sum[:])
}

func (m *memBackend) PutObject(ctx context.Context, bucket, key string, body io.Reader) (ObjectInfo, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return ObjectInfo{}, err
	}
	etag := md5Hex(data)
	m.mu.Lock()
	m.objects[objKey(bucket, key)] = memObject{data: data, etag: etag, mod: time.Now()}
	m.mu.Unlock()
	return ObjectInfo{Key: key, Size: int64(len(data)), ETag: etag, LastModified: time.Now()}, nil
}

type memReadSeekCloser struct {
	*bytes.Reader
}

func (memReadSeekCloser) Close() error { return nil }

func (m *memBackend) GetObject(ctx context.Context, bucket, key string) (ReadSeekCloser, ObjectInfo, error) {
	m.mu.Lock()
	obj, ok := m.objects[objKey(bucket, key)]
	m.mu.Unlock()
	if !ok {
		return nil, ObjectInfo{}, ErrNotFound
	}
	return memReadSeekCloser{bytes.NewReader(obj.data)}, ObjectInfo{Key: key, Size: int64(len(obj.data)), ETag: obj.etag, LastModified: obj.mod}, nil
}

func (m *memBackend) HeadObject(ctx context.Context, bucket, key string) (ObjectInfo, error) {
	m.mu.Lock()
	obj, ok := m.objects[objKey(bucket, key)]
	m.mu.Unlock()
	if !ok {
		return ObjectInfo{}, ErrNotFound
	}
	return ObjectInfo{Key: key, Size: int64(len(obj.data)), ETag: obj.etag, LastModified: obj.mod}, nil
}

func (m *memBackend) DeleteObject(ctx context.Context, bucket, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.objects[objKey(bucket, key)]; !ok {
		return ErrNotFound
	}
	delete(m.objects, objKey(bucket, key))
	return nil
}

func (m *memBackend) ListObjects(ctx context.Context, bucket, prefix, delimiter, startAfter string, maxKeys int) ([]ObjectInfo, []string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []ObjectInfo
	for k, obj := range m.objects {
		if !strings.HasPrefix(k, bucket+"/") {
			continue
		}
		key := strings.TrimPrefix(k, bucket+"/")
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		out = append(out, ObjectInfo{Key: key, Size: int64(len(obj.data)), ETag: obj.etag, LastModified: obj.mod})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil, false, nil
}

func (m *memBackend) CreateMultipartUpload(ctx context.Context, bucket, key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextID++
	id := "upload-" + itoaTest(m.nextID)
	m.uploads[id] = &memUpload{bucket: bucket, key: key, parts: map[int][]byte{}}
	return id, nil
}

func itoaTest(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

func (m *memBackend) UploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, body io.Reader) (string, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	u, ok := m.uploads[uploadID]
	if !ok {
		return "", ErrNotFound
	}
	u.parts[partNumber] = data
	return md5Hex(data), nil
}

func (m *memBackend) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []Part) (ObjectInfo, error) {
	m.mu.Lock()
	u, ok := m.uploads[uploadID]
	if !ok {
		m.mu.Unlock()
		return ObjectInfo{}, ErrNotFound
	}
	var buf bytes.Buffer
	for _, p := range parts {
		data, ok := u.parts[p.PartNumber]
		if !ok {
			m.mu.Unlock()
			return ObjectInfo{}, ErrNotFound
		}
		buf.Write(data)
	}
	delete(m.uploads, uploadID)
	m.mu.Unlock()

	return m.PutObject(ctx, bucket, key, &buf)
}

func (m *memBackend) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.uploads[uploadID]; !ok {
		return ErrNotFound
	}
	delete(m.uploads, uploadID)
	return nil
}
