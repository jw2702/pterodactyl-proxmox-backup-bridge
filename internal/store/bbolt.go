// Package store implements the bridge's authoritative embedded metadata
// database (bbolt): the mapping from S3 bucket/key to PBS backup
// coordinates, and bookkeeping for in-progress multipart uploads.
package store

import (
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	bucketObjects          = []byte("objects")
	bucketMultipartUploads = []byte("multipart_uploads")
	bucketMultipartParts   = []byte("multipart_parts")
	bucketMeta             = []byte("meta")

	keySchemaVersion = []byte("schema_version")
)

const currentSchemaVersion = "1"

// DB wraps a bbolt database with the bridge's schema and access methods.
type DB struct {
	bolt *bolt.DB
}

// Open opens (creating if necessary) the bbolt database at path and ensures
// all required top-level buckets exist.
func Open(path string) (*DB, error) {
	bdb, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("store: opening bbolt db at %s: %w", path, err)
	}

	err = bdb.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{bucketObjects, bucketMultipartUploads, bucketMultipartParts, bucketMeta} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return fmt.Errorf("creating bucket %s: %w", name, err)
			}
		}
		meta := tx.Bucket(bucketMeta)
		if meta.Get(keySchemaVersion) == nil {
			if err := meta.Put(keySchemaVersion, []byte(currentSchemaVersion)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		_ = bdb.Close()
		return nil, err
	}

	return &DB{bolt: bdb}, nil
}

func (db *DB) Close() error {
	return db.bolt.Close()
}

// objectMapKey builds the composite bbolt key for an object mapping,
// separating bucket and key with a NUL byte to avoid ambiguity (S3 bucket
// names cannot contain NUL, unlike keys).
func objectMapKey(bucket, key string) []byte {
	return []byte(bucket + "\x00" + key)
}

func partKey(uploadID string, partNumber int) []byte {
	return []byte(fmt.Sprintf("%s/%08d", uploadID, partNumber))
}
