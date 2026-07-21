package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"time"

	bolt "go.etcd.io/bbolt"
)

// ErrNotFound is returned when a lookup finds no matching record.
var ErrNotFound = errors.New("store: not found")

// ObjectMapping is the authoritative record of where an S3 bucket/key's
// bytes actually live in PBS.
type ObjectMapping struct {
	Bucket        string    `json:"bucket"`
	Key           string    `json:"key"`
	Namespace     string    `json:"namespace"`
	PBSBackupType string    `json:"pbs_backup_type"`
	PBSBackupID   string    `json:"pbs_backup_id"`
	PBSBackupTime int64     `json:"pbs_backup_time"` // unix seconds
	Size          int64     `json:"size"`
	ETag          string    `json:"etag"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func (db *DB) PutObjectMapping(m ObjectMapping) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return db.bolt.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketObjects).Put(objectMapKey(m.Bucket, m.Key), data)
	})
}

func (db *DB) GetObjectMapping(bucket, key string) (ObjectMapping, error) {
	var m ObjectMapping
	err := db.bolt.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketObjects).Get(objectMapKey(bucket, key))
		if v == nil {
			return ErrNotFound
		}
		return json.Unmarshal(v, &m)
	})
	return m, err
}

func (db *DB) DeleteObjectMapping(bucket, key string) error {
	return db.bolt.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketObjects)
		k := objectMapKey(bucket, key)
		if b.Get(k) == nil {
			return ErrNotFound
		}
		return b.Delete(k)
	})
}

// ListObjects performs a prefix scan over bucket, ordered by key, returning
// up to maxKeys mappings whose key is > startAfter and has the given prefix.
// isTruncated is true if more matching results exist beyond what was
// returned.
func (db *DB) ListObjects(bucket, prefix, startAfter string, maxKeys int) (mappings []ObjectMapping, isTruncated bool, err error) {
	prefixBytes := []byte(bucket + "\x00" + prefix)
	var startAfterKey []byte
	if startAfter != "" {
		startAfterKey = []byte(bucket + "\x00" + startAfter)
	}

	err = db.bolt.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketObjects).Cursor()
		for k, v := c.Seek(prefixBytes); k != nil && bytes.HasPrefix(k, prefixBytes); k, v = c.Next() {
			if startAfterKey != nil && bytes.Compare(k, startAfterKey) <= 0 {
				continue
			}
			if len(mappings) >= maxKeys {
				isTruncated = true
				break
			}
			var m ObjectMapping
			if err := json.Unmarshal(v, &m); err != nil {
				return err
			}
			mappings = append(mappings, m)
		}
		return nil
	})
	return mappings, isTruncated, err
}
