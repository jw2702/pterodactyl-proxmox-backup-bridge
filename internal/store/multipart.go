package store

import (
	"bytes"
	"encoding/json"
	"time"

	bolt "go.etcd.io/bbolt"
)

// MultipartUpload tracks an in-progress S3 multipart upload.
type MultipartUpload struct {
	UploadID       string    `json:"upload_id"`
	Bucket         string    `json:"bucket"`
	Key            string    `json:"key"`
	Namespace      string    `json:"namespace"`
	InitiatedAt    time.Time `json:"initiated_at"`
	LastActivityAt time.Time `json:"last_activity_at"`
}

// PartInfo records a single uploaded part's staging location and metadata.
type PartInfo struct {
	PartNumber int       `json:"part_number"`
	ETag       string    `json:"etag"`
	Size       int64     `json:"size"`
	TempPath   string    `json:"temp_path"`
	UploadedAt time.Time `json:"uploaded_at"`
}

func (db *DB) CreateUpload(u MultipartUpload) error {
	data, err := json.Marshal(u)
	if err != nil {
		return err
	}
	return db.bolt.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMultipartUploads).Put([]byte(u.UploadID), data)
	})
}

func (db *DB) GetUpload(uploadID string) (MultipartUpload, error) {
	var u MultipartUpload
	err := db.bolt.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketMultipartUploads).Get([]byte(uploadID))
		if v == nil {
			return ErrNotFound
		}
		return json.Unmarshal(v, &u)
	})
	return u, err
}

func (db *DB) TouchUpload(uploadID string, t time.Time) error {
	return db.bolt.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketMultipartUploads)
		v := b.Get([]byte(uploadID))
		if v == nil {
			return ErrNotFound
		}
		var u MultipartUpload
		if err := json.Unmarshal(v, &u); err != nil {
			return err
		}
		u.LastActivityAt = t
		data, err := json.Marshal(u)
		if err != nil {
			return err
		}
		return b.Put([]byte(uploadID), data)
	})
}

// DeleteUpload removes the upload record and all of its part records.
func (db *DB) DeleteUpload(uploadID string) error {
	return db.bolt.Update(func(tx *bolt.Tx) error {
		uploads := tx.Bucket(bucketMultipartUploads)
		if uploads.Get([]byte(uploadID)) == nil {
			return ErrNotFound
		}
		if err := uploads.Delete([]byte(uploadID)); err != nil {
			return err
		}
		parts := tx.Bucket(bucketMultipartParts)
		c := parts.Cursor()
		prefix := []byte(uploadID + "/")
		var toDelete [][]byte
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			toDelete = append(toDelete, append([]byte(nil), k...))
		}
		for _, k := range toDelete {
			if err := parts.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *DB) PutPart(uploadID string, p PartInfo) error {
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return db.bolt.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMultipartParts).Put(partKey(uploadID, p.PartNumber), data)
	})
}

// ListParts returns all parts for uploadID, ordered by part number (bbolt's
// key ordering already sorts correctly since partKey zero-pads the number).
func (db *DB) ListParts(uploadID string) ([]PartInfo, error) {
	var parts []PartInfo
	err := db.bolt.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketMultipartParts).Cursor()
		prefix := []byte(uploadID + "/")
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			var p PartInfo
			if err := json.Unmarshal(v, &p); err != nil {
				return err
			}
			parts = append(parts, p)
		}
		return nil
	})
	return parts, err
}

// ListUploadsOlderThan returns all multipart uploads whose LastActivityAt is
// before cutoff, for garbage collection of abandoned uploads.
func (db *DB) ListUploadsOlderThan(cutoff time.Time) ([]MultipartUpload, error) {
	var uploads []MultipartUpload
	err := db.bolt.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMultipartUploads).ForEach(func(k, v []byte) error {
			var u MultipartUpload
			if err := json.Unmarshal(v, &u); err != nil {
				return err
			}
			if u.LastActivityAt.Before(cutoff) {
				uploads = append(uploads, u)
			}
			return nil
		})
	})
	return uploads, err
}

// ListAllUploadIDs returns every tracked upload ID, used at startup to
// reconcile against orphaned scratch directories.
func (db *DB) ListAllUploadIDs() ([]string, error) {
	var ids []string
	err := db.bolt.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMultipartUploads).ForEach(func(k, v []byte) error {
			ids = append(ids, string(k))
			return nil
		})
	})
	return ids, err
}
