// Package backend implements s3api.Backend by combining internal/store
// (authoritative metadata), internal/stage (local scratch buffering),
// internal/pbs (the proxmox-backup-client wrapper) and internal/idmap (S3
// key/bucket -> PBS backup-id/namespace sanitization). This is the
// production storage engine wired up in cmd/bridge/main.go.
package backend

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/idmap"
	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/pbs"
	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/s3api"
	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/stage"
	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/store"
)

// Backend is the production s3api.Backend implementation.
type Backend struct {
	Store      *store.DB
	Stage      *stage.Manager
	PBS        *pbs.Client
	BackupType string // PBS backup-type, e.g. "host"
	Locks      *store.KeyedMutex
	Log        *slog.Logger
}

func New(db *store.DB, stg *stage.Manager, pbsClient *pbs.Client, backupType string, log *slog.Logger) *Backend {
	if backupType == "" {
		backupType = "host"
	}
	return &Backend{
		Store:      db,
		Stage:      stg,
		PBS:        pbsClient,
		BackupType: backupType,
		Locks:      store.NewKeyedMutex(),
		Log:        log,
	}
}

func (b *Backend) log() *slog.Logger {
	if b.Log != nil {
		return b.Log
	}
	return slog.Default()
}

func lockKey(bucket, key string) string { return bucket + "\x00" + key }

var _ s3api.Backend = (*Backend)(nil)

// mapNotFound converts the store's not-found sentinel into the one
// s3api.Backend implementations are expected to return.
func mapNotFound(err error) error {
	if errors.Is(err, store.ErrNotFound) {
		return s3api.ErrNotFound
	}
	return err
}

func newUploadID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// commitObject runs the create-new-then-forget-old overwrite protocol and
// writes the resulting mapping to the store. filePath's bytes become the new
// snapshot's content; on success the temp file is removed.
func (b *Backend) commitObject(ctx context.Context, bucket, key, filePath string, size int64, md5Hex string) (s3api.ObjectInfo, error) {
	unlock := b.Locks.Lock(lockKey(bucket, key))
	defer unlock()

	ns := idmap.SanitizeNamespace(bucket)
	if err := b.PBS.EnsureNamespace(ctx, ns); err != nil {
		return s3api.ObjectInfo{}, fmt.Errorf("backend: ensuring namespace %q: %w", ns, err)
	}

	backupID := idmap.SanitizeBackupID(key)
	backupTime := time.Now().UTC()

	usedTime, err := b.PBS.Backup(ctx, filePath, b.BackupType, backupID, backupTime, ns)
	if err != nil {
		return s3api.ObjectInfo{}, fmt.Errorf("backend: pbs backup failed: %w", err)
	}
	_ = os.Remove(filePath)

	if err := b.PBS.UpdateNotes(ctx, b.BackupType, backupID, usedTime, ns, bucket+"/"+key); err != nil {
		// Best-effort reconciliation aid only; the store mapping below is
		// authoritative, so a notes-update failure must not fail the
		// upload.
		b.log().Warn("backend: updating snapshot notes failed (non-fatal)", "bucket", bucket, "key", key, "error", err)
	}

	old, oldErr := b.Store.GetObjectMapping(bucket, key)
	if oldErr == nil {
		if err := b.PBS.Forget(ctx, old.PBSBackupType, old.PBSBackupID, time.Unix(old.PBSBackupTime, 0), old.Namespace); err != nil {
			b.log().Error("backend: forgetting superseded snapshot failed (leaked snapshot)", "bucket", bucket, "key", key, "old_backup_id", old.PBSBackupID, "error", err)
		}
	} else if !errors.Is(oldErr, store.ErrNotFound) {
		return s3api.ObjectInfo{}, fmt.Errorf("backend: checking for existing mapping: %w", oldErr)
	}

	mapping := store.ObjectMapping{
		Bucket:        bucket,
		Key:           key,
		Namespace:     ns,
		PBSBackupType: b.BackupType,
		PBSBackupID:   backupID,
		PBSBackupTime: usedTime.Unix(),
		Size:          size,
		ETag:          md5Hex,
		UpdatedAt:     time.Now().UTC(),
	}
	if err := b.Store.PutObjectMapping(mapping); err != nil {
		return s3api.ObjectInfo{}, fmt.Errorf("backend: persisting object mapping: %w", err)
	}

	return s3api.ObjectInfo{Key: key, Size: size, ETag: md5Hex, LastModified: mapping.UpdatedAt}, nil
}

func (b *Backend) PutObject(ctx context.Context, bucket, key string, body io.Reader) (s3api.ObjectInfo, error) {
	res, err := b.Stage.WriteToTemp("puts", "put-*.tmp", body)
	if err != nil {
		return s3api.ObjectInfo{}, err
	}
	return b.commitObject(ctx, bucket, key, res.Path, res.Size, res.MD5)
}

type deletingFile struct {
	*os.File
	path string
}

func (d *deletingFile) Close() error {
	err := d.File.Close()
	_ = os.Remove(d.path)
	return err
}

func (b *Backend) GetObject(ctx context.Context, bucket, key string) (s3api.ReadSeekCloser, s3api.ObjectInfo, error) {
	mapping, err := b.Store.GetObjectMapping(bucket, key)
	if err != nil {
		return nil, s3api.ObjectInfo{}, mapNotFound(err)
	}

	outPath, err := b.Stage.TempFilePath("gets", "get-*.tmp")
	if err != nil {
		return nil, s3api.ObjectInfo{}, err
	}

	if err := b.PBS.Restore(ctx, mapping.PBSBackupType, mapping.PBSBackupID, time.Unix(mapping.PBSBackupTime, 0), mapping.Namespace, outPath); err != nil {
		return nil, s3api.ObjectInfo{}, fmt.Errorf("backend: pbs restore failed: %w", err)
	}

	f, err := os.Open(outPath)
	if err != nil {
		return nil, s3api.ObjectInfo{}, err
	}

	info := s3api.ObjectInfo{Key: key, Size: mapping.Size, ETag: mapping.ETag, LastModified: mapping.UpdatedAt}
	return &deletingFile{File: f, path: outPath}, info, nil
}

func (b *Backend) HeadObject(ctx context.Context, bucket, key string) (s3api.ObjectInfo, error) {
	mapping, err := b.Store.GetObjectMapping(bucket, key)
	if err != nil {
		return s3api.ObjectInfo{}, mapNotFound(err)
	}
	return s3api.ObjectInfo{Key: key, Size: mapping.Size, ETag: mapping.ETag, LastModified: mapping.UpdatedAt}, nil
}

func (b *Backend) DeleteObject(ctx context.Context, bucket, key string) error {
	unlock := b.Locks.Lock(lockKey(bucket, key))
	defer unlock()

	mapping, err := b.Store.GetObjectMapping(bucket, key)
	if err != nil {
		return mapNotFound(err)
	}
	if err := b.PBS.Forget(ctx, mapping.PBSBackupType, mapping.PBSBackupID, time.Unix(mapping.PBSBackupTime, 0), mapping.Namespace); err != nil {
		return fmt.Errorf("backend: pbs forget failed: %w", err)
	}
	if err := b.Store.DeleteObjectMapping(bucket, key); err != nil {
		return mapNotFound(err)
	}
	return nil
}

func (b *Backend) ListObjects(ctx context.Context, bucket, prefix, delimiter, startAfter string, maxKeys int) ([]s3api.ObjectInfo, []string, bool, error) {
	mappings, truncated, err := b.Store.ListObjects(bucket, prefix, startAfter, maxKeys*4) // over-fetch to allow delimiter grouping
	if err != nil {
		return nil, nil, false, err
	}

	var objects []s3api.ObjectInfo
	seenPrefixes := map[string]bool{}
	var commonPrefixes []string

	for _, m := range mappings {
		rest := m.Key[len(prefix):]
		if delimiter != "" {
			if idx := indexOf(rest, delimiter); idx >= 0 {
				cp := prefix + rest[:idx+len(delimiter)]
				if !seenPrefixes[cp] {
					seenPrefixes[cp] = true
					commonPrefixes = append(commonPrefixes, cp)
				}
				continue
			}
		}
		if len(objects)+len(commonPrefixes) >= maxKeys {
			truncated = true
			break
		}
		objects = append(objects, s3api.ObjectInfo{Key: m.Key, Size: m.Size, ETag: m.ETag, LastModified: m.UpdatedAt})
	}

	return objects, commonPrefixes, truncated, nil
}

func indexOf(s, substr string) int {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func (b *Backend) CreateMultipartUpload(ctx context.Context, bucket, key string) (string, error) {
	uploadID := newUploadID()
	now := time.Now().UTC()
	err := b.Store.CreateUpload(store.MultipartUpload{
		UploadID:       uploadID,
		Bucket:         bucket,
		Key:            key,
		Namespace:      idmap.SanitizeNamespace(bucket),
		InitiatedAt:    now,
		LastActivityAt: now,
	})
	if err != nil {
		return "", err
	}
	return uploadID, nil
}

func (b *Backend) UploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, body io.Reader) (string, error) {
	if _, err := b.Store.GetUpload(uploadID); err != nil {
		return "", mapNotFound(err)
	}

	res, err := b.Stage.WritePart(uploadID, partNumber, body)
	if err != nil {
		return "", err
	}

	if err := b.Store.PutPart(uploadID, store.PartInfo{
		PartNumber: partNumber,
		ETag:       res.MD5,
		Size:       res.Size,
		TempPath:   res.Path,
		UploadedAt: time.Now().UTC(),
	}); err != nil {
		return "", err
	}
	_ = b.Store.TouchUpload(uploadID, time.Now().UTC())

	return res.MD5, nil
}

func (b *Backend) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []s3api.Part) (s3api.ObjectInfo, error) {
	upload, err := b.Store.GetUpload(uploadID)
	if err != nil {
		return s3api.ObjectInfo{}, mapNotFound(err)
	}

	stored, err := b.Store.ListParts(uploadID)
	if err != nil {
		return s3api.ObjectInfo{}, err
	}
	byNumber := make(map[int]store.PartInfo, len(stored))
	for _, p := range stored {
		byNumber[p.PartNumber] = p
	}

	if len(parts) == 0 {
		return s3api.ObjectInfo{}, fmt.Errorf("backend: CompleteMultipartUpload called with zero parts")
	}

	var paths []string
	prevPartNumber := 0
	for _, p := range parts {
		if p.PartNumber <= prevPartNumber {
			return s3api.ObjectInfo{}, fmt.Errorf("backend: parts must be listed in strictly ascending order (got %d after %d)", p.PartNumber, prevPartNumber)
		}
		prevPartNumber = p.PartNumber

		sp, ok := byNumber[p.PartNumber]
		if !ok {
			return s3api.ObjectInfo{}, fmt.Errorf("backend: part %d was never uploaded", p.PartNumber)
		}
		if sp.ETag != p.ETag {
			return s3api.ObjectInfo{}, fmt.Errorf("backend: part %d ETag mismatch (client claimed %s, stored %s)", p.PartNumber, p.ETag, sp.ETag)
		}
		paths = append(paths, sp.TempPath)
	}

	final, err := b.Stage.ConcatParts(uploadID, paths)
	if err != nil {
		return s3api.ObjectInfo{}, err
	}

	info, err := b.commitObject(ctx, upload.Bucket, upload.Key, final.Path, final.Size, final.MD5)
	if err != nil {
		return s3api.ObjectInfo{}, err
	}

	if err := b.Stage.RemoveUploadDir(uploadID); err != nil {
		b.log().Error("backend: removing upload scratch dir after complete failed", "upload_id", uploadID, "error", err)
	}
	if err := b.Store.DeleteUpload(uploadID); err != nil {
		b.log().Error("backend: deleting upload record after complete failed", "upload_id", uploadID, "error", err)
	}

	return info, nil
}

func (b *Backend) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	if _, err := b.Store.GetUpload(uploadID); err != nil {
		return mapNotFound(err)
	}
	if err := b.Stage.RemoveUploadDir(uploadID); err != nil {
		return err
	}
	return b.Store.DeleteUpload(uploadID)
}
