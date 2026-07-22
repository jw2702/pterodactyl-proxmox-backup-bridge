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

// groupLockKey serializes PBS.Backup calls that target the same PBS backup
// group (server), independent of which S3 key each individual backup uses.
// Two different backups for the same server share one group but have
// different (bucket,key) locks, so without this a concurrent backup-time
// collision between them could — in the worst case — have both sides'
// collision-retry loops (see pbs.Client.Backup) repeatedly bump into each
// other and both exhaust their retries. Serializing the actual PBS.Backup
// invocation per group removes the race outright: at most one Backup call
// for a given group is ever in flight, so a collision can only ever be
// against already-committed history, never a moving target. Prefixed to
// keep this key space disjoint from lockKey's (bucket,key) space, since
// namespace/backup-type/backup-id strings could otherwise coincide with a
// real bucket/key pair.
func groupLockKey(namespace, backupType, backupID string) string {
	return "group\x00" + namespace + "\x00" + backupType + "\x00" + backupID
}

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
	return b.commitObjectLocked(ctx, bucket, key, filePath, size, md5Hex)
}

// commitObjectLocked is commitObject's body, factored out so
// CompleteMultipartUpload — which must hold the (bucket,key) lock for its
// own ConcatParts phase too, not just this final commit step — can call it
// directly without acquiring the lock a second time. KeyedMutex.Lock is not
// reentrant: a second Lock call for the same key from the same goroutine
// would deadlock against itself. Every caller of this function must already
// hold lockKey(bucket, key).
func (b *Backend) commitObjectLocked(ctx context.Context, bucket, key, filePath string, size int64, md5Hex string) (s3api.ObjectInfo, error) {
	// Namespaces are NOT created by the bridge (that requires
	// Datastore.Modify, which the bridge's PBS user/token intentionally
	// does not have) — an administrator must pre-create the namespace for
	// each bucket. See README.md.
	ns := idmap.SanitizeNamespace(bucket)

	// All backups belonging to the same server share one PBS backup group
	// (derived from the server-UUID path segment of the key), with each
	// individual backup becoming a new snapshot within that group, rather
	// than every backup getting its own single-snapshot group.
	backupID := idmap.GroupIDFromKey(key)
	backupTime := time.Now().UTC()

	// Serialize the actual PBS.Backup call (including its internal
	// backup-time-collision retry loop) per group, not just per
	// (bucket,key) — see groupLockKey's comment for why.
	unlockGroup := b.Locks.Lock(groupLockKey(ns, b.BackupType, backupID))
	usedTime, err := b.PBS.Backup(ctx, filePath, b.BackupType, backupID, backupTime, ns)
	unlockGroup()
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
	if oldErr != nil && !errors.Is(oldErr, store.ErrNotFound) {
		return s3api.ObjectInfo{}, fmt.Errorf("backend: checking for existing mapping: %w", oldErr)
	}

	// Point the key at the new snapshot before touching the old one in PBS.
	// If this write fails (bbolt error, crash, disk full), the old mapping
	// is still untouched and still valid — worst case the new snapshot
	// leaks in PBS (recoverable via its notes field, see
	// docs/LIMITATIONS.md), which is the same tolerance already accepted
	// below if Forget fails. Doing this the other way around (forget old,
	// then write the new mapping) would risk the key pointing at a
	// just-deleted snapshot if the mapping write failed afterwards —
	// breaking the "key always has a valid backing snapshot" guarantee
	// instead of merely leaking storage.
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

	if oldErr == nil {
		if err := b.PBS.Forget(ctx, old.PBSBackupType, old.PBSBackupID, time.Unix(old.PBSBackupTime, 0), old.Namespace); err != nil {
			b.log().Error("backend: forgetting superseded snapshot failed (leaked snapshot)", "bucket", bucket, "key", key, "old_backup_id", old.PBSBackupID, "error", err)
		}
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

// deletingFile wraps a local temp file, limiting reads to a byte range
// (used for Range requests, which still need a seekable local copy) and
// removing the underlying file on Close.
type deletingFile struct {
	r    io.Reader
	file *os.File
	path string
}

func (d *deletingFile) Read(p []byte) (int, error) { return d.r.Read(p) }

func (d *deletingFile) Close() error {
	err := d.file.Close()
	_ = os.Remove(d.path)
	return err
}

func (b *Backend) GetObject(ctx context.Context, bucket, key string, rangeSpec *s3api.RangeSpec) (io.ReadCloser, s3api.ObjectInfo, error) {
	mapping, err := b.Store.GetObjectMapping(bucket, key)
	if err != nil {
		return nil, s3api.ObjectInfo{}, mapNotFound(err)
	}
	info := s3api.ObjectInfo{Key: key, Size: mapping.Size, ETag: mapping.ETag, LastModified: mapping.UpdatedAt}

	if rangeSpec == nil {
		// Stream directly from proxmox-backup-client's stdout rather than
		// buffering the whole object to local disk first: Wings blocks its
		// own response to Panel on receiving HTTP response headers from
		// this request, so time-to-first-byte here directly determines how
		// long Panel waits before it can show the restore as "accepted".
		rc, err := b.PBS.RestoreStream(ctx, mapping.PBSBackupType, mapping.PBSBackupID, time.Unix(mapping.PBSBackupTime, 0), mapping.Namespace)
		if err != nil {
			return nil, s3api.ObjectInfo{}, fmt.Errorf("backend: pbs restore stream failed: %w", err)
		}
		return rc, info, nil
	}

	// A specific byte range was requested. A live subprocess pipe can't be
	// seeked, so fall back to restoring to a local file and slicing that -
	// this is the pre-existing "restore-then-slice" behavior, now scoped to
	// only the (rare, for backup restores) ranged-request case.
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
	if _, err := f.Seek(rangeSpec.Start, io.SeekStart); err != nil {
		f.Close()
		_ = os.Remove(outPath)
		return nil, s3api.ObjectInfo{}, err
	}
	length := rangeSpec.End - rangeSpec.Start + 1
	return &deletingFile{r: io.LimitReader(f, length), file: f, path: outPath}, info, nil
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
		if !errors.Is(err, store.ErrNotFound) {
			return err
		}
		// No completed object at this key. Pterodactyl Panel's "delete
		// backup" action on a still-in-progress (never-completed) backup
		// issues exactly this DeleteObject call rather than
		// AbortMultipartUpload, so clean up any matching abandoned
		// multipart upload(s) now instead of leaving their scratch data
		// for the GC's TTL sweep.
		return b.abortMatchingUploads(bucket, key)
	}
	if err := b.PBS.Forget(ctx, mapping.PBSBackupType, mapping.PBSBackupID, time.Unix(mapping.PBSBackupTime, 0), mapping.Namespace); err != nil {
		return fmt.Errorf("backend: pbs forget failed: %w", err)
	}
	if err := b.Store.DeleteObjectMapping(bucket, key); err != nil {
		return mapNotFound(err)
	}
	return nil
}

// abortMatchingUploads finds and removes any in-progress multipart upload(s)
// for bucket/key. Always returns nil (mapNotFound-equivalent) so DeleteObject
// stays idempotent from the caller's perspective even when there was nothing
// to clean up, matching S3 DeleteObject semantics.
func (b *Backend) abortMatchingUploads(bucket, key string) error {
	uploads, err := b.Store.FindUploadsByBucketKey(bucket, key)
	if err != nil {
		return err
	}
	for _, u := range uploads {
		if err := b.Stage.RemoveUploadDir(u.UploadID); err != nil {
			b.log().Error("backend: removing scratch dir for aborted upload failed", "upload_id", u.UploadID, "bucket", bucket, "key", key, "error", err)
		}
		if err := b.Store.DeleteUpload(u.UploadID); err != nil {
			b.log().Error("backend: deleting aborted upload record failed", "upload_id", u.UploadID, "bucket", bucket, "key", key, "error", err)
		}
	}
	return s3api.ErrNotFound
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

// CreateMultipartUpload, UploadPart and AbortMultipartUpload all take the
// (bucket,key) lock — the same one DeleteObject and commitObject use — for
// their whole body, not just their metadata writes. DeleteObject on a
// still-in-progress (never-completed) backup falls through to
// abortMatchingUploads, which removes an upload's scratch directory and
// bbolt record; without this lock, that removal could run concurrently with
// an in-flight UploadPart's disk write to a file inside the very directory
// being removed, or with a PutPart bbolt write registering a part against an
// upload record that Abort/Delete just deleted. Locking the entire body
// (including the disk write, not just the metadata calls) is what actually
// closes that window — a shorter critical section around only the metadata
// steps would still race against RemoveUploadDir's os.RemoveAll. In
// practice this doesn't cost real throughput: Wings uploads a backup's parts
// sequentially already, so this lock is very rarely contended.
func (b *Backend) CreateMultipartUpload(ctx context.Context, bucket, key string) (string, error) {
	unlock := b.Locks.Lock(lockKey(bucket, key))
	defer unlock()

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
	unlock := b.Locks.Lock(lockKey(bucket, key))
	defer unlock()

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

func (b *Backend) ListParts(ctx context.Context, bucket, key, uploadID string) ([]s3api.Part, error) {
	if _, err := b.Store.GetUpload(uploadID); err != nil {
		return nil, mapNotFound(err)
	}

	stored, err := b.Store.ListParts(uploadID)
	if err != nil {
		return nil, err
	}

	parts := make([]s3api.Part, len(stored))
	for i, p := range stored {
		parts[i] = s3api.Part{
			PartNumber:   p.PartNumber,
			ETag:         p.ETag,
			Size:         p.Size,
			LastModified: p.UploadedAt,
		}
	}
	return parts, nil
}

// CompleteMultipartUpload holds the (bucket,key) lock for its entire body —
// covering ConcatParts (see CreateMultipartUpload's comment for why) as well
// as the final commit — and therefore calls commitObjectLocked directly
// rather than commitObject, which would try to acquire the same lock a
// second time and deadlock.
func (b *Backend) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []s3api.Part) (s3api.ObjectInfo, error) {
	unlock := b.Locks.Lock(lockKey(bucket, key))
	defer unlock()

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
		// ConcatParts may have partially run before failing; whatever part
		// files remain plus any partial output are cleaned up immediately
		// rather than left for the GC's TTL sweep.
		b.cleanupUpload(uploadID)
		return s3api.ObjectInfo{}, err
	}

	info, err := b.commitObjectLocked(ctx, upload.Bucket, upload.Key, final.Path, final.Size, final.MD5)
	if err != nil {
		// Deliberately do NOT clean up here: Panel's AWS SDK client
		// automatically retries a failed CompleteMultipartUpload with the
		// same upload ID (observed directly in production logs), and
		// stage.ConcatParts already deleted the source parts as it went —
		// so final.img (still on disk; commitObject only removes it on
		// success) is the only thing a retry can succeed against. It's
		// reclaimed on a later successful retry, an explicit
		// Abort/DeleteObject call, or the GC's TTL sweep if the upload is
		// truly abandoned.
		return s3api.ObjectInfo{}, err
	}

	b.cleanupUpload(uploadID)
	return info, nil
}

// cleanupUpload removes an upload's scratch directory and bbolt record,
// logging (but not failing on) any error — used both on the success and
// failure paths of CompleteMultipartUpload so scratch disk usage is
// reclaimed immediately instead of waiting on the GC's TTL sweep.
func (b *Backend) cleanupUpload(uploadID string) {
	if err := b.Stage.RemoveUploadDir(uploadID); err != nil {
		b.log().Error("backend: removing upload scratch dir failed", "upload_id", uploadID, "error", err)
	}
	if err := b.Store.DeleteUpload(uploadID); err != nil {
		b.log().Error("backend: deleting upload record failed", "upload_id", uploadID, "error", err)
	}
}

func (b *Backend) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	unlock := b.Locks.Lock(lockKey(bucket, key))
	defer unlock()

	if _, err := b.Store.GetUpload(uploadID); err != nil {
		return mapNotFound(err)
	}
	if err := b.Stage.RemoveUploadDir(uploadID); err != nil {
		return err
	}
	return b.Store.DeleteUpload(uploadID)
}
