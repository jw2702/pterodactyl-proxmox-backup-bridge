// Package stage manages local scratch-disk buffering of upload bodies:
// writing request bodies to temp files (since proxmox-backup-client needs a
// real file, not an arbitrary stream) and concatenating multipart parts in
// order before handing the result to the PBS client.
package stage

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Manager owns a scratch root directory used for all temp-file staging.
type Manager struct {
	Root string
}

func New(root string) (*Manager, error) {
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, fmt.Errorf("stage: creating scratch root %s: %w", root, err)
	}
	return &Manager{Root: root}, nil
}

// WriteResult describes a body that has been fully buffered to disk.
type WriteResult struct {
	Path string
	Size int64
	MD5  string // hex, used as the S3 ETag for single parts
}

// WriteToTemp streams body to a new temp file under dir (relative to Root,
// created if necessary) and returns its path, size, and MD5 checksum.
func (m *Manager) WriteToTemp(dir, namePattern string, body io.Reader) (WriteResult, error) {
	fullDir := filepath.Join(m.Root, dir)
	if err := os.MkdirAll(fullDir, 0o750); err != nil {
		return WriteResult{}, fmt.Errorf("stage: creating dir %s: %w", fullDir, err)
	}

	f, err := os.CreateTemp(fullDir, namePattern)
	if err != nil {
		return WriteResult{}, fmt.Errorf("stage: creating temp file: %w", err)
	}
	defer f.Close()

	h := md5.New()
	n, err := io.Copy(io.MultiWriter(f, h), body)
	if err != nil {
		_ = os.Remove(f.Name())
		return WriteResult{}, fmt.Errorf("stage: writing body: %w", err)
	}

	return WriteResult{
		Path: f.Name(),
		Size: n,
		MD5:  hex.EncodeToString(h.Sum(nil)),
	}, nil
}

// UploadDir returns the scratch directory used for a given multipart
// upload's part files.
func (m *Manager) UploadDir(uploadID string) string {
	return filepath.Join(m.Root, "multipart", uploadID)
}

// RemoveUploadDir deletes an upload's entire scratch directory tree.
func (m *Manager) RemoveUploadDir(uploadID string) error {
	return os.RemoveAll(m.UploadDir(uploadID))
}

// ConcatParts concatenates the given file paths, in order, into a single new
// temp file and returns its path and total size. It does not delete the
// source part files; callers should do so (via RemoveUploadDir) once the PBS
// backup invocation using the result has completed.
func (m *Manager) ConcatParts(uploadID string, partPaths []string) (WriteResult, error) {
	dir := m.UploadDir(uploadID)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return WriteResult{}, fmt.Errorf("stage: creating dir %s: %w", dir, err)
	}

	out, err := os.CreateTemp(dir, "final-*.blob")
	if err != nil {
		return WriteResult{}, fmt.Errorf("stage: creating final file: %w", err)
	}
	defer out.Close()

	h := md5.New()
	var total int64
	for _, p := range partPaths {
		in, err := os.Open(p)
		if err != nil {
			return WriteResult{}, fmt.Errorf("stage: opening part %s: %w", p, err)
		}
		n, err := io.Copy(io.MultiWriter(out, h), in)
		in.Close()
		if err != nil {
			return WriteResult{}, fmt.Errorf("stage: concatenating part %s: %w", p, err)
		}
		total += n
	}

	return WriteResult{Path: out.Name(), Size: total, MD5: hex.EncodeToString(h.Sum(nil))}, nil
}

// TempFilePath allocates a fresh, unique file path under dir (relative to
// Root) without leaving it open — callers (e.g. a PBS restore invocation)
// create/write the actual file themselves.
func (m *Manager) TempFilePath(dir, namePattern string) (string, error) {
	fullDir := filepath.Join(m.Root, dir)
	if err := os.MkdirAll(fullDir, 0o750); err != nil {
		return "", fmt.Errorf("stage: creating dir %s: %w", fullDir, err)
	}
	f, err := os.CreateTemp(fullDir, namePattern)
	if err != nil {
		return "", fmt.Errorf("stage: allocating temp path: %w", err)
	}
	path := f.Name()
	f.Close()
	if err := os.Remove(path); err != nil {
		return "", fmt.Errorf("stage: freeing allocated temp path: %w", err)
	}
	return path, nil
}

// PartPath returns the deterministic scratch path for a given upload's part
// file.
func (m *Manager) PartPath(uploadID string, partNumber int) string {
	return filepath.Join(m.UploadDir(uploadID), fmt.Sprintf("part-%05d", partNumber))
}

// WritePart streams body directly to the deterministic path for
// (uploadID, partNumber), overwriting any prior attempt for the same part
// (S3 allows re-uploading a part on client retry).
func (m *Manager) WritePart(uploadID string, partNumber int, body io.Reader) (WriteResult, error) {
	dir := m.UploadDir(uploadID)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return WriteResult{}, fmt.Errorf("stage: creating dir %s: %w", dir, err)
	}
	path := m.PartPath(uploadID, partNumber)

	f, err := os.Create(path)
	if err != nil {
		return WriteResult{}, fmt.Errorf("stage: creating part file %s: %w", path, err)
	}
	defer f.Close()

	h := md5.New()
	n, err := io.Copy(io.MultiWriter(f, h), body)
	if err != nil {
		_ = os.Remove(path)
		return WriteResult{}, fmt.Errorf("stage: writing part: %w", err)
	}

	return WriteResult{Path: path, Size: n, MD5: hex.EncodeToString(h.Sum(nil))}, nil
}
