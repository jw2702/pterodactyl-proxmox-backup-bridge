// Package idmap turns S3 object keys into valid, collision-resistant PBS
// backup-ids and bucket names into valid PBS namespace path components.
package idmap

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

const (
	maxBackupIDLen = 60
	hashSuffixLen  = 8
)

// SanitizeBackupID converts an arbitrary S3 key into a valid PBS backup-id
// ([A-Za-z0-9_-]+). It is deterministic: the same key always maps to the
// same id. Since the store's ObjectMapping (bucket+key -> PBS coordinates)
// is the authoritative lookup path, this function does not need to be
// invertible — it only needs to be valid, deterministic, reasonably
// human-readable, and collision-resistant across different inputs.
//
// Pterodactyl backup keys are UUIDs in practice (already a near no-op for
// this function), but the implementation must stay correct for arbitrary
// keys since Panel's exact key format isn't a stable guarantee.
func SanitizeBackupID(key string) string {
	sanitized := sanitizeChars(stripKnownExtensions(key))

	if sanitized == "" {
		sanitized = "key"
	}

	if isCleanLossless(key, sanitized) && len(sanitized) <= maxBackupIDLen {
		return sanitized
	}

	suffix := hashSuffix(key)
	maxPrefixLen := maxBackupIDLen - len(suffix) - 1
	if len(sanitized) > maxPrefixLen {
		sanitized = sanitized[:maxPrefixLen]
	}
	return sanitized + "-" + suffix
}

// GroupIDFromKey extracts the part of an S3 key that identifies the PBS
// backup group and sanitizes it into a valid backup-id, so that every
// backup belonging to the same server becomes a new snapshot within one
// shared group instead of each backup getting its own single-snapshot
// group. Pterodactyl backup keys are shaped
// "<server-uuid>/<backup-uuid>.tar.gz", so the group id is the first path
// segment (the server UUID); if the key has no path segment, the whole
// (sanitized) key is used as a fallback so behavior stays sane for
// unexpected key shapes.
func GroupIDFromKey(key string) string {
	segment := key
	if idx := strings.IndexByte(key, '/'); idx >= 0 {
		segment = key[:idx]
	}
	return SanitizeBackupID(segment)
}

// stripKnownExtensions removes common Pterodactyl backup archive suffixes so
// the resulting id reads cleanly (e.g. "<uuid>.tar.gz" -> "<uuid>").
func stripKnownExtensions(key string) string {
	for _, ext := range []string{".tar.gz", ".tgz", ".tar", ".gz", ".zip"} {
		if strings.HasSuffix(key, ext) {
			return strings.TrimSuffix(key, ext)
		}
	}
	return key
}

func sanitizeChars(s string) string {
	var buf strings.Builder
	buf.Grow(len(s))
	for _, r := range s {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-' || r == '_':
			buf.WriteRune(r)
		default:
			buf.WriteByte('_')
		}
	}
	return buf.String()
}

// isCleanLossless reports whether sanitizing didn't destroy information that
// could plausibly cause a collision with a different original key (i.e. the
// original key, minus a known extension, was already a valid backup-id).
func isCleanLossless(originalKey, sanitized string) bool {
	stripped := stripKnownExtensions(originalKey)
	return stripped == sanitized
}

func hashSuffix(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])[:hashSuffixLen]
}

// SanitizeNamespace converts an S3 bucket name into a valid PBS namespace
// path component. S3 bucket names are already restricted to lowercase
// alphanumerics, '.', and '-', so this is close to a no-op; '.' (valid in
// bucket names but not typically desired in a single namespace path
// component) is replaced with '-'.
func SanitizeNamespace(bucket string) string {
	return strings.ReplaceAll(bucket, ".", "-")
}
