package idmap

import (
	"regexp"
	"strings"
	"testing"
)

var validBackupID = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

func TestSanitizeBackupID_UUIDIsNearNoOp(t *testing.T) {
	key := "550e8400-e29b-41d4-a716-446655440000.tar.gz"
	got := SanitizeBackupID(key)
	want := "550e8400-e29b-41d4-a716-446655440000"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestSanitizeBackupID_ValidCharsetAlways(t *testing.T) {
	keys := []string{
		"550e8400-e29b-41d4-a716-446655440000.tar.gz",
		"nested/path/backup.tar.gz",
		"unicode-äöü-key.tar.gz",
		"",
		"a b c/d.zip",
		strings.Repeat("x", 200) + ".tar.gz",
	}
	for _, k := range keys {
		got := SanitizeBackupID(k)
		if !validBackupID.MatchString(got) {
			t.Errorf("SanitizeBackupID(%q) = %q, contains invalid characters", k, got)
		}
		if len(got) > maxBackupIDLen {
			t.Errorf("SanitizeBackupID(%q) = %q, exceeds max length %d", k, got, maxBackupIDLen)
		}
	}
}

func TestSanitizeBackupID_Deterministic(t *testing.T) {
	key := "some/nested/key-with-chars!@#.tar.gz"
	a := SanitizeBackupID(key)
	b := SanitizeBackupID(key)
	if a != b {
		t.Fatalf("not deterministic: %q != %q", a, b)
	}
}

func TestSanitizeBackupID_DifferentKeysDoNotCollide(t *testing.T) {
	// These sanitize to the same "clean" prefix (slashes -> underscores)
	// but are different original keys, so must get different hash suffixes.
	k1 := "backups/mykey"
	k2 := "backups_mykey"
	id1 := SanitizeBackupID(k1)
	id2 := SanitizeBackupID(k2)
	if id1 == id2 {
		t.Fatalf("expected different ids for different keys, both got %q", id1)
	}
}

func TestSanitizeBackupID_EmptyKey(t *testing.T) {
	got := SanitizeBackupID("")
	if !validBackupID.MatchString(got) || got == "" {
		t.Fatalf("SanitizeBackupID(\"\") = %q, want non-empty valid id", got)
	}
}

func TestSanitizeNamespace(t *testing.T) {
	if got := SanitizeNamespace("my.bucket.name"); got != "my-bucket-name" {
		t.Fatalf("got %q", got)
	}
	if got := SanitizeNamespace("plain-bucket"); got != "plain-bucket" {
		t.Fatalf("got %q", got)
	}
}
