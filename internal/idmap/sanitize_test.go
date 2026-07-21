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

func TestGroupIDFromKey_SameServerSharesGroup(t *testing.T) {
	serverUUID := "49327629-76a6-4077-be9c-978818e4654b"
	key1 := serverUUID + "/ba2bc3cf-9939-42f2-af86-fc7d72a76466.tar.gz"
	key2 := serverUUID + "/240e4129-afb5-4d24-9735-3df79ec53636.tar.gz"

	id1 := GroupIDFromKey(key1)
	id2 := GroupIDFromKey(key2)
	if id1 != id2 {
		t.Fatalf("expected same group id for two backups of the same server, got %q and %q", id1, id2)
	}
	if id1 != serverUUID {
		t.Fatalf("expected group id to be the server UUID, got %q", id1)
	}
}

func TestGroupIDFromKey_DifferentServersGetDifferentGroups(t *testing.T) {
	id1 := GroupIDFromKey("server-a/backup-1.tar.gz")
	id2 := GroupIDFromKey("server-b/backup-1.tar.gz")
	if id1 == id2 {
		t.Fatalf("expected different group ids for different servers, both got %q", id1)
	}
}

func TestGroupIDFromKey_NoPathSegmentFallsBackToWholeKey(t *testing.T) {
	got := GroupIDFromKey("no-slash-key.tar.gz")
	if !validBackupID.MatchString(got) || got == "" {
		t.Fatalf("expected a valid fallback id, got %q", got)
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
