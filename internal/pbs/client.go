// Package pbs wraps the proxmox-backup-client CLI: the bridge shells out to
// it for every backup/restore/forget operation rather than reimplementing
// the PBS wire protocol. CLI flag names/syntax verified against the
// proxmox-backup source (proxmox-backup-client/src/main.rs,
// pbs-client/src/backup_specification.rs) as of this writing:
//   - backup:  proxmox-backup-client backup <name>.img:<path> --backup-type T --backup-id ID --backup-time UNIX --ns NS
//   - restore: proxmox-backup-client restore T/ID/RFC3339TIME <name>.img <target> --ns NS
//   - forget:  proxmox-backup-client forget T/ID/RFC3339TIME --ns NS   (alias for "snapshot forget")
//   - notes:   proxmox-backup-client snapshot notes update T/ID/RFC3339TIME "<notes>" --ns NS
//   - ns:      proxmox-backup-client namespace create --ns NS
//
// The ".img" archive type is used (rather than ".pxar" or ".blob", which
// doesn't exist) because the client uploads it via plain
// tokio::fs::File::open + chunked dedup with no block-device requirement,
// making it the correct choice for an opaque binary blob (the backup
// archive) of arbitrary size.
package pbs

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"time"
)

// ArchiveName is the fixed archive name the bridge stores every S3 object's
// bytes under within a snapshot.
const ArchiveName = "data.img"

// Client shells out to proxmox-backup-client for a single configured
// repository/credential set.
type Client struct {
	Repository  string
	Password    string // user password or API token secret
	Fingerprint string
	BinPath     string // defaults to "proxmox-backup-client"
	Timeout     time.Duration
}

func (c *Client) binPath() string {
	if c.BinPath != "" {
		return c.BinPath
	}
	return "proxmox-backup-client"
}

// snapshotID formats the "<type>/<id>/<RFC3339-time>" identifier
// proxmox-backup-client uses to address an existing snapshot.
func snapshotID(backupType, backupID string, backupTime time.Time) string {
	return fmt.Sprintf("%s/%s/%s", backupType, backupID, backupTime.UTC().Format("2006-01-02T15:04:05Z"))
}

func (c *Client) run(ctx context.Context, args ...string) (stdout, stderr string, err error) {
	if c.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.Timeout)
		defer cancel()
	}

	cmd := exec.CommandContext(ctx, c.binPath(), args...)
	// Start from the process environment (tests rely on this to pass
	// STUB_* vars through to scripts/stub-proxmox-backup-client), then layer
	// the PBS auth vars on top.
	cmd.Env = append(cmd.Env, os.Environ()...)
	cmd.Env = append(cmd.Env, envIfSet("PBS_REPOSITORY", c.Repository)...)
	cmd.Env = append(cmd.Env, envIfSet("PBS_PASSWORD", c.Password)...)
	cmd.Env = append(cmd.Env, envIfSet("PBS_FINGERPRINT", c.Fingerprint)...)

	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	runErr := cmd.Run()
	stdout, stderr = outBuf.String(), errBuf.String()
	if runErr != nil {
		return stdout, stderr, classifyError(args, runErr, stderr)
	}
	return stdout, stderr, nil
}

func envIfSet(key, value string) []string {
	if value == "" {
		return nil
	}
	return []string{key + "=" + value}
}

// Backup uploads filePath as the snapshot's sole archive. On a
// backup-time collision (PBS requires strictly ascending, unique times per
// group), it retries with an incremented timestamp up to maxCollisionRetries
// times.
func (c *Client) Backup(ctx context.Context, filePath, backupType, backupID string, backupTime time.Time, namespace string) (usedTime time.Time, err error) {
	const maxCollisionRetries = 5
	t := backupTime
	for attempt := 0; attempt <= maxCollisionRetries; attempt++ {
		args := []string{
			"backup",
			ArchiveName + ":" + filePath,
			"--backup-type", backupType,
			"--backup-id", backupID,
			"--backup-time", strconv.FormatInt(t.Unix(), 10),
			// proxmox-backup-client defaults --crypt-mode to "encrypt", which
			// requires a keyfile the bridge does not manage; encryption is a
			// documented future enhancement (see docs/LIMITATIONS.md), so
			// explicitly disable it rather than depend on the client default.
			"--crypt-mode", "none",
		}
		if namespace != "" {
			args = append(args, "--ns", namespace)
		}
		_, _, err = c.run(ctx, args...)
		if err == nil {
			return t, nil
		}
		if IsBackupTimeCollision(err) {
			t = t.Add(time.Second)
			continue
		}
		return time.Time{}, err
	}
	return time.Time{}, fmt.Errorf("pbs: exhausted %d retries resolving backup-time collisions: %w", maxCollisionRetries, err)
}

// Restore downloads the snapshot's archive to outFile.
func (c *Client) Restore(ctx context.Context, backupType, backupID string, backupTime time.Time, namespace, outFile string) error {
	args := []string{"restore", snapshotID(backupType, backupID, backupTime), ArchiveName, outFile, "--crypt-mode", "none"}
	if namespace != "" {
		args = append(args, "--ns", namespace)
	}
	_, _, err := c.run(ctx, args...)
	return err
}

// Forget removes a snapshot. A "snapshot not found" error is treated as
// success (idempotent), since the bridge's overwrite/delete flows may call
// Forget on a snapshot that's already gone.
func (c *Client) Forget(ctx context.Context, backupType, backupID string, backupTime time.Time, namespace string) error {
	args := []string{"forget", snapshotID(backupType, backupID, backupTime)}
	if namespace != "" {
		args = append(args, "--ns", namespace)
	}
	_, _, err := c.run(ctx, args...)
	if err != nil && IsNotFound(err) {
		return nil
	}
	return err
}

// UpdateNotes stores notes (typically the original S3 bucket/key, as a
// reconciliation aid if the bbolt metadata DB is ever lost) on a snapshot.
// Failures are non-fatal to callers by design (see errors.go), since this is
// a defense-in-depth aid, not the authoritative record.
func (c *Client) UpdateNotes(ctx context.Context, backupType, backupID string, backupTime time.Time, namespace, notes string) error {
	args := []string{"snapshot", "notes", "update", snapshotID(backupType, backupID, backupTime), notes}
	if namespace != "" {
		args = append(args, "--ns", namespace)
	}
	_, _, err := c.run(ctx, args...)
	return err
}

// EnsureNamespace creates namespace if it doesn't already exist. Idempotent.
func (c *Client) EnsureNamespace(ctx context.Context, namespace string) error {
	if namespace == "" {
		return nil
	}
	args := []string{"namespace", "create", "--ns", namespace}
	_, _, err := c.run(ctx, args...)
	if err != nil && IsAlreadyExists(err) {
		return nil
	}
	return err
}
