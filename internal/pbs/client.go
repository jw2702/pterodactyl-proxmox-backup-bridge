// Package pbs wraps the proxmox-backup-client CLI: the bridge shells out to
// it for every backup/restore/forget operation rather than reimplementing
// the PBS wire protocol. CLI flag names/syntax verified against the
// proxmox-backup source (proxmox-backup-client/src/main.rs,
// pbs-client/src/backup_specification.rs) as of this writing:
//   - backup:  proxmox-backup-client backup <name>.img:<path> --backup-type T --backup-id ID --backup-time UNIX --ns NS
//   - restore: proxmox-backup-client restore T/ID/RFC3339TIME <name>.img <target> --ns NS
//   - forget:  proxmox-backup-client forget T/ID/RFC3339TIME --ns NS   (alias for "snapshot forget")
//   - notes:   proxmox-backup-client snapshot notes update T/ID/RFC3339TIME "<notes>" --ns NS
//
// The bridge does NOT create PBS namespaces itself (there is no
// "namespace create" call anywhere in this package): doing so requires
// Datastore.Modify, which is intentionally not granted to the bridge's PBS
// user/token. Every namespace referenced by a bucket must be created
// up front by an administrator (see README.md). With namespace creation
// out of the picture, the bridge only ever needs Datastore.Backup (create/
// restore/notes-update on groups it owns) and Datastore.Prune (forget its
// own groups) — the built-in "DatastorePowerUser" PBS role grants exactly
// that, with no Datastore.Modify at all.
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
	"io"
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

// env builds the child process environment: the current process environment
// (tests rely on this to pass STUB_* vars through to
// scripts/stub-proxmox-backup-client) with the PBS auth vars layered on top.
func (c *Client) env() []string {
	env := append([]string(nil), os.Environ()...)
	env = append(env, envIfSet("PBS_REPOSITORY", c.Repository)...)
	env = append(env, envIfSet("PBS_PASSWORD", c.Password)...)
	env = append(env, envIfSet("PBS_FINGERPRINT", c.Fingerprint)...)
	return env
}

func (c *Client) run(ctx context.Context, args ...string) (stdout, stderr string, err error) {
	if c.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.Timeout)
		defer cancel()
	}

	cmd := exec.CommandContext(ctx, c.binPath(), args...)
	cmd.Env = c.env()

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

// RestoreStream runs `restore <snapshot> data.img -`, piping the archive
// directly to the returned io.ReadCloser instead of writing it to a local
// file first. This matters beyond just disk usage: Wings' own restore
// handler blocks on receiving HTTP response headers from this GET before it
// responds to Panel at all (it only backgrounds the actual file restore
// afterwards), so any delay here before the bridge can start writing a
// response directly inflates the Panel-visible request time and can trip
// Panel's own HTTP client timeout. The caller must read the stream to EOF
// (or cancel ctx) and always call Close(), which waits for the subprocess
// and surfaces any error it reported.
//
// Not used for ranged reads: an arbitrary byte range can't be sliced out of
// a live pipe, so callers wanting a specific range should use Restore (to a
// local file) instead and slice that.
func (c *Client) RestoreStream(ctx context.Context, backupType, backupID string, backupTime time.Time, namespace string) (io.ReadCloser, error) {
	args := []string{"restore", snapshotID(backupType, backupID, backupTime), ArchiveName, "-", "--crypt-mode", "none"}
	if namespace != "" {
		args = append(args, "--ns", namespace)
	}

	cmd := exec.CommandContext(ctx, c.binPath(), args...)
	cmd.Env = c.env()

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("pbs: creating stdout pipe: %w", err)
	}
	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("pbs: starting restore: %w", err)
	}

	return &restoreStream{stdout: stdout, cmd: cmd, stderr: &stderrBuf, args: args}, nil
}

// restoreStream adapts a running `restore ... -` subprocess to io.ReadCloser.
type restoreStream struct {
	stdout io.ReadCloser
	cmd    *exec.Cmd
	stderr *bytes.Buffer
	args   []string
}

func (r *restoreStream) Read(p []byte) (int, error) {
	return r.stdout.Read(p)
}

// Close closes the stdout pipe (if not already fully drained) and waits for
// the subprocess to exit, returning a classified error if it failed. Callers
// that abandon the read (e.g. an HTTP client disconnects mid-download) will
// have the underlying process killed automatically since it's started with
// exec.CommandContext against the same context the caller's request derives
// from.
func (r *restoreStream) Close() error {
	_ = r.stdout.Close()
	if err := r.cmd.Wait(); err != nil {
		return classifyError(r.args, err, r.stderr.String())
	}
	return nil
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
