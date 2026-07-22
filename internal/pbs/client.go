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
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/logging"
)

// ArchiveName is the fixed archive name the bridge stores every S3 object's
// bytes under within a snapshot.
const ArchiveName = "data.img"

// MaxTransientRetries bounds how many times a single PBS CLI invocation is
// attempted in total when it keeps failing with a classified-transient error
// (see IsTransient) before the failure is surfaced to the caller. This is
// deliberately separate from Backup's own backup-time-collision loop (each
// iteration there is a legitimate distinct attempt with a bumped timestamp,
// not a blind retry of an identical call) and from the retry safety net
// CompleteMultipartUpload relies on Panel's AWS SDK client for (see
// backend.CompleteMultipartUpload) — this one exists for the operations
// nothing upstream is known to retry: Restore/RestoreStream (GetObject) and
// Forget (DeleteObject). Exported so tests can reason about exactly how many
// injected failures are needed to exhaust it.
const MaxTransientRetries = 3

const (
	transientRetryBaseDelay = 250 * time.Millisecond
	transientRetryMaxDelay  = 750 * time.Millisecond
)

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

// run does not apply c.Timeout itself — callers that want a bounded overall
// budget (across every retry attempt, not just this one call) apply it once,
// higher up: see runWithRetry.
func (c *Client) run(ctx context.Context, args ...string) (stdout, stderr string, err error) {
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

// runWithRetry calls run, retrying with a short backoff if the failure is
// classified as transient (see IsTransient), up to MaxTransientRetries total
// attempts. Only safe for callers where repeating an identical invocation
// after a failed attempt has no harmful side effect — true for Backup (the
// source file is untouched until a run succeeds), Forget (already
// idempotent) and UpdateNotes (best-effort, non-fatal on failure either
// way). RestoreStream needs the extra care in startRestoreStream instead,
// since it can't blindly repeat an invocation once bytes have started
// flowing to an HTTP caller.
//
// c.Timeout, if set, bounds the *entire* call — every attempt plus every
// backoff wait between them combined — rather than being applied fresh to
// each individual attempt. Three attempts each getting their own full
// c.Timeout budget would let a single logical operation run up to 3x longer
// than configured; a single shared budget keeps the configured timeout
// meaning what it says regardless of how many attempts it takes. This is
// safe here because run() is fully synchronous and returns nothing that
// outlives this function call — unlike RestoreStream, which hands back a
// still-running subprocess the caller keeps reading from long after this
// function returns, and therefore needs a different treatment (see
// RestoreStream and startRestoreStream).
func (c *Client) runWithRetry(ctx context.Context, args ...string) (stdout, stderr string, err error) {
	if c.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.Timeout)
		defer cancel()
	}

	delay := transientRetryBaseDelay
	for attempt := 1; ; attempt++ {
		stdout, stderr, err = c.run(ctx, args...)
		if err == nil || !IsTransient(err) || attempt >= MaxTransientRetries {
			return stdout, stderr, err
		}
		slog.Default().Warn("pbs: transient error, retrying", "request_id", logging.RequestIDFromContext(ctx), "args", args, "attempt", attempt, "error", err)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return stdout, stderr, err
		}
		delay *= 3
		if delay > transientRetryMaxDelay {
			delay = transientRetryMaxDelay
		}
	}
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
		_, _, err = c.runWithRetry(ctx, args...)
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

// Restore downloads the snapshot's archive to outFile. Safe to retry on a
// transient failure: proxmox-backup-client (re)creates outFile from scratch
// on every invocation, so a retry after a failed partial write still
// produces a correct file.
func (c *Client) Restore(ctx context.Context, backupType, backupID string, backupTime time.Time, namespace, outFile string) error {
	args := []string{"restore", snapshotID(backupType, backupID, backupTime), ArchiveName, outFile, "--crypt-mode", "none"}
	if namespace != "" {
		args = append(args, "--ns", namespace)
	}
	_, _, err := c.runWithRetry(ctx, args...)
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
//
// A transient failure is retried, but only up to the point where the first
// byte would be handed back to the caller: s3api.handleGetObject writes HTTP
// response headers as soon as RestoreStream returns (before any byte is
// actually read), so once we've returned, a retry is no longer possible.
// startRestoreStream below peeks the subprocess's output before returning
// for exactly this reason.
//
// c.Timeout bounds only that pre-first-byte phase — every retry attempt plus
// backoff, combined, the same shared-budget treatment runWithRetry gives
// Backup/Restore/Forget/UpdateNotes (see its comment for why a single shared
// budget matters). It deliberately does NOT bound the subprocess itself:
// once a stream is successfully started, the caller keeps reading from it
// for as long as the HTTP response takes, tied only to ctx (the real request
// context, cancelled on client disconnect) — not to an internal retry
// budget that has nothing to do with how long a large restore legitimately
// takes. startRestoreStream is where that split is implemented: ctx drives
// the subprocess's lifetime, startCtx only bounds how long a single attempt
// is allowed to take before producing its first byte (or failing).
func (c *Client) RestoreStream(ctx context.Context, backupType, backupID string, backupTime time.Time, namespace string) (io.ReadCloser, error) {
	args := []string{"restore", snapshotID(backupType, backupID, backupTime), ArchiveName, "-", "--crypt-mode", "none"}
	if namespace != "" {
		args = append(args, "--ns", namespace)
	}

	startCtx := ctx
	if c.Timeout > 0 {
		var cancel context.CancelFunc
		startCtx, cancel = context.WithTimeout(ctx, c.Timeout)
		defer cancel()
	}

	delay := transientRetryBaseDelay
	for attempt := 1; ; attempt++ {
		rs, err := c.startRestoreStream(ctx, startCtx, args)
		if err == nil {
			return rs, nil
		}
		if !IsTransient(err) || attempt >= MaxTransientRetries {
			return nil, err
		}
		slog.Default().Warn("pbs: transient error starting restore stream, retrying", "request_id", logging.RequestIDFromContext(ctx), "args", args, "attempt", attempt, "error", err)
		select {
		case <-time.After(delay):
		case <-startCtx.Done():
			return nil, err
		}
		delay *= 3
		if delay > transientRetryMaxDelay {
			delay = transientRetryMaxDelay
		}
	}
}

// startRestoreStream starts a single `restore ... -` subprocess and peeks at
// its output before returning. If the process fails before producing any
// output (the common shape for a connection-level failure against PBS), we
// still hold nothing the caller can see, so the error is safe to classify
// and retry. Once at least one byte has been read, the stream is handed back
// immediately and can no longer be retried transparently.
//
// The subprocess itself is tied to ctx (unbounded by c.Timeout, so a
// successful long-running restore is never killed by an internal retry
// budget). The peek read races against startCtx instead: if startCtx's
// budget elapses before any output (or failure) arrives, this one attempt's
// process is killed and an error returned for the caller to retry — a fresh
// attempt gets a fresh process, still governed by the same unbounded ctx.
func (c *Client) startRestoreStream(ctx, startCtx context.Context, args []string) (*restoreStream, error) {
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

	type peekResult struct {
		n   int
		err error
	}
	peek := make([]byte, 32*1024)
	resultCh := make(chan peekResult, 1)
	go func() {
		n, err := stdout.Read(peek)
		resultCh <- peekResult{n, err}
	}()

	var n int
	var readErr error
	select {
	case res := <-resultCh:
		n, readErr = res.n, res.err
	case <-startCtx.Done():
		// This attempt's budget is up before it produced anything. Kill
		// just this attempt's process and let the caller retry with a
		// fresh one — ctx (and therefore future attempts) is unaffected.
		// Closing stdout explicitly (rather than relying on Wait() to do it
		// internally) deterministically unblocks the still-pending Read in
		// the goroutine above.
		_ = cmd.Process.Kill()
		_ = stdout.Close()
		_ = cmd.Wait()
		return nil, fmt.Errorf("pbs: restore did not produce output within the retry budget: %w", startCtx.Err())
	}

	if n > 0 {
		return &restoreStream{
			stdout: io.MultiReader(bytes.NewReader(peek[:n]), stdout),
			closer: stdout,
			cmd:    cmd,
			stderr: &stderrBuf,
			args:   args,
		}, nil
	}

	if readErr != nil && readErr != io.EOF {
		_ = stdout.Close()
		_ = cmd.Wait()
		return nil, fmt.Errorf("pbs: reading restore stream: %w", readErr)
	}

	// No output was produced before the pipe closed. Wait() tells us
	// whether that's a legitimately empty archive (exit 0) or the process
	// failing outright — safe to call here since the pipe is already fully
	// drained (StdoutPipe's own doc requirement).
	if waitErr := cmd.Wait(); waitErr != nil {
		return nil, classifyError(args, waitErr, stderrBuf.String())
	}
	return &restoreStream{
		stdout: bytes.NewReader(nil),
		closer: stdout,
		cmd:    cmd,
		stderr: &stderrBuf,
		args:   args,
		waited: true,
	}, nil
}

// restoreStream adapts a running `restore ... -` subprocess to io.ReadCloser.
type restoreStream struct {
	stdout io.Reader // possibly prefixed with peeked bytes, see startRestoreStream
	closer io.Closer // the underlying stdout pipe
	cmd    *exec.Cmd
	stderr *bytes.Buffer
	args   []string
	waited bool // true if Wait() was already called (empty-output case)
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
	_ = r.closer.Close()
	if r.waited {
		return nil
	}
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
	_, _, err := c.runWithRetry(ctx, args...)
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
	_, _, err := c.runWithRetry(ctx, args...)
	return err
}
