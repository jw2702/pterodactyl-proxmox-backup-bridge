package pbs

import (
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

// PBSError wraps a failed proxmox-backup-client invocation with enough
// context (command, exit code, stderr) to classify and log it.
type PBSError struct {
	Args     []string
	ExitCode int
	Stderr   string
	Err      error
}

func (e *PBSError) Error() string {
	return fmt.Sprintf("pbs: %s failed (exit %d): %s", strings.Join(e.Args, " "), e.ExitCode, strings.TrimSpace(e.Stderr))
}

func (e *PBSError) Unwrap() error { return e.Err }

func classifyError(args []string, runErr error, stderr string) error {
	pe := &PBSError{Args: args, Err: runErr, Stderr: stderr}
	var exitErr *exec.ExitError
	if errors.As(runErr, &exitErr) {
		pe.ExitCode = exitErr.ExitCode()
	} else {
		pe.ExitCode = -1
	}
	return pe
}

// The following classifiers use best-effort, case-insensitive substring
// matching against stderr, since proxmox-backup-client does not expose
// stable machine-readable error codes over its CLI. Verify substrings
// against the actually-deployed client version if PBS updates change
// wording; a false negative here just means the collision-retry loop / the
// notes-update failure-tolerance treats a real error as unclassified
// (surfaced normally) instead of specially, so misclassification fails safe.

func IsBackupTimeCollision(err error) bool {
	return stderrContainsAny(err, "backup time in the past", "already exists", "atime")
}

func IsNotFound(err error) bool {
	return stderrContainsAny(err, "no such", "not found", "does not exist")
}

func IsAlreadyExists(err error) bool {
	return stderrContainsAny(err, "already exists")
}

func stderrContainsAny(err error, substrs ...string) bool {
	var pe *PBSError
	if !errors.As(err, &pe) {
		return false
	}
	lower := strings.ToLower(pe.Stderr)
	for _, s := range substrs {
		if strings.Contains(lower, s) {
			return true
		}
	}
	return false
}
