# Architecture

## Request flow

`internal/s3api.Handler` is the top-level `http.Handler`. For every request
it:

1. Verifies AWS SigV4 (`internal/sigv4`), either the `Authorization` header
   form or the presigned query-string form, and transparently decodes
   `aws-chunked` bodies when present.
2. Dispatches on method + path + query parameters to one of the S3
   operation handlers (Put/Get/Head/DeleteObject, the four multipart
   operations, ListObjectsV2).
3. Delegates the actual storage operation to an `s3api.Backend`
   implementation.

In production that `Backend` is `internal/backend.Backend`, which combines:

- `internal/store` — an embedded bbolt database that is the **authoritative**
  mapping from `(bucket, key)` to PBS coordinates
  (`namespace/backup-type/backup-id/backup-time`), plus bookkeeping for
  in-progress multipart uploads (which parts have been uploaded, and where
  their bytes are staged on local disk).
- `internal/stage` — buffers PUT/UploadPart bodies to scratch disk (PBS's
  client needs a real file, not an arbitrary stream) and concatenates
  multipart parts in order before handing the result to PBS. Also runs a
  background sweep that removes abandoned multipart uploads and reconciles
  orphaned scratch directories left behind by a crash.
- `internal/pbs` — a thin wrapper around `exec.Command("proxmox-backup-client", ...)`.
  No PBS wire protocol is reimplemented; every operation is a real CLI
  invocation, with PBS auth passed through via the environment variables the
  client itself already understands (`PBS_REPOSITORY`, `PBS_PASSWORD`,
  `PBS_FINGERPRINT`).
- `internal/idmap` — turns an S3 key into a valid, collision-resistant PBS
  `backup-id`, and a bucket name into a PBS namespace.

## Object <-> snapshot mapping

- One S3 **bucket** -> one PBS **namespace** (dots in the bucket name are
  replaced with dashes). The bridge does **not** create namespaces itself —
  that requires `Datastore.Modify`, which its PBS user/token intentionally
  does not have (see README.md). An administrator must create each bucket's
  namespace up front.
- All backups belonging to the same server share one PBS **backup group**
  (`backup-type/backup-id`, with `backup-id` derived from the server-UUID
  path segment of the key via `idmap.GroupIDFromKey`), with each individual
  backup becoming a new **snapshot** within that group — matching normal PBS
  usage (one group per "thing being backed up", a growing history of
  snapshots within it) rather than a new single-snapshot group per backup.
- Every object's bytes are stored as a single `data.img` archive inside the
  snapshot (`.img` is used — not `.blob`, which doesn't exist as an archive
  type in `proxmox-backup-client` — because the client uploads it via a
  plain file read with chunked dedup, with no block-device requirement,
  making it the correct choice for an opaque binary blob of arbitrary size).
- The original `bucket/key` is also written to the snapshot's notes
  (`proxmox-backup-client snapshot notes update`) as a best-effort
  reconciliation aid — see [LIMITATIONS.md](LIMITATIONS.md).

## Overwrite semantics

A `PutObject`/`CompleteMultipartUpload` on a key that already has a mapping
runs in this order, serialized per `(bucket, key)` via an in-process keyed
mutex (`internal/store.KeyedMutex`):

1. Create the **new** PBS snapshot.
2. Update the bbolt mapping to point at the new snapshot.
3. Forget the **old** snapshot (best-effort; a failure here is logged, not
   fatal — it just leaks an old snapshot rather than ever leaving the key
   pointing at nothing).

This guarantees there's never a window where the key has no valid backing
snapshot. The mapping write (step 2) deliberately happens *before* the old
snapshot is forgotten (step 3), not after: if it happened after and the
mapping write then failed (bbolt error, crash, disk full), the key would be
left pointing at a snapshot that had just been deleted, rather than merely
leaking the new snapshot the way a failure in step 3 does.

The `(bucket, key)` lock only prevents two operations on the *same* key from
racing. Two different backups for the *same server* (same PBS backup group,
different keys — a different backup UUID each) aren't covered by it, but
step 1's `pbs.Client.Backup` call is additionally serialized per group via
`groupLockKey`, independent of the per-key lock: without that, two
concurrent backups for one server could each hit a backup-time collision
against the other and, since both run their own collision-retry loop (see
`pbs.Client.Backup`), in the worst case repeatedly bump into each other and
both exhaust their retries. Serializing the actual PBS call per group means
at most one is ever in flight for a given server, so a collision can only
ever be against already-committed history.

## GetObject: streaming vs. restore-then-slice

`s3api.Backend.GetObject` takes an optional `*RangeSpec`. When nil (the
common case — Wings' own S3 restore path never requests a range), the
production backend calls `pbs.Client.RestoreStream`, which runs
`proxmox-backup-client restore ... -` and pipes stdout directly into the
HTTP response; no local scratch file is involved. This matters beyond disk
usage: Wings' restore handler blocks on receiving HTTP response *headers*
from this request before it responds to Panel at all (the real file restore
only happens in a background goroutine afterwards), so any delay before the
bridge can start writing a response directly inflates the time Panel waits
— and can trip Panel's own client-side HTTP timeout on larger backups if the
bridge instead buffered the whole restore to disk first before responding.

When a `*RangeSpec` is present, a live subprocess pipe can't be seeked, so
the backend falls back to restoring the full snapshot to a scratch file via
`pbs.Client.Restore` and slicing the requested byte range from that file
(the original "restore-then-slice" approach), cleaning up the scratch file
on `Close`.

## Multipart upload lifecycle

`CreateMultipartUpload` allocates an upload ID and a bbolt record.
`UploadPart` streams each part to a deterministic scratch path
(`<scratch>/multipart/<uploadId>/part-NNNNN`) and records its ETag/size.
`CompleteMultipartUpload` validates the client's claimed part list against
what was actually stored (order, existence, ETag match), concatenates the
parts into one file, and runs the same commit path as a single-shot
`PutObject`. `ListParts` reads the same stored part records back out
(Panel calls this itself, instead of supplying its own part list, whenever
Wings reports a completed backup without one). `AbortMultipartUpload` and
the background GC both just remove the scratch directory and bbolt record.

`CreateMultipartUpload`, `UploadPart`, `AbortMultipartUpload` and
`CompleteMultipartUpload` all hold the same `(bucket, key)` lock as
`DeleteObject`/`commitObject`, for their whole body — not just their bbolt
writes. Panel's "delete backup" action on a still-in-progress backup issues
`DeleteObject` directly (see `abortMatchingUploads`), and without this lock
that could race a concurrent `UploadPart`: a part's disk write landing in a
scratch directory that `AbortMultipartUpload`/`abortMatchingUploads` is
concurrently `os.RemoveAll`-ing, or a `PutPart` bbolt write registering a
part against an upload record that a concurrent abort/delete just removed.
Locking the entire body (not just the metadata calls) is what actually
closes the window — a lock held only around `GetUpload`/`PutPart` would
still race the disk write in between. `CompleteMultipartUpload` holds the
lock across `ConcatParts` too, then calls `commitObjectLocked` (not
`commitObject`) for its final commit step, since `KeyedMutex.Lock` isn't
reentrant and a second `Lock` call for the same key from the same call stack
would deadlock.

## Retrying transient PBS failures

`internal/pbs.Client` retries a CLI invocation itself (short exponential
backoff, `pbs.MaxTransientRetries` attempts total) when the failure looks
like a temporary connectivity problem (`pbs.IsTransient` — connection
refused/reset, timeouts, TLS handshake failures, etc.), as opposed to a
permanent one (bad auth, missing namespace, malformed arguments). This is
separate from two other retry-shaped things already in this codebase:

- `Backup`'s own backup-time-collision loop, where each iteration is a
  legitimate distinct attempt with a bumped timestamp, not a blind retry of
  an identical call.
- `CompleteMultipartUpload`'s reliance on Panel's own AWS SDK client
  automatically retrying a failed call with the same upload ID — the backend
  just has to make that retry succeed against already-staged data (see
  "Multipart upload lifecycle" above). That safety net only covers the one
  call Panel is known to retry; `Backup`, `Restore`/`RestoreStream` and
  `Forget` have no such upstream retry to lean on, hence the retry living in
  `pbs.Client` itself instead.

`Backup`, `Restore` and `Forget` retry safely because repeating an identical
invocation after a failed attempt has no harmful side effect (the source
file is untouched until `Backup` succeeds; `Restore` truncates its output
file on every invocation; `Forget` is already idempotent). `RestoreStream` is
the delicate case: `s3api.handleGetObject` writes HTTP response headers as
soon as `RestoreStream` returns, before reading anything, so a retry is only
possible up to that point. `Client.startRestoreStream` peeks the subprocess's
first chunk of output before returning — if the process fails before
producing any bytes (the common shape for a connection-level failure), the
peek sees the failure via `cmd.Wait()` instead of a partially-consumed
stream, and that's still safe to retry; once a byte has been read, the
stream is handed back immediately and Close() surfaces any later failure
as-is (no more retries at that point).

`Client.Timeout`, if set, bounds each of these calls as a single shared
budget — every attempt plus every backoff wait between them, combined —
rather than being handed out fresh to each attempt (which would let a
persistently-failing call run up to `MaxTransientRetries × Timeout` instead
of `Timeout`). For `Backup`/`Restore`/`Forget`/`UpdateNotes` this is
straightforward: `runWithRetry` wraps the whole retry loop in one
`context.WithTimeout`. `RestoreStream` needs a split instead: the budget may
only bound the pre-first-byte phase (`startCtx` in `startRestoreStream`), not
the subprocess itself — once a stream has been successfully started and
handed back to the caller, it's read from `ctx` (the real, uncapped request
context) for as long as the HTTP response takes, same as before this
retry logic existed. Binding the returned stream to the retry budget would
mean a large restore gets killed mid-transfer as soon as `Timeout` elapses,
even though it was already succeeding.

Every retry log line also carries the originating request's ID
(`internal/logging.RequestIDFromContext`), the same one `internal/s3api`
attaches to the request context and returns to the client — `internal/pbs`
has no notion of an HTTP request of its own, so without this a retry logged
deep inside a PBS CLI invocation couldn't be tied back to the Panel/Wings
call that triggered it.

## Verified against the real PBS client

CLI flag names and archive-type syntax (`--backup-type`, `--backup-id`,
`--backup-time`, `--ns`, `--crypt-mode`, the `<type>/<id>/<RFC3339-time>`
snapshot address format, `snapshot notes update`) were
confirmed both against the proxmox-backup source and by running
`proxmox-backup-client <cmd> --help` inside the built Docker image against
the actually-installed client (3.4.7 at the time of writing). Notably,
`--crypt-mode` defaults to `encrypt` on both `backup` and `restore` — the
bridge explicitly passes `--crypt-mode none` on both, since it does not
manage encryption keys (see LIMITATIONS.md).
