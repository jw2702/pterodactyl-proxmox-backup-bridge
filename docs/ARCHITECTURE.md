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
  replaced with dashes; namespaces are created lazily via
  `proxmox-backup-client namespace create`).
- One S3 **key** -> one PBS **backup group** (`backup-type/backup-id`, with
  `backup-id` derived deterministically from the key via
  `idmap.SanitizeBackupID`).
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
2. Forget the **old** snapshot (best-effort; a failure here is logged, not
   fatal — it just leaks an old snapshot rather than ever leaving the key
   pointing at nothing).
3. Update the bbolt mapping to point at the new snapshot.

This guarantees there's never a window where the key has no valid backing
snapshot.

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
`PutObject`. `AbortMultipartUpload` and the background GC both just remove
the scratch directory and bbolt record.

## Verified against the real PBS client

CLI flag names and archive-type syntax (`--backup-type`, `--backup-id`,
`--backup-time`, `--ns`, `--crypt-mode`, the `<type>/<id>/<RFC3339-time>`
snapshot address format, `snapshot notes update`, `namespace create`) were
confirmed both against the proxmox-backup source and by running
`proxmox-backup-client <cmd> --help` inside the built Docker image against
the actually-installed client (3.4.7 at the time of writing). Notably,
`--crypt-mode` defaults to `encrypt` on both `backup` and `restore` — the
bridge explicitly passes `--crypt-mode none` on both, since it does not
manage encryption keys (see LIMITATIONS.md).
