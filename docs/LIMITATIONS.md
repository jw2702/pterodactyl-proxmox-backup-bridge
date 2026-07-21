# Known limitations

## bbolt metadata DB is the single source of truth

`internal/store`'s bbolt database (`$BRIDGE_DATA_DIR/bridge.db`) is the
**authoritative** mapping from S3 `bucket/key` to PBS snapshot coordinates.
If this file is lost or corrupted, the bridge can no longer serve
`GetObject`/`HeadObject`/`DeleteObject` for existing backups even though the
underlying PBS snapshots still exist.

As a partial mitigation, every snapshot's original `bucket/key` is also
written to its PBS notes field (`proxmox-backup-client snapshot notes
update`), so a lost database could in principle be reconstructed manually by
walking `proxmox-backup-client snapshot list` output — but this is not
automated. **Back up `$BRIDGE_DATA_DIR/bridge.db` regularly** (it's small —
one record per backup).

## No encryption

The bridge always passes `--crypt-mode none` to `proxmox-backup-client`
(both for `backup` and `restore`), since it does not manage per-tenant
encryption keys. Backups are protected only by transport security (TLS, if
you put a reverse proxy in front of the bridge and/or the PBS connection)
and PBS server-side access control, not by end-to-end encryption. Adding key
management is a possible future enhancement, not implemented here.

## No true partial restore (Range requests)

PBS's `restore` always produces the full archive file; there is no
server-side partial-object fetch. The bridge implements HTTP Range requests
by restoring the full snapshot to a scratch file and then serving the
requested byte range from it ("restore-then-slice"). This is correct but not
bandwidth-efficient for large backups if a client requests a small range —
functionally equivalent to a real S3 Range GET from the client's
perspective, just not lazy on the backend.

## Path-style addressing only

The bridge routes purely by `/{bucket}/{key}` request path. It does not
implement virtual-hosted-style addressing (`{bucket}.host/{key}`). Pterodactyl
Panel's S3 disk config must have path-style addressing enabled
(`AWS_USE_PATH_STYLE_ENDPOINT=true`).

## Single static credential pair

The bridge authenticates all callers against one configured
access-key/secret-key pair (`BRIDGE_ACCESS_KEY`/`BRIDGE_SECRET_KEY`). There
is no per-tenant credential or multi-user support; anyone with that one
credential pair can read/write/delete any bucket/key the bridge knows about.
This matches Pterodactyl's own usage pattern (Panel holds one S3 credential
for the whole instance) but is worth knowing if you're considering reusing
this bridge for something else.

## Synchronous, potentially long-running CompleteMultipartUpload / PutObject

Because a `proxmox-backup-client backup` invocation runs synchronously
inside the HTTP request handling `CompleteMultipartUpload` (or `PutObject`
for small backups), a very large backup can make that single HTTP call take
a long time. The bridge's own `http.Server` has no fixed read/write timeout
for this reason (see `cmd/bridge/main.go`), but Panel's own HTTP client
timeout for that call must also be able to tolerate this. This was a
deliberate choice to keep S3 semantics simple (the client gets a definitive
success/failure for the upload) rather than introducing an async
"upload accepted, still processing" state that S3 doesn't have.

## ListObjectsV2 is a secondary feature, not battle-tested against Panel

`ListObjectsV2` is implemented (delimiter/common-prefix grouping over the
bbolt mapping) but Pterodactyl Panel's actual usage of it (if any) wasn't
confirmed against this bridge's implementation specifically — it exists for
completeness/robustness and passes its own unit tests, but treat it as lower
confidence than Put/Get/Head/Delete/Multipart, which mirror Panel/Wings'
confirmed usage patterns.

## Testing without a real PBS server

This repository's test suite (`internal/pbs`, `internal/backend`,
`test/e2e`) runs against `scripts/stub-proxmox-backup-client`, a fake CLI
that mimics the real client's argument shapes and snapshot semantics, not a
real PBS server. CLI flag names and syntax were additionally cross-checked
against the real `proxmox-backup-client --help` output inside the built
Docker image (version 3.4.7), which caught two real bugs during development
(`--crypt-mode` defaulting to `encrypt` on both `backup` and `restore`).
That said, this test suite cannot exercise real PBS-server-side behavior
(actual chunk dedup, garbage collection interactions, real auth failures,
network conditions). **Run a validation pass against a real (even small
test) PBS instance before relying on this in production.**
