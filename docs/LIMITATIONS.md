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

A full (non-Range) `GetObject` streams directly from
`proxmox-backup-client restore ... -` (stdout) straight into the HTTP
response, with no local scratch file involved. This isn't just a disk-usage
optimization: Wings' own restore handler blocks on receiving HTTP response
headers from this request before it responds to Panel at all (the actual
file restore only happens in the background afterwards), so any delay
before the bridge can start writing a response directly inflates the
Panel-visible request time and can trip Panel's own ~15s HTTP client
timeout on large backups.

Range requests are the exception: PBS's `restore` always produces the full
archive file with no server-side partial-object fetch, and a live
subprocess pipe can't be seeked. So a Range request still restores the full
snapshot to a scratch file first and serves the requested byte range from
it ("restore-then-slice"), same as before. This is correct but not
bandwidth-efficient for large backups if a client requests a small range,
and — since it's the same synchronous-before-headers pattern — could in
principle hit the same Wings/Panel timeout for very large backups. In
practice Wings' own S3 restore path (see above) always issues a full GET,
never a Range request, so this only matters if some other client requests a
specific range.

## Path-style addressing only

The bridge routes purely by `/{bucket}/{key}` request path. It does not
implement virtual-hosted-style addressing (`{bucket}.host/{key}`). Pterodactyl
Panel's S3 disk config must have path-style addressing enabled
(`AWS_USE_PATH_STYLE_ENDPOINT=true`).

## No transport encryption (bridge itself doesn't do TLS)

The bridge speaks plain HTTP; SigV4 authenticates and integrity-protects
requests but does not encrypt them. See the "Network / transport security"
section in README.md for the recommended mitigation (private network, or a
TLS-terminating reverse proxy in front of the bridge).

## Internal error details are not returned to clients

`InternalError` (HTTP 500) responses only ever contain a generic message
plus the request ID — the actual error detail (PBS stderr, internal file
paths, etc.) is logged server-side only (`docker logs`/the bridge's own
structured logs), keyed by that same request ID. This is deliberate: some
`GetObject` requests are reachable via presigned URLs handed to end users
for direct backup downloads (not just Wings/Panel), so internal details
must not leak into a response body an end user could see. The practical
consequence: troubleshooting a `500` now requires the bridge's own logs,
not just Panel's Laravel log — grep the bridge's logs for the request ID
shown in Panel's error message/log entry.

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

## Operations confirmed against Pterodactyl's actual source, vs. implemented for general S3 completeness

Verified directly against the Panel and Wings source (not just assumed):
Panel calls `CreateMultipartUpload`, `UploadPart` URLs (presigned, consumed
by Wings), `AbortMultipartUpload` (on a failed backup report),
`CompleteMultipartUpload`, `ListParts` (Panel's own fallback if Wings
reports a completed backup without its own parts list — `parts` is
`nullable` in Panel's request validation), `DeleteObject`, and a presigned
`GetObject` (used by both Wings for restore and, separately, for direct
end-user backup downloads). All of these are implemented and covered by
tests mirroring Panel/Wings' exact request shapes.

Pterodactyl **never** calls single-shot `PutObject` (every backup, even a
1-byte one, goes through `CreateMultipartUpload` with at least one part),
`HeadObject`, or `ListObjectsV2` — these three are implemented for general
S3-compatibility/debugging convenience (e.g. testing with `aws-cli`) rather
than because Pterodactyl needs them. They're simple, low-risk, and
independently tested, so there's little reason to remove them, but treat
them as "nice to have" rather than load-bearing, and they're the first
place to look if you ever want to trim the surface further.

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
