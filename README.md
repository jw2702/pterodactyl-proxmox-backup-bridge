# pterodactyl-proxmox-backup-bridge

An S3-compatible HTTP bridge that lets [Pterodactyl Panel](https://pterodactyl.io/)
store server backups on a [Proxmox Backup Server](https://www.proxmox.com/en/products/proxmox-backup-server/overview)
instead of the Wing's local disk or a real S3 bucket.

Pterodactyl's built-in S3 backup driver talks to the bridge exactly as it
would talk to real S3 (including AWS Signature V4-signed requests and
presigned upload/download URLs). The bridge verifies those requests, buffers
the bytes to local scratch disk, and shells out to `proxmox-backup-client` to
store/retrieve/delete the corresponding snapshot on PBS.

```
Panel/Wings (S3 SDK, presigned URLs)
        │  HTTP + SigV4
        ▼
   pterodactyl-proxmox-backup-bridge
        │  exec: proxmox-backup-client
        ▼
   Proxmox Backup Server
```

## How Pterodactyl's S3 flow maps onto this bridge

- **Create backup**: Panel initiates a multipart upload; Wings uploads the
  backup archive in parts directly to presigned PUT URLs; Panel completes
  the upload. The bridge concatenates the parts on `CompleteMultipartUpload`
  and runs a single `proxmox-backup-client backup` for the whole archive.
- **Restore**: Panel gives Wings a presigned GET URL; the bridge streams
  `proxmox-backup-client restore ... -` directly into the HTTP response with
  no local scratch file (important for time-to-first-byte — see
  [docs/LIMITATIONS.md](docs/LIMITATIONS.md)). Range requests are the
  exception and still restore-then-slice via a scratch file.
- **Delete**: Panel calls `DeleteObject` directly; the bridge runs
  `proxmox-backup-client forget`.

Each S3 bucket maps to one PBS namespace; each object key maps to one PBS
backup group (`<backup-type>/<sanitized-key>`), with the original
`bucket/key` also stored in the snapshot's notes as a reconciliation aid.
See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full design.

## Configuring Pterodactyl Panel

In Panel's `.env` (or the S3 backup disk settings in the admin UI), point
the S3 client at the bridge:

```
BACKUP_DRIVER=s3
AWS_DEFAULT_REGION=us-east-1        # must match BRIDGE_REGION below
AWS_ACCESS_KEY_ID=<BRIDGE_ACCESS_KEY>
AWS_SECRET_ACCESS_KEY=<BRIDGE_SECRET_KEY>
AWS_BUCKET=pterodactyl-backups
AWS_ENDPOINT=http://pbs-bridge:8080
AWS_USE_PATH_STYLE_ENDPOINT=true    # required — the bridge only implements path-style routing
```

`AWS_USE_PATH_STYLE_ENDPOINT=true` is required: the bridge routes purely by
`/{bucket}/{key}` path and does not implement virtual-hosted-style
(`{bucket}.host`) addressing.

## Running

### Docker (recommended)

```sh
cp .env.example .env   # fill in BRIDGE_*, PBS_* values
docker compose -f docker-compose.example.yml up -d --build
```

### Locally

```sh
go build -o bin/bridge ./cmd/bridge
export $(cat .env | xargs)   # or set the vars another way
./bin/bridge
```

Requires `proxmox-backup-client` on `$PATH` (see the [PBS client repository
setup](https://pbs.proxmox.com/docs/installation.html) for non-Docker
hosts).

## Configuration

See [.env.example](.env.example) for the full list of environment variables.
The bridge fails fast at startup with a clear error if a required variable
(`BRIDGE_ACCESS_KEY`, `BRIDGE_SECRET_KEY`, `PBS_REPOSITORY`, and one of
`PBS_PASSWORD`/`PBS_API_TOKEN`) is missing.

## Development

```sh
go build ./...
go vet ./...
go test ./...
```

Tests do not require a real PBS server: `internal/pbs` and the end-to-end
test in `test/e2e` run against `scripts/stub-proxmox-backup-client`, a fake
CLI that mimics the real client's argument shape and snapshot semantics.
`internal/sigv4` includes a test verified against AWS's own published SigV4
example vector. See [docs/LIMITATIONS.md](docs/LIMITATIONS.md) for what this
test suite does and does not prove, and what to check against a real PBS
instance before going to production.

## Repository layout

```
cmd/bridge/           entrypoint: wiring + HTTP server
internal/sigv4/        AWS SigV4 verification (header + presigned + chunked)
internal/s3api/         S3 HTTP router, handlers, XML request/response shapes
internal/backend/        production Backend: combines store+stage+pbs+idmap
internal/store/           bbolt metadata DB (bucket/key -> PBS coordinates)
internal/stage/            multipart part staging + garbage collection
internal/pbs/                proxmox-backup-client exec wrapper
internal/idmap/                S3 key/bucket -> PBS backup-id/namespace
internal/testsign/              independent SigV4 signer, used only by tests
scripts/stub-proxmox-backup-client   fake PBS client for local testing
test/e2e/                            full-stack HTTP lifecycle test
```
