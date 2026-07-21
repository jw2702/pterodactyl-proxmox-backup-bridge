# syntax=docker/dockerfile:1

# ---- builder ----
FROM golang:bookworm AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go test ./...
RUN CGO_ENABLED=0 go build -o /out/bridge ./cmd/bridge

# ---- runtime ----
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl gnupg \
    && install -d -m 0755 /usr/share/keyrings \
    && curl -fsSL https://enterprise.proxmox.com/debian/proxmox-archive-keyring-trixie.gpg \
        -o /usr/share/keyrings/proxmox-archive-keyring.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/proxmox-archive-keyring.gpg] http://download.proxmox.com/debian/pbs-client bookworm main" \
        > /etc/apt/sources.list.d/pbs-client.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends proxmox-backup-client \
    && apt-get purge -y gnupg curl \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/* /etc/apt/sources.list.d/pbs-client.list

COPY --from=builder /out/bridge /usr/local/bin/bridge

RUN useradd -u 1000 -m -d /var/lib/bridge bridge \
    && mkdir -p /var/lib/bridge/data /var/lib/bridge/scratch \
    && chown -R bridge:bridge /var/lib/bridge

USER bridge
ENV BRIDGE_DATA_DIR=/var/lib/bridge/data \
    BRIDGE_SCRATCH_DIR=/var/lib/bridge/scratch \
    BRIDGE_LISTEN_ADDR=:8080

VOLUME ["/var/lib/bridge/data", "/var/lib/bridge/scratch"]
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s CMD ["/usr/local/bin/bridge", "-healthcheck"]

ENTRYPOINT ["/usr/local/bin/bridge"]
