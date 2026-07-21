.PHONY: build test vet fmt run docker-build

build:
	go build -o bin/bridge ./cmd/bridge

test:
	go test ./...

vet:
	go vet ./...

fmt:
	gofmt -l .

run: build
	./bin/bridge

docker-build:
	docker build -t pterodactyl-proxmox-backup-bridge:latest .
