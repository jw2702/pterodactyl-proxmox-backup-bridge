package s3api

import (
	"net/http"

	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/logging"
)

// withRequestID mints a request ID and stores it in the request's context
// via internal/logging, so packages further down the call chain that have
// no HTTP request of their own — like internal/pbs, when it logs a retry of
// a PBS CLI invocation — can still tag their log lines with it.
func withRequestID(r *http.Request) (*http.Request, string) {
	id := logging.NewRequestID()
	ctx := logging.WithRequestID(r.Context(), id)
	return r.WithContext(ctx), id
}
