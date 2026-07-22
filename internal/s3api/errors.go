package s3api

import (
	"encoding/xml"
	"log/slog"
	"net/http"

	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/logging"
	"github.com/pterodactyl-proxmox-backup-bridge/bridge/internal/sigv4"
)

// s3Error is the standard S3 XML error body shape.
type s3Error struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId,omitempty"`
}

// writeError writes a standard S3-style XML error response.
func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("x-amz-request-id", requestID(r))
	w.WriteHeader(status)
	_ = xml.NewEncoder(w).Encode(s3Error{
		Code:      code,
		Message:   message,
		Resource:  r.URL.Path,
		RequestID: requestID(r),
	})
}

// statusForCode maps an S3 error code to its conventional HTTP status.
func statusForCode(code string) int {
	switch code {
	case "NoSuchBucket", "NoSuchKey", "NoSuchUpload":
		return http.StatusNotFound
	case "SignatureDoesNotMatch", "AccessDenied", "InvalidAccessKeyId", "RequestTimeTooSkewed":
		return http.StatusForbidden
	case "InvalidArgument", "InvalidPart", "InvalidPartOrder", "EntityTooSmall", "MalformedXML", "BadDigest":
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}

func writeErrorCode(w http.ResponseWriter, r *http.Request, code, message string) {
	writeError(w, r, statusForCode(code), code, message)
}

// writeAuthError maps a sigv4.AuthError onto the equivalent S3 XML error.
func writeAuthError(w http.ResponseWriter, r *http.Request, err *sigv4.AuthError) {
	writeErrorCode(w, r, string(err.Code), err.Message)
}

func writeInternalError(w http.ResponseWriter, r *http.Request, err error) {
	// Full error detail (PBS stderr, internal paths, etc.) is logged
	// server-side only, keyed by request ID. The client only ever sees a
	// generic message plus that request ID for correlation — some GetObject
	// requests are reachable via presigned URLs handed to end users (direct
	// backup downloads, not just Wings/Panel), so internal details must not
	// leak into the response body. Debugging now requires the bridge's own
	// logs rather than Panel's Laravel log.
	reqID := requestID(r)
	slog.Default().Error("internal error handling S3 request",
		"method", r.Method, "path", r.URL.Path, "request_id", reqID, "error", err)
	writeErrorCode(w, r, "InternalError", "an internal error occurred; see bridge server logs for request_id "+reqID)
}

func requestID(r *http.Request) string {
	return logging.RequestIDFromContext(r.Context())
}
