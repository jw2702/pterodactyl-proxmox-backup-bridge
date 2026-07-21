package sigv4

import "fmt"

// ErrorCode is a stable identifier callers can map onto S3-style XML error
// codes (see internal/s3api/errors.go) without depending on error string
// matching.
type ErrorCode string

const (
	ErrMissingAuth           ErrorCode = "AccessDenied"
	ErrSignatureDoesNotMatch ErrorCode = "SignatureDoesNotMatch"
	ErrRequestTimeTooSkewed  ErrorCode = "RequestTimeTooSkewed"
	ErrExpiredRequest        ErrorCode = "AccessDenied"
	ErrInvalidArgument       ErrorCode = "InvalidArgument"
	ErrInvalidAccessKey      ErrorCode = "InvalidAccessKeyId"
)

// AuthError is returned by Verify when a request fails SigV4 verification.
type AuthError struct {
	Code    ErrorCode
	Message string
}

func (e *AuthError) Error() string {
	return string(e.Code) + ": " + e.Message
}

func authErr(code ErrorCode, format string, args ...any) *AuthError {
	return &AuthError{Code: code, Message: fmt.Sprintf(format, args...)}
}
