package sigv4

import (
	"fmt"
	"time"
)

// Clock is injectable for tests; defaults to time.Now.
type Clock func() time.Time

func defaultClock() time.Time { return time.Now().UTC() }

func parseAmzDate(s string) (time.Time, error) {
	t, err := time.Parse(amzDateLayout, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("sigv4: invalid x-amz-date %q: %w", s, err)
	}
	return t, nil
}

// checkSkew rejects timestamps too far from now in either direction.
func checkSkew(now, signedAt time.Time, tolerance time.Duration) error {
	diff := now.Sub(signedAt)
	if diff < 0 {
		diff = -diff
	}
	if diff > tolerance {
		return fmt.Errorf("sigv4: request time %s is outside the allowed clock skew of %s (server time %s)", signedAt.Format(amzDateLayout), tolerance, now.Format(amzDateLayout))
	}
	return nil
}

// checkExpiry rejects presigned URLs that have expired, or whose expiry
// window is unreasonably in the future relative to signedAt. tolerance
// accounts for minor clock skew between signer and verifier.
func checkExpiry(now, signedAt time.Time, expiresSeconds int, tolerance time.Duration) error {
	if expiresSeconds <= 0 {
		return fmt.Errorf("sigv4: invalid X-Amz-Expires %d", expiresSeconds)
	}
	expiry := signedAt.Add(time.Duration(expiresSeconds) * time.Second)
	if now.After(expiry.Add(tolerance)) {
		return fmt.Errorf("sigv4: presigned request expired at %s (server time %s)", expiry.Format(amzDateLayout), now.Format(amzDateLayout))
	}
	if signedAt.After(now.Add(tolerance)) {
		return fmt.Errorf("sigv4: presigned request signed in the future at %s (server time %s)", signedAt.Format(amzDateLayout), now.Format(amzDateLayout))
	}
	return nil
}
