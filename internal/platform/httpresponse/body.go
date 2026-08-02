// Package httpresponse contains provider-neutral safeguards for remote HTTP
// responses. It intentionally never preserves lower-level read errors because
// those errors can contain remote or transport-controlled sensitive data.
package httpresponse

import (
	"errors"
	"io"
	"net/http"
)

const MaxBodyBytes int64 = 1 << 20

var (
	ErrBodyTooLarge   = errors.New("remote response body exceeds limit")
	ErrBodyUnreadable = errors.New("remote response body is unreadable")
)

// Read consumes at most MaxBodyBytes plus one sentinel byte. Callers must close
// the response body. Exact-limit bodies are accepted; larger bodies are
// rejected without returning their attacker-controlled prefix.
func Read(response *http.Response) ([]byte, error) {
	if response == nil || response.Body == nil {
		return nil, ErrBodyUnreadable
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, MaxBodyBytes+1))
	if err != nil {
		return nil, ErrBodyUnreadable
	}
	if int64(len(body)) > MaxBodyBytes {
		return nil, ErrBodyTooLarge
	}
	return body, nil
}
