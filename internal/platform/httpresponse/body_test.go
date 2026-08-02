package httpresponse

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestReadBoundsRemoteResponse(t *testing.T) {
	t.Parallel()

	t.Run("exact limit", func(t *testing.T) {
		t.Parallel()
		response := &http.Response{
			Body: io.NopCloser(strings.NewReader(strings.Repeat("a", int(MaxBodyBytes)))),
		}
		body, err := Read(response)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if int64(len(body)) != MaxBodyBytes {
			t.Fatalf("body length = %d, want %d", len(body), MaxBodyBytes)
		}
	})

	t.Run("oversized", func(t *testing.T) {
		t.Parallel()
		response := &http.Response{
			Body: io.NopCloser(strings.NewReader(strings.Repeat("b", int(MaxBodyBytes+1)))),
		}
		body, err := Read(response)
		if body != nil || !errors.Is(err, ErrBodyTooLarge) {
			t.Fatalf("body/error = %d/%v, want nil ErrBodyTooLarge", len(body), err)
		}
	})

	t.Run("read failure is redacted", func(t *testing.T) {
		t.Parallel()
		const marker = "REMOTE_SECRET_MUST_NOT_LEAK"
		response := &http.Response{Body: io.NopCloser(failingReader{err: errors.New(marker)})}
		body, err := Read(response)
		if body != nil || !errors.Is(err, ErrBodyUnreadable) {
			t.Fatalf("body/error = %d/%v, want nil ErrBodyUnreadable", len(body), err)
		}
		if strings.Contains(err.Error(), marker) {
			t.Fatalf("read error leaked remote content: %v", err)
		}
	})

	t.Run("nil response", func(t *testing.T) {
		t.Parallel()
		if _, err := Read(nil); !errors.Is(err, ErrBodyUnreadable) {
			t.Fatalf("Read(nil) = %v, want ErrBodyUnreadable", err)
		}
	})
}

type failingReader struct {
	err error
}

func (reader failingReader) Read([]byte) (int, error) {
	return 0, reader.err
}
