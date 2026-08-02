package telegram

import (
	"errors"
	"net/http"
	"strings"
	"testing"
)

type failingTransport func(*http.Request) (*http.Response, error)

func (f failingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestTelegramTransportErrorRedactsBotTokenURL(t *testing.T) {
	const (
		tokenMarker = "TELEGRAM_BOT_TOKEN_MUST_NOT_LEAK"
		pathMarker  = "TELEGRAM_PATH_MUST_NOT_LEAK"
		queryMarker = "TELEGRAM_QUERY_MUST_NOT_LEAK"
	)
	adapter := &TelegramAdapter{client: &http.Client{Transport: failingTransport(func(request *http.Request) (*http.Response, error) {
		return nil, errors.New("dial failed for " + request.URL.String())
	})}}
	request, err := http.NewRequest(
		http.MethodPost,
		"https://api.telegram.org/bot"+tokenMarker+"/"+pathMarker+"?secret="+queryMarker,
		nil,
	)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	_, err = adapter.executeRequest(request)
	if err == nil {
		t.Fatal("executeRequest error = nil")
	}
	for _, marker := range []string{tokenMarker, pathMarker, queryMarker} {
		assertTelegramErrorChainOmits(t, err, marker)
	}
}

func assertTelegramErrorChainOmits(t *testing.T, err error, marker string) {
	t.Helper()
	for current := err; current != nil; current = errors.Unwrap(current) {
		if strings.Contains(current.Error(), marker) {
			t.Fatalf("error chain leaked %q: %v", marker, current)
		}
	}
}
