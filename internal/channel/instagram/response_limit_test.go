package instagram

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pablojhp.pergo/internal/channel"
	"github.com/pablojhp.pergo/internal/platform/httpresponse"
)

func TestSendRequestBoundsAndRedactsRemoteBodies(t *testing.T) {
	t.Parallel()

	const marker = "INSTAGRAM_REMOTE_SECRET_MUST_NOT_LEAK"
	tests := []struct {
		name          string
		status        int
		body          string
		wantTerminal  bool
		wantUncertain bool
	}{
		{
			name:          "oversized success is uncertain",
			status:        http.StatusOK,
			body:          marker + strings.Repeat("x", int(httpresponse.MaxBodyBytes)),
			wantUncertain: true,
		},
		{
			name:         "oversized client error is terminal",
			status:       http.StatusBadRequest,
			body:         marker + strings.Repeat("x", int(httpresponse.MaxBodyBytes)),
			wantTerminal: true,
		},
		{
			name:         "provider message is redacted",
			status:       http.StatusBadRequest,
			body:         `{"error":{"message":"` + marker + `","code":100,"error_subcode":33}}`,
			wantTerminal: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer server.Close()

			adapter := NewAdapter(nil, server.Client(), "")
			adapter.SetBaseURL(server.URL)
			providerID, err := adapter.sendRequest(context.Background(), "account", "token", []byte(`{}`))
			if err == nil {
				t.Fatal("sendRequest error = nil")
			}
			if providerID != "" {
				t.Fatalf("provider ID = %q, want empty", providerID)
			}
			if channel.IsTerminal(err) != tt.wantTerminal {
				t.Fatalf("terminal = %t, want %t; error=%v", channel.IsTerminal(err), tt.wantTerminal, err)
			}
			if channel.IsUncertain(err) != tt.wantUncertain {
				t.Fatalf("uncertain = %t, want %t; error=%v", channel.IsUncertain(err), tt.wantUncertain, err)
			}
			if strings.Contains(err.Error(), marker) {
				t.Fatalf("error leaked remote body: %v", err)
			}
		})
	}
}

type instagramFailingTransport func(*http.Request) (*http.Response, error)

func (f instagramFailingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestSendRequestRedactsTransportErrorChain(t *testing.T) {
	t.Parallel()

	const (
		baseMarker    = "INSTAGRAM_BASE_PATH_MUST_NOT_LEAK"
		accountMarker = "INSTAGRAM_ACCOUNT_ID_MUST_NOT_LEAK"
		tokenMarker   = "INSTAGRAM_ACCESS_TOKEN_MUST_NOT_LEAK"
		bodyMarker    = "INSTAGRAM_PROVIDER_ERROR_MUST_NOT_LEAK"
	)
	client := &http.Client{Transport: instagramFailingTransport(func(request *http.Request) (*http.Response, error) {
		return nil, errors.New(
			request.URL.String() + " " +
				request.Header.Get("Authorization") + " " +
				bodyMarker,
		)
	})}
	adapter := NewAdapter(nil, client, "")
	adapter.SetBaseURL("https://example.invalid/" + baseMarker)

	providerID, err := adapter.sendRequest(
		context.Background(),
		accountMarker,
		tokenMarker,
		[]byte(`{}`),
	)
	if err == nil {
		t.Fatal("sendRequest error = nil")
	}
	if providerID != "" {
		t.Fatalf("provider ID = %q, want empty", providerID)
	}
	for _, marker := range []string{baseMarker, accountMarker, tokenMarker, bodyMarker} {
		assertInstagramErrorChainOmits(t, err, marker)
	}
}

func assertInstagramErrorChainOmits(t *testing.T, err error, marker string) {
	t.Helper()
	for current := err; current != nil; current = errors.Unwrap(current) {
		if strings.Contains(current.Error(), marker) {
			t.Fatalf("error chain leaked %q: %v", marker, current)
		}
	}
}
