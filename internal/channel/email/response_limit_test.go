package email

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

func TestHTTPEmailProvidersBoundRemoteBodies(t *testing.T) {
	t.Parallel()

	const marker = "EMAIL_REMOTE_SECRET_MUST_NOT_LEAK"
	providers := []struct {
		name string
		new  func(*httptest.Server) Provider
		msg  *EmailMessage
	}{
		{
			name: "mautic",
			new: func(server *httptest.Server) Provider {
				return NewMauticProvider(MauticConfig{
					BaseURL:    server.URL,
					HTTPClient: server.Client(),
				})
			},
			msg: &EmailMessage{ID: "mautic-id", To: []string{"to@example.com"}, TextBody: "body"},
		},
		{
			name: "SES",
			new: func(server *httptest.Server) Provider {
				return NewSESProvider(SESConfig{
					EndpointURL: server.URL,
					HTTPClient:  server.Client(),
				})
			},
			msg: &EmailMessage{
				ID:       "ses-id",
				From:     "from@example.com",
				To:       []string{"to@example.com"},
				TextBody: "body",
			},
		},
	}
	responses := []struct {
		name          string
		status        int
		wantTerminal  bool
		wantUncertain bool
	}{
		{
			name:          "oversized success",
			status:        http.StatusOK,
			wantUncertain: true,
		},
		{
			name:         "oversized client error",
			status:       http.StatusBadRequest,
			wantTerminal: true,
		},
	}

	for _, providerCase := range providers {
		providerCase := providerCase
		for _, responseCase := range responses {
			responseCase := responseCase
			t.Run(providerCase.name+"/"+responseCase.name, func(t *testing.T) {
				t.Parallel()
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(responseCase.status)
					_, _ = w.Write([]byte(marker + strings.Repeat("x", int(httpresponse.MaxBodyBytes))))
				}))
				defer server.Close()

				providerID, err := providerCase.new(server).Send(context.Background(), providerCase.msg)
				if err == nil {
					t.Fatal("Send error = nil")
				}
				if providerID != "" {
					t.Fatalf("provider ID = %q, want empty", providerID)
				}
				if channel.IsTerminal(err) != responseCase.wantTerminal {
					t.Fatalf(
						"terminal = %t, want %t; error=%v",
						channel.IsTerminal(err),
						responseCase.wantTerminal,
						err,
					)
				}
				if channel.IsUncertain(err) != responseCase.wantUncertain {
					t.Fatalf(
						"uncertain = %t, want %t; error=%v",
						channel.IsUncertain(err),
						responseCase.wantUncertain,
						err,
					)
				}
				if strings.Contains(err.Error(), marker) {
					t.Fatalf("error leaked remote body: %v", err)
				}
			})
		}
	}
}

type emailFailingTransport func(*http.Request) (*http.Response, error)

func (f emailFailingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestHTTPEmailProvidersRedactTransportErrorChains(t *testing.T) {
	t.Parallel()

	const (
		baseMarker     = "EMAIL_BASE_PATH_MUST_NOT_LEAK"
		usernameMarker = "EMAIL_USERNAME_MUST_NOT_LEAK"
		passwordMarker = "EMAIL_PASSWORD_MUST_NOT_LEAK"
		bodyMarker     = "EMAIL_PROVIDER_ERROR_MUST_NOT_LEAK"
	)
	client := &http.Client{Transport: emailFailingTransport(func(request *http.Request) (*http.Response, error) {
		return nil, errors.New(
			request.URL.String() + " " +
				request.Header.Get("Authorization") + " " +
				usernameMarker + " " +
				passwordMarker + " " +
				bodyMarker,
		)
	})}
	providers := []struct {
		name     string
		provider Provider
		message  *EmailMessage
	}{
		{
			name: "mautic",
			provider: NewMauticProvider(MauticConfig{
				BaseURL:    "https://example.invalid/" + baseMarker,
				Username:   usernameMarker,
				Password:   passwordMarker,
				HTTPClient: client,
			}),
			message: &EmailMessage{ID: "mautic-id", To: []string{"to@example.com"}, TextBody: "body"},
		},
		{
			name: "SES",
			provider: NewSESProvider(SESConfig{
				EndpointURL: "https://example.invalid/" + baseMarker,
				HTTPClient:  client,
			}),
			message: &EmailMessage{
				ID:       "ses-id",
				From:     "from@example.com",
				To:       []string{"to@example.com"},
				TextBody: "body",
			},
		},
	}

	for _, providerCase := range providers {
		providerCase := providerCase
		t.Run(providerCase.name, func(t *testing.T) {
			t.Parallel()
			providerID, err := providerCase.provider.Send(context.Background(), providerCase.message)
			if err == nil {
				t.Fatal("Send error = nil")
			}
			if providerID != "" {
				t.Fatalf("provider ID = %q, want empty", providerID)
			}
			for _, marker := range []string{
				baseMarker,
				usernameMarker,
				passwordMarker,
				bodyMarker,
			} {
				assertEmailErrorChainOmits(t, err, marker)
			}
		})
	}
}

func assertEmailErrorChainOmits(t *testing.T, err error, marker string) {
	t.Helper()
	for current := err; current != nil; current = errors.Unwrap(current) {
		if strings.Contains(current.Error(), marker) {
			t.Fatalf("error chain leaked %q: %v", marker, current)
		}
	}
}
