package telegram

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pablojhp.pergo/internal/channel"
	"github.com/pablojhp.pergo/internal/platform/httpresponse"
)

func TestExecuteRequestBoundsAndRedactsRemoteBodies(t *testing.T) {
	t.Parallel()

	const marker = "TELEGRAM_REMOTE_SECRET_MUST_NOT_LEAK"
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
			name:         "provider description is redacted",
			status:       http.StatusBadRequest,
			body:         `{"ok":false,"error_code":400,"description":"` + marker + `"}`,
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

			request, err := http.NewRequest(http.MethodPost, server.URL, nil)
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			adapter := &TelegramAdapter{client: server.Client()}
			providerID, err := adapter.executeRequest(request)
			if err == nil {
				t.Fatal("executeRequest error = nil")
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

func TestExecuteRequestReturnsTelegramMessageID(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":12345}}`))
	}))
	defer server.Close()

	request, err := http.NewRequest(http.MethodPost, server.URL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	adapter := &TelegramAdapter{client: server.Client()}
	providerID, err := adapter.executeRequest(request)
	if err != nil {
		t.Fatalf("executeRequest: %v", err)
	}
	if providerID != "12345" {
		t.Fatalf("provider ID = %q, want 12345", providerID)
	}
}
