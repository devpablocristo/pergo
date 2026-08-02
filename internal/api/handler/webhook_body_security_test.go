package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/pablojhp.pergo/internal/platform/postgres/tenant"
)

func TestIntegrationWebhooksRejectOversizedBodyBeforeDependencies(t *testing.T) {
	oversized := strings.NewReader(strings.Repeat("x", int(maxIntegrationWebhookBodyBytes)+1))

	t.Run("telegram", func(t *testing.T) {
		h := NewTelegramWebhookHandler(nil, nil, nil)
		req := httptest.NewRequest(http.MethodPost, "/webhooks/telegram/"+uuid.New().String(), oversized)
		req.Header.Set("X-Telegram-Bot-Api-Secret-Token", "untrusted-but-present")
		rec := httptest.NewRecorder()
		c := echo.New().NewContext(req, rec)
		c.SetPathValues(echo.PathValues{{Name: "workspace_id", Value: uuid.New().String()}})

		if err := h.Handle(c); err != nil {
			t.Fatalf("Handle: %v", err)
		}
		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want 413", rec.Code)
		}
	})

	tests := []struct {
		name string
		call func(*echo.Context) error
	}{
		{name: "chatwoot", call: NewChatwootWebhookHandler(nil, nil, nil, nil).Handle},
		{name: "typebot", call: NewTypebotWebhookHandler(nil, nil).Handle},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(
				http.MethodPost,
				"/api/integrations/"+tt.name,
				strings.NewReader(strings.Repeat("x", int(maxIntegrationWebhookBodyBytes)+1)),
			)
			req = req.WithContext(tenant.WithWorkspaceID(context.Background(), uuid.New()))
			rec := httptest.NewRecorder()
			c := echo.New().NewContext(req, rec)

			if err := tt.call(c); err != nil {
				t.Fatalf("Handle: %v", err)
			}
			if rec.Code != http.StatusRequestEntityTooLarge {
				t.Fatalf("status = %d, want 413; body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}
