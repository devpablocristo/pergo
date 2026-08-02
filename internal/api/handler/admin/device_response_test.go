package admin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/pablojhp.pergo/internal/platform/httpresponse"
	"github.com/pablojhp.pergo/templates/pages"
)

func TestSyncTemplatesFromMetaBoundsAndRedactsRemoteBodies(t *testing.T) {
	t.Parallel()

	const (
		marker        = "DEVICE_META_SECRET_MUST_NOT_LEAK"
		accountMarker = "DEVICE_ACCOUNT_ID_MUST_NOT_LEAK"
		tokenMarker   = "DEVICE_ACCESS_TOKEN_MUST_NOT_LEAK"
	)
	for _, status := range []int{http.StatusOK, http.StatusBadRequest} {
		status := status
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				_, _ = w.Write([]byte(marker + strings.Repeat("x", int(httpresponse.MaxBodyBytes))))
			}))
			defer server.Close()

			handler := &DeviceHandler{MetaGraphBaseURL: server.URL}
			err := handler.syncTemplatesFromMeta(
				context.Background(),
				uuid.New(),
				uuid.New(),
				pages.WABAConfig{WABAAccountID: accountMarker, Token: tokenMarker},
				false,
			)
			if err == nil {
				t.Fatal("syncTemplatesFromMeta error = nil")
			}
			for _, secret := range []string{marker, accountMarker, tokenMarker} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("error leaked %q: %v", secret, err)
				}
			}
		})
	}
}

func TestSyncTemplatesFromMetaRequestErrorIsRedacted(t *testing.T) {
	t.Parallel()

	const (
		baseMarker    = "DEVICE_META_BASE_MUST_NOT_LEAK"
		accountMarker = "DEVICE_META_ACCOUNT_MUST_NOT_LEAK"
		tokenMarker   = "DEVICE_META_TOKEN_MUST_NOT_LEAK"
	)
	handler := &DeviceHandler{MetaGraphBaseURL: "://" + baseMarker}
	err := handler.syncTemplatesFromMeta(
		context.Background(),
		uuid.New(),
		uuid.New(),
		pages.WABAConfig{WABAAccountID: accountMarker, Token: tokenMarker},
		false,
	)
	if err == nil {
		t.Fatal("syncTemplatesFromMeta error = nil")
	}
	for _, marker := range []string{baseMarker, accountMarker, tokenMarker} {
		if strings.Contains(err.Error(), marker) {
			t.Fatalf("error leaked %q: %v", marker, err)
		}
	}
}
