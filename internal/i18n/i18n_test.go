package i18n

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestResolvePrecedence(t *testing.T) {
	tests := []struct {
		name   string
		cookie string
		header string
		want   Locale
	}{
		{"cookie wins", "pt-BR", "en-US,en;q=0.9", BrazilianPortuguese},
		{"browser english", "", "en-GB,en;q=0.9", English},
		{"browser portuguese", "", "pt-PT,es;q=0.7", BrazilianPortuguese},
		{"fallback", "", "de-DE", Spanish},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/", nil)
			req.Header.Set("Accept-Language", tt.header)
			if tt.cookie != "" {
				req.AddCookie(&http.Cookie{Name: CookieName, Value: tt.cookie})
			}
			if got := Resolve(req); got != tt.want {
				t.Fatalf("Resolve() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCatalogParityAndHTMLLocalization(t *testing.T) {
	for _, locale := range supported {
		if len(catalogs[locale]) != len(catalogs[Spanish]) {
			t.Fatalf("catalog %s has a different key count", locale)
		}
	}
	ctx := WithLocale(context.Background(), English)
	got := LocalizeHTML(ctx, `<div title="Visão Geral">Visão Geral</div><textarea>Visão Geral</textarea>`)
	if !strings.Contains(got, `title="Overview"`) || !strings.Contains(got, `>Overview</div>`) {
		t.Fatalf("localized HTML = %q", got)
	}
	if !strings.Contains(got, `<textarea>Visão Geral</textarea>`) {
		t.Fatalf("textarea content was translated: %q", got)
	}
}
