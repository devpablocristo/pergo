package i18n

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
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

func TestCatalogParityAndSemanticTranslation(t *testing.T) {
	for _, locale := range supported {
		if len(catalogs[locale]) != len(catalogs[Spanish]) {
			t.Fatalf("catalog %s has a different key count", locale)
		}
	}
	for _, tc := range []struct {
		locale Locale
		want   string
	}{
		{Spanish, "Bandeja de entrada"},
		{English, "Inbox"},
		{BrazilianPortuguese, "Caixa de entrada"},
	} {
		if got := T(WithLocale(context.Background(), tc.locale), "nav.inbox"); got != tc.want {
			t.Errorf("T(nav.inbox, %s) = %q, want %q", tc.locale, got, tc.want)
		}
	}
}

func TestParseAcceptsOnlyCanonicalCookieValues(t *testing.T) {
	for _, value := range []string{"es", "en", "pt-BR"} {
		if _, ok := Parse(value); !ok {
			t.Errorf("Parse(%q) should be valid", value)
		}
	}
	for _, value := range []string{"ES", "en-US", "pt", "pt-br", "fr"} {
		if _, ok := Parse(value); ok {
			t.Errorf("Parse(%q) should be invalid", value)
		}
	}
}

func TestLocalizedFormats(t *testing.T) {
	value := time.Date(2026, time.July, 31, 17, 6, 5, 0, time.UTC)
	if got := FormatDateTime(WithLocale(context.Background(), Spanish), value); got != "31/07/2026 17:06" {
		t.Errorf("Spanish date-time = %q", got)
	}
	if got := FormatDateTime(WithLocale(context.Background(), BrazilianPortuguese), value); got != "31/07/2026 17:06" {
		t.Errorf("Portuguese date-time = %q", got)
	}
	if got := FormatDateTime(WithLocale(context.Background(), English), value); got != "07/31/2026 5:06 PM" {
		t.Errorf("English date-time = %q", got)
	}
	if got := Plural(WithLocale(context.Background(), English), 1, "new_chat.parameter", "new_chat.parameter_optional", 1); got != "Variable 1" {
		t.Errorf("singular = %q", got)
	}
}
