// Package i18n resolves and translates the locale used by PerGo's web UI.
package i18n

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"golang.org/x/text/language"
)

// Locale identifies a supported web-interface language.
type Locale string

const (
	Spanish             Locale = "es"
	English             Locale = "en"
	BrazilianPortuguese Locale = "pt-BR"
	CookieName                 = "pergo-locale"
)

type contextKey struct{}

var supported = []Locale{Spanish, English, BrazilianPortuguese}

var matcher = language.NewMatcher([]language.Tag{
	language.Spanish,
	language.English,
	language.BrazilianPortuguese,
})

//go:embed locales/es.json
var spanishCatalogJSON []byte

//go:embed locales/en.json
var englishCatalogJSON []byte

//go:embed locales/pt-BR.json
var portugueseCatalogJSON []byte

var catalogs = map[Locale]map[string]string{
	Spanish:             mustLoadCatalog(Spanish, spanishCatalogJSON),
	English:             mustLoadCatalog(English, englishCatalogJSON),
	BrazilianPortuguese: mustLoadCatalog(BrazilianPortuguese, portugueseCatalogJSON),
}

func init() {
	if err := validateCatalogs(); err != nil {
		panic("invalid embedded i18n catalogs: " + err.Error())
	}
}

func mustLoadCatalog(locale Locale, raw []byte) map[string]string {
	var catalog map[string]string
	if err := json.Unmarshal(raw, &catalog); err != nil {
		panic(fmt.Sprintf("decode %s catalog: %v", locale, err))
	}
	return catalog
}

func validateCatalogs() error {
	base := catalogs[Spanish]
	for _, locale := range supported {
		catalog := catalogs[locale]
		if len(catalog) != len(base) {
			return fmt.Errorf("catalog %s has %d keys, expected %d", locale, len(catalog), len(base))
		}
		for key := range base {
			if strings.TrimSpace(catalog[key]) == "" {
				return fmt.Errorf("catalog %s has empty value for %q", locale, key)
			}
		}
	}
	return nil
}

// Parse validates a locale value received from a cookie or form.
func Parse(value string) (Locale, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "es", "es-es", "es-419":
		return Spanish, true
	case "en", "en-us", "en-gb":
		return English, true
	case "pt", "pt-br", "pt_br":
		return BrazilianPortuguese, true
	default:
		return "", false
	}
}

// Resolve returns the request locale in precedence order: cookie,
// Accept-Language, then Spanish.
func Resolve(r *http.Request) Locale {
	if cookie, err := r.Cookie(CookieName); err == nil {
		if locale, ok := Parse(cookie.Value); ok {
			return locale
		}
	}

	if tags, _, err := language.ParseAcceptLanguage(r.Header.Get("Accept-Language")); err == nil && len(tags) > 0 {
		_, index, _ := matcher.Match(tags...)
		return supported[index]
	}
	return Spanish
}

// WithLocale attaches a validated locale to a request context.
func WithLocale(ctx context.Context, locale Locale) context.Context {
	if _, ok := Parse(string(locale)); !ok {
		locale = Spanish
	}
	return context.WithValue(ctx, contextKey{}, locale)
}

// LocaleFromContext returns the active locale, defaulting to Spanish.
func LocaleFromContext(ctx context.Context) Locale {
	if locale, ok := ctx.Value(contextKey{}).(Locale); ok {
		if parsed, valid := Parse(string(locale)); valid {
			return parsed
		}
	}
	return Spanish
}

// HasLocale reports whether the context was prepared by the locale middleware.
func HasLocale(ctx context.Context) bool {
	_, ok := ctx.Value(contextKey{}).(Locale)
	return ok
}

// HTMLLang returns a valid value for the html lang attribute.
func HTMLLang(ctx context.Context) string {
	return string(LocaleFromContext(ctx))
}

// T translates key for the current locale. Missing keys fall back to Spanish
// and finally to the key itself, which keeps an incomplete catalog readable.
func T(ctx context.Context, key string, args ...any) string {
	locale := LocaleFromContext(ctx)
	value := catalogs[locale][key]
	if value == "" {
		value = catalogs[Spanish][key]
	}
	if value == "" {
		value = key
	}
	if len(args) == 0 {
		return value
	}
	return fmt.Sprintf(value, args...)
}
