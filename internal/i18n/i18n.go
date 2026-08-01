// Package i18n resolves and translates the locale used by PerGo's web UI.
package i18n

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

// Locale identifies a supported web-interface language.
type Locale string

// Key is a stable, semantic identifier for a UI string. Keys are internal to
// the web interface; API payloads and persisted values never use them.
type Key string

const (
	Spanish             Locale = "es"
	English             Locale = "en"
	BrazilianPortuguese Locale = "pt-BR"
	CookieName                 = "pergo-locale"

	CommonCancel Key = "common.cancel"
	CommonClose  Key = "common.close"
	CommonSave   Key = "common.save"
	CommonSend   Key = "common.send"
	Language     Key = "common.language"
)

type contextKey struct{}

var supported = []Locale{Spanish, English, BrazilianPortuguese}

var matcher = language.NewMatcher([]language.Tag{
	language.Spanish,
	language.English,
	language.BrazilianPortuguese,
})

var placeholderPattern = regexp.MustCompile(`%((\[[0-9]+\])?[-+#0 ]*[0-9]*(\.[0-9]+)?[vTtbcdoOqxXUeEfFgGsp])`)

//go:embed locales/es.json
var spanishCatalogJSON []byte

//go:embed locales/en.json
var englishCatalogJSON []byte

//go:embed locales/pt-BR.json
var portugueseCatalogJSON []byte

var catalogs = map[Locale]map[Key]string{
	Spanish:             mustLoadCatalog(Spanish, spanishCatalogJSON),
	English:             mustLoadCatalog(English, englishCatalogJSON),
	BrazilianPortuguese: mustLoadCatalog(BrazilianPortuguese, portugueseCatalogJSON),
}

func init() {
	if err := validateCatalogs(); err != nil {
		panic("invalid embedded i18n catalogs: " + err.Error())
	}
}

func mustLoadCatalog(locale Locale, raw []byte) map[Key]string {
	var catalog map[Key]string
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
			value := catalog[key]
			if strings.TrimSpace(value) == "" {
				return fmt.Errorf("catalog %s has empty value for %q", locale, key)
			}
			if signature(value) != signature(base[key]) {
				return fmt.Errorf("catalog %s has incompatible placeholders for %q", locale, key)
			}
		}
	}
	return nil
}

func signature(value string) string {
	matches := placeholderPattern.FindAllString(value, -1)
	return strings.Join(matches, "|")
}

// Parse validates canonical locale values received from cookies and forms.
// Browser language variants are handled exclusively by Resolve's matcher.
func Parse(value string) (Locale, bool) {
	switch strings.TrimSpace(value) {
	case "es":
		return Spanish, true
	case "en":
		return English, true
	case "pt-BR":
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
func T(ctx context.Context, key Key, args ...any) string {
	locale := LocaleFromContext(ctx)
	value := catalogs[locale][key]
	if value == "" {
		value = catalogs[Spanish][key]
	}
	if value == "" {
		value = string(key)
	}
	if len(args) == 0 {
		return value
	}
	return fmt.Sprintf(value, args...)
}

// FormatDate renders a date using the selected interface convention.
func FormatDate(ctx context.Context, value time.Time) string {
	if LocaleFromContext(ctx) == English {
		return value.Format("01/02/2006")
	}
	return value.Format("02/01/2006")
}

// FormatDateTime renders a date-time without seconds using the selected locale.
func FormatDateTime(ctx context.Context, value time.Time) string {
	if LocaleFromContext(ctx) == English {
		return value.Format("01/02/2006 3:04 PM")
	}
	return value.Format("02/01/2006 15:04")
}

// FormatDateTimeSeconds renders a date-time with seconds using the selected locale.
func FormatDateTimeSeconds(ctx context.Context, value time.Time) string {
	if LocaleFromContext(ctx) == English {
		return value.Format("01/02/2006 3:04:05 PM")
	}
	return value.Format("02/01/2006 15:04:05")
}

// FormatTime renders a clock value using the selected locale.
func FormatTime(ctx context.Context, value time.Time) string {
	if LocaleFromContext(ctx) == English {
		return value.Format("3:04 PM")
	}
	return value.Format("15:04")
}

// FormatNumber localizes integer grouping without changing the value itself.
func FormatNumber(ctx context.Context, value int64) string {
	var tag language.Tag
	switch LocaleFromContext(ctx) {
	case English:
		tag = language.English
	case BrazilianPortuguese:
		tag = language.BrazilianPortuguese
	default:
		tag = language.Spanish
	}
	return message.NewPrinter(tag).Sprintf("%d", value)
}

// Plural selects the singular or plural semantic key. The three supported
// locales use the same one/other cardinal split for the UI counts we expose.
func Plural(ctx context.Context, count int64, one, other Key, args ...any) string {
	key := other
	if count == 1 {
		key = one
	}
	return T(ctx, key, args...)
}
