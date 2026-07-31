package handler

import (
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/pablojhp.pergo/internal/i18n"
)

// LocalePost stores the browser-local UI preference without changing API
// contracts or persistent workspace data.
func LocalePost(c *echo.Context) error {
	locale, ok := i18n.Parse(c.FormValue("locale"))
	if !ok {
		return c.String(http.StatusBadRequest, "unsupported locale")
	}

	req := c.Request()
	c.SetCookie(&http.Cookie{
		Name:     i18n.CookieName,
		Value:    string(locale),
		Path:     "/",
		Expires:  time.Now().AddDate(1, 0, 0),
		MaxAge:   365 * 24 * 60 * 60,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   req.TLS != nil,
	})
	return c.Redirect(http.StatusSeeOther, localReferer(req))
}

func localReferer(req *http.Request) string {
	referer := req.Referer()
	if referer == "" {
		return "/"
	}
	u, err := url.Parse(referer)
	if err != nil || u.Host != "" && !strings.EqualFold(u.Host, req.Host) || !strings.HasPrefix(u.Path, "/") {
		return "/"
	}
	if u.Path == "/locale" {
		return "/"
	}
	if u.RawQuery != "" {
		return u.Path + "?" + u.RawQuery
	}
	return u.Path
}
