package admin

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"

	mw "github.com/pablojhp.pergo/internal/api/middleware"
	"github.com/pablojhp.pergo/templates/layout"
	"github.com/pablojhp.pergo/templates/pages"
)

const maxLoginBodyBytes int64 = 16 << 10

// AdminSessionStore is owned by the login/logout consumer. Production wiring
// supplies a durable PostgreSQL implementation.
type AdminSessionStore interface {
	CreateAdminSession(ctx context.Context, sessionID string, expiresAt time.Time) error
	RevokeAdminSession(ctx context.Context, sessionID string, now time.Time) error
}

// LoginPage renders the login form using a minimal layout (no sidebar, no HTMX polling).
func LoginPage(c *echo.Context, showError bool) error {
	msg := ""
	if showError {
		msg = "Invalid password"
	}
	login := pages.Login(msg)
	return mw.Render(c, http.StatusOK, layout.LoginBase("Login", login))
}

// LoginPost handles the login form submission.
func LoginPost(c *echo.Context, sessions AdminSessionStore, adminPassword string) error {
	c.Request().Body = http.MaxBytesReader(
		c.Response(),
		c.Request().Body,
		maxLoginBodyBytes,
	)
	if err := c.Request().ParseForm(); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return c.String(http.StatusRequestEntityTooLarge, "login payload too large")
		}
		return c.String(http.StatusBadRequest, "invalid login payload")
	}
	password := c.FormValue("password")

	if password != adminPassword {
		login := pages.Login("Invalid password")
		return mw.Render(c, http.StatusUnauthorized, layout.LoginBase("Login", login))
	}

	// Persist the revocable identity before emitting the browser cookie.
	secret := mw.GetSessionSecret()
	prepared := mw.PrepareSessionCookie(c, secret)
	if sessions != nil {
		if err := sessions.CreateAdminSession(
			c.Request().Context(),
			prepared.Identity.ID,
			prepared.Identity.ExpiresAt,
		); err != nil {
			return c.String(http.StatusServiceUnavailable, "admin session unavailable")
		}
	}
	mw.SetPreparedSessionCookie(c, prepared)

	return c.Redirect(http.StatusFound, "/admin/")
}

// Logout revokes the durable session before clearing the browser cookie.
func Logout(c *echo.Context, stores ...AdminSessionStore) error {
	var store AdminSessionStore
	if len(stores) > 0 {
		store = stores[0]
	}
	if store != nil {
		cookie, err := c.Cookie("pergo-session")
		if err == nil && cookie.Value != "" {
			identity, ok := mw.SessionIdentityFromCookie(cookie.Value, mw.GetSessionSecret())
			if ok {
				if err := store.RevokeAdminSession(
					c.Request().Context(),
					identity.ID,
					time.Now().UTC(),
				); err != nil {
					return c.String(http.StatusServiceUnavailable, "admin session unavailable")
				}
			}
		}
	}
	mw.ClearSessionCookie(c)
	return c.Redirect(http.StatusFound, "/admin/login")
}
