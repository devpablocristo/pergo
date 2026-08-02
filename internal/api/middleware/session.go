// Package middleware provides Echo v5 middleware functions for the PerGo API.
package middleware

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo/v5"
)

const (
	sessionCookieName = "pergo-session"
	sessionSecretLen  = 32
	sessionDuration   = 8 * time.Hour
	sessionNonceLen   = 16
	sessionKeyContext = "pergo-admin-session:v1"
)

var (
	cachedSecret     []byte
	cachedSecretOnce sync.Once
)

// SessionAuthMiddleware returns an Echo middleware that checks for an
// authenticated session cookie and redirects to /admin/login if not authenticated.
// The session is a signed, server-expiring cookie with a random nonce.
//
// For HTMX requests (HX-Request: true), it responds with an HX-Redirect header
// instead of a standard 302 redirect to prevent HTMX from injecting the full
// login page HTML into the DOM (which would cause infinite rendering loops via
// hx-trigger="load" attributes in the injected page's sidebar).
func SessionAuthMiddleware(validators ...SessionValidator) echo.MiddlewareFunc {
	secret := getSessionSecret()
	var validator SessionValidator
	if len(validators) > 0 {
		validator = validators[0]
	}
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			cookie, err := c.Cookie(sessionCookieName)
			if err != nil || cookie.Value == "" {
				return redirectOrHTMX(c, "/admin/login")
			}

			identity, ok := parseSessionCookieAt(cookie.Value, secret, time.Now().UTC())
			if !ok {
				return redirectOrHTMX(c, "/admin/login")
			}
			if validator != nil {
				active, validateErr := validator.IsAdminSessionActive(
					c.Request().Context(),
					identity.ID,
					time.Now().UTC(),
				)
				if validateErr != nil || !active {
					return redirectOrHTMX(c, "/admin/login")
				}
			}

			return next(c)
		}
	}
}

// redirectOrHTMX performs a standard redirect for normal requests, but for HTMX
// requests it sets the HX-Redirect response header and returns 401, instructing
// the HTMX client to perform a full-page navigation instead of injecting HTML.
func redirectOrHTMX(c *echo.Context, target string) error {
	if c.Request().Header.Get("HX-Request") == "true" {
		c.Response().Header().Set("HX-Redirect", target)
		return c.NoContent(http.StatusUnauthorized)
	}
	return c.Redirect(http.StatusFound, target)
}

// PrepareSessionCookie creates a signed cookie and its server-side identity
// without modifying the response. Callers persist Identity before setting
// Cookie, preventing a session from escaping without a durable revocation row.
func PrepareSessionCookie(c *echo.Context, secret []byte) PreparedSession {
	now := time.Now().UTC()
	payload := newSessionPayload(now)
	value := signSessionPayload(payload, secret)
	identity, ok := parseSessionCookieAt(value, secret, now)
	if !ok {
		panic("newly generated admin session failed validation")
	}
	return PreparedSession{
		Cookie: &http.Cookie{
			Name:     sessionCookieName,
			Value:    value,
			Path:     "/",
			HttpOnly: true,
			Secure:   requestUsesHTTPS(c.Request()),
			SameSite: http.SameSiteLaxMode,
			MaxAge:   int(sessionDuration.Seconds()),
			Expires:  identity.ExpiresAt,
		},
		Identity: identity,
	}
}

// SetSessionCookie sets a signed session cookie on the response. Production
// login handlers use PrepareSessionCookie so the identity can be persisted
// before this header is emitted.
func SetSessionCookie(c *echo.Context, secret []byte) SessionIdentity {
	prepared := PrepareSessionCookie(c, secret)
	c.SetCookie(prepared.Cookie)
	return prepared.Identity
}

// SessionIdentityFromCookie verifies a cookie and returns its durable identity.
func SessionIdentityFromCookie(value string, secret []byte) (SessionIdentity, bool) {
	return parseSessionCookieAt(value, secret, time.Now().UTC())
}

// SetPreparedSessionCookie writes a session prepared and persisted by a login
// handler.
func SetPreparedSessionCookie(c *echo.Context, prepared PreparedSession) {
	c.SetCookie(prepared.Cookie)
}

// ClearSessionCookie removes the session cookie.
func ClearSessionCookie(c *echo.Context) {
	cookie := &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   requestUsesHTTPS(c.Request()),
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	}
	c.SetCookie(cookie)
}

// GetSessionSecret returns the session signing secret.
func GetSessionSecret() []byte {
	return getSessionSecret()
}

func getSessionSecret() []byte {
	cachedSecretOnce.Do(func() {
		if secret := os.Getenv("PERGO_SESSION_SECRET"); secret != "" {
			cachedSecret = []byte(secret)
			return
		}
		// Generate a random secret at boot (single-operator model — cookie survives restarts only within same process)
		secret := make([]byte, sessionSecretLen)
		if _, err := rand.Read(secret); err != nil {
			panic("crypto/rand failed while generating development session secret: " + err.Error())
		}
		cachedSecret = secret
	})
	return cachedSecret
}

func requestUsesHTTPS(request *http.Request) bool {
	if !isDevelopmentSessionEnvironment(os.Getenv("PERGO_ENVIRONMENT")) {
		return true
	}
	if request.TLS != nil {
		return true
	}
	proto := strings.TrimSpace(strings.Split(request.Header.Get("X-Forwarded-Proto"), ",")[0])
	return strings.EqualFold(proto, "https")
}

func isDevelopmentSessionEnvironment(environment string) bool {
	switch strings.ToLower(strings.TrimSpace(environment)) {
	case "", "development", "dev", "local", "test":
		return true
	default:
		return false
	}
}

type sessionPayload struct {
	Version   int    `json:"v"`
	ExpiresAt int64  `json:"exp"`
	Nonce     string `json:"nonce"`
}

// SessionIdentity is the server-side identity of an authenticated admin
// session. ID is a one-way SHA-256 digest of the random nonce carried by the
// signed cookie, so a database disclosure cannot be used to forge a cookie.
type SessionIdentity struct {
	ID        string
	ExpiresAt time.Time
}

// SessionValidator is owned by this middleware and implemented by the
// persistence adapter. A nil validator is supported only for isolated unit
// tests; production wiring always supplies the durable repository.
type SessionValidator interface {
	IsAdminSessionActive(ctx context.Context, sessionID string, now time.Time) (bool, error)
}

// PreparedSession contains the cookie and durable identity created for one
// successful login. Persist Identity before writing Cookie to the response.
type PreparedSession struct {
	Cookie   *http.Cookie
	Identity SessionIdentity
}

func newSessionPayload(now time.Time) sessionPayload {
	nonce := make([]byte, sessionNonceLen)
	if _, err := rand.Read(nonce); err != nil {
		panic("crypto/rand failed while generating admin session nonce: " + err.Error())
	}
	return sessionPayload{
		Version:   1,
		ExpiresAt: now.Add(sessionDuration).Unix(),
		Nonce:     base64.RawURLEncoding.EncodeToString(nonce),
	}
}

func newSessionCookieValue(secret []byte, now time.Time) string {
	return signSessionPayload(newSessionPayload(now), secret)
}

func signSessionPayload(payload sessionPayload, secret []byte) string {
	encoded, err := json.Marshal(payload)
	if err != nil {
		panic("failed to encode admin session payload: " + err.Error())
	}
	return signSessionCookie(string(encoded), deriveSessionSigningKey(secret))
}

// signSessionCookie creates an HMAC-signed cookie value: payload.signature
func signSessionCookie(payload string, secret []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(payload))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return base64.RawURLEncoding.EncodeToString([]byte(payload)) + "." + sig
}

// VerifySessionCookie verifies the HMAC signature of a session cookie.
func VerifySessionCookie(value string, secret []byte) bool {
	_, ok := parseSessionCookieAt(value, secret, time.Now().UTC())
	return ok
}

func verifySessionCookieAt(value string, secret []byte, now time.Time) bool {
	_, ok := parseSessionCookieAt(value, secret, now)
	return ok
}

func parseSessionCookieAt(value string, secret []byte, now time.Time) (SessionIdentity, bool) {
	parts := strings.SplitN(value, ".", 2)
	if len(parts) != 2 {
		return SessionIdentity{}, false
	}
	payloadDecoded, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return SessionIdentity{}, false
	}
	sigDecoded, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return SessionIdentity{}, false
	}

	mac := hmac.New(sha256.New, deriveSessionSigningKey(secret))
	mac.Write(payloadDecoded)
	expected := mac.Sum(nil)
	if !hmac.Equal(sigDecoded, expected) {
		return SessionIdentity{}, false
	}

	var payload sessionPayload
	if err := json.Unmarshal(payloadDecoded, &payload); err != nil {
		return SessionIdentity{}, false
	}
	if payload.Version != 1 || payload.ExpiresAt <= now.Unix() || payload.Nonce == "" {
		return SessionIdentity{}, false
	}
	nonce, err := base64.RawURLEncoding.DecodeString(payload.Nonce)
	if err != nil || len(nonce) != sessionNonceLen {
		return SessionIdentity{}, false
	}
	digest := sha256.Sum256(nonce)
	return SessionIdentity{
		ID:        hex.EncodeToString(digest[:]),
		ExpiresAt: time.Unix(payload.ExpiresAt, 0).UTC(),
	}, true
}

// deriveSessionSigningKey domain-separates the revocable v1 session protocol
// from the legacy stateless cookie signer. An older binary that only verifies
// HMAC(masterSecret, payload) therefore cannot accept v1 cookies during a
// rolling deploy or rollback while ignoring their expiry/revocation state.
func deriveSessionSigningKey(secret []byte) []byte {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(sessionKeyContext))
	return mac.Sum(nil)
}
