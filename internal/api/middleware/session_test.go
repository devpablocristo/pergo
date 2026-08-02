package middleware

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
)

type stubSessionValidator struct {
	active bool
	err    error
	id     string
}

func (s *stubSessionValidator) IsAdminSessionActive(
	_ context.Context,
	sessionID string,
	_ time.Time,
) (bool, error) {
	s.id = sessionID
	return s.active, s.err
}

func TestSessionCookieSecurityAttributes(t *testing.T) {
	tests := []struct {
		name        string
		environment string
		forwarded   string
		wantSecure  bool
	}{
		{name: "local development", environment: "development", wantSecure: false},
		{name: "TLS terminated proxy", environment: "development", forwarded: "https", wantSecure: true},
		{name: "production is secure without trusting proxy headers", environment: "production", wantSecure: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("PERGO_ENVIRONMENT", tt.environment)
			req := httptest.NewRequest(http.MethodPost, "/admin/login", nil)
			if tt.forwarded != "" {
				req.Header.Set("X-Forwarded-Proto", tt.forwarded)
			}
			rec := httptest.NewRecorder()
			c := echo.New().NewContext(req, rec)

			SetSessionCookie(c, []byte("test-session-secret"))
			cookies := rec.Result().Cookies()
			if len(cookies) != 1 {
				t.Fatalf("cookies = %d, want 1", len(cookies))
			}
			cookie := cookies[0]
			if cookie.Secure != tt.wantSecure {
				t.Fatalf("Secure = %v, want %v", cookie.Secure, tt.wantSecure)
			}
			if !cookie.HttpOnly || cookie.SameSite != http.SameSiteLaxMode {
				t.Fatalf("cookie attributes = HttpOnly:%v SameSite:%v", cookie.HttpOnly, cookie.SameSite)
			}
			if cookie.MaxAge != 8*60*60 {
				t.Fatalf("MaxAge = %d, want 28800", cookie.MaxAge)
			}
		})
	}
}

func TestSessionCookieHasServerSideExpiryAndNonce(t *testing.T) {
	secret := []byte("test-session-secret-at-least-32-bytes")
	now := time.Unix(1_800_000_000, 0).UTC()
	first := newSessionCookieValue(secret, now)
	second := newSessionCookieValue(secret, now)
	if first == second {
		t.Fatal("two admin logins produced the same session cookie")
	}
	if !verifySessionCookieAt(first, secret, now.Add(sessionDuration-time.Second)) {
		t.Fatal("valid session was rejected before expiry")
	}
	if verifySessionCookieAt(first, secret, now.Add(sessionDuration)) {
		t.Fatal("expired session remained valid server-side")
	}
	if verifySessionCookieAt(first, []byte("different-session-secret-32-bytes"), now) {
		t.Fatal("session accepted under a different secret")
	}
}

func TestLegacyStaticSessionPayloadIsRejected(t *testing.T) {
	secret := []byte("test-session-secret-at-least-32-bytes")
	legacy := signSessionCookie("authenticated=true", secret)
	if VerifySessionCookie(legacy, secret) {
		t.Fatal("legacy non-expiring session cookie was accepted")
	}
}

func TestVersionedSessionCookieCannotBeAcceptedByLegacyRawSecretVerifier(t *testing.T) {
	secret := []byte("test-session-secret-at-least-32-bytes")
	value := newSessionCookieValue(secret, time.Now().UTC())
	parts := strings.SplitN(value, ".", 2)
	if len(parts) != 2 {
		t.Fatal("new session cookie has invalid wire shape")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode signature: %v", err)
	}
	legacyMAC := hmac.New(sha256.New, secret)
	_, _ = legacyMAC.Write(payload)
	if hmac.Equal(signature, legacyMAC.Sum(nil)) {
		t.Fatal("v1 cookie remained valid under the legacy raw-secret verifier")
	}
	if !VerifySessionCookie(value, secret) {
		t.Fatal("v1 cookie was rejected by the domain-separated verifier")
	}
}

func TestSessionMiddlewareRequiresDurableActiveSession(t *testing.T) {
	t.Setenv("PERGO_SESSION_SECRET", "durable-session-test-secret-32-bytes")
	tests := []struct {
		name       string
		active     bool
		storeErr   error
		wantStatus int
	}{
		{name: "active", active: true, wantStatus: http.StatusNoContent},
		{name: "revoked", active: false, wantStatus: http.StatusFound},
		{name: "store unavailable fails closed", storeErr: errors.New("database unavailable"), wantStatus: http.StatusFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := &stubSessionValidator{active: tt.active, err: tt.storeErr}
			e := echo.New()
			e.GET("/admin/", func(c *echo.Context) error {
				return c.NoContent(http.StatusNoContent)
			}, SessionAuthMiddleware(validator))

			loginReq := httptest.NewRequest(http.MethodPost, "/admin/login", nil)
			loginRec := httptest.NewRecorder()
			loginContext := e.NewContext(loginReq, loginRec)
			identity := SetSessionCookie(loginContext, GetSessionSecret())
			sessionCookie := loginRec.Result().Cookies()[0]

			req := httptest.NewRequest(http.MethodGet, "/admin/", nil)
			req.AddCookie(sessionCookie)
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if validator.id != identity.ID {
				t.Fatalf("validated session ID = %q, want %q", validator.id, identity.ID)
			}
		})
	}
}
