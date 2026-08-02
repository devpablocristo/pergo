package middleware

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"

	"github.com/pablojhp.pergo/internal/repository"
)

type failingAPIKeyLookup struct {
	err error
}

func (f failingAPIKeyLookup) FindActiveCandidates(
	context.Context,
	string,
) ([]repository.APIKey, error) {
	return nil, f.err
}

func TestAuthMiddlewareRejectsSecretsInQuery(t *testing.T) {
	e := echo.New()
	e.Use(AuthMiddleware(failingAPIKeyLookup{err: errors.New("must not be called")}))
	e.GET("/api/v1/me", func(c *echo.Context) error {
		return c.NoContent(http.StatusOK)
	})

	for _, query := range []string{
		"?api_key=12345678secret",
		"?token=12345678secret",
	} {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/me"+query, nil)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("query %q status=%d, want 401", query, rec.Code)
		}
	}
}

func TestAuthMiddlewareReturnsRetryableDependencyFailure(t *testing.T) {
	const secretMarker = "postgres://user:secret-marker@database"
	e := echo.New()
	e.Use(AuthMiddleware(failingAPIKeyLookup{err: errors.New(secretMarker)}))
	e.GET("/api/v1/me", func(c *echo.Context) error {
		return c.NoContent(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/me", nil)
	req.Header.Set("Authorization", "Bearer 12345678secret")
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503: %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("missing Retry-After")
	}
	if strings.Contains(rec.Body.String(), secretMarker) {
		t.Fatal("dependency error leaked in response")
	}
}
