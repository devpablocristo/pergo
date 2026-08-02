// Package middleware provides Echo v5 middleware functions for the PerGo API.
package middleware

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v5"

	"github.com/pablojhp.pergo/internal/platform/crypto"
	"github.com/pablojhp.pergo/internal/platform/postgres/tenant"
	"github.com/pablojhp.pergo/internal/repository"
)

// APIKeyLookup is the authentication adapter's consumer-owned credential port.
type APIKeyLookup interface {
	FindActiveCandidates(ctx context.Context, plaintext string) ([]repository.APIKey, error)
}

// AuthMiddleware returns an Echo middleware that validates API keys from the
// Authorization header and injects workspace_id into the request context.
func AuthMiddleware(repo APIKeyLookup) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			path := c.Request().URL.Path
			if path == "/" || path == "/locale" || path == "/healthz" || path == "/readyz" || strings.HasPrefix(path, "/admin") || strings.HasPrefix(path, "/webhooks") || strings.HasPrefix(path, "/static") {
				return next(c)
			}

			authHeader := c.Request().Header.Get("Authorization")
			var key string
			if authHeader != "" {
				parts := strings.SplitN(authHeader, " ", 2)
				if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
					key = parts[1]
				}
			}

			if key == "" || len(key) < 8 {
				return c.JSON(http.StatusUnauthorized, map[string]string{
					"code":    "unauthorized",
					"message": "invalid or missing API key",
				})
			}

			candidates, err := repo.FindActiveCandidates(c.Request().Context(), key)
			if err != nil {
				if !errors.Is(err, pgx.ErrNoRows) {
					slog.Error("API key dependency unavailable")
					c.Response().Header().Set("Retry-After", "1")
					return c.JSON(http.StatusServiceUnavailable, map[string]string{
						"code":    "authentication_unavailable",
						"message": "authentication dependency is temporarily unavailable",
					})
				}
				return c.JSON(http.StatusUnauthorized, map[string]string{
					"code":    "unauthorized",
					"message": "invalid or missing API key",
				})
			}

			var apiKey *repository.APIKey
			for i := range candidates {
				if crypto.VerifyAPIKey(key, candidates[i].KeyHash) {
					apiKey = &candidates[i]
					break
				}
			}
			if apiKey == nil {
				return c.JSON(http.StatusUnauthorized, map[string]string{
					"code":    "unauthorized",
					"message": "invalid or missing API key",
				})
			}

			// Inject workspace_id into request context
			ctx := tenant.WithWorkspaceID(c.Request().Context(), apiKey.WorkspaceID)
			c.SetRequest(c.Request().WithContext(ctx))
			c.Set("api_key", apiKey)

			return next(c)
		}
	}
}
