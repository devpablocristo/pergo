package middleware

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/pablojhp.pergo/internal/repository"
)

// ActionLogInserter defines the database persistence operation needed for auditing.
type ActionLogInserter interface {
	Insert(ctx context.Context, log *repository.UserActionLog) error
}

// AuditMiddleware returns an Echo middleware that automatically audits public API key actions.
func AuditMiddleware(repo ActionLogInserter) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			path := c.Request().URL.Path

			// Bypass check
			if path == "/" || path == "/healthz" || path == "/readyz" ||
				strings.HasPrefix(path, "/static") || strings.HasPrefix(path, "/webhooks/") {
				return next(c)
			}

			// Process request first to ensure the operation succeeded/completed
			err := next(c)

			// Get authenticated key from context
			val := c.Get("api_key")
			if apiKey, ok := val.(*repository.APIKey); ok {
				ip := c.RealIP()
				ua := c.Request().UserAgent()

				// Determine action name
				action := determineAction(c.Request().Method, path)

				// Determine metadata
				// Request payloads are deliberately excluded: message bodies,
				// recipients, media and provider credentials are not audit
				// metadata. The domain operation keeps its own durable record.
				metadataBytes, _ := json.Marshal(map[string]any{
					"method": c.Request().Method,
					"path":   path,
				})

				logEntry := &repository.UserActionLog{
					WorkspaceID: apiKey.WorkspaceID,
					ActorType:   "api_key",
					ActorID:     apiKey.ID.String(),
					ActorName:   apiKey.Name,
					Action:      action,
					Source:      "api",
					IPAddress:   &ip,
					UserAgent:   &ua,
					Metadata:    metadataBytes,
				}

				auditCtx, cancel := context.WithTimeout(context.WithoutCancel(c.Request().Context()), 2*time.Second)
				defer cancel()
				if insertErr := repo.Insert(auditCtx, logEntry); insertErr != nil {
					slog.Error("failed to insert audit log", "error", insertErr, "action", logEntry.Action)
				}
			}

			return err
		}
	}
}

func determineAction(method, path string) string {
	if strings.HasSuffix(path, "/messages") && method == http.MethodPost {
		return "message.send"
	}
	if strings.HasSuffix(path, "/me") && method == http.MethodGet {
		return "me.view"
	}

	// generic fallback resource.action
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) >= 3 { // e.g. api/v1/messages
		resource := parts[2]
		action := "view"
		switch method {
		case http.MethodPost:
			action = "create"
		case http.MethodPut, http.MethodPatch:
			action = "update"
		case http.MethodDelete:
			action = "delete"
		}
		return resource + "." + action
	}
	return "api.request"
}

// DashboardAuditMiddleware returns an Echo middleware that automatically audits operator dashboard actions.
func DashboardAuditMiddleware(repo ActionLogInserter) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			path := c.Request().URL.Path
			method := c.Request().Method

			// Only audit state-changing operations
			if method == http.MethodGet || method == http.MethodHead || method == http.MethodOptions {
				return next(c)
			}

			// Skip public login/logout endpoints
			if path == "/admin/login" || path == "/admin/logout" || path == "/admin/login/" || path == "/admin/logout/" {
				return next(c)
			}

			// Execute request
			err := next(c)
			if err != nil {
				return err
			}

			ip := c.RealIP()
			ua := c.Request().UserAgent()

			// Resolve workspace ID
			var workspaceID uuid.UUID
			if wsIDStr, _ := echo.PathParam[string](c, "workspace_id"); wsIDStr != "" {
				workspaceID, _ = uuid.Parse(wsIDStr)
			}
			if workspaceID == uuid.Nil {
				if cookie, cookieErr := c.Cookie("pergo-active-workspace"); cookieErr == nil && cookie != nil {
					workspaceID, _ = uuid.Parse(cookie.Value)
				}
			}

			if workspaceID != uuid.Nil {
				action := determineDashboardAction(method, path)
				metadataBytes, _ := json.Marshal(map[string]any{
					"method": method,
					"path":   path,
				})

				logEntry := &repository.UserActionLog{
					WorkspaceID: workspaceID,
					ActorType:   "user",
					ActorID:     "admin",
					ActorName:   "Administrador",
					Action:      action,
					Source:      "dashboard",
					IPAddress:   &ip,
					UserAgent:   &ua,
					Metadata:    metadataBytes,
				}

				auditCtx, cancel := context.WithTimeout(context.WithoutCancel(c.Request().Context()), 2*time.Second)
				defer cancel()
				if insertErr := repo.Insert(auditCtx, logEntry); insertErr != nil {
					slog.Error("failed to insert dashboard audit log", "error", insertErr, "action", logEntry.Action)
				}
			}

			return err
		}
	}
}

func determineDashboardAction(method, path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")

	resource := "dashboard"
	if len(parts) >= 2 {
		if parts[0] == "admin" && len(parts) >= 4 && parts[1] == "workspaces" {
			resource = parts[3]
		} else if parts[0] == "admin" {
			resource = parts[1]
		}
	}

	action := "action"
	switch method {
	case http.MethodPost:
		action = "create"
	case http.MethodPut, http.MethodPatch:
		action = "update"
	case http.MethodDelete:
		action = "delete"
	}

	// Normalize resource names
	if resource == "devices" {
		resource = "connection"
	} else if strings.HasSuffix(resource, "s") {
		resource = resource[:len(resource)-1]
	}

	return resource + "." + action
}
