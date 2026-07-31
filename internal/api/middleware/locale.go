package middleware

import (
	"bytes"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/pablojhp.pergo/internal/i18n"
)

// LocaleMiddleware resolves the UI language once per request.
func LocaleMiddleware() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			ctx := i18n.WithLocale(c.Request().Context(), i18n.Resolve(c.Request()))
			c.SetRequest(c.Request().WithContext(ctx))
			return next(c)
		}
	}
}

// HTMLLocalizer translates direct HTML responses produced by admin handlers.
// templ responses are localized by Render; this middleware covers error and
// confirmation fragments that use c.HTML directly.
func HTMLLocalizer() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			if strings.EqualFold(c.Request().Header.Get("Upgrade"), "websocket") {
				return next(c)
			}

			original := c.Response()
			buffered := &localizingResponseWriter{ResponseWriter: original, status: http.StatusOK}
			c.SetResponse(buffered)
			err := next(c)
			c.SetResponse(original)
			if err != nil || !buffered.wroteHeader {
				return err
			}

			body := buffered.body.Bytes()
			if strings.HasPrefix(strings.ToLower(buffered.Header().Get(echo.HeaderContentType)), "text/html") {
				body = []byte(i18n.LocalizeHTML(c.Request().Context(), string(body)))
			}
			buffered.Header().Del(echo.HeaderContentLength)
			original.WriteHeader(buffered.status)
			_, writeErr := original.Write(body)
			return writeErr
		}
	}
}

type localizingResponseWriter struct {
	http.ResponseWriter
	body        bytes.Buffer
	status      int
	wroteHeader bool
}

func (w *localizingResponseWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.status = status
	w.wroteHeader = true
}

func (w *localizingResponseWriter) Write(body []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.body.Write(body)
}

func (w *localizingResponseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
