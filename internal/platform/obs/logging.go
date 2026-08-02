// Package obs provides observability utilities for PerGo, including
// structured logging with trace context propagation.
package obs

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/pablojhp.pergo/internal/api/middleware"
)

type loggerKey struct{}

// RedactedValue is emitted instead of values whose attribute keys identify
// credentials, message content, or recipient/provider identities.
const RedactedValue = "[REDACTED]"

var sensitiveAttributeKeys = map[string]struct{}{
	"body":            {},
	"content":         {},
	"cuit":            {},
	"from":            {},
	"jid":             {},
	"payload":         {},
	"phone":           {},
	"phone_number":    {},
	"phone_number_id": {},
	"proxy_url":       {},
	"recipient":       {},
	"recipient_e164":  {},
	"request_body":    {},
	"response_body":   {},
	"sender":          {},
	"sender_identity": {},
	"tax_id":          {},
	"to":              {},
	"url":             {},
	"username":        {},
	"xml":             {},
}

var sensitiveAttributeFragments = []string{
	"authorization",
	"certificate",
	"cookie",
	"credential",
	"password",
	"secret",
	"token",
}

// NewJSONLogger creates the process JSON logger with defense-in-depth
// redaction for fields that must never be emitted in plaintext.
func NewJSONLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{
		ReplaceAttr: redactSensitiveAttribute,
	}))
}

func redactSensitiveAttribute(_ []string, attr slog.Attr) slog.Attr {
	key := strings.ToLower(strings.TrimSpace(attr.Key))
	key = strings.ReplaceAll(key, "-", "_")
	if _, sensitive := sensitiveAttributeKeys[key]; sensitive {
		return slog.String(attr.Key, RedactedValue)
	}
	for _, fragment := range sensitiveAttributeFragments {
		if strings.Contains(key, fragment) {
			return slog.String(attr.Key, RedactedValue)
		}
	}
	return attr
}

// NewLogger creates a slog.Logger with a trace_id attribute.
func NewLogger(traceID string) *slog.Logger {
	return NewJSONLogger(os.Stdout).With("trace_id", traceID)
}

// NewLoggerWithWriter creates a slog.Logger that writes to the given writer
// with a trace_id attribute. Useful for testing.
func NewLoggerWithWriter(traceID string, w io.Writer) *slog.Logger {
	return NewJSONLogger(w).With("trace_id", traceID)
}

// LoggerFromContext extracts the trace_id from context and returns a logger
// with the trace_id attribute. If no trace_id is present, a default logger
// is returned.
func LoggerFromContext(ctx context.Context) *slog.Logger {
	traceID, ok := middleware.TraceIDFrom(ctx)
	if !ok {
		return slog.Default()
	}
	return slog.Default().With("trace_id", traceID)
}

// WithContext stores a logger in the context.
func WithContext(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

// LoggerFromCtx retrieves a logger stored in the context. Falls back to
// slog.Default() if none is stored.
func LoggerFromCtx(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok {
		return l
	}
	return slog.Default()
}
