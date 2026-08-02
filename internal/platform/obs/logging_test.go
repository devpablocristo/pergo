package obs

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestJSONLoggerRedactsSensitiveAttributes(t *testing.T) {
	t.Parallel()

	const (
		recipient = "+5491112345678"
		body      = "private message marker"
		token     = "secret-token-marker"
		traceID   = "trace-safe-123"
	)
	var output bytes.Buffer
	logger := NewJSONLogger(&output)
	logger.Info(
		"message accepted",
		"to",
		recipient,
		"body",
		body,
		"access_token",
		token,
		"trace_id",
		traceID,
		slog.Group(
			"provider",
			"phone_number",
			recipient,
			"phone_number_id",
			"meta-phone-id-sensitive",
		),
	)

	logged := output.String()
	for _, forbidden := range []string{recipient, body, token, "meta-phone-id-sensitive"} {
		if strings.Contains(logged, forbidden) {
			t.Fatalf("sensitive marker leaked in log: %s", logged)
		}
	}
	if !strings.Contains(logged, traceID) {
		t.Fatalf("operational trace ID was removed: %s", logged)
	}
	if strings.Count(logged, RedactedValue) != 5 {
		t.Fatalf("redaction count=%d log=%s", strings.Count(logged, RedactedValue), logged)
	}
}

func TestLoggerFromContextKeepsConfiguredRedaction(t *testing.T) {
	const recipient = "+5491198765432"
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(NewJSONLogger(&output))
	t.Cleanup(func() { slog.SetDefault(previous) })

	LoggerFromContext(t.Context()).Info(
		"message failed",
		"recipient_e164",
		recipient,
	)
	if strings.Contains(output.String(), recipient) {
		t.Fatalf("context logger bypassed redaction: %s", output.String())
	}
}
