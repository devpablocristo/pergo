// Package whatsappmock provides a network-free WhatsApp dispatcher for local
// development and transport/orchestration tests.
package whatsappmock

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"

	"github.com/pablojhp.pergo/internal/channel"
)

const providerIDPrefix = "whatsapp-mock-"

// Adapter simulates successful WhatsApp sends without accessing any external
// provider or account. It is stateless and safe for concurrent use.
type Adapter struct{}

// NewAdapter creates a local WhatsApp mock dispatcher.
func NewAdapter() *Adapter {
	return &Adapter{}
}

// Dispatch returns a deterministic provider ID derived from the trace/message
// identity. Message content is deliberately excluded from logs.
func (a *Adapter) Dispatch(ctx context.Context, message *channel.MessagePayload) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if message == nil {
		return "", channel.NewTerminalError(errors.New("whatsapp mock: message payload is required"))
	}

	identity := message.TraceID
	if identity == "" {
		identity = message.MessageID
	}
	if identity == "" {
		return "", channel.NewTerminalError(errors.New("whatsapp mock: trace or message ID is required"))
	}

	sum := sha256.Sum256([]byte(identity))
	providerID := providerIDPrefix + hex.EncodeToString(sum[:16])
	slog.WarnContext(ctx, "whatsapp mock: simulated local send",
		"trace_id", message.TraceID,
		"message_id", message.MessageID,
		"connection_id", message.ConnectionID,
		"provider_message_id", providerID,
	)
	return providerID, nil
}

var _ channel.Dispatcher = (*Adapter)(nil)
