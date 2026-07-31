package whatsappmock

import (
	"context"
	"errors"
	"testing"

	"github.com/pablojhp.pergo/internal/channel"
)

func TestAdapterDispatchDeterministicAndNetworkFree(t *testing.T) {
	adapter := NewAdapter()
	payload := &channel.MessagePayload{
		MessageID: "message-ignored-when-trace-present",
		TraceID:   "trace-known-123",
		Body:      "must not influence provider ID",
	}

	first, err := adapter.Dispatch(context.Background(), payload)
	if err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	payload.Body = "different content"
	second, err := adapter.Dispatch(context.Background(), payload)
	if err != nil {
		t.Fatalf("second dispatch: %v", err)
	}
	if first != second {
		t.Fatalf("provider ID is not deterministic: %q != %q", first, second)
	}
	if first != "whatsapp-mock-60a0558755c642804fb381fdbb179bcf" {
		t.Fatalf("unexpected provider ID %q", first)
	}
}

func TestAdapterDispatchUsesMessageIDWhenTraceMissing(t *testing.T) {
	got, err := NewAdapter().Dispatch(context.Background(), &channel.MessagePayload{MessageID: "message-123"})
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if got != "whatsapp-mock-f7d5a40b066049550b3921ed89c04ea8" {
		t.Fatalf("unexpected provider ID %q", got)
	}
}

func TestAdapterDispatchRespectsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := NewAdapter().Dispatch(ctx, &channel.MessagePayload{TraceID: "trace"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}

func TestAdapterDispatchRejectsMissingIdentityTerminally(t *testing.T) {
	_, err := NewAdapter().Dispatch(context.Background(), &channel.MessagePayload{})
	if !channel.IsTerminal(err) {
		t.Fatalf("expected terminal error, got %v", err)
	}
}
