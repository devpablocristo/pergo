package queue

import (
	"context"
	"strings"
	"testing"

	"github.com/pablojhp.pergo/internal/channel"
	"github.com/pablojhp.pergo/internal/domain"
)

func TestDispatchToChannelMissingRegistryIsTerminal(t *testing.T) {
	orchestrator := &DispatchOrchestrator{}
	_, err := orchestrator.dispatchToChannel(context.Background(), "whatsapp_mock", &domain.QueueMessage{TraceID: "trace-nil-registry"})
	if !channel.IsTerminal(err) {
		t.Fatalf("expected terminal error, got %v", err)
	}
	if !strings.Contains(err.Error(), "registry is not configured") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDispatchToChannelMissingEntryIsTerminal(t *testing.T) {
	orchestrator := &DispatchOrchestrator{dispatchers: channel.NewRegistry(nil)}
	_, err := orchestrator.dispatchToChannel(context.Background(), "whatsapp_mock", &domain.QueueMessage{TraceID: "trace-missing-entry"})
	if !channel.IsTerminal(err) {
		t.Fatalf("expected terminal error, got %v", err)
	}
	if !strings.Contains(err.Error(), "no dispatcher registered") {
		t.Fatalf("unexpected error: %v", err)
	}
}
