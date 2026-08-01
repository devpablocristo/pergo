package pages

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/pablojhp.pergo/internal/repository"
)

func TestInboxContentMountsModalsAndShowsMockSimulator(t *testing.T) {
	connection := &repository.Connection{
		ID:             uuid.New(),
		Channel:        "whatsapp_mock",
		SenderIdentity: "whatsapp-mock:test",
		Status:         "connected",
	}

	var rendered bytes.Buffer
	err := InboxContent(nil, nil, "", 0, nil, []*repository.Connection{connection}).Render(context.Background(), &rendered)
	if err != nil {
		t.Fatalf("render InboxContent: %v", err)
	}

	html := rendered.String()
	if !strings.Contains(html, `id="modal-container"`) {
		t.Fatal("InboxContent must mount the modal container")
	}
	if !strings.Contains(html, "Simular entrada") {
		t.Fatal("InboxContent must expose the local inbound simulator for a connected mock")
	}
}

func TestInboxContentHidesMockSimulatorWithoutConnectedMock(t *testing.T) {
	var rendered bytes.Buffer
	err := InboxContent(nil, nil, "", 0, nil, nil).Render(context.Background(), &rendered)
	if err != nil {
		t.Fatalf("render InboxContent: %v", err)
	}
	if strings.Contains(rendered.String(), "Simular entrada") {
		t.Fatal("InboxContent must hide the local inbound simulator without a connected mock")
	}
}
