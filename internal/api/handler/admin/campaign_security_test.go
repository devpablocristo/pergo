package admin

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/pablojhp.pergo/internal/domain"
	"github.com/pablojhp.pergo/internal/platform/queue"
	"github.com/pablojhp.pergo/internal/repository"
)

func TestNeutralizeCampaignCSVCell(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "formula", input: "=HYPERLINK(\"https://example.invalid\")", want: "'=HYPERLINK(\"https://example.invalid\")"},
		{name: "leading whitespace", input: " \t@SUM(1,1)", want: "' \t@SUM(1,1)"},
		{name: "plus", input: "+1+1", want: "'+1+1"},
		{name: "minus", input: "-1+1", want: "'-1+1"},
		{name: "plain", input: "invalid-phone,Bad", want: "invalid-phone,Bad"},
		{name: "empty", input: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := neutralizeCampaignCSVCell(tt.input); got != tt.want {
				t.Fatalf("neutralizeCampaignCSVCell(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestBuildCampaignBatchesSplitsBeforeNATSMaxPayload(t *testing.T) {
	const recipientsCount = 300
	recipients := make([]domain.CampaignRecipient, recipientsCount)
	for index := range recipients {
		recipients[index] = domain.CampaignRecipient{
			To: "5511999999999",
			Variables: map[string]string{
				"large": strings.Repeat("x", 8<<10),
			},
		}
	}
	campaign := &domain.Campaign{
		ID:           uuid.New(),
		WorkspaceID:  uuid.New(),
		BatchSize:    maxCampaignBatchSize,
		DelaySeconds: 1,
		Recipients:   recipients,
	}

	batches, err := buildCampaignBatches(campaign, nil, json.Marshal)
	if err != nil {
		t.Fatalf("buildCampaignBatches: %v", err)
	}
	if len(batches) < 2 {
		t.Fatalf("batches = %d, want byte-aware split", len(batches))
	}

	seenRecipients := 0
	for index, batch := range batches {
		if len(batch.Payload) > repository.MaxCampaignBatchPayloadBytes {
			t.Fatalf(
				"batch %d payload = %d bytes, max %d",
				index+1,
				len(batch.Payload),
				repository.MaxCampaignBatchPayloadBytes,
			)
		}
		var task queue.CampaignBatchTask
		if err := json.Unmarshal(batch.Payload, &task); err != nil {
			t.Fatalf("decode batch %d: %v", index+1, err)
		}
		if task.BatchIndex != index+1 || task.TotalBatches != len(batches) {
			t.Fatalf(
				"batch identity = %d/%d, want %d/%d",
				task.BatchIndex,
				task.TotalBatches,
				index+1,
				len(batches),
			)
		}
		seenRecipients += len(task.Recipients)
	}
	if seenRecipients != recipientsCount {
		t.Fatalf("reconstructed recipients = %d, want %d", seenRecipients, recipientsCount)
	}
}

func TestBuildCampaignBatchesRejectsSingleUnpublishableRecipient(t *testing.T) {
	campaign := &domain.Campaign{
		ID:          uuid.New(),
		WorkspaceID: uuid.New(),
		BatchSize:   1,
		Recipients: []domain.CampaignRecipient{{
			To: "5511999999999",
		}},
	}
	oversizedMarshal := func(any) ([]byte, error) {
		return make([]byte, repository.MaxCampaignBatchPayloadBytes+1), nil
	}

	_, err := buildCampaignBatches(campaign, nil, oversizedMarshal)
	if !errors.Is(err, errCampaignBatchTooLarge) {
		t.Fatalf("error = %v, want errCampaignBatchTooLarge", err)
	}
}
