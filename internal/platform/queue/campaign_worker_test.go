package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pablojhp.pergo/internal/domain"
	"github.com/pablojhp.pergo/internal/platform/messagebus"
	"github.com/pablojhp.pergo/internal/repository"
)

func TestValidateCampaignBatchOutboundPayloadsRejectsExpandedMessage(t *testing.T) {
	channel := "whatsapp"
	body := "{{large}}"
	campaignID := uuid.New()
	workspaceID := uuid.New()
	campaign := &domain.Campaign{
		ID:           campaignID,
		WorkspaceID:  workspaceID,
		Channel:      &channel,
		TemplateName: &body,
	}
	task := CampaignBatchTask{
		CampaignID:   campaignID,
		WorkspaceID:  workspaceID,
		BatchIndex:   1,
		TotalBatches: 1,
		Recipients: []domain.CampaignRecipient{{
			To: "5511999999999",
			Variables: map[string]string{
				"large": strings.Repeat("x", messagebus.MaxPayloadBytes),
			},
		}},
	}

	err := ValidateCampaignBatchOutboundPayloads(
		campaign,
		task,
		uuid.New(),
		"5511988887777",
	)
	if !errors.Is(err, messagebus.ErrPayloadTooLarge) {
		t.Fatalf("error = %v, want ErrPayloadTooLarge", err)
	}
}

func TestValidateCampaignBatchOutboundPayloadsAcceptsBoundedMessage(t *testing.T) {
	channel := "whatsapp"
	body := "Hello {{name}}"
	campaignID := uuid.New()
	workspaceID := uuid.New()
	campaign := &domain.Campaign{
		ID:           campaignID,
		WorkspaceID:  workspaceID,
		Channel:      &channel,
		TemplateName: &body,
	}
	task := CampaignBatchTask{
		CampaignID:   campaignID,
		WorkspaceID:  workspaceID,
		BatchIndex:   1,
		TotalBatches: 1,
		Recipients: []domain.CampaignRecipient{{
			To:        "5511999999999",
			Variables: map[string]string{"name": "Maria"},
		}},
	}

	if err := ValidateCampaignBatchOutboundPayloads(
		campaign,
		task,
		uuid.New(),
		"5511988887777",
	); err != nil {
		t.Fatalf("validation failed: %v", err)
	}
}

func TestCampaignWorker_Success(t *testing.T) {
	nc := connectNATS(t)
	pool := getTestPool(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	_ = js.DeleteStream(ctx, CampaignStreamName)
	_ = js.DeleteStream(ctx, StreamName)
	t.Cleanup(func() {
		_ = js.DeleteStream(context.Background(), CampaignStreamName)
		_ = js.DeleteStream(context.Background(), StreamName)
	})

	// Initialize repos
	wsRepo := repository.NewWorkspaceRepository(pool)
	connRepo := repository.NewConnectionRepository(pool, nil)
	campRepo := repository.NewCampaignRepository(pool)
	dispatchRepo := repository.NewMessageDispatchRepository(pool)

	// Create workspace
	ws, err := wsRepo.Create(ctx, "camp_worker_ws_"+uuid.New().String())
	if err != nil {
		t.Fatalf("failed to create workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(ctx, ws.ID) }()

	// Ensure Streams
	_, err = EnsureCampaignStream(ctx, nc)
	if err != nil {
		t.Fatalf("EnsureCampaignStream failed: %v", err)
	}

	messagesStream, err := EnsureStream(ctx, nc)
	if err != nil {
		t.Fatalf("EnsureStream failed: %v", err)
	}

	consumerName := "test-campaign-worker-consumer-" + uuid.New().String()
	campStream, err := js.Stream(ctx, CampaignStreamName)
	if err != nil {
		t.Fatalf("get campaigns stream failed: %v", err)
	}

	consumer, err := EnsureCampaignConsumer(ctx, campStream, consumerName)
	if err != nil {
		t.Fatalf("EnsureCampaignConsumer failed: %v", err)
	}

	// Create campaign
	tmplName := "Ola {{nome}}!"
	channel := "whatsapp"
	camp := &domain.Campaign{
		WorkspaceID:  ws.ID,
		Name:         "Success Camp",
		Status:       domain.CampaignStatusDraft,
		BatchSize:    1,
		DelaySeconds: 1,
		TemplateName: &tmplName,
		Channel:      &channel,
		Recipients: []domain.CampaignRecipient{
			{To: "5511999998888", Variables: map[string]string{"nome": "Maria"}},
		},
	}
	camp, err = campRepo.Create(ctx, camp)
	if err != nil {
		t.Fatalf("failed to create campaign: %v", err)
	}

	// Create messages outbound consumer to verify worker sends messages
	outboundConsumer, err := EnsureConsumer(ctx, messagesStream, "test-outbound-verifier-"+uuid.New().String())
	if err != nil {
		t.Fatalf("EnsureConsumer for MESSAGES failed: %v", err)
	}

	// Publish batch task
	publisher := NewJetStreamPublisher(nc)
	task := CampaignBatchTask{
		CampaignID:   camp.ID,
		WorkspaceID:  ws.ID,
		BatchIndex:   1,
		TotalBatches: 1,
		Recipients:   camp.Recipients,
		DelaySeconds: 0, // no delay for test speed
	}
	taskBytes, _ := json.Marshal(task)
	if _, err := campRepo.PrepareCampaignStart(ctx, camp.ID, ws.ID, camp.UpdatedAt, []repository.CampaignBatch{{
		BatchIndex:   1,
		TotalBatches: 1,
		TraceID:      fmt.Sprintf("campaign_%s_batch_1", camp.ID),
		Payload:      taskBytes,
	}}); err != nil {
		t.Fatalf("prepare durable campaign batch: %v", err)
	}

	// Start Worker. Its outbox relay must publish the durable batch.
	worker := NewCampaignWorker(ctx, consumer, campRepo, connRepo, dispatchRepo, publisher)
	defer worker.Stop()

	// Wait for completion in database
	var finalCamp *domain.Campaign
	for i := 0; i < 20; i++ {
		finalCamp, _ = campRepo.GetByID(ctx, camp.ID)
		if finalCamp.Status == domain.CampaignStatusCompleted {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if finalCamp.Status != domain.CampaignStatusCompleted {
		t.Fatalf("campaign expected to be completed, got: %s", finalCamp.Status)
	}

	// Verify dispatch record was created
	traceID := fmt.Sprintf("campaign_%s_batch_1_recipient_1", camp.ID.String())
	disp, err := dispatchRepo.GetByTraceID(ctx, ws.ID, traceID)
	if err != nil {
		t.Fatalf("failed to fetch dispatch log: %v", err)
	}
	if disp.CampaignID == nil || *disp.CampaignID != camp.ID {
		t.Errorf("dispatch CampaignID mismatch")
	}

	// Verify NATS outbound queue message was received
	msgs, err := outboundConsumer.Messages()
	if err != nil {
		t.Fatalf("failed to get messages context: %v", err)
	}
	defer msgs.Stop()

	msg, err := msgs.Next()
	if err != nil {
		t.Fatalf("failed to get message: %v", err)
	}
	var qMsg domain.QueueMessage
	_ = json.Unmarshal(msg.Data(), &qMsg)

	if qMsg.To != "5511999998888" {
		t.Errorf("expected QueueMessage.To 5511999998888, got %s", qMsg.To)
	}
	if qMsg.Body != "Ola Maria!" {
		t.Errorf("expected QueueMessage.Body 'Ola Maria!', got %s", qMsg.Body)
	}
	_ = msg.Ack()
}

func TestCampaignWorker_Cancelled(t *testing.T) {
	nc := connectNATS(t)
	pool := getTestPool(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wsRepo := repository.NewWorkspaceRepository(pool)
	connRepo := repository.NewConnectionRepository(pool, nil)
	campRepo := repository.NewCampaignRepository(pool)
	dispatchRepo := repository.NewMessageDispatchRepository(pool)

	ws, _ := wsRepo.Create(ctx, "camp_worker_ws_cancel_"+uuid.New().String())
	defer func() { _ = wsRepo.Delete(ctx, ws.ID) }()

	js, _ := jetstream.New(nc)
	_ = js.DeleteStream(ctx, CampaignStreamName)
	t.Cleanup(func() {
		_ = js.DeleteStream(context.Background(), CampaignStreamName)
	})
	_, _ = EnsureCampaignStream(ctx, nc)
	campStream, _ := js.Stream(ctx, CampaignStreamName)
	consumerName := "test-cancel-consumer-" + uuid.New().String()
	consumer, _ := EnsureCampaignConsumer(ctx, campStream, consumerName)

	// Create campaign
	camp := &domain.Campaign{
		WorkspaceID:  ws.ID,
		Name:         "Cancel Camp",
		Status:       domain.CampaignStatusDraft,
		BatchSize:    1,
		DelaySeconds: 1,
	}
	camp, _ = campRepo.Create(ctx, camp)

	publisher := NewJetStreamPublisher(nc)
	task := CampaignBatchTask{
		CampaignID:   camp.ID,
		WorkspaceID:  ws.ID,
		BatchIndex:   1,
		TotalBatches: 1,
		Recipients: []domain.CampaignRecipient{
			{To: "5511999998888", Variables: map[string]string{"nome": "Maria"}},
		},
		DelaySeconds: 0,
	}
	taskBytes, _ := json.Marshal(task)
	if _, err := campRepo.PrepareCampaignStart(ctx, camp.ID, ws.ID, camp.UpdatedAt, []repository.CampaignBatch{{
		BatchIndex:   1,
		TotalBatches: 1,
		TraceID:      fmt.Sprintf("campaign_%s_batch_1", camp.ID),
		Payload:      taskBytes,
	}}); err != nil {
		t.Fatalf("prepare durable campaign batch: %v", err)
	}
	if err := campRepo.CancelForWorkspace(ctx, camp.ID, ws.ID); err != nil {
		t.Fatalf("cancel campaign: %v", err)
	}
	_ = publisher.Publish(ctx, CampaignBatchSubject, taskBytes, uuid.New().String())

	worker := NewCampaignWorker(ctx, consumer, campRepo, connRepo, dispatchRepo, publisher)
	defer worker.Stop()

	// Wait to see if NATS message gets Acked without creating dispatches
	// Fetch from the campaigns stream consumer to verify it is empty (acked)
	time.Sleep(500 * time.Millisecond)

	traceID := fmt.Sprintf("campaign_%s_batch_1_recipient_1", camp.ID.String())
	_, err := dispatchRepo.GetByTraceID(ctx, ws.ID, traceID)
	if err == nil {
		t.Errorf("expected no dispatch log for cancelled campaign, but found one")
	}
}

func TestCampaignWorkerStopInterruptsActiveBatch(t *testing.T) {
	nc := connectNATS(t)
	pool := getTestPool(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	wsRepo := repository.NewWorkspaceRepository(pool)
	connRepo := repository.NewConnectionRepository(pool, nil)
	campRepo := repository.NewCampaignRepository(pool)
	dispatchRepo := repository.NewMessageDispatchRepository(pool)

	ws, err := wsRepo.Create(ctx, "camp_worker_ws_active_stop_"+uuid.New().String())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(context.Background(), ws.ID) }()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	_ = js.DeleteStream(ctx, CampaignStreamName)
	_ = js.DeleteStream(ctx, StreamName)
	t.Cleanup(func() {
		_ = js.DeleteStream(context.Background(), CampaignStreamName)
		_ = js.DeleteStream(context.Background(), StreamName)
	})
	stream, err := EnsureCampaignStream(ctx, nc)
	if err != nil {
		t.Fatalf("EnsureCampaignStream: %v", err)
	}
	if _, err := EnsureStream(ctx, nc); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	consumer, err := EnsureCampaignConsumer(ctx, stream, "campaign-active-stop-"+uuid.New().String())
	if err != nil {
		t.Fatalf("EnsureCampaignConsumer: %v", err)
	}

	channel := "whatsapp"
	camp, err := campRepo.Create(ctx, &domain.Campaign{
		WorkspaceID:  ws.ID,
		Name:         "Active Stop",
		Status:       domain.CampaignStatusDraft,
		BatchSize:    1000,
		DelaySeconds: 0,
		Channel:      &channel,
	})
	if err != nil {
		t.Fatalf("create campaign: %v", err)
	}

	recipients := make([]domain.CampaignRecipient, 1000)
	for i := range recipients {
		recipients[i] = domain.CampaignRecipient{To: fmt.Sprintf("5511%09d", i)}
	}
	taskBytes, err := json.Marshal(CampaignBatchTask{
		CampaignID:   camp.ID,
		WorkspaceID:  ws.ID,
		BatchIndex:   1,
		TotalBatches: 1,
		Recipients:   recipients,
	})
	if err != nil {
		t.Fatalf("marshal task: %v", err)
	}
	publisher := NewJetStreamPublisher(nc)
	if _, err := campRepo.PrepareCampaignStart(ctx, camp.ID, ws.ID, camp.UpdatedAt, []repository.CampaignBatch{{
		BatchIndex:   1,
		TotalBatches: 1,
		TraceID:      fmt.Sprintf("campaign_%s_batch_1", camp.ID),
		Payload:      taskBytes,
	}}); err != nil {
		t.Fatalf("prepare durable campaign batch: %v", err)
	}

	worker := NewCampaignWorker(ctx, consumer, campRepo, connRepo, dispatchRepo, publisher)
	t.Cleanup(worker.Stop)

	var beforeStop int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := pool.QueryRow(
			ctx,
			`SELECT count(*) FROM message_dispatches WHERE campaign_id = $1`,
			camp.ID,
		).Scan(&beforeStop); err != nil {
			t.Fatalf("count dispatches: %v", err)
		}
		if beforeStop > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if beforeStop == 0 {
		worker.Stop()
		t.Fatal("worker never entered the active recipient loop")
	}

	stopStarted := time.Now()
	worker.Stop()
	if elapsed := time.Since(stopStarted); elapsed > time.Second {
		t.Fatalf("Stop took %s while an active batch was running", elapsed)
	}

	var afterStop int
	if err := pool.QueryRow(
		context.Background(),
		`SELECT count(*) FROM message_dispatches WHERE campaign_id = $1`,
		camp.ID,
	).Scan(&afterStop); err != nil {
		t.Fatalf("count dispatches after stop: %v", err)
	}
	if afterStop >= len(recipients) {
		t.Fatalf("active batch was not interrupted: processed %d/%d recipients", afterStop, len(recipients))
	}
}

func TestCampaignWorkerRetriesWholeBatchAndCompletesOnlyAfterEveryPublish(t *testing.T) {
	pool := getTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wsRepo := repository.NewWorkspaceRepository(pool)
	campRepo := repository.NewCampaignRepository(pool)
	dispatchRepo := repository.NewMessageDispatchRepository(pool)
	ws, err := wsRepo.Create(ctx, "camp_worker_retry_"+uuid.NewString())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(context.Background(), ws.ID) }()

	channel := "whatsapp"
	camp, err := campRepo.Create(ctx, &domain.Campaign{
		WorkspaceID: ws.ID,
		Name:        "Retry every recipient",
		Status:      domain.CampaignStatusDraft,
		BatchSize:   2,
		Channel:     &channel,
		Recipients: []domain.CampaignRecipient{
			{To: "5511999990001"},
			{To: "5511999990002"},
		},
	})
	if err != nil {
		t.Fatalf("create campaign: %v", err)
	}

	task := CampaignBatchTask{
		CampaignID:   camp.ID,
		WorkspaceID:  ws.ID,
		BatchIndex:   1,
		TotalBatches: 1,
		Recipients:   camp.Recipients,
	}
	payload, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal task: %v", err)
	}
	if _, err := campRepo.PrepareCampaignStart(ctx, camp.ID, ws.ID, camp.UpdatedAt, []repository.CampaignBatch{{
		BatchIndex:   1,
		TotalBatches: 1,
		TraceID:      fmt.Sprintf("campaign_%s_batch_1", camp.ID),
		Payload:      payload,
	}}); err != nil {
		t.Fatalf("prepare campaign: %v", err)
	}

	publisher := &failOnceCampaignPublisher{failCall: 2}
	worker := &CampaignWorker{
		campaignRepo: campRepo,
		dispatchRepo: dispatchRepo,
		publisher:    publisher,
	}

	first := &campaignTestMsg{data: payload}
	worker.processBatch(ctx, first)
	if first.acks != 0 || first.naks != 1 {
		t.Fatalf("first delivery ack/nak = %d/%d, want 0/1", first.acks, first.naks)
	}
	stored, err := campRepo.GetByID(ctx, camp.ID)
	if err != nil {
		t.Fatalf("get campaign after failure: %v", err)
	}
	if stored.Status != domain.CampaignStatusSending {
		t.Fatalf("campaign status after partial publish = %s, want sending", stored.Status)
	}

	second := &campaignTestMsg{data: payload}
	worker.processBatch(ctx, second)
	if second.acks != 1 || second.naks != 0 {
		t.Fatalf("second delivery ack/nak = %d/%d, want 1/0", second.acks, second.naks)
	}
	stored, err = campRepo.GetByID(ctx, camp.ID)
	if err != nil {
		t.Fatalf("get completed campaign: %v", err)
	}
	if stored.Status != domain.CampaignStatusCompleted {
		t.Fatalf("campaign status after recovered publish = %s, want completed", stored.Status)
	}

	calls := publisher.TraceIDs()
	want := []string{
		fmt.Sprintf("campaign_%s_batch_1_recipient_1", camp.ID),
		fmt.Sprintf("campaign_%s_batch_1_recipient_2", camp.ID),
		fmt.Sprintf("campaign_%s_batch_1_recipient_1", camp.ID),
		fmt.Sprintf("campaign_%s_batch_1_recipient_2", camp.ID),
	}
	if fmt.Sprint(calls) != fmt.Sprint(want) {
		t.Fatalf("publish trace sequence = %v, want %v", calls, want)
	}
	for _, traceID := range calls {
		if strings.Contains(traceID, "5511") {
			t.Fatalf("campaign trace leaked recipient phone: %q", traceID)
		}
	}
}

func TestCampaignWorkerUsesDurableWABATemplateSnapshot(t *testing.T) {
	pool := getTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	workspaceRepo := repository.NewWorkspaceRepository(pool)
	campaignRepo := repository.NewCampaignRepository(pool)
	dispatchRepo := repository.NewMessageDispatchRepository(pool)
	workspace, err := workspaceRepo.Create(ctx, "campaign_waba_snapshot_"+uuid.NewString())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _ = workspaceRepo.Delete(context.Background(), workspace.ID) }()

	channel := "whatsapp_cloud"
	templateName := "appointment_confirmation"
	templateLanguage := "en_US"
	campaign, err := campaignRepo.Create(ctx, &domain.Campaign{
		WorkspaceID:      workspace.ID,
		Name:             "Frozen WABA snapshot",
		Status:           domain.CampaignStatusDraft,
		BatchSize:        1,
		Channel:          &channel,
		TemplateName:     &templateName,
		TemplateLanguage: &templateLanguage,
		Recipients: []domain.CampaignRecipient{{
			To:        "5511999990001",
			Variables: map[string]string{"1": "Alice"},
		}},
	})
	if err != nil {
		t.Fatalf("create campaign: %v", err)
	}

	task := CampaignBatchTask{
		CampaignID:   campaign.ID,
		WorkspaceID:  workspace.ID,
		BatchIndex:   1,
		TotalBatches: 1,
		Recipients:   campaign.Recipients,
		TemplateSnapshot: &CampaignTemplateSnapshot{
			Language:           templateLanguage,
			BodyParameterCount: 1,
		},
	}
	payload, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal WABA task: %v", err)
	}
	if _, err := campaignRepo.PrepareCampaignStart(
		ctx,
		campaign.ID,
		workspace.ID,
		campaign.UpdatedAt,
		[]repository.CampaignBatch{{
			BatchIndex:   1,
			TotalBatches: 1,
			TraceID:      fmt.Sprintf("campaign_%s_batch_1", campaign.ID),
			Payload:      payload,
		}},
	); err != nil {
		t.Fatalf("prepare WABA campaign: %v", err)
	}

	publisher := &recordingCampaignPublisher{}
	worker := &CampaignWorker{
		campaignRepo: campaignRepo,
		dispatchRepo: dispatchRepo,
		publisher:    publisher,
	}
	message := &campaignTestMsg{data: payload}
	worker.processBatch(ctx, message)
	if message.acks != 1 || message.naks != 0 {
		t.Fatalf("WABA batch ack/nak = %d/%d, want 1/0", message.acks, message.naks)
	}

	published := publisher.Payloads()
	if len(published) != 1 {
		t.Fatalf("published messages = %d, want 1", len(published))
	}
	var outbound domain.QueueMessage
	if err := json.Unmarshal(published[0], &outbound); err != nil {
		t.Fatalf("decode outbound WABA message: %v", err)
	}
	if outbound.TemplateName != templateName || outbound.Language != templateLanguage {
		t.Fatalf(
			"outbound template = %q/%q, want %q/%q",
			outbound.TemplateName,
			outbound.Language,
			templateName,
			templateLanguage,
		)
	}
	if len(outbound.Components) != 1 ||
		len(outbound.Components[0].Parameters) != 1 ||
		outbound.Components[0].Parameters[0].Text != "Alice" {
		t.Fatalf("outbound template components = %#v", outbound.Components)
	}
}

func TestCampaignWorkerDoesNotCompleteWABAWithoutTemplateSnapshot(t *testing.T) {
	pool := getTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	workspaceRepo := repository.NewWorkspaceRepository(pool)
	campaignRepo := repository.NewCampaignRepository(pool)
	dispatchRepo := repository.NewMessageDispatchRepository(pool)
	workspace, err := workspaceRepo.Create(ctx, "campaign_waba_missing_snapshot_"+uuid.NewString())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _ = workspaceRepo.Delete(context.Background(), workspace.ID) }()

	channel := "whatsapp_cloud"
	templateName := "missing_snapshot"
	campaign, err := campaignRepo.Create(ctx, &domain.Campaign{
		WorkspaceID:  workspace.ID,
		Name:         "Missing WABA snapshot",
		Status:       domain.CampaignStatusDraft,
		BatchSize:    1,
		Channel:      &channel,
		TemplateName: &templateName,
		Recipients: []domain.CampaignRecipient{{
			To:        "5511999990001",
			Variables: map[string]string{"1": "Alice"},
		}},
	})
	if err != nil {
		t.Fatalf("create campaign: %v", err)
	}
	task := CampaignBatchTask{
		CampaignID:   campaign.ID,
		WorkspaceID:  workspace.ID,
		BatchIndex:   1,
		TotalBatches: 1,
		Recipients:   campaign.Recipients,
	}
	payload, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal WABA task: %v", err)
	}
	if _, err := campaignRepo.PrepareCampaignStart(
		ctx,
		campaign.ID,
		workspace.ID,
		campaign.UpdatedAt,
		[]repository.CampaignBatch{{
			BatchIndex:   1,
			TotalBatches: 1,
			TraceID:      fmt.Sprintf("campaign_%s_batch_1", campaign.ID),
			Payload:      payload,
		}},
	); err != nil {
		t.Fatalf("prepare WABA campaign: %v", err)
	}

	publisher := &recordingCampaignPublisher{}
	worker := &CampaignWorker{
		campaignRepo: campaignRepo,
		dispatchRepo: dispatchRepo,
		publisher:    publisher,
	}
	message := &campaignTestMsg{data: payload}
	worker.processBatch(ctx, message)
	if message.acks != 0 || message.naks != 1 {
		t.Fatalf("invalid WABA batch ack/nak = %d/%d, want 0/1", message.acks, message.naks)
	}
	stored, err := campaignRepo.GetByID(ctx, campaign.ID)
	if err != nil {
		t.Fatalf("get invalid WABA campaign: %v", err)
	}
	if stored.Status != domain.CampaignStatusSending {
		t.Fatalf("invalid WABA campaign status = %s, want sending", stored.Status)
	}
	if len(publisher.Payloads()) != 0 {
		t.Fatal("invalid WABA snapshot published an outbound message")
	}
}

func TestCampaignWorkerHeartbeatsSlowProviderByElapsedTime(t *testing.T) {
	pool := getTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wsRepo := repository.NewWorkspaceRepository(pool)
	campRepo := repository.NewCampaignRepository(pool)
	dispatchRepo := repository.NewMessageDispatchRepository(pool)
	workspace, err := wsRepo.Create(ctx, "campaign_heartbeat_"+uuid.NewString())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(context.Background(), workspace.ID) }()

	channel := "whatsapp"
	campaign, err := campRepo.Create(ctx, &domain.Campaign{
		WorkspaceID: workspace.ID,
		Name:        "Slow provider heartbeat",
		Status:      domain.CampaignStatusDraft,
		BatchSize:   1,
		Channel:     &channel,
		Recipients:  []domain.CampaignRecipient{{To: "5511999990001"}},
	})
	if err != nil {
		t.Fatalf("create campaign: %v", err)
	}
	task := CampaignBatchTask{
		CampaignID:   campaign.ID,
		WorkspaceID:  workspace.ID,
		BatchIndex:   1,
		TotalBatches: 1,
		Recipients:   campaign.Recipients,
	}
	payload, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal task: %v", err)
	}
	if _, err := campRepo.PrepareCampaignStart(
		ctx,
		campaign.ID,
		workspace.ID,
		campaign.UpdatedAt,
		[]repository.CampaignBatch{{
			BatchIndex:   1,
			TotalBatches: 1,
			TraceID:      fmt.Sprintf("campaign_%s_batch_1", campaign.ID),
			Payload:      payload,
		}},
	); err != nil {
		t.Fatalf("prepare campaign: %v", err)
	}

	worker := &CampaignWorker{
		campaignRepo:   campRepo,
		dispatchRepo:   dispatchRepo,
		publisher:      &failOnceCampaignPublisher{delay: 40 * time.Millisecond},
		heartbeatEvery: 5 * time.Millisecond,
	}
	msg := &campaignTestMsg{data: payload}
	worker.processBatch(ctx, msg)

	if msg.acks != 1 || msg.naks != 0 {
		t.Fatalf("slow batch ack/nak = %d/%d, want 1/0", msg.acks, msg.naks)
	}
	if msg.inProgress < 3 {
		t.Fatalf("slow provider emitted %d temporal heartbeats, want at least 3", msg.inProgress)
	}
}

func TestCampaignBatchOutboxRecoversPublishFailureWithoutLosingRecipients(t *testing.T) {
	pool := getTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wsRepo := repository.NewWorkspaceRepository(pool)
	campRepo := repository.NewCampaignRepository(pool)
	ws, err := wsRepo.Create(ctx, "campaign_outbox_recovery_"+uuid.NewString())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(context.Background(), ws.ID) }()

	campaign, err := campRepo.Create(ctx, &domain.Campaign{
		WorkspaceID: ws.ID,
		Name:        "Recover enqueue",
		Status:      domain.CampaignStatusDraft,
		BatchSize:   1,
		Recipients: []domain.CampaignRecipient{
			{To: "5511999990001"},
		},
	})
	if err != nil {
		t.Fatalf("create campaign: %v", err)
	}
	taskPayload, err := json.Marshal(CampaignBatchTask{
		CampaignID:   campaign.ID,
		WorkspaceID:  ws.ID,
		BatchIndex:   1,
		TotalBatches: 1,
		Recipients:   campaign.Recipients,
	})
	if err != nil {
		t.Fatalf("marshal task: %v", err)
	}
	baseTraceID := fmt.Sprintf("campaign_%s_batch_1", campaign.ID)
	if _, err := campRepo.PrepareCampaignStart(ctx, campaign.ID, ws.ID, campaign.UpdatedAt, []repository.CampaignBatch{{
		BatchIndex:   1,
		TotalBatches: 1,
		TraceID:      baseTraceID,
		Payload:      taskPayload,
	}}); err != nil {
		t.Fatalf("prepare campaign: %v", err)
	}

	publisher := &failOnceCampaignPublisher{failCall: 1}
	worker := &CampaignWorker{campaignRepo: campRepo, publisher: publisher}
	replacementWorker := &CampaignWorker{campaignRepo: campRepo, publisher: publisher}
	replacementWorker.enqueueDueBatches(ctx)

	var attempts int
	var lastError *string
	var processed bool
	if err := pool.QueryRow(
		ctx,
		`SELECT publish_attempts, last_error, processed_at IS NOT NULL
		   FROM campaign_batches
		  WHERE campaign_id = $1 AND batch_index = 1`,
		campaign.ID,
	).Scan(&attempts, &lastError, &processed); err != nil {
		t.Fatalf("read failed outbox attempt: %v", err)
	}
	if attempts != 1 || lastError == nil || *lastError != "CAMPAIGN_BATCH_PUBLISH_FAILED" || processed {
		t.Fatalf("failed outbox state = attempts:%d error:%v processed:%t", attempts, lastError, processed)
	}
	stored, err := campRepo.GetByID(ctx, campaign.ID)
	if err != nil {
		t.Fatalf("get campaign after enqueue failure: %v", err)
	}
	if stored.Status != domain.CampaignStatusSending {
		t.Fatalf("enqueue failure changed campaign status to %s", stored.Status)
	}

	// Make the durable retry due immediately, as if its backoff elapsed or a
	// replacement worker recovered the row after restart.
	if _, err := pool.Exec(
		ctx,
		`UPDATE campaign_batches
		    SET next_publish_at = now()
		  WHERE campaign_id = $1 AND batch_index = 1`,
		campaign.ID,
	); err != nil {
		t.Fatalf("make outbox retry due: %v", err)
	}
	worker.enqueueDueBatches(ctx)

	var publishedAt *time.Time
	if err := pool.QueryRow(
		ctx,
		`SELECT publish_attempts, last_error, last_published_at
		   FROM campaign_batches
		  WHERE campaign_id = $1 AND batch_index = 1`,
		campaign.ID,
	).Scan(&attempts, &lastError, &publishedAt); err != nil {
		t.Fatalf("read recovered outbox attempt: %v", err)
	}
	if attempts != 2 || lastError != nil || publishedAt == nil {
		t.Fatalf("recovered outbox state = attempts:%d error:%v published_at:%v", attempts, lastError, publishedAt)
	}
	wantCalls := []string{baseTraceID + "_attempt_1", baseTraceID + "_attempt_2"}
	if calls := publisher.TraceIDs(); fmt.Sprint(calls) != fmt.Sprint(wantCalls) {
		t.Fatalf("outbox publish IDs = %v, want %v", calls, wantCalls)
	}
}

type failOnceCampaignPublisher struct {
	mu       sync.Mutex
	failCall int
	calls    []string
	delay    time.Duration
}

type recordingCampaignPublisher struct {
	mu       sync.Mutex
	payloads [][]byte
}

func (p *recordingCampaignPublisher) Publish(_ context.Context, _ string, data []byte, _ string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.payloads = append(p.payloads, append([]byte(nil), data...))
	return nil
}

func (p *recordingCampaignPublisher) Payloads() [][]byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	payloads := make([][]byte, len(p.payloads))
	for index := range p.payloads {
		payloads[index] = append([]byte(nil), p.payloads[index]...)
	}
	return payloads
}

func (p *failOnceCampaignPublisher) Publish(ctx context.Context, _ string, _ []byte, traceID string) error {
	if p.delay > 0 {
		timer := time.NewTimer(p.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, traceID)
	if len(p.calls) == p.failCall {
		return errors.New("injected publish failure")
	}
	return nil
}

func (p *failOnceCampaignPublisher) TraceIDs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.calls...)
}

type campaignTestMsg struct {
	data       []byte
	acks       int
	naks       int
	terminated int
	inProgress int
}

func (m *campaignTestMsg) Metadata() (*jetstream.MsgMetadata, error) { return nil, nil }
func (m *campaignTestMsg) Data() []byte                              { return m.data }
func (m *campaignTestMsg) Headers() nats.Header                      { return nil }
func (m *campaignTestMsg) Subject() string                           { return CampaignBatchSubject }
func (m *campaignTestMsg) Reply() string                             { return "" }
func (m *campaignTestMsg) Ack() error {
	m.acks++
	return nil
}
func (m *campaignTestMsg) DoubleAck(context.Context) error { return m.Ack() }
func (m *campaignTestMsg) Nak() error {
	m.naks++
	return nil
}
func (m *campaignTestMsg) NakWithDelay(time.Duration) error { return m.Nak() }
func (m *campaignTestMsg) InProgress() error {
	m.inProgress++
	return nil
}
func (m *campaignTestMsg) Term() error {
	m.terminated++
	return nil
}
func (m *campaignTestMsg) TermWithReason(string) error { return m.Term() }

func TestCampaignWorkerStopIsPromptBeforeAndAfterConsumerStartup(t *testing.T) {
	for _, delay := range []time.Duration{0, 50 * time.Millisecond} {
		t.Run(delay.String(), func(t *testing.T) {
			nc := connectNATS(t)
			ctx := context.Background()
			js, err := jetstream.New(nc)
			if err != nil {
				t.Fatalf("jetstream.New: %v", err)
			}
			_ = js.DeleteStream(ctx, CampaignStreamName)
			t.Cleanup(func() {
				_ = js.DeleteStream(context.Background(), CampaignStreamName)
			})
			stream, err := EnsureCampaignStream(ctx, nc)
			if err != nil {
				t.Fatalf("EnsureCampaignStream: %v", err)
			}
			consumer, err := EnsureCampaignConsumer(ctx, stream, "campaign-stop-"+uuid.New().String())
			if err != nil {
				t.Fatalf("EnsureCampaignConsumer: %v", err)
			}

			worker := NewCampaignWorker(ctx, consumer, nil, nil, nil, nil)
			time.Sleep(delay)
			done := make(chan struct{})
			go func() {
				worker.Stop()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("CampaignWorker.Stop did not promptly unblock MessagesContext.Next")
			}
		})
	}
}
