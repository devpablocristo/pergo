package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pablojhp.pergo/internal/domain"
	"github.com/pablojhp.pergo/internal/platform/messagebus"
	"github.com/pablojhp.pergo/internal/repository"
)

const (
	campaignBatchLeaseDuration = 30 * time.Second
	campaignBatchRecoveryAfter = time.Minute
	campaignBatchPollInterval  = time.Second
	campaignBatchRetryDelay    = 2 * time.Second
	campaignHeartbeatInterval  = 20 * time.Second
	campaignWorkerConcurrency  = campaignConsumerMaxPending
	campaignMaxTemplateParams  = 64
)

// CampaignBatchTask represents the payload for a campaign batch message.
type CampaignBatchTask struct {
	CampaignID       uuid.UUID                  `json:"campaign_id"`
	WorkspaceID      uuid.UUID                  `json:"workspace_id"`
	BatchIndex       int                        `json:"batch_index"`
	TotalBatches     int                        `json:"total_batches"`
	Recipients       []domain.CampaignRecipient `json:"recipients"`
	DelaySeconds     int                        `json:"delay_seconds"`
	TemplateSnapshot *CampaignTemplateSnapshot  `json:"template_snapshot,omitempty"`
}

// CampaignTemplateSnapshot freezes the provider-relevant subset validated by
// the admin preflight before the durable batch is persisted.
type CampaignTemplateSnapshot struct {
	Language           string `json:"language"`
	BodyParameterCount int    `json:"body_parameter_count"`
}

type CampaignPublisher interface {
	Publish(ctx context.Context, subject string, data []byte, traceID string) error
}

// CampaignWorker consumes campaign batches sequentially and publishes individual messages.
type CampaignWorker struct {
	consumer        jetstream.Consumer
	cancel          context.CancelFunc
	done            chan struct{}
	campaignRepo    *repository.CampaignRepository
	connectionsRepo *repository.ConnectionRepository
	dispatchRepo    *repository.MessageDispatchRepository
	publisher       CampaignPublisher
	heartbeatEvery  time.Duration
	msgMu           sync.Mutex
	msgCtx          jetstream.MessagesContext
	stopped         bool
	stopOnce        sync.Once
}

// NewCampaignWorker creates and starts a new CampaignWorker.
func NewCampaignWorker(
	ctx context.Context,
	consumer jetstream.Consumer,
	campaignRepo *repository.CampaignRepository,
	connectionsRepo *repository.ConnectionRepository,
	dispatchRepo *repository.MessageDispatchRepository,
	publisher CampaignPublisher,
) *CampaignWorker {
	ctx, cancel := context.WithCancel(ctx)
	w := &CampaignWorker{
		consumer:        consumer,
		cancel:          cancel,
		done:            make(chan struct{}),
		campaignRepo:    campaignRepo,
		connectionsRepo: connectionsRepo,
		dispatchRepo:    dispatchRepo,
		publisher:       publisher,
	}
	go w.run(ctx)
	return w
}

func (w *CampaignWorker) run(ctx context.Context) {
	defer close(w.done)

	var loops sync.WaitGroup
	if w.consumer != nil {
		loops.Add(1)
		go func() {
			defer loops.Done()
			w.consumeLoop(ctx)
		}()
	}
	if w.campaignRepo != nil && w.publisher != nil {
		loops.Add(1)
		go func() {
			defer loops.Done()
			w.enqueueLoop(ctx)
		}()
	}
	loops.Wait()
}

func (w *CampaignWorker) consumeLoop(ctx context.Context) {
	msgCtx, err := w.consumer.Messages()
	if err != nil {
		slog.Error("campaign_worker: failed to create messages context", "error", err)
		return
	}
	if !w.installMessagesContext(msgCtx) {
		return
	}
	defer func() {
		msgCtx.Stop()
		w.clearMessagesContext()
	}()

	slog.Info("campaign worker started", "consumer", w.consumer.CachedInfo().Config.Name)

	sem := make(chan struct{}, campaignWorkerConcurrency)
	var active sync.WaitGroup
	defer active.Wait()

	for {
		msg, err := msgCtx.Next()
		if err != nil {
			if ctx.Err() != nil {
				slog.Info("campaign worker stopped")
				return
			}
			slog.Error("campaign_worker: failed to get next message, recreating messages context", "error", err)
			msgCtx.Stop()

			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
			}

			newMsgCtx, err := w.consumer.Messages()
			if err != nil {
				slog.Error("campaign_worker: failed to recreate messages context", "error", err)
				continue
			}
			if !w.installMessagesContext(newMsgCtx) {
				return
			}
			msgCtx = newMsgCtx
			continue
		}

		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		active.Add(1)
		go func(batchMsg jetstream.Msg) {
			defer active.Done()
			defer func() { <-sem }()
			w.processBatch(ctx, batchMsg)
		}(msg)
	}
}

func (w *CampaignWorker) processBatch(ctx context.Context, msg jetstream.Msg) {
	rawPayload := msg.Data()
	var task CampaignBatchTask
	if err := json.Unmarshal(rawPayload, &task); err != nil {
		slog.Error("campaign_worker: failed to unmarshal batch task", "error", err)
		w.terminateBatch(msg, "invalid JSON")
		return
	}
	if task.CampaignID == uuid.Nil ||
		task.WorkspaceID == uuid.Nil ||
		task.BatchIndex < 1 ||
		task.TotalBatches < task.BatchIndex {
		slog.Error("campaign_worker: invalid batch task identity")
		w.terminateBatch(msg, "invalid identity")
		return
	}
	stopHeartbeat := w.startBatchHeartbeat(ctx, msg, task.CampaignID)
	defer stopHeartbeat()

	processed, err := w.campaignRepo.ValidateCampaignBatch(
		ctx,
		task.CampaignID,
		task.WorkspaceID,
		task.BatchIndex,
		rawPayload,
	)
	if err != nil {
		if errors.Is(err, repository.ErrCampaignBatchNotFound) ||
			errors.Is(err, repository.ErrCampaignBatchPayloadMismatch) {
			slog.Error(
				"campaign_worker: rejected non-durable or altered batch task",
				"campaign_id", task.CampaignID,
				"batch_index", task.BatchIndex,
				"error", err,
			)
			w.terminateBatch(msg, "durable payload validation failed")
			return
		}
		slog.Error("campaign_worker: failed to validate durable batch", "campaign_id", task.CampaignID, "error", err)
		w.retryBatch(msg, task.CampaignID, err)
		return
	}
	if processed {
		slog.Info(
			"campaign_worker: duplicate processed batch acknowledged",
			"campaign_id", task.CampaignID,
			"batch_index", task.BatchIndex,
		)
		w.ackBatch(msg, task.CampaignID)
		return
	}

	campaign, err := w.campaignRepo.GetByID(ctx, task.CampaignID)
	if err != nil {
		slog.Error("campaign_worker: failed to get campaign from DB", "campaign_id", task.CampaignID, "error", err)
		w.retryBatch(msg, task.CampaignID, err)
		return
	}

	if campaign.Status == domain.CampaignStatusCancelled {
		slog.Info("campaign_worker: campaign is cancelled, skipping batch", "campaign_id", task.CampaignID, "batch_index", task.BatchIndex)
		w.ackBatch(msg, task.CampaignID)
		return
	}
	if campaign.WorkspaceID != task.WorkspaceID {
		slog.Error("campaign_worker: workspace mismatch in batch task", "campaign_id", task.CampaignID)
		w.terminateBatch(msg, "workspace mismatch")
		return
	}
	if campaign.Status != domain.CampaignStatusSending {
		err := fmt.Errorf("campaign status is %s", campaign.Status)
		slog.Error("campaign_worker: durable batch observed outside sending state", "campaign_id", task.CampaignID, "error", err)
		w.retryBatch(msg, task.CampaignID, err)
		return
	}

	channel := "whatsapp"
	if campaign.Channel != nil {
		channel = *campaign.Channel
	}
	if channel == "whatsapp_cloud" {
		if err := validateCampaignWABATask(campaign, task); err != nil {
			slog.Error(
				"campaign_worker: rejected invalid WABA template snapshot",
				"campaign_id", task.CampaignID,
				"error", err,
			)
			w.retryBatch(msg, task.CampaignID, err)
			return
		}
	}

	slog.Info("campaign_worker: processing batch", "campaign_id", task.CampaignID, "batch_index", task.BatchIndex, "recipients_count", len(task.Recipients))

	var connID uuid.UUID
	var senderIdentity string
	if campaign.ConnectionID != nil {
		connID = *campaign.ConnectionID
		conn, err := w.connectionsRepo.GetByIDForWorkspace(ctx, campaign.WorkspaceID, connID)
		if err != nil || conn == nil {
			if err == nil {
				err = errors.New("campaign connection not found")
			}
			slog.Error("campaign_worker: failed to load campaign connection", "campaign_id", task.CampaignID, "error", err)
			w.retryBatch(msg, task.CampaignID, err)
			return
		}
		senderIdentity = conn.SenderIdentity
	}

	for recipientIndex, recipient := range task.Recipients {
		if ctx.Err() != nil {
			slog.Info("campaign_worker: shutdown interrupted active batch", "campaign_id", task.CampaignID)
			return
		}
		// Double-check cancellation before sending each message
		recipientCampaign, err := w.campaignRepo.GetByID(ctx, task.CampaignID)
		if err != nil {
			slog.Error("campaign_worker: failed to recheck campaign status", "campaign_id", task.CampaignID, "error", err)
			w.retryBatch(msg, task.CampaignID, err)
			return
		}
		if recipientCampaign.Status == domain.CampaignStatusCancelled {
			slog.Info("campaign_worker: campaign cancelled mid-batch, halting batch processing", "campaign_id", task.CampaignID)
			w.ackBatch(msg, task.CampaignID)
			return
		}

		traceID := fmt.Sprintf(
			"campaign_%s_batch_%d_recipient_%d",
			task.CampaignID.String(),
			task.BatchIndex,
			recipientIndex+1,
		)

		var templateName *string
		if channel == "whatsapp_cloud" {
			templateName = campaign.TemplateName
		}
		qMsg, err := buildCampaignRecipientQueueMessage(
			campaign,
			task,
			recipient,
			traceID,
			connID,
			senderIdentity,
			time.Now().UTC(),
		)
		if err != nil {
			slog.Error(
				"campaign_worker: invalid outbound recipient payload",
				"campaign_id", task.CampaignID,
				"batch_index", task.BatchIndex,
				"recipient_index", recipientIndex+1,
				"error", err,
			)
			w.terminateBatch(msg, "invalid outbound recipient payload")
			return
		}

		// Create database dispatch record
		_, err = w.dispatchRepo.GetOrCreateDispatch(
			ctx,
			task.WorkspaceID,
			traceID,
			channel,
			&task.CampaignID,
			templateName,
			recipient.Variables,
		)
		if err != nil {
			slog.Error("campaign_worker: failed to get or create dispatch", "trace_id", traceID, "error", err)
			w.retryBatch(msg, task.CampaignID, err)
			return
		}

		// Publish to NATS MESSAGES stream
		payload, err := json.Marshal(qMsg)
		if err != nil {
			slog.Error("campaign_worker: failed to marshal QueueMessage", "trace_id", traceID, "error", err)
			w.retryBatch(msg, task.CampaignID, err)
			return
		}

		err = w.publisher.Publish(ctx, "messages.outbound", payload, traceID)
		if err != nil {
			slog.Error("campaign_worker: failed to publish message to JetStream", "trace_id", traceID, "error", err)
			w.retryBatch(msg, task.CampaignID, err)
			return
		}
	}

	completed, err := w.campaignRepo.MarkCampaignBatchProcessed(
		ctx,
		task.CampaignID,
		task.WorkspaceID,
		task.BatchIndex,
		rawPayload,
	)
	if err != nil {
		slog.Error("campaign_worker: failed to confirm durable batch", "campaign_id", task.CampaignID, "error", err)
		w.retryBatch(msg, task.CampaignID, err)
		return
	}
	if completed {
		slog.Info("campaign_worker: campaign marked as completed", "campaign_id", task.CampaignID)
	}

	w.ackBatch(msg, task.CampaignID)
}

// ValidateCampaignBatchOutboundPayloads proves every recipient command fits
// the supported queue transport before the durable campaign start snapshot is
// committed. A permanent oversize payload must never enter the retry loop.
func ValidateCampaignBatchOutboundPayloads(
	campaign *domain.Campaign,
	task CampaignBatchTask,
	connectionID uuid.UUID,
	senderIdentity string,
) error {
	// RFC3339Nano at its longest keeps the validation envelope at least as
	// large as the runtime timestamp representation.
	longTimestamp := time.Date(
		9999, time.December, 31, 23, 59, 59, 999999999, time.UTC,
	)
	for recipientIndex, recipient := range task.Recipients {
		traceID := fmt.Sprintf(
			"campaign_%s_batch_%d_recipient_%d",
			task.CampaignID,
			task.BatchIndex,
			recipientIndex+1,
		)
		message, err := buildCampaignRecipientQueueMessage(
			campaign,
			task,
			recipient,
			traceID,
			connectionID,
			senderIdentity,
			longTimestamp,
		)
		if err != nil {
			return err
		}
		payload, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("marshal campaign recipient envelope: %w", err)
		}
		if len(payload) > messagebus.MaxPayloadBytes {
			return fmt.Errorf(
				"%w: campaign batch %d recipient %d",
				messagebus.ErrPayloadTooLarge,
				task.BatchIndex,
				recipientIndex+1,
			)
		}
	}
	return nil
}

func buildCampaignRecipientQueueMessage(
	campaign *domain.Campaign,
	task CampaignBatchTask,
	recipient domain.CampaignRecipient,
	traceID string,
	connectionID uuid.UUID,
	senderIdentity string,
	queuedAt time.Time,
) (domain.QueueMessage, error) {
	channel := "whatsapp"
	if campaign.Channel != nil {
		channel = *campaign.Channel
	}
	message := domain.QueueMessage{
		WorkspaceID:    task.WorkspaceID,
		ConnectionID:   connectionID,
		SenderIdentity: senderIdentity,
		TraceID:        traceID,
		To:             recipient.To,
		Channel:        channel,
		QueuedAt:       queuedAt,
		CampaignID:     &task.CampaignID,
		VariablesJSON:  recipient.Variables,
	}
	if channel != "whatsapp_cloud" {
		if campaign.TemplateName != nil {
			message.Body = domain.ResolveVariables(
				*campaign.TemplateName,
				recipient.Variables,
			)
		}
		return message, nil
	}
	if campaign.TemplateName == nil || task.TemplateSnapshot == nil {
		return domain.QueueMessage{}, errors.New("campaign WABA template snapshot is missing")
	}
	message.TemplateName = *campaign.TemplateName
	message.Language = task.TemplateSnapshot.Language
	params := make(
		[]domain.TemplateParameter,
		0,
		task.TemplateSnapshot.BodyParameterCount,
	)
	for index := 1; index <= task.TemplateSnapshot.BodyParameterCount; index++ {
		params = append(params, domain.TemplateParameter{
			Type: "text",
			Text: recipient.Variables[strconv.Itoa(index)],
		})
	}
	if len(params) > 0 {
		message.Components = []domain.TemplateComponent{{
			Type:       "body",
			Parameters: params,
		}}
	}
	return message, nil
}

func validateCampaignWABATask(campaign *domain.Campaign, task CampaignBatchTask) error {
	if campaign.TemplateName == nil || strings.TrimSpace(*campaign.TemplateName) == "" {
		return errors.New("campaign WABA template name is missing")
	}
	if task.TemplateSnapshot == nil {
		return errors.New("campaign WABA template snapshot is missing")
	}
	language := task.TemplateSnapshot.Language
	if len(language) < 2 ||
		len(language) > 32 ||
		strings.TrimSpace(language) != language ||
		strings.ContainsAny(language, " \t\r\n") ||
		task.TemplateSnapshot.BodyParameterCount < 0 ||
		task.TemplateSnapshot.BodyParameterCount > campaignMaxTemplateParams {
		return errors.New("campaign WABA template snapshot is invalid")
	}
	for recipientIndex, recipient := range task.Recipients {
		for parameterIndex := 1; parameterIndex <= task.TemplateSnapshot.BodyParameterCount; parameterIndex++ {
			value, ok := recipient.Variables[strconv.Itoa(parameterIndex)]
			if !ok || strings.TrimSpace(value) == "" {
				return fmt.Errorf(
					"campaign WABA template parameter %d is missing for recipient %d",
					parameterIndex,
					recipientIndex+1,
				)
			}
		}
	}
	return nil
}

func (w *CampaignWorker) enqueueLoop(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		w.enqueueDueBatches(ctx)
		timer.Reset(campaignBatchPollInterval)
	}
}

func (w *CampaignWorker) enqueueDueBatches(ctx context.Context) {
	claims, err := w.campaignRepo.ClaimDueCampaignBatches(ctx, 100, campaignBatchLeaseDuration)
	if err != nil {
		if ctx.Err() == nil {
			slog.Error("campaign_worker: failed to claim durable batches", "error", err)
		}
		return
	}

	for _, claim := range claims {
		if ctx.Err() != nil {
			return
		}
		// The attempt suffix is stable while a lease result is ambiguous, but
		// changes after a recorded attempt. That permits recovery after NATS
		// MaxDeliver while duplicate processing remains fenced by PostgreSQL.
		publishID := fmt.Sprintf("%s_attempt_%d", claim.TraceID, claim.PublishAttempts+1)
		if err := w.publisher.Publish(ctx, CampaignBatchSubject, claim.Payload, publishID); err != nil {
			if ctx.Err() != nil {
				return
			}
			retryAfter := campaignBatchPublishBackoff(claim.PublishAttempts + 1)
			if stateErr := w.campaignRepo.MarkCampaignBatchPublishFailed(
				ctx,
				claim,
				retryAfter,
				"CAMPAIGN_BATCH_PUBLISH_FAILED",
			); stateErr != nil {
				slog.Error(
					"campaign_worker: failed to persist batch publish failure",
					"campaign_id", claim.CampaignID,
					"batch_index", claim.BatchIndex,
					"error", stateErr,
				)
			}
			slog.Error(
				"campaign_worker: durable batch publish failed",
				"campaign_id", claim.CampaignID,
				"batch_index", claim.BatchIndex,
				"retry_after", retryAfter,
				"error", err,
			)
			continue
		}

		if err := w.campaignRepo.MarkCampaignBatchPublished(
			ctx,
			claim,
			campaignBatchRecoveryAfter,
		); err != nil {
			slog.Error(
				"campaign_worker: batch publish accepted but durable confirmation failed",
				"campaign_id", claim.CampaignID,
				"batch_index", claim.BatchIndex,
				"error", err,
			)
		}
	}
}

func campaignBatchPublishBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 5 {
		attempt = 5
	}
	return time.Duration(1<<(attempt-1)) * time.Second
}

func (w *CampaignWorker) retryBatch(msg jetstream.Msg, campaignID uuid.UUID, cause error) {
	if err := msg.NakWithDelay(campaignBatchRetryDelay); err != nil {
		slog.Error(
			"campaign_worker: failed to defer batch retry",
			"campaign_id", campaignID,
			"cause", cause,
			"error", err,
		)
	}
}

func (w *CampaignWorker) ackBatch(msg jetstream.Msg, campaignID uuid.UUID) {
	if err := msg.Ack(); err != nil {
		slog.Error("campaign_worker: failed to acknowledge batch", "campaign_id", campaignID, "error", err)
	}
}

func (w *CampaignWorker) terminateBatch(msg jetstream.Msg, reason string) {
	if err := msg.TermWithReason(reason); err != nil {
		slog.Error("campaign_worker: failed to terminate invalid batch", "reason", reason, "error", err)
	}
}

func (w *CampaignWorker) startBatchHeartbeat(
	ctx context.Context,
	msg jetstream.Msg,
	campaignID uuid.UUID,
) func() {
	interval := w.heartbeatEvery
	if interval <= 0 {
		interval = campaignHeartbeatInterval
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-ticker.C:
				if err := msg.InProgress(); err != nil {
					slog.Warn(
						"campaign_worker: failed to extend batch ack deadline",
						"campaign_id", campaignID,
						"error", err,
					)
				}
			}
		}
	}()

	return func() {
		close(stop)
		<-done
	}
}

// Stop stops the campaign worker loop and blocks until it finishes.
func (w *CampaignWorker) Stop() {
	w.stopOnce.Do(func() {
		w.cancel()
		w.msgMu.Lock()
		w.stopped = true
		msgCtx := w.msgCtx
		w.msgMu.Unlock()
		if msgCtx != nil {
			msgCtx.Stop()
		}
	})
	<-w.done
}

func (w *CampaignWorker) installMessagesContext(msgCtx jetstream.MessagesContext) bool {
	w.msgMu.Lock()
	defer w.msgMu.Unlock()
	if w.stopped {
		msgCtx.Stop()
		return false
	}
	w.msgCtx = msgCtx
	return true
}

func (w *CampaignWorker) clearMessagesContext() {
	w.msgMu.Lock()
	w.msgCtx = nil
	w.msgMu.Unlock()
}
