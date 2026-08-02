package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/google/uuid"

	"github.com/pablojhp.pergo/internal/channel"
	"github.com/pablojhp.pergo/internal/domain"
	"github.com/pablojhp.pergo/internal/platform/audit"
	"github.com/pablojhp.pergo/internal/repository"
)

// QueueDepthTracker defines the interface for workspace-scoped queue depth tracking.
type QueueDepthTracker interface {
	Decrement(workspaceID uuid.UUID)
}

// EventPublisher is the consumer-owned port for queue and webhook events.
type EventPublisher interface {
	Publish(ctx context.Context, subject string, data []byte, dedupID string) error
}

const (
	orchDefaultMaxRetries  = 5
	orchDefaultMaxBackoff  = 60 * time.Second
	orchDefaultBaseBackoff = 1 * time.Second
	orchDeliveryLease      = 2 * time.Minute
	orchProviderTimeout    = 90 * time.Second
	deliveryExpiredCode    = "DELIVERY_EXPIRED"
	deliveryFailedCode     = "DELIVERY_FAILED"
	deliveryTerminalCode   = "DELIVERY_TERMINAL"
	deliveryTransientCode  = "DELIVERY_TRANSIENT"
	deliveryUncertainCode  = "DELIVERY_UNCERTAIN"
	deliveryRetriesCode    = "DELIVERY_RETRIES_EXHAUSTED"
)

// DispatchOrchestrator owns all outbound dispatch logic: idempotency,
// TTL enforcement, fallback routing, channel dispatch, audit, and
// webhook events. Tests exercise the full pipeline through Process.
type DispatchOrchestrator struct {
	dispatchers  *channel.Registry
	dispatchRepo *repository.MessageDispatchRepository
	publisher    EventPublisher
	queueDepth   QueueDepthTracker
	auditWriter  audit.Writer
	contactRepo  *repository.ContactRepository

	maxRetries int
	maxBackoff time.Duration
}

// NewDispatchOrchestrator creates an orchestrator with the given adapters.
func NewDispatchOrchestrator(
	dispatchers *channel.Registry,
	dispatchRepo *repository.MessageDispatchRepository,
	publisher EventPublisher,
	queueDepth QueueDepthTracker,
	auditWriter audit.Writer,
	contactRepo *repository.ContactRepository,
	maxRetries int,
	maxBackoff time.Duration,
) *DispatchOrchestrator {
	if maxRetries <= 0 {
		maxRetries = orchDefaultMaxRetries
	}
	if maxBackoff <= 0 {
		maxBackoff = orchDefaultMaxBackoff
	}

	return &DispatchOrchestrator{
		dispatchers:  dispatchers,
		dispatchRepo: dispatchRepo,
		publisher:    publisher,
		queueDepth:   queueDepth,
		auditWriter:  auditWriter,
		contactRepo:  contactRepo,
		maxRetries:   maxRetries,
		maxBackoff:   maxBackoff,
	}
}

// Process handles a single message through the full dispatch pipeline.
// The caller is responsible for JSON deserialization and trace context
// injection. The msg port abstracts JetStream ack/nak operations.
func (o *DispatchOrchestrator) Process(
	ctx context.Context,
	msg DispatchMessage,
	qMsg *domain.QueueMessage,
	attempt int,
) error {
	traceID := qMsg.TraceID
	workspaceID := qMsg.WorkspaceID
	if o.dispatchRepo != nil {
		// JetStream delivery count also includes scheduler deferrals. Until a
		// fenced database claim exists there is no provider attempt to charge.
		attempt = 0
	}

	// --- Database State Check / Idempotency ---
	var dispatch *repository.MessageDispatch
	var deliveryClaim repository.DispatchClaim
	if o.dispatchRepo != nil && traceID != "" {
		var err error
		var tempName *string
		if qMsg.TemplateName != "" {
			tempName = &qMsg.TemplateName
		}
		dispatch, err = o.dispatchRepo.GetOrCreateDispatch(ctx, workspaceID, traceID, qMsg.Channel, qMsg.CampaignID, tempName, qMsg.VariablesJSON)
		if err != nil {
			slog.Error("orchestrator: failed to get/create dispatch state", "error", err, "trace_id", traceID)
			o.handleFailure(msg, workspaceID, traceID, attempt)
			return err
		}
		if qMsg.MessageID != uuid.Nil {
			if err := o.dispatchRepo.BindReceipt(ctx, dispatch.ID, qMsg.MessageID); err != nil {
				slog.Error("orchestrator: failed to bind durable receipt", "error", err, "trace_id", traceID)
				o.handleFailure(msg, workspaceID, traceID, attempt)
				return err
			}
			receiptID := qMsg.MessageID
			dispatch.ReceiptID = &receiptID
		}

		if terminalDispatchStatus(dispatch.Status) {
			slog.Info("orchestrator: duplicate delivery prevented (terminal status)",
				"trace_id", traceID,
				"status", dispatch.Status,
			)
			o.ack(msg, workspaceID)
			return nil
		}

		var retryAfter time.Duration
		deliveryClaim, retryAfter, err = o.dispatchRepo.ClaimDelivery(ctx, dispatch.ID, orchDeliveryLease)
		if err != nil {
			switch {
			case errors.Is(err, repository.ErrDispatchTerminal):
				o.ack(msg, workspaceID)
				return nil
			case errors.Is(err, repository.ErrDispatchDeliveryUncertain):
				errorCode := deliveryUncertainCode
				o.publishWebhookEvent(ctx, workspaceID, traceID, dispatchReceiptID(dispatch), "failed", dispatch.CurrentChannel, &errorCode)
				o.ack(msg, workspaceID)
				return nil
			case errors.Is(err, repository.ErrDispatchClaimActive):
				if retryAfter <= 0 {
					retryAfter = time.Second
				}
				slog.Info("orchestrator: duplicate delivery deferred behind durable claim",
					"trace_id", traceID,
					"retry_after", retryAfter,
				)
				if nakErr := msg.NakWithDelay(retryAfter); nakErr != nil {
					slog.Error("orchestrator: failed to defer duplicate delivery", "error", nakErr, "trace_id", traceID)
				}
				return err
			default:
				slog.Error("orchestrator: failed to claim provider delivery", "error", err, "trace_id", traceID)
				o.handleFailure(msg, workspaceID, traceID, attempt)
				return err
			}
		}
		// PostgreSQL claim generations count actual provider dispatch rights.
		// Broker redeliveries used only for fair scheduling must not consume the
		// provider retry budget.
		if deliveryClaim.Generation > 0 {
			attempt = int(deliveryClaim.Generation - 1)
		}
		if dispatch.Status == "queued" {
			if err := o.dispatchRepo.EnsureDispatchWebhookEvent(ctx, dispatch.ID, "queued", nil); err != nil {
				slog.Error("orchestrator: failed to persist queued webhook event", "error", err, "trace_id", traceID)
				o.handleFailure(msg, workspaceID, traceID, attempt)
				return err
			}
			o.publishWebhookEvent(ctx, workspaceID, traceID, dispatchReceiptID(dispatch), "queued", qMsg.Channel, nil)
		}
	}

	// --- TTL enforcement ---
	if o.isExpired(qMsg) {
		slog.Warn("orchestrator: message expired (TTL), dropping", "trace_id", traceID)
		if o.dispatchRepo != nil && dispatch != nil {
			errMsg := deliveryExpiredCode
			if err := o.dispatchRepo.UpdateClaimedDelivery(
				ctx,
				dispatch.ID,
				deliveryClaim,
				"failed",
				dispatch.CurrentChannel,
				dispatch.FallbackIndex,
				&errMsg,
				nil,
				true,
			); err != nil {
				slog.Error("orchestrator: failed to persist expired delivery", "error", err, "trace_id", traceID)
				o.handleFailure(msg, workspaceID, traceID, attempt)
				return err
			}
			o.publishWebhookEvent(ctx, workspaceID, traceID, dispatchReceiptID(dispatch), "failed", dispatch.CurrentChannel, &errMsg)
		}
		o.ack(msg, workspaceID)
		return nil
	}

	// --- Fallback Loop ---
	startIndex := 0
	if dispatch != nil {
		startIndex = dispatch.FallbackIndex
	}

	allChannels := append([]string{qMsg.Channel}, qMsg.FallbackChannels...)
	if startIndex < 0 || startIndex >= len(allChannels) {
		slog.Error("orchestrator: fallback index out of bounds", "index", startIndex, "channels_count", len(allChannels), "trace_id", traceID)
		if o.dispatchRepo != nil && dispatch != nil {
			errMsg := deliveryFailedCode
			if err := o.dispatchRepo.UpdateClaimedDelivery(
				ctx, dispatch.ID, deliveryClaim, "failed", dispatch.CurrentChannel, dispatch.FallbackIndex, &errMsg, nil, true,
			); err != nil {
				o.handleFailure(msg, workspaceID, traceID, attempt)
				return err
			}
			o.publishWebhookEvent(ctx, workspaceID, traceID, dispatchReceiptID(dispatch), "failed", dispatch.CurrentChannel, &errMsg)
		}
		o.ack(msg, workspaceID)
		return nil
	}

	var finalErr error
	var lastChannel string
	currentIndex := startIndex

	for i := startIndex; i < len(allChannels); i++ {
		channelName := allChannels[i]
		lastChannel = channelName
		currentIndex = i

		if o.dispatchRepo != nil && dispatch != nil {
			var err error
			if deliveryClaim.Token == uuid.Nil {
				var retryAfter time.Duration
				deliveryClaim, retryAfter, err = o.dispatchRepo.ClaimDelivery(ctx, dispatch.ID, orchDeliveryLease)
				if errors.Is(err, repository.ErrDispatchClaimActive) {
					if retryAfter <= 0 {
						retryAfter = time.Second
					}
					if nakErr := msg.NakWithDelay(retryAfter); nakErr != nil {
						slog.Error("orchestrator: failed to defer fallback claim", "error", nakErr, "trace_id", traceID)
					}
					return err
				}
				if errors.Is(err, repository.ErrDispatchTerminal) {
					o.ack(msg, workspaceID)
					return nil
				}
				if err == nil && deliveryClaim.Generation > 0 {
					attempt = int(deliveryClaim.Generation - 1)
				}
			} else {
				deliveryClaim, err = o.dispatchRepo.RenewDeliveryClaim(ctx, dispatch.ID, deliveryClaim, orchDeliveryLease)
			}
			if err != nil {
				slog.Error("orchestrator: failed to renew provider delivery claim", "error", err, "trace_id", traceID)
				o.handleFailure(msg, workspaceID, traceID, attempt)
				return err
			}
			err = o.dispatchRepo.UpdateClaimedDelivery(
				ctx, dispatch.ID, deliveryClaim, "sending", channelName, i, nil, nil, false,
			)
			if err != nil {
				slog.Error("orchestrator: failed to update status to sending", "error", err, "trace_id", traceID)
				o.handleFailure(msg, workspaceID, traceID, attempt)
				return err
			}
		}

		slog.Info("orchestrator: attempting dispatch", "channel", channelName, "trace_id", traceID, "index", i, "attempt", attempt)
		providerCtx, cancelProvider := context.WithTimeout(ctx, orchProviderTimeout)
		respStr, err := o.dispatchToChannel(providerCtx, channelName, qMsg)
		cancelProvider()
		if err == nil {
			if o.dispatchRepo != nil && dispatch != nil {
				var providerMessageID *string
				if channelName == "whatsapp_cloud" && respStr != "" {
					providerMessageID = &respStr
				}
				if errUpdate := o.dispatchRepo.UpdateClaimedDelivery(
					ctx, dispatch.ID, deliveryClaim, "sent", channelName, i, nil, providerMessageID, true,
				); errUpdate != nil {
					slog.Error(
						"orchestrator: provider accepted message but durable completion failed",
						"error", errUpdate,
						"dispatch_id", dispatch.ID,
						"trace_id", traceID,
					)
					if nakErr := msg.NakWithDelay(time.Second); nakErr != nil {
						slog.Error("orchestrator: failed to defer uncertain provider completion", "error", nakErr, "trace_id", traceID)
					}
					return errUpdate
				}
				o.publishWebhookEvent(ctx, workspaceID, traceID, dispatchReceiptID(dispatch), "sent", channelName, nil)
			}
			slog.Info("orchestrator: message dispatched successfully", "channel", channelName, "trace_id", traceID)

			if o.auditWriter != nil {
				auditPayload := map[string]any{
					"request":  qMsg,
					"response": respStr,
					"status":   "sent",
				}
				payloadBytes, _ := json.Marshal(auditPayload)
				_ = o.auditWriter.Write(audit.NewEvent(workspaceID, traceID, "outbound_message", payloadBytes))
			}

			o.ack(msg, workspaceID)
			return nil
		}

		finalErr = err
		if channel.IsUncertain(err) {
			errMsg := deliveryUncertainCode
			if o.dispatchRepo != nil && dispatch != nil {
				if stateErr := o.dispatchRepo.UpdateClaimedDelivery(
					ctx, dispatch.ID, deliveryClaim, "uncertain", channelName, i, &errMsg, nil, true,
				); stateErr != nil {
					slog.Error("orchestrator: failed to persist uncertain provider outcome", "error", stateErr, "trace_id", traceID)
					if nakErr := msg.NakWithDelay(time.Second); nakErr != nil {
						slog.Error("orchestrator: failed to defer uncertain provider outcome", "error", nakErr, "trace_id", traceID)
					}
					return stateErr
				}
				errorCode := deliveryUncertainCode
				o.publishWebhookEvent(ctx, workspaceID, traceID, dispatchReceiptID(dispatch), "failed", channelName, &errorCode)
			}
			if o.auditWriter != nil {
				auditPayload := map[string]any{
					"request": qMsg,
					"status":  "uncertain",
					"error":   deliveryUncertainCode,
				}
				payloadBytes, _ := json.Marshal(auditPayload)
				_ = o.auditWriter.Write(audit.NewEvent(workspaceID, traceID, "outbound_message", payloadBytes))
			}
			slog.Error(
				"orchestrator: provider outcome uncertain; retry and fallback blocked",
				"error_code", deliveryUncertainCode,
				"channel", channelName,
				"trace_id", traceID,
			)
			o.ack(msg, workspaceID)
			return err
		}
		if channel.IsTerminal(err) {
			slog.Warn(
				"orchestrator: terminal error, triggering fallback",
				"channel", channelName,
				"error_code", deliveryTerminalCode,
				"trace_id", traceID,
			)

			if o.auditWriter != nil {
				auditPayload := map[string]any{
					"request":  qMsg,
					"response": respStr,
					"status":   "failed",
					"error":    deliveryTerminalCode,
				}
				payloadBytes, _ := json.Marshal(auditPayload)
				_ = o.auditWriter.Write(audit.NewEvent(workspaceID, traceID, "outbound_message", payloadBytes))
			}
			if i+1 < len(allChannels) && o.dispatchRepo != nil && dispatch != nil {
				nextIndex := i + 1
				errMsg := deliveryTerminalCode
				if stateErr := o.dispatchRepo.UpdateClaimedDelivery(
					ctx,
					dispatch.ID,
					deliveryClaim,
					"failed_transient",
					channelName,
					nextIndex,
					&errMsg,
					nil,
					true,
				); stateErr != nil {
					slog.Error(
						"orchestrator: failed to persist safe fallback progress",
						"error", stateErr,
						"trace_id", traceID,
						"next_index", nextIndex,
					)
					o.handleFailure(msg, workspaceID, traceID, attempt)
					return stateErr
				}
				// The terminal provider result is known and safe. Release the
				// old fence so a crash resumes at the persisted next channel
				// instead of misclassifying the completed call as uncertain.
				deliveryClaim = repository.DispatchClaim{}
			}
			continue
		}

		// Transient error — NAK with delay, JetStream retries
		errMsg := deliveryTransientCode
		if attempt >= o.maxRetries {
			errMsg = deliveryRetriesCode
			if o.dispatchRepo != nil && dispatch != nil {
				if stateErr := o.dispatchRepo.UpdateClaimedDelivery(
					ctx, dispatch.ID, deliveryClaim, "failed", channelName, i, &errMsg, nil, true,
				); stateErr != nil {
					slog.Error("orchestrator: failed to persist exhausted delivery retries", "error", stateErr, "trace_id", traceID)
					if nakErr := msg.NakWithDelay(time.Second); nakErr != nil {
						slog.Error("orchestrator: failed to defer exhausted delivery persistence", "error", nakErr, "trace_id", traceID)
					}
					return stateErr
				}
				o.publishWebhookEvent(ctx, workspaceID, traceID, dispatchReceiptID(dispatch), "failed", channelName, &errMsg)
			}
			slog.Error(
				"orchestrator: transient provider retries exhausted",
				"channel", channelName,
				"error_code", deliveryRetriesCode,
				"trace_id", traceID,
				"attempt", attempt,
			)
			if o.auditWriter != nil {
				auditPayload := map[string]any{
					"request": qMsg,
					"status":  "failed",
					"error":   deliveryRetriesCode,
				}
				payloadBytes, _ := json.Marshal(auditPayload)
				_ = o.auditWriter.Write(audit.NewEvent(workspaceID, traceID, "outbound_message", payloadBytes))
			}
			o.ack(msg, workspaceID)
			return err
		}
		if o.dispatchRepo != nil && dispatch != nil {
			if stateErr := o.dispatchRepo.UpdateClaimedDelivery(
				ctx, dispatch.ID, deliveryClaim, "failed_transient", channelName, i, &errMsg, nil, true,
			); stateErr != nil {
				slog.Error("orchestrator: failed to release transient delivery claim", "error", stateErr, "trace_id", traceID)
				if nakErr := msg.NakWithDelay(time.Second); nakErr != nil {
					slog.Error("orchestrator: failed to defer uncertain transient result", "error", nakErr, "trace_id", traceID)
				}
				return stateErr
			}
		}
		slog.Warn(
			"orchestrator: transient error, NAK for retry",
			"channel", channelName,
			"error_code", deliveryTransientCode,
			"trace_id", traceID,
		)

		if o.auditWriter != nil {
			auditPayload := map[string]any{
				"request":  qMsg,
				"response": respStr,
				"status":   "failed_transient",
				"error":    deliveryTransientCode,
			}
			payloadBytes, _ := json.Marshal(auditPayload)
			_ = o.auditWriter.Write(audit.NewEvent(workspaceID, traceID, "outbound_message", payloadBytes))
		}

		o.handleFailure(msg, workspaceID, traceID, attempt)
		return err
	}

	// All channels exhausted terminally
	errMsg := deliveryFailedCode
	if o.dispatchRepo != nil && dispatch != nil {
		if err := o.dispatchRepo.UpdateClaimedDelivery(
			ctx, dispatch.ID, deliveryClaim, "failed", lastChannel, currentIndex, &errMsg, nil, true,
		); err != nil {
			slog.Error("orchestrator: failed to persist terminal delivery failure", "error", err, "trace_id", traceID)
			o.handleFailure(msg, workspaceID, traceID, attempt)
			return err
		}
		o.publishWebhookEvent(ctx, workspaceID, traceID, dispatchReceiptID(dispatch), "failed", lastChannel, &errMsg)
	}
	slog.Error(
		"orchestrator: all fallback channels exhausted (terminal failure)",
		"error_code", deliveryFailedCode,
		"trace_id", traceID,
	)
	o.ack(msg, workspaceID)
	return finalErr
}

func terminalDispatchStatus(status string) bool {
	switch status {
	case "sent", "delivered", "read", "failed", "uncertain":
		return true
	default:
		return false
	}
}

// SetContactRepository registers the Contact repository.
func (o *DispatchOrchestrator) SetContactRepository(repo *repository.ContactRepository) {
	o.contactRepo = repo
}

func (o *DispatchOrchestrator) dispatchToChannel(ctx context.Context, channelName string, qMsg *domain.QueueMessage) (string, error) {
	if o.dispatchers == nil {
		return "", channel.NewTerminalError(fmt.Errorf("orchestrator: dispatcher registry is not configured for channel %q", channelName))
	}

	dispatcher, ok := o.dispatchers.Get(channelName)
	if !ok {
		slog.Warn("orchestrator: no dispatcher for channel", "channel", channelName, "trace_id", qMsg.TraceID)
		return "", channel.NewTerminalError(fmt.Errorf("orchestrator: no dispatcher registered for channel %q", channelName))
	}

	to := qMsg.To
	if channelName == "telegram" && o.contactRepo != nil {
		if resolvedChatID, err := o.contactRepo.ResolveTelegramChatID(ctx, qMsg.WorkspaceID, qMsg.To); err == nil && resolvedChatID != "" {
			slog.Info("orchestrator: resolved telegram contact identifier", "trace_id", qMsg.TraceID, "workspace_id", qMsg.WorkspaceID.String())
			to = resolvedChatID
			qMsg.To = resolvedChatID // Normalize so that audit logs also use the resolved numeric ID
		}
	}

	messageID := qMsg.TraceID
	if qMsg.MessageID != uuid.Nil {
		messageID = qMsg.MessageID.String()
	}
	return dispatcher.Dispatch(ctx, &channel.MessagePayload{
		MessageID:        messageID,
		ConnectionID:     qMsg.ConnectionID,
		SenderIdentity:   qMsg.SenderIdentity,
		TraceID:          qMsg.TraceID,
		To:               to,
		Channel:          channelName,
		Body:             qMsg.Body,
		Media:            qMsg.Media,
		Metadata:         qMsg.Metadata,
		TemplateName:     qMsg.TemplateName,
		Language:         qMsg.Language,
		Components:       qMsg.Components,
		Interactive:      qMsg.Interactive,
		ChannelOverrides: qMsg.ChannelOverrides,
		FallbackBehavior: qMsg.FallbackBehavior,
	})
}

// ack ACKs the message and decrements workspace queue depth.
func (o *DispatchOrchestrator) ack(msg DispatchMessage, workspaceID uuid.UUID) {
	_ = msg.Ack()
	if o.queueDepth != nil && workspaceID != (uuid.UUID{}) {
		o.queueDepth.Decrement(workspaceID)
	}
}

// handleFailure NAKs with exponential backoff or acks as terminal.
func (o *DispatchOrchestrator) handleFailure(msg DispatchMessage, workspaceID uuid.UUID, traceID string, attempt int) {
	if attempt >= o.maxRetries {
		slog.Error("orchestrator: terminal failure after max retries", "trace_id", traceID, "attempts", attempt, "max_retries", o.maxRetries)
		o.ack(msg, workspaceID)
		return
	}

	delay := time.Duration(math.Pow(2, float64(attempt))) * orchDefaultBaseBackoff
	if delay > o.maxBackoff {
		delay = o.maxBackoff
	}

	slog.Warn("orchestrator: retrying with backoff", "trace_id", traceID, "attempt", attempt+1, "backoff", delay)

	if nakErr := msg.NakWithDelay(delay); nakErr != nil {
		slog.Error("orchestrator: failed to NAK message", "error", nakErr, "trace_id", traceID)
		o.ack(msg, workspaceID)
	}
}

// isExpired checks if the message TTL has elapsed using the already-parsed QueueMessage.
func (o *DispatchOrchestrator) isExpired(qMsg *domain.QueueMessage) bool {
	if qMsg.TTLSeconds == nil || *qMsg.TTLSeconds <= 0 {
		return false
	}
	if qMsg.QueuedAt.IsZero() {
		return false
	}
	expiry := qMsg.QueuedAt.Add(time.Duration(*qMsg.TTLSeconds) * time.Second)
	return time.Now().UTC().After(expiry)
}

// retryAttempt obtains the zero-based broker delivery attempt. JetStream
// metadata is authoritative because NAK redeliveries do not mutate headers.
// The header fallback only supports non-JetStream adapters during migration.
func retryAttempt(msg DispatchMessage) int {
	if attempt, ok := msg.DeliveryAttempt(); ok {
		return attempt
	}
	headers := msg.Headers()
	if headers == nil {
		return 0
	}
	val := headers["X-Retry-Attempt"]
	if val == "" {
		return 0
	}
	var n int
	if _, err := fmt.Sscanf(val, "%d", &n); err == nil {
		return n
	}
	return 0
}

// publishWebhookEvent creates and publishes a status event to NATS.
func (o *DispatchOrchestrator) publishWebhookEvent(ctx context.Context, workspaceID uuid.UUID, traceID string, messageID uuid.UUID, event string, channelName string, errMsg *string) {
	if o.publisher == nil {
		return
	}

	evt := struct {
		Event       string `json:"event"`
		TraceID     string `json:"trace_id"`
		MessageID   string `json:"message_id"`
		Channel     string `json:"channel"`
		Timestamp   string `json:"timestamp"`
		WorkspaceID string `json:"workspace_id"`
		Error       string `json:"error,omitempty"`
	}{
		Event:       event,
		TraceID:     traceID,
		MessageID:   messageID.String(),
		Channel:     channelName,
		Timestamp:   time.Now().UTC().Format(time.RFC3339),
		WorkspaceID: workspaceID.String(),
	}
	if errMsg != nil {
		evt.Error = *errMsg
	}

	payload, err := json.Marshal(evt)
	if err != nil {
		slog.Error("orchestrator: failed to marshal webhook event", "error", err, "trace_id", traceID)
		return
	}

	if err := o.publisher.Publish(ctx, "webhooks.events", payload, traceID+".delivery."+event); err != nil {
		slog.Error("orchestrator: failed to publish webhook event", "error", err, "trace_id", traceID)
	}
}

func dispatchReceiptID(dispatch *repository.MessageDispatch) uuid.UUID {
	if dispatch != nil && dispatch.ReceiptID != nil && *dispatch.ReceiptID != uuid.Nil {
		return *dispatch.ReceiptID
	}
	if dispatch != nil {
		return dispatch.ID
	}
	return uuid.Nil
}
