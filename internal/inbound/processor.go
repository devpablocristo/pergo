package inbound

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/pablojhp.pergo/internal/domain"
	"github.com/pablojhp.pergo/internal/media"
	"github.com/pablojhp.pergo/internal/platform/audit"
	"github.com/pablojhp.pergo/internal/repository"
)

// ChatwootSyncer defines the interface to sync inbound customer messages into Chatwoot.
type ChatwootSyncer interface {
	SyncInboundMessage(ctx context.Context, contact *domain.Contact, ev *InboundEvent) error
}

// TypebotForwarder defines the interface to forward inbound customer messages to Typebot.
type TypebotForwarder interface {
	SyncInboundMessage(ctx context.Context, contact *domain.Contact, ev *InboundEvent) error
}

// InboundRouter defines the interface for routing unified inbound events to integration syncers.
type InboundRouter interface {
	Route(ctx context.Context, contact *domain.Contact, ev *InboundEvent) error
}

// InboundMedia carries media bytes and metadata downloaded by the caller/adapter.
type InboundMedia struct {
	Bytes     []byte `json:"-"`
	MediaType string `json:"media_type"` // "image", "document", "audio", "video"
	Filename  string `json:"filename,omitempty"`
	Caption   string `json:"caption,omitempty"`
	MediaURL  string `json:"media_url,omitempty"`
}

// InboundLocation carries location data.
type InboundLocation struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Name      string  `json:"name,omitempty"`
	Address   string  `json:"address,omitempty"`
}

// InboundContact carries contact data.
type InboundContact struct {
	Name  string `json:"name"`
	Phone string `json:"phone"`
}

// InboundButtonReply represents a button interaction reply.
type InboundButtonReply struct {
	ID    string `json:"id"`
	Title string `json:"title"`
}

// InboundInteractive represents the unified inbound interactive payload.
type InboundInteractive struct {
	Type        string              `json:"type"` // e.g. "button_reply"
	ButtonReply *InboundButtonReply `json:"button_reply,omitempty"`
}

// InboundStoryEvent represents an Instagram story mention or reply.
type InboundStoryEvent struct {
	Subtype  string `json:"subtype"`
	MediaURL string `json:"media_url,omitempty"`
}

// InboundEvent is the channel-agnostic inbound payload.
type InboundEvent struct {
	WorkspaceID  uuid.UUID
	ConnectionID uuid.UUID
	MessageID    string // Provider-specific unique message/update ID
	Channel      string // "whatsapp", "whatsapp_cloud", "telegram"
	From         string // Sender JID/phone/chat ID
	To           string // Recipient identity (our bot/phone)
	Body         string
	Media        *InboundMedia
	Location     *InboundLocation
	Contacts     []InboundContact
	Interactive  *InboundInteractive
	Story        *InboundStoryEvent
	SenderName   string
	Metadata     map[string]string
}

// InboundEventPayload is the standard format published to NATS and webhooks.
type InboundEventPayload struct {
	Event       string              `json:"event"`
	TraceID     string              `json:"trace_id"`
	MessageID   string              `json:"message_id"`
	Channel     string              `json:"channel"`
	Timestamp   string              `json:"timestamp"`
	WorkspaceID string              `json:"workspace_id"`
	From        string              `json:"from"`
	To          string              `json:"to"`
	Body        string              `json:"body,omitempty"`
	Media       *EventMedia         `json:"media,omitempty"`
	Location    *InboundLocation    `json:"location,omitempty"`
	Contacts    []InboundContact    `json:"contacts,omitempty"`
	Interactive *InboundInteractive `json:"interactive,omitempty"`
	Story       *InboundStoryEvent  `json:"story_event,omitempty"`
}

// DeliveryEventPayload is the canonical contract consumed by webhook
// subscribers such as Pymes.
type DeliveryEventPayload struct {
	Event       string `json:"event"`
	TraceID     string `json:"trace_id"`
	MessageID   string `json:"message_id"`
	Channel     string `json:"channel"`
	Timestamp   string `json:"timestamp"`
	WorkspaceID string `json:"workspace_id"`
	Error       string `json:"error,omitempty"`
}

type EventMedia struct {
	MediaURL  string `json:"media_url"`
	MediaType string `json:"media_type"`
	Filename  string `json:"filename,omitempty"`
	Caption   string `json:"caption,omitempty"`
}

// Publisher defines the port for publishing event payloads to a messaging queue.
type Publisher interface {
	Publish(ctx context.Context, subject string, data []byte, traceID string) error
}

// InboundProcessor handles workspace verification, deduplication, PII checking,
// S3 uploading, NATS publishing, and audit logging for all messaging channels.
type InboundProcessor struct {
	dedupRepo            *repository.InboundDedupRepository
	wsRepo               *repository.WorkspaceRepository
	mediaEngine          media.Engine
	publisher            Publisher
	auditWriter          audit.Writer
	recipientSessionRepo *repository.RecipientSessionRepository
	contactRepo          *repository.ContactRepository
	dispatchRepo         *repository.MessageDispatchRepository
	router               InboundRouter
}

// NewInboundProcessor creates a new InboundProcessor.
func NewInboundProcessor(
	dedupRepo *repository.InboundDedupRepository,
	wsRepo *repository.WorkspaceRepository,
	mediaEngine media.Engine,
	publisher Publisher,
	auditWriter audit.Writer,
	recipientSessionRepo *repository.RecipientSessionRepository,
	contactRepo *repository.ContactRepository,
	dispatchRepo *repository.MessageDispatchRepository,
	router InboundRouter,
) *InboundProcessor {
	return &InboundProcessor{
		dedupRepo:            dedupRepo,
		wsRepo:               wsRepo,
		mediaEngine:          mediaEngine,
		publisher:            publisher,
		auditWriter:          auditWriter,
		recipientSessionRepo: recipientSessionRepo,
		contactRepo:          contactRepo,
		dispatchRepo:         dispatchRepo,
		router:               router,
	}
}

// Process executes the ingestion pipeline for an inbound event.
func (p *InboundProcessor) Process(ctx context.Context, ev *InboundEvent) error {
	if ev.WorkspaceID == uuid.Nil {
		return fmt.Errorf("inbound: workspace ID is required")
	}

	if ev.Metadata != nil && ev.Metadata["type"] == "status_update" {
		if p.dispatchRepo == nil {
			slog.Warn("inbound processor: status_update received but dispatchRepo is nil")
			return nil
		}
		dispatch, err := p.dispatchRepo.GetByProviderMessageID(ctx, ev.WorkspaceID, ev.MessageID)
		if err != nil {
			if errors.Is(err, repository.ErrDispatchNotFound) {
				return fmt.Errorf(
					"inbound processor: provider receipt arrived before dispatch persistence: %w",
					repository.ErrDispatchNotFound,
				)
			}
			return fmt.Errorf("inbound processor: failed to get dispatch by provider message ID: %w", err)
		}

		if !isCanonicalDeliveryStatus(ev.Body) {
			return nil
		}

		messageID := dispatch.ID
		if dispatch.ReceiptID != nil && *dispatch.ReceiptID != uuid.Nil {
			messageID = *dispatch.ReceiptID
		}
		eventKey := dispatch.TraceID + ".delivery." + ev.Body
		deliveryData, marshalErr := json.Marshal(DeliveryEventPayload{
			Event:       ev.Body,
			TraceID:     dispatch.TraceID,
			MessageID:   messageID.String(),
			Channel:     dispatch.CurrentChannel,
			Timestamp:   time.Now().UTC().Format(time.RFC3339),
			WorkspaceID: dispatch.WorkspaceID.String(),
		})
		if marshalErr != nil {
			return fmt.Errorf("inbound processor: failed to marshal delivery event: %w", marshalErr)
		}
		outboxEvent, err := p.dispatchRepo.RecordProviderDeliveryReceipt(
			ctx,
			dispatch.WorkspaceID,
			dispatch.ID,
			ev.Body,
			eventKey,
			deliveryData,
		)
		if err != nil {
			return fmt.Errorf("inbound processor: failed to record delivery receipt: %w", err)
		}
		if outboxEvent == nil || outboxEvent.PublishedAt != nil || p.publisher == nil {
			return nil
		}
		if publishErr := p.publisher.Publish(
			ctx,
			"webhooks.events",
			outboxEvent.Payload,
			outboxEvent.EventKey,
		); publishErr != nil {
			return fmt.Errorf("inbound processor: failed to publish delivery event: %w", publishErr)
		}
		if err := p.dispatchRepo.MarkProviderDeliveryEventPublished(ctx, outboxEvent.ID); err != nil {
			return fmt.Errorf("inbound processor: failed to complete delivery event: %w", err)
		}
		return nil
	}

	traceID := uuid.NewString()
	var inboundClaim *repository.InboundClaim
	claimCompleted := false
	if p.dedupRepo != nil && ev.MessageID != "" {
		claim, replay, retryAfter, err := p.dedupRepo.Claim(
			ctx,
			ev.WorkspaceID,
			ev.ConnectionID,
			ev.Channel,
			ev.MessageID,
			0,
		)
		if err != nil {
			if errors.Is(err, repository.ErrInboundClaimActive) {
				return fmt.Errorf(
					"inbound: durable handoff is active; retry after %s: %w",
					retryAfter,
					err,
				)
			}
			return fmt.Errorf("inbound: acquire durable handoff: %w", err)
		}
		if replay {
			slog.Info(
				"inbound processor: published duplicate ignored",
				"message_id",
				ev.MessageID,
				"channel",
				ev.Channel,
			)
			return nil
		}
		traceID = claim.TraceID
		inboundClaim = &claim
		defer func() {
			if claimCompleted {
				return
			}
			releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := p.dedupRepo.Release(
				releaseCtx,
				ev.WorkspaceID,
				ev.ConnectionID,
				ev.Channel,
				ev.MessageID,
				claim,
			); err != nil && !errors.Is(err, repository.ErrInboundClaimLost) {
				slog.Error(
					"inbound processor: failed to release durable handoff claim",
					"error",
					err,
					"workspace_id",
					ev.WorkspaceID.String(),
					"channel",
					ev.Channel,
				)
			}
		}()
	}

	completeClaim := func() error {
		if inboundClaim == nil {
			return nil
		}
		if err := p.dedupRepo.MarkPublished(
			ctx,
			ev.WorkspaceID,
			ev.ConnectionID,
			ev.Channel,
			ev.MessageID,
			*inboundClaim,
		); err != nil {
			return fmt.Errorf("inbound: complete durable handoff: %w", err)
		}
		claimCompleted = true
		return nil
	}

	// Resolve/Create Contact Profile
	var contact *domain.Contact
	if p.contactRepo != nil {
		var username, phone string
		if ev.Metadata != nil {
			username = ev.Metadata["username"]
			phone = ev.Metadata["phone_number"]
		}
		if ev.Channel == "whatsapp" || ev.Channel == "whatsapp_cloud" {
			phone = ev.From
		}
		var err error
		contact, err = p.contactRepo.ResolveContact(ctx, ev.WorkspaceID, ev.Channel, ev.From, ev.SenderName, username, phone)
		if err != nil {
			slog.Error("inbound processor: failed to resolve contact profile", "error", err, "workspace_id", ev.WorkspaceID.String())
		}

		if contact != nil && !contact.BotActive && contact.BotPausedAt != nil {
			if time.Since(*contact.BotPausedAt) > 12*time.Hour {
				slog.Info("inbound processor: bot inactive for > 12 hours, auto-resetting to active", "contact_id", contact.ID)
				err := p.contactRepo.UpdateBotState(ctx, ev.WorkspaceID, contact.ID, true, nil)
				if err != nil {
					slog.Error("inbound processor: failed to reset bot state to active", "error", err, "contact_id", contact.ID)
				} else {
					contact.BotActive = true
					contact.BotPausedAt = nil
				}
			}
		}
	}

	// 1. Recipient Session Tracking
	if p.recipientSessionRepo != nil {
		err := p.recipientSessionRepo.Upsert(ctx, ev.WorkspaceID, ev.From, ev.Channel, ev.To, time.Now().UTC())
		if err != nil {
			slog.Error("inbound processor: failed to upsert recipient session", "error", err, "workspace_id", ev.WorkspaceID.String())
		}
	}

	// 3. Retrieve Workspace PII Opt-In
	var piiOptIn bool
	if p.wsRepo != nil {
		if ws, err := p.wsRepo.GetByID(ctx, ev.WorkspaceID); err == nil && ws != nil {
			piiOptIn = ws.PIIOptIn
		}
	}

	// 4. Construct base event payload
	payload := InboundEventPayload{
		Event:       "inbound_message",
		TraceID:     traceID,
		MessageID:   ev.MessageID,
		Channel:     ev.Channel,
		Timestamp:   time.Now().UTC().Format(time.RFC3339),
		WorkspaceID: ev.WorkspaceID.String(),
		From:        ev.From,
		To:          ev.To,
		Body:        ev.Body,
		Interactive: ev.Interactive,
		Story:       ev.Story,
	}

	// 5. Upload media to S3 if present
	if ev.Media != nil && len(ev.Media.Bytes) > 0 {
		if p.mediaEngine == nil {
			slog.Error("inbound processor: skipped S3 upload; S3 client/media engine is not configured")
		} else {
			mediaURL, err := p.mediaEngine.ProcessInbound(ctx, ev.WorkspaceID, ev.Media.MediaType, ev.Media.Bytes)
			if err != nil {
				slog.Error("inbound processor: media upload/process failed", "error", err)
			} else {
				payload.Media = &EventMedia{
					MediaURL:  mediaURL,
					MediaType: ev.Media.MediaType,
					Filename:  ev.Media.Filename,
					Caption:   ev.Media.Caption,
				}
				ev.Media.MediaURL = mediaURL
			}
		}
	}

	// 6. PII Opt-In check (Locations and Contacts)
	if piiOptIn {
		payload.Location = ev.Location
		payload.Contacts = ev.Contacts
	}

	// 7. Drop event if it's completely empty
	if payload.Body == "" && payload.Media == nil && payload.Location == nil && len(payload.Contacts) == 0 && payload.Interactive == nil && payload.Story == nil {
		slog.Debug("inbound processor: ignoring empty inbound event payload")
		return completeClaim()
	}

	// 8. Publish to NATS JetStream and Audit Log
	if p.publisher != nil {
		eventData, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("inbound: failed to marshal event payload: %w", err)
		}

		subject := fmt.Sprintf("inbound.events.%s", ev.WorkspaceID.String())
		err = p.publisher.Publish(ctx, subject, eventData, traceID)
		if err != nil {
			return fmt.Errorf("inbound: failed to publish event to NATS: %w", err)
		}
		if err := completeClaim(); err != nil {
			return err
		}

		if p.auditWriter != nil {
			err = p.auditWriter.Write(audit.NewEvent(ev.WorkspaceID, traceID, "inbound_message", eventData))
			if err != nil {
				slog.Error("inbound processor: failed to write audit log", "error", err, "trace_id", traceID)
			}
		}
	} else if err := completeClaim(); err != nil {
		return err
	}

	// 9. Route inbound event via InboundRouter
	if p.router != nil && contact != nil {
		if err := p.router.Route(ctx, contact, ev); err != nil {
			slog.Error("inbound processor: router failed to route event", "error", err, "contact_id", contact.ID)
		}
	}

	return nil
}

func isCanonicalDeliveryStatus(status string) bool {
	switch status {
	case "sent", "delivered", "read", "failed":
		return true
	default:
		return false
	}
}
