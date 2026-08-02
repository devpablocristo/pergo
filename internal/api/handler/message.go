package handler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/pablojhp.pergo/internal/api/middleware"
	"github.com/pablojhp.pergo/internal/domain"
	"github.com/pablojhp.pergo/internal/media"
	"github.com/pablojhp.pergo/internal/outbound"
	"github.com/pablojhp.pergo/internal/platform/messagebus"
	"github.com/pablojhp.pergo/internal/platform/postgres/tenant"
	"github.com/pablojhp.pergo/internal/platform/storage"
	"github.com/pablojhp.pergo/internal/repository"
)

// Publisher defines the interface for publishing messages to a queue.
// JetStream implementation provides dedup via Nats-Msg-Id = traceID.
type Publisher interface {
	Publish(ctx context.Context, subject string, data []byte, traceID string) error
}

// ConnectionFinder abstracts querying connection details for routing.
type ConnectionFinder interface {
	GetBySenderIdentity(ctx context.Context, workspaceID uuid.UUID, senderIdentity string) (*repository.Connection, error)
	GetDefaultChannelConnection(ctx context.Context, workspaceID uuid.UUID, channel string) (*repository.Connection, error)
}

// MessageIdempotencyStore preserves the legacy optional idempotency contract.
// Production uses MessageIngressLedger; this port remains available during the
// compatibility window and is covered by the original durable-replay tests.
type MessageIdempotencyStore interface {
	Acquire(
		context.Context,
		uuid.UUID,
		string,
		string,
		string,
		time.Duration,
	) (repository.MessageIdempotency, bool, error)
	Get(
		context.Context,
		uuid.UUID,
		string,
	) (repository.MessageIdempotency, error)
	MarkAccepted(
		context.Context,
		repository.MessageIdempotency,
	) (repository.MessageIdempotency, error)
	Release(context.Context, repository.MessageIdempotency) error
}

// MessageIngressLedger is the consumer-owned port for the durable
// HTTP-to-JetStream handoff.
type MessageIngressLedger interface {
	Claim(
		ctx context.Context,
		workspaceID uuid.UUID,
		idempotencyKey string,
		payloadHash [32]byte,
		traceID string,
		receiptID uuid.UUID,
		lease time.Duration,
	) (
		storedReceipt uuid.UUID,
		queuedAt time.Time,
		claimToken uuid.UUID,
		claimGeneration int64,
		replay bool,
		retryAfter time.Duration,
		err error,
	)
	MarkQueued(
		ctx context.Context,
		workspaceID uuid.UUID,
		idempotencyKey string,
		claimToken uuid.UUID,
		claimGeneration int64,
		queuedAt time.Time,
	) error
}

// receiptIngestor is the durable extension implemented by the production
// outbound processor. Legacy callers can continue to use OutboundProcessor.
type receiptIngestor interface {
	IngestWithReceipt(
		ctx context.Context,
		workspaceID uuid.UUID,
		traceID string,
		receiptID uuid.UUID,
		req *domain.CreateMessageRequest,
	) (*domain.QueueMessage, error)
}

// MessageHandler holds dependencies for the POST /messages endpoint.
type MessageHandler struct {
	Ingestor       outbound.OutboundProcessor
	IngressLedger  MessageIngressLedger
	IngressLease   time.Duration
	Idempotency    MessageIdempotencyStore
	Publisher      Publisher
	QueueDepth     *middleware.QueueDepthTracker
	S3Client       *storage.S3Client
	ConnectionRepo ConnectionFinder
}

// RegisterRoutes wires the message endpoints onto the Echo router.
// Optional middlewares are applied before the handler.
func (h *MessageHandler) RegisterRoutes(e *echo.Echo, middlewares ...echo.MiddlewareFunc) {
	e.POST("/api/v1/messages", h.Create, middlewares...)
}

// Create handles POST /messages — validates the payload, checks backpressure,
// generates a message ID, publishes to JetStream, and returns 202 Accepted
// with trace correlation.
func (h *MessageHandler) Create(c *echo.Context) error {
	// Extract trace_id from context (set by trace middleware)
	traceID, _ := middleware.TraceIDFrom(c.Request().Context())

	// Extract workspace_id from context (set by auth middleware)
	workspaceID, _ := tenant.WorkspaceIDFrom(c.Request().Context())

	var (
		rawBody        []byte
		idempotencyKey string
	)
	if h.IngressLedger != nil {
		idempotencyKey = c.Request().Header.Get("Idempotency-Key")
		if strings.TrimSpace(idempotencyKey) == "" {
			return c.JSON(http.StatusBadRequest, domain.ErrorResponse{
				Code:    "missing_idempotency_key",
				Message: "Idempotency-Key header is required and must contain at most 255 characters",
			})
		}
		if !validMessageIdempotencyKey(idempotencyKey) {
			return c.JSON(http.StatusBadRequest, domain.ErrorResponse{
				Code:    "invalid_idempotency_key",
				Message: "Idempotency-Key is invalid",
			})
		}

		traceID = c.Request().Header.Get("X-Trace-ID")
		if strings.TrimSpace(traceID) == "" || len(traceID) > 255 {
			return c.JSON(http.StatusBadRequest, domain.ErrorResponse{
				Code:    "missing_trace_id",
				Message: "X-Trace-ID header is required and must contain at most 255 characters",
			})
		}

		var err error
		rawBody, err = io.ReadAll(io.LimitReader(c.Request().Body, maxDurableMessageBodyBytes+1))
		if err != nil {
			return c.JSON(http.StatusBadRequest, domain.ErrorResponse{
				Code:    "invalid_payload",
				Message: "request body validation failed",
			})
		}
		if len(rawBody) > maxDurableMessageBodyBytes {
			return c.JSON(http.StatusRequestEntityTooLarge, domain.ErrorResponse{
				Code:    "payload_too_large",
				Message: "request body exceeds the 1 MiB limit",
			})
		}
		c.Request().Body = io.NopCloser(bytes.NewReader(rawBody))
	}

	// Bind JSON body to request struct.
	var req domain.CreateMessageRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, domain.ErrorResponse{
			Code:    "invalid_payload",
			Message: "request body validation failed",
			Details: []domain.FieldError{
				{Field: "body", Message: "invalid JSON or missing required fields"},
			},
		})
	}
	if valErr := domain.ValidateMessage(&req); valErr != nil {
		return c.JSON(http.StatusBadRequest, *valErr)
	}

	var (
		idempotency repository.MessageIdempotency
		acquired    bool
	)
	if h.IngressLedger == nil {
		idempotencyKey = c.Request().Header.Get("Idempotency-Key")
		if idempotencyKey != "" {
			if !validMessageIdempotencyKey(idempotencyKey) {
				return c.JSON(http.StatusBadRequest, domain.ErrorResponse{
					Code:    "invalid_idempotency_key",
					Message: "Idempotency-Key is invalid",
				})
			}
			if h.Idempotency == nil {
				return c.JSON(http.StatusServiceUnavailable, domain.ErrorResponse{
					Code:    "idempotency_unavailable",
					Message: "durable message ingestion is unavailable",
				})
			}
			payloadHash, hashErr := messagePayloadHash(req)
			if hashErr != nil {
				return c.JSON(http.StatusBadRequest, domain.ErrorResponse{
					Code:    "invalid_payload",
					Message: "request body validation failed",
				})
			}
			var err error
			idempotency, acquired, err = h.Idempotency.Acquire(
				c.Request().Context(),
				workspaceID,
				idempotencyKey,
				payloadHash,
				traceID,
				h.ingressLease(),
			)
			if errors.Is(err, repository.ErrMessageIdempotencyConflict) {
				return c.JSON(http.StatusConflict, domain.ErrorResponse{
					Code:    "idempotency_key_reused",
					Message: "Idempotency-Key was already used with another payload",
				})
			}
			if err != nil {
				return c.JSON(http.StatusServiceUnavailable, domain.ErrorResponse{
					Code:    "idempotency_unavailable",
					Message: "durable message ingestion is unavailable",
				})
			}
			traceID = idempotency.TraceID
			if idempotency.Accepted() {
				return h.accepted(c, workspaceID, req.Channel, idempotency, true)
			}
			if !acquired {
				replayed, waitErr := h.waitForAccepted(
					c.Request().Context(),
					workspaceID,
					idempotencyKey,
					500*time.Millisecond,
				)
				if waitErr == nil && replayed.Accepted() {
					return h.accepted(
						c,
						workspaceID,
						req.Channel,
						replayed,
						true,
					)
				}
				c.Response().Header().Set("Retry-After", "1")
				return c.JSON(http.StatusTooEarly, domain.ErrorResponse{
					Code:    "idempotency_in_progress",
					Message: "an identical request is still being accepted",
				})
			}
		}
	}

	// Dynamically wrap legacy fields if Ingestor is not injected
	ingestor := h.Ingestor
	if ingestor == nil {
		var mediaEngine media.Engine
		if h.S3Client != nil {
			mediaEngine = media.NewDefaultEngine(h.S3Client)
		}
		var tracker outbound.QueueDepthChecker
		if h.QueueDepth != nil {
			tracker = h.QueueDepth
		}
		ingestor = outbound.NewProcessor(tracker, mediaEngine, h.ConnectionRepo, h.Publisher)
	}

	var (
		qMsg            *domain.QueueMessage
		msgID           uuid.UUID
		claimToken      uuid.UUID
		claimGeneration int64
		err             error
	)

	if h.IngressLedger != nil {
		if workspaceID == uuid.Nil {
			return c.JSON(http.StatusInternalServerError, domain.ErrorResponse{
				Code:    "internal_error",
				Message: "workspace context is not configured",
			})
		}

		payloadHash := hashIngressPayload(traceID, rawBody)
		proposedReceipt := deterministicReceiptID(workspaceID, idempotencyKey)
		var (
			queuedAt   time.Time
			replay     bool
			retryAfter time.Duration
		)
		msgID, queuedAt, claimToken, claimGeneration, replay, retryAfter, err = h.IngressLedger.Claim(
			c.Request().Context(),
			workspaceID,
			idempotencyKey,
			payloadHash,
			traceID,
			proposedReceipt,
			h.ingressLease(),
		)
		if err != nil {
			switch {
			case errors.Is(err, repository.ErrIngressIdempotencyKeyReused):
				return c.JSON(http.StatusConflict, domain.ErrorResponse{
					Code:    "idempotency_key_reused",
					Message: "idempotency identity was already used with different content or trace",
				})
			case errors.Is(err, repository.ErrIngressClaimActive):
				seconds := int(math.Ceil(retryAfter.Seconds()))
				if seconds < 1 {
					seconds = 1
				}
				c.Response().Header().Set("Retry-After", strconv.Itoa(seconds))
				return c.JSON(http.StatusTooEarly, domain.ErrorResponse{
					Code:    "idempotency_in_progress",
					Message: "an identical delivery request is still being published",
				})
			default:
				slog.Error("failed to claim message ingress", "error", err, "trace_id", traceID, "workspace_id", workspaceID.String())
				return c.JSON(http.StatusInternalServerError, domain.ErrorResponse{
					Code:    "internal_error",
					Message: "failed to persist message ingress",
				})
			}
		}
		if replay {
			c.Response().Header().Set("X-Trace-Id", traceID)
			c.Response().Header().Set("Idempotency-Replayed", "true")
			return c.JSON(http.StatusAccepted, domain.CreateMessageResponse{
				MessageID: msgID,
				Status:    domain.StatusQueued,
				QueuedAt:  queuedAt,
			})
		}

		durableIngestor, ok := ingestor.(receiptIngestor)
		if !ok {
			return c.JSON(http.StatusInternalServerError, domain.ErrorResponse{
				Code:    "internal_error",
				Message: "durable outbound processor is not configured",
			})
		}
		qMsg, err = durableIngestor.IngestWithReceipt(
			c.Request().Context(),
			workspaceID,
			traceID,
			msgID,
			&req,
		)
	} else {
		qMsg, err = ingestor.Ingest(c.Request().Context(), workspaceID, traceID, &req)
		msgID = uuid.New()
	}

	if err != nil {
		if acquired {
			if releaseErr := h.Idempotency.Release(
				c.Request().Context(),
				idempotency,
			); releaseErr != nil {
				slog.Error(
					"failed to release message idempotency lease",
					"error",
					releaseErr,
					"trace_id",
					traceID,
					"workspace_id",
					workspaceID.String(),
				)
			}
		}
		if errors.Is(err, outbound.ErrQueueFull) {
			c.Response().Header().Set("Retry-After", "5")
			return c.JSON(http.StatusTooManyRequests, domain.ErrorResponse{
				Code:     "queue_full",
				Message:  "per-session message queue limit exceeded",
				MoreInfo: "https://docs.pergo.dev/errors/queue_full",
			})
		}
		if errors.Is(err, messagebus.ErrPayloadTooLarge) {
			return c.JSON(http.StatusRequestEntityTooLarge, domain.ErrorResponse{
				Code:    "payload_too_large",
				Message: "serialized message exceeds the queue transport limit",
			})
		}

		var valErr *outbound.ValidationError
		if errors.As(err, &valErr) {
			return c.JSON(http.StatusBadRequest, *valErr.Response)
		}

		var mediaErr *outbound.MediaError
		if errors.As(err, &mediaErr) {
			if mediaErr.Code == "media_size_exceeded" {
				return c.JSON(http.StatusUnprocessableEntity, domain.ErrorResponse{
					Code:    "media_size_exceeded",
					Message: mediaErr.Message,
					Details: []domain.FieldError{
						{Field: mediaErr.Field, Message: "file exceeds 25MB limit"},
					},
				})
			}
			if mediaErr.Code == "internal_error" {
				return c.JSON(http.StatusInternalServerError, domain.ErrorResponse{
					Code:    "internal_error",
					Message: mediaErr.Message,
				})
			}
			return c.JSON(http.StatusUnprocessableEntity, domain.ErrorResponse{
				Code:    "media_download_failed",
				Message: mediaErr.Message,
				Details: []domain.FieldError{
					{Field: mediaErr.Field, Message: mediaErr.Err.Error()},
				},
			})
		}

		var routeErr *outbound.RouteError
		if errors.As(err, &routeErr) {
			if routeErr.Message == "route resolver is not configured" {
				return c.JSON(http.StatusInternalServerError, domain.ErrorResponse{
					Code:    "internal_error",
					Message: routeErr.Message,
				})
			}
			return c.JSON(http.StatusUnprocessableEntity, domain.ErrorResponse{
				Code:    "route_not_found",
				Message: routeErr.Message,
			})
		}

		// Generic internal server error
		return c.JSON(http.StatusInternalServerError, domain.ErrorResponse{
			Code:    "internal_error",
			Message: "failed to process message",
		})
	}

	if h.IngressLedger != nil {
		if err := h.IngressLedger.MarkQueued(
			c.Request().Context(),
			workspaceID,
			idempotencyKey,
			claimToken,
			claimGeneration,
			qMsg.QueuedAt,
		); err != nil {
			slog.Error("failed to complete message ingress", "error", err, "trace_id", traceID, "workspace_id", workspaceID.String())
			return c.JSON(http.StatusInternalServerError, domain.ErrorResponse{
				Code:    "internal_error",
				Message: "message publication is awaiting reconciliation",
			})
		}
	} else if acquired {
		idempotency, err = h.Idempotency.MarkAccepted(
			c.Request().Context(),
			idempotency,
		)
		if err != nil {
			return c.JSON(http.StatusServiceUnavailable, domain.ErrorResponse{
				Code:    "idempotency_unavailable",
				Message: "message acceptance could not be recorded",
			})
		}
		return h.accepted(c, workspaceID, req.Channel, idempotency, false)
	}

	// Log the ingestion event
	slog.Info("message ingested",
		"trace_id", traceID,
		"workspace_id", workspaceID.String(),
		"message_id", msgID.String(),
		"channel", req.Channel,
	)

	// Set X-Trace-Id response header
	c.Response().Header().Set("X-Trace-Id", traceID)

	// Return 202 Accepted
	return c.JSON(http.StatusAccepted, domain.CreateMessageResponse{
		MessageID: msgID,
		Status:    domain.StatusQueued,
		QueuedAt:  qMsg.QueuedAt,
	})
}

var messageIdempotencyKeyPattern = regexp.MustCompile(
	`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,254}$`,
)

func validMessageIdempotencyKey(value string) bool {
	return value == strings.TrimSpace(value) &&
		messageIdempotencyKeyPattern.MatchString(value)
}

func messagePayloadHash(request domain.CreateMessageRequest) (string, error) {
	payload, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func (h *MessageHandler) waitForAccepted(
	ctx context.Context,
	workspaceID uuid.UUID,
	idempotencyKey string,
	maxWait time.Duration,
) (repository.MessageIdempotency, error) {
	timer := time.NewTimer(maxWait)
	defer timer.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		record, err := h.Idempotency.Get(ctx, workspaceID, idempotencyKey)
		if err != nil {
			return repository.MessageIdempotency{}, err
		}
		if record.Accepted() {
			return record, nil
		}
		select {
		case <-ctx.Done():
			return repository.MessageIdempotency{}, ctx.Err()
		case <-timer.C:
			return record, context.DeadlineExceeded
		case <-ticker.C:
		}
	}
}

func (h *MessageHandler) accepted(
	c *echo.Context,
	workspaceID uuid.UUID,
	channel string,
	record repository.MessageIdempotency,
	replayed bool,
) error {
	slog.Info(
		"message ingested",
		"trace_id",
		record.TraceID,
		"workspace_id",
		workspaceID.String(),
		"message_id",
		record.MessageID.String(),
		"channel",
		channel,
		"replayed",
		replayed,
	)
	c.Response().Header().Set("X-Trace-Id", record.TraceID)
	if replayed {
		c.Response().Header().Set("Idempotency-Replayed", "true")
	}
	return c.JSON(http.StatusAccepted, domain.CreateMessageResponse{
		MessageID: record.MessageID,
		Status:    domain.StatusQueued,
		QueuedAt:  record.QueuedAt,
	})
}

var messageReceiptNamespace = uuid.MustParse("e0316231-814e-5f7a-9f9d-56d046879a17")

const maxDurableMessageBodyBytes = 1 << 20

func deterministicReceiptID(workspaceID uuid.UUID, idempotencyKey string) uuid.UUID {
	name := workspaceID.String() + "\x00" + idempotencyKey
	return uuid.NewSHA1(messageReceiptNamespace, []byte(name))
}

func hashIngressPayload(traceID string, payload []byte) [32]byte {
	hashInput := make([]byte, 0, len(traceID)+1+len(payload))
	hashInput = append(hashInput, traceID...)
	hashInput = append(hashInput, 0)
	hashInput = append(hashInput, payload...)
	return sha256.Sum256(hashInput)
}

func (h *MessageHandler) ingressLease() time.Duration {
	if h.IngressLease > 0 {
		return h.IngressLease
	}
	return 30 * time.Second
}
