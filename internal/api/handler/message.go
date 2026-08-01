package handler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/pablojhp.pergo/internal/api/middleware"
	"github.com/pablojhp.pergo/internal/domain"
	"github.com/pablojhp.pergo/internal/media"
	"github.com/pablojhp.pergo/internal/outbound"
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

// MessageHandler holds dependencies for the POST /messages endpoint.
type MessageHandler struct {
	Ingestor       outbound.OutboundProcessor
	Publisher      Publisher
	QueueDepth     *middleware.QueueDepthTracker
	S3Client       *storage.S3Client
	ConnectionRepo ConnectionFinder
	Idempotency    MessageIdempotencyStore
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

	// Bind JSON body to request struct
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

	idempotencyKey := c.Request().Header.Get("Idempotency-Key")
	var idempotency repository.MessageIdempotency
	acquired := false
	var err error
	if idempotencyKey != "" {
		if !validMessageIdempotencyKey(idempotencyKey) {
			return c.JSON(http.StatusBadRequest, domain.ErrorResponse{
				Code:    "invalid_idempotency_key",
				Message: "Idempotency-Key is invalid",
			})
		}
		if validationError := domain.ValidateMessage(&req); validationError != nil {
			return c.JSON(http.StatusBadRequest, *validationError)
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
		idempotency, acquired, err = h.Idempotency.Acquire(
			c.Request().Context(),
			workspaceID,
			idempotencyKey,
			payloadHash,
			traceID,
			30*time.Second,
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
					c, workspaceID, req.Channel, replayed, true,
				)
			}
			c.Response().Header().Set("Retry-After", "1")
			return c.JSON(http.StatusTooEarly, domain.ErrorResponse{
				Code:    "idempotency_in_progress",
				Message: "an identical request is still being accepted",
			})
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

	// Ingest using OutboundProcessor
	qMsg, err := ingestor.Ingest(c.Request().Context(), workspaceID, traceID, &req)
	if err != nil {
		if acquired {
			if releaseErr := h.Idempotency.Release(
				c.Request().Context(), idempotency,
			); releaseErr != nil {
				slog.Error(
					"failed to release message idempotency lease",
					"error", releaseErr,
					"trace_id", traceID,
					"workspace_id", workspaceID.String(),
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

	if acquired {
		idempotency, err = h.Idempotency.MarkAccepted(
			c.Request().Context(), idempotency,
		)
		if err != nil {
			return c.JSON(http.StatusServiceUnavailable, domain.ErrorResponse{
				Code:    "idempotency_unavailable",
				Message: "message acceptance could not be recorded",
			})
		}
		return h.accepted(c, workspaceID, req.Channel, idempotency, false)
	}
	return h.accepted(
		c,
		workspaceID,
		req.Channel,
		repository.MessageIdempotency{
			MessageID: uuid.New(), TraceID: traceID, QueuedAt: qMsg.QueuedAt,
		},
		false,
	)
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
		"trace_id", record.TraceID,
		"workspace_id", workspaceID.String(),
		"message_id", record.MessageID.String(),
		"channel", channel,
		"replayed", replayed,
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
