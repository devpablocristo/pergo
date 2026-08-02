package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/pablojhp.pergo/internal/platform/netpolicy"
	"github.com/pablojhp.pergo/internal/repository"
)

// SubscriptionStore defines the database abstraction for webhook subscription retrieval.
type SubscriptionStore interface {
	Get(ctx context.Context, id uuid.UUID) (*repository.WebhookSubscription, error)
}

// DLQStore defines the database abstraction for DLQ persistence.
type DLQStore interface {
	InsertDLQ(ctx context.Context, workspaceID uuid.UUID, subscriptionID uuid.UUID, traceID, messageID, eventType string, payload []byte, url string, attempts int, failureReason *string) error
}

// WorkspaceStore defines the database abstraction for workspace settings.
type WorkspaceStore interface {
	GetByID(ctx context.Context, id uuid.UUID) (*repository.Workspace, error)
}

// HTTPClient defines the client abstraction for executing HTTP requests.
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// HTTPError is returned when a webhook endpoint responds with a non-2xx status code.
type HTTPError struct {
	StatusCode int
	Status     string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("http status %s", e.Status)
}

var (
	ErrSubscriptionInactive = errors.New("webhook subscription is inactive")
	ErrSubscriptionNotFound = errors.New("webhook subscription not found")
	ErrPIIRedactionFailed   = errors.New("webhook PII redaction failed")
)

const maxWebhookResponseBodyBytes int64 = 1 << 20

type WebhookDeliveryTask struct {
	ID             uuid.UUID `json:"id"`
	SubscriptionID uuid.UUID `json:"subscription_id"`
	WorkspaceID    uuid.UUID `json:"workspace_id"`
	Event          string    `json:"event"`
	TraceID        string    `json:"trace_id"`
	MessageID      string    `json:"message_id"`
	Payload        []byte    `json:"payload"`
	Mode           string    `json:"mode"` // "inbound" | "outbound"
}

// WebhookDispatcher defines the interface for webhook payload processing and delivery.
type WebhookDispatcher interface {
	Dispatch(ctx context.Context, task WebhookDeliveryTask) error
	WriteToDLQ(ctx context.Context, workspaceID uuid.UUID, subscriptionID uuid.UUID, traceID, messageID, event string, rawEvent []byte, attempts int, failReason string) error
}

// DefaultDispatcher is the concrete implementation of WebhookDispatcher.
type DefaultDispatcher struct {
	subStore    SubscriptionStore
	dlqStore    DLQStore
	wsStore     WorkspaceStore
	client      HTTPClient
	verbsEngine *VerbsEngine
}

// NewDefaultDispatcher creates a new DefaultDispatcher instance.
func NewDefaultDispatcher(subStore SubscriptionStore, dlqStore DLQStore, wsStore WorkspaceStore, client HTTPClient, verbsEngine *VerbsEngine) *DefaultDispatcher {
	if client == nil {
		client = netpolicy.NewPublicHTTPPolicy().Client(10 * time.Second)
	}
	return &DefaultDispatcher{
		subStore:    subStore,
		dlqStore:    dlqStore,
		wsStore:     wsStore,
		client:      client,
		verbsEngine: verbsEngine,
	}
}

// Dispatch processes compliance, signs the payload, and posts it to the subscription's configured webhook URL.
func (d *DefaultDispatcher) Dispatch(ctx context.Context, task WebhookDeliveryTask) error {
	// 1. Fetch Subscription by SubscriptionID
	sub, err := d.subStore.Get(ctx, task.SubscriptionID)
	if err != nil {
		if errors.Is(err, repository.ErrWebhookSubscriptionNotFound) {
			return ErrSubscriptionNotFound
		}
		return err
	}

	if !sub.Active {
		return ErrSubscriptionInactive
	}
	if sub.WorkspaceID != task.WorkspaceID {
		return ErrSubscriptionNotFound
	}

	payloadBytes := task.Payload

	// 2. Compliance PII Redaction for inbound events
	if task.Mode == "inbound" {
		var wsOptIn bool
		if d.wsStore != nil {
			if ws, err := d.wsStore.GetByID(ctx, task.WorkspaceID); err == nil && ws != nil {
				wsOptIn = ws.PIIOptIn
			}
		}

		if !wsOptIn {
			var inboundPayload struct {
				Event       string `json:"event"`
				TraceID     string `json:"trace_id"`
				MessageID   string `json:"message_id"`
				Channel     string `json:"channel"`
				Timestamp   string `json:"timestamp"`
				WorkspaceID string `json:"workspace_id"`
				From        string `json:"from"`
				Body        string `json:"body,omitempty"`
				Media       any    `json:"media,omitempty"`
				Location    any    `json:"location,omitempty"`
				Contacts    any    `json:"contacts,omitempty"`
			}
			if err := json.Unmarshal(task.Payload, &inboundPayload); err != nil {
				return fmt.Errorf("%w: invalid inbound payload", ErrPIIRedactionFailed)
			}
			// Pseudonymize the sender with the tenant subscription secret so a
			// phone-number dictionary cannot reverse a plain SHA-256 digest.
			hasher := hmac.New(sha256.New, sub.Secret)
			hasher.Write([]byte(inboundPayload.From))
			inboundPayload.From = hex.EncodeToString(hasher.Sum(nil))

			// Strip structured high-risk fields. Message body is deliberately
			// retained as the subscribed business event content.
			inboundPayload.Location = nil
			inboundPayload.Contacts = nil

			payloadBytes, err = json.Marshal(inboundPayload)
			if err != nil {
				return fmt.Errorf("%w: encode redacted inbound payload", ErrPIIRedactionFailed)
			}
		}
	}

	// 3. Dispatch HTTP Post request
	timestamp := fmt.Sprintf("%d", time.Now().Unix())
	signature := SignPayload(payloadBytes, sub.Secret, timestamp)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sub.URL, bytes.NewReader(payloadBytes))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-PerGo-Signature", signature)
	req.Header.Set("X-PerGo-Delivery-ID", task.ID.String())
	if task.TraceID != "" {
		req.Header.Set("X-Trace-ID", task.TraceID)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("http dispatch error: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &HTTPError{StatusCode: resp.StatusCode, Status: resp.Status}
	}

	// Read response body to extract potential messaging verbs
	bodyBytes, err := io.ReadAll(io.LimitReader(resp.Body, maxWebhookResponseBodyBytes+1))
	if err != nil {
		slog.Error("failed to read webhook response body", "error", err, "trace_id", task.TraceID)
		return nil
	}
	if int64(len(bodyBytes)) > maxWebhookResponseBodyBytes {
		// The callback already returned 2xx, so retrying the POST could duplicate
		// a tenant-visible side effect. Verbs are optional; ignore an abusive
		// response body while treating the accepted delivery as successful.
		slog.Warn("ignoring oversized webhook response body", "trace_id", task.TraceID)
		return nil
	}

	var verbs []Verb
	if err := json.Unmarshal(bodyBytes, &verbs); err == nil && len(verbs) > 0 {
		if d.verbsEngine != nil {
			if err := d.verbsEngine.Execute(ctx, task, verbs); err != nil {
				slog.Error("verbs engine execution failed", "error", err, "trace_id", task.TraceID)
			}
		}
	}

	return nil
}

// WriteToDLQ writes a permanently failed webhook event to the database DLQ repository.
func (d *DefaultDispatcher) WriteToDLQ(
	ctx context.Context,
	workspaceID uuid.UUID,
	subscriptionID uuid.UUID,
	traceID, messageID, event string,
	rawEvent []byte,
	attempts int,
	failReason string,
) error {
	// Retrieve subscription to get webhook URL for archiving
	sub, err := d.subStore.Get(ctx, subscriptionID)
	if err != nil {
		return fmt.Errorf("failed to retrieve subscription for DLQ: %w", err)
	}
	if sub.WorkspaceID != workspaceID {
		return fmt.Errorf("failed to retrieve subscription for DLQ: %w", ErrSubscriptionNotFound)
	}

	return d.dlqStore.InsertDLQ(
		ctx,
		workspaceID,
		subscriptionID,
		traceID,
		messageID,
		event,
		rawEvent,
		sub.URL,
		attempts,
		&failReason,
	)
}

// SignPayload computes the HMAC-SHA256 signature for a webhook delivery request.
func SignPayload(payload []byte, secret []byte, timestamp string) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(timestamp))
	mac.Write([]byte("."))
	mac.Write(payload)
	signature := hex.EncodeToString(mac.Sum(nil))
	return fmt.Sprintf("t=%s,v1=%s", timestamp, signature)
}
