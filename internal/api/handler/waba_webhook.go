package handler

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/pablojhp.pergo/internal/channel"
	"github.com/pablojhp.pergo/internal/channel/whatsapp"
	"github.com/pablojhp.pergo/internal/inbound"
	"github.com/pablojhp.pergo/internal/media"
	"github.com/pablojhp.pergo/internal/repository"
)

const maxWABAWebhookBodyBytes int64 = 2 * 1024 * 1024

// WABAWebhookHandler handles verification and inbound payloads for Meta's WhatsApp Cloud API (WABA).
type WABAWebhookHandler struct {
	connectionsRepo  WABAConnectionStore
	inboundProcessor *inbound.InboundProcessor
	adapter          channel.InboundAdapter
}

type WABAConnectionStore interface {
	ListByWorkspace(ctx context.Context, workspaceID uuid.UUID) ([]*repository.Connection, error)
}

func NewWABAWebhookHandler(
	connectionsRepo WABAConnectionStore,
	inboundProcessor *inbound.InboundProcessor,
	mediaEngine media.Engine,
) *WABAWebhookHandler {
	return &WABAWebhookHandler{
		connectionsRepo:  connectionsRepo,
		inboundProcessor: inboundProcessor,
		adapter:          whatsapp.NewWABAInboundAdapter(mediaEngine),
	}
}

// SetBaseURL overrides the base Meta Graph API URL (useful for testing).
func (h *WABAWebhookHandler) SetBaseURL(url string) {
	if wa, ok := h.adapter.(*whatsapp.WABAInboundAdapter); ok {
		wa.SetBaseURL(url)
	}
}

// HandleGet verification from Meta
func (h *WABAWebhookHandler) HandleGet(c *echo.Context) error {
	workspaceIDStr, err := echo.PathParam[string](c, "workspace_id")
	if err != nil || workspaceIDStr == "" {
		return c.NoContent(http.StatusBadRequest)
	}
	workspaceID, err := uuid.Parse(workspaceIDStr)
	if err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	verifyToken := c.Request().URL.Query().Get("hub.verify_token")
	challenge := c.Request().URL.Query().Get("hub.challenge")

	// Load registered connections for the workspace
	conns, err := h.connectionsRepo.ListByWorkspace(c.Request().Context(), workspaceID)
	if err != nil {
		return c.NoContent(http.StatusForbidden)
	}

	if !matchesPersistedWABAVerifyToken(conns, verifyToken) {
		return c.NoContent(http.StatusForbidden)
	}

	return c.String(http.StatusOK, challenge)
}

// HandlePost ingests inbound messages from Meta
func (h *WABAWebhookHandler) HandlePost(c *echo.Context) error {
	workspaceIDStr, err := echo.PathParam[string](c, "workspace_id")
	if err != nil || workspaceIDStr == "" {
		return c.NoContent(http.StatusBadRequest)
	}
	workspaceID, err := uuid.Parse(workspaceIDStr)
	if err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	// Bound the raw body before tenant lookup or HMAC work. Meta media bytes are
	// downloaded separately and must never be embedded in this envelope.
	body, err := io.ReadAll(io.LimitReader(c.Request().Body, maxWABAWebhookBodyBytes+1))
	if err != nil {
		return c.NoContent(http.StatusBadRequest)
	}
	if int64(len(body)) > maxWABAWebhookBodyBytes {
		return c.NoContent(http.StatusRequestEntityTooLarge)
	}

	phoneNumberID, err := extractWABAPhoneNumberID(body)
	if err != nil {
		slog.Warn("waba webhook: invalid phone number identity", "workspace_id", workspaceID, "error", err)
		return c.NoContent(http.StatusForbidden)
	}

	// Load registered connections for the workspace and select the exact tenant
	// credential referenced by the signed payload. Never use the first WABA
	// connection: a workspace can own multiple Meta phone numbers.
	conns, err := h.connectionsRepo.ListByWorkspace(c.Request().Context(), workspaceID)
	if err != nil {
		return c.NoContent(http.StatusForbidden)
	}
	matchingConn, creds, err := selectWABAConnection(conns, phoneNumberID)
	if err != nil {
		slog.Warn("waba webhook: matching connection not found", "workspace_id", workspaceID, "phone_number_id", phoneNumberID)
		return c.NoContent(http.StatusForbidden)
	}

	signature := c.Request().Header.Get("X-Hub-Signature-256")
	if !validWABASignature(body, signature, creds.AppSecret) {
		slog.Warn("waba webhook: invalid signature", "workspace_id", workspaceID, "phone_number_id", phoneNumberID)
		return c.NoContent(http.StatusForbidden)
	}

	events, err := h.adapter.Parse(
		c.Request().Context(),
		body,
		map[string]string{"X-Hub-Signature-256": signature},
		matchingConn,
	)
	if err != nil {
		slog.Warn("waba webhook: adapter failed to parse", "error", err)
		if errors.Is(err, media.ErrDisabled) || errors.Is(err, whatsapp.ErrWABAMediaRetryable) {
			c.Response().Header().Set("Retry-After", "300")
			return c.NoContent(http.StatusServiceUnavailable)
		}
		return c.NoContent(http.StatusForbidden)
	}

	ctx := c.Request().Context()
	for _, event := range events {
		if h.inboundProcessor != nil {
			err := h.inboundProcessor.Process(ctx, event)
			if err != nil {
				slog.Error("waba webhook: inbound processor failed", "error", err, "message_id", event.MessageID)
				return c.NoContent(http.StatusInternalServerError)
			}
		}
	}

	return c.NoContent(http.StatusOK)
}

func matchesPersistedWABAVerifyToken(conns []*repository.Connection, supplied string) bool {
	if !acceptableWABAVerifyToken(supplied) {
		return false
	}
	for _, conn := range conns {
		if conn == nil || conn.Channel != "whatsapp_cloud" {
			continue
		}
		var creds whatsapp.WABAConfig
		if err := json.Unmarshal(conn.Credentials, &creds); err != nil || !acceptableWABAVerifyToken(creds.VerifyToken) {
			continue
		}
		if subtle.ConstantTimeCompare([]byte(supplied), []byte(creds.VerifyToken)) == 1 {
			return true
		}
	}
	return false
}

func acceptableWABAVerifyToken(token string) bool {
	return whatsapp.ValidateVerifyToken(token) == nil
}

func extractWABAPhoneNumberID(payload []byte) (string, error) {
	var envelope struct {
		Entry []struct {
			Changes []struct {
				Value struct {
					Metadata struct {
						PhoneNumberID string `json:"phone_number_id"`
					} `json:"metadata"`
				} `json:"value"`
			} `json:"changes"`
		} `json:"entry"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return "", fmt.Errorf("decode payload: %w", err)
	}

	ids := make(map[string]struct{})
	changeCount := 0
	for _, entry := range envelope.Entry {
		for _, change := range entry.Changes {
			changeCount++
			id := strings.TrimSpace(change.Value.Metadata.PhoneNumberID)
			if id == "" {
				return "", errors.New("missing phone_number_id")
			}
			ids[id] = struct{}{}
		}
	}
	if changeCount == 0 {
		return "", errors.New("missing phone_number_id")
	}
	if len(ids) > 1 {
		return "", errors.New("payload contains multiple phone_number_id values")
	}
	for id := range ids {
		return id, nil
	}
	return "", errors.New("missing phone_number_id")
}

func selectWABAConnection(conns []*repository.Connection, phoneNumberID string) (*repository.Connection, whatsapp.WABAConfig, error) {
	var selected *repository.Connection
	var selectedCreds whatsapp.WABAConfig

	for _, conn := range conns {
		if conn == nil || conn.Channel != "whatsapp_cloud" {
			continue
		}
		var creds whatsapp.WABAConfig
		if err := json.Unmarshal(conn.Credentials, &creds); err != nil {
			continue
		}
		if creds.PhoneNumberID != phoneNumberID {
			continue
		}
		if selected != nil {
			return nil, whatsapp.WABAConfig{}, errors.New("multiple connections match phone_number_id")
		}
		selected = conn
		selectedCreds = creds
	}

	if selected == nil {
		return nil, whatsapp.WABAConfig{}, errors.New("connection not found")
	}
	if err := whatsapp.ValidateAppSecret(selectedCreds.AppSecret); err != nil {
		return nil, whatsapp.WABAConfig{}, errors.New("connection app_secret is invalid")
	}
	return selected, selectedCreds, nil
}

func validWABASignature(payload []byte, signatureHeader string, appSecret string) bool {
	if appSecret == "" {
		return false
	}

	const prefix = "sha256="
	signatureHeader = strings.TrimSpace(signatureHeader)
	if !strings.HasPrefix(signatureHeader, prefix) {
		return false
	}

	provided, err := hex.DecodeString(strings.TrimPrefix(signatureHeader, prefix))
	if err != nil || len(provided) != sha256.Size {
		return false
	}

	mac := hmac.New(sha256.New, []byte(appSecret))
	_, _ = mac.Write(payload)
	return hmac.Equal(provided, mac.Sum(nil))
}
