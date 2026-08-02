package email

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/pablojhp.pergo/internal/channel"
	"github.com/pablojhp.pergo/internal/platform/httpresponse"
)

// MauticConfig holds configuration for Mautic REST API integration.
type MauticConfig struct {
	BaseURL    string
	Username   string
	Password   string
	HTTPClient *http.Client
}

// MauticProvider implements Provider for Mautic API.
type MauticProvider struct {
	config MauticConfig
}

// NewMauticProvider creates a new Mautic provider instance.
func NewMauticProvider(cfg MauticConfig) *MauticProvider {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	return &MauticProvider{config: cfg}
}

type mauticSendRequest struct {
	Subject   string            `json:"subject"`
	Body      string            `json:"body"`
	PlainBody string            `json:"plainText"`
	To        []string          `json:"to"`
	Headers   map[string]string `json:"headers,omitempty"`
}

// Send dispatches an email via Mautic API.
func (m *MauticProvider) Send(ctx context.Context, msg *EmailMessage) (string, error) {
	providerMsgID := msg.ID
	if providerMsgID == "" {
		providerMsgID = uuid.New().String()
	}

	payload := mauticSendRequest{
		Subject:   msg.Subject,
		Body:      msg.HTMLBody,
		PlainBody: msg.TextBody,
		To:        msg.To,
		Headers:   msg.Headers,
	}
	if payload.Body == "" {
		payload.Body = msg.TextBody
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal Mautic payload: %w", err)
	}

	endpoint := fmt.Sprintf("%s/api/emails/send", m.config.BaseURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", channel.NewTerminalError(errors.New("failed to create Mautic request"))
	}

	req.Header.Set("Content-Type", "application/json")
	if m.config.Username != "" && m.config.Password != "" {
		req.SetBasicAuth(m.config.Username, m.config.Password)
	}

	resp, err := m.config.HTTPClient.Do(req)
	if err != nil {
		return "", channel.NewUncertainError(errors.New("mautic transport response lost"))
	}
	defer func() { _ = resp.Body.Close() }()

	_, readErr := httpresponse.Read(resp)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if readErr != nil {
			return "", channel.NewUncertainError(errors.New("mautic response body is invalid"))
		}
		return providerMsgID, nil
	}
	return "", classifyEmailProviderHTTPStatus("mautic", resp.StatusCode)
}
