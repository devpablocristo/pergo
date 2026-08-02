package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"

	"github.com/pablojhp.pergo/internal/channel"
	"github.com/pablojhp.pergo/internal/platform/httpresponse"
	"github.com/pablojhp.pergo/internal/platform/postgres/tenant"
	"github.com/pablojhp.pergo/internal/platform/storage"
	"github.com/pablojhp.pergo/internal/repository"
)

// TelegramAdapter implements channel.Dispatcher for Telegram Bot API.
type TelegramAdapter struct {
	connectionsRepo *repository.ConnectionRepository
	client          *http.Client
	baseURL         string
	s3Client        *storage.S3Client
}

// TelegramConfig represents the Telegram credentials JSON structure.
type TelegramConfig struct {
	Token string `json:"token"`
}

type inlineKeyboardButton struct {
	Text         string `json:"text"`
	CallbackData string `json:"callback_data"`
}

type inlineKeyboardMarkup struct {
	InlineKeyboard [][]inlineKeyboardButton `json:"inline_keyboard"`
}

type telegramMessageRequest struct {
	ChatID          string                `json:"chat_id"`
	MessageThreadID string                `json:"message_thread_id,omitempty"`
	Text            string                `json:"text"`
	ReplyMarkup     *inlineKeyboardMarkup `json:"reply_markup,omitempty"`
}

// TelegramErrorResponse represents the Telegram Bot API error body.
type TelegramErrorResponse struct {
	OK          bool   `json:"ok"`
	ErrorCode   int    `json:"error_code"`
	Description string `json:"description"`
}

// NewTelegramAdapter creates a new TelegramAdapter.
func NewTelegramAdapter(connectionsRepo *repository.ConnectionRepository, client *http.Client, s3Client *storage.S3Client) *TelegramAdapter {
	if client == nil {
		client = http.DefaultClient
	}
	return &TelegramAdapter{
		connectionsRepo: connectionsRepo,
		client:          client,
		baseURL:         "https://api.telegram.org",
		s3Client:        s3Client,
	}
}

// SetBaseURL overrides the base API URL (useful for testing).
func (a *TelegramAdapter) SetBaseURL(url string) {
	a.baseURL = url
}

// Dispatch sends a message through the Telegram Bot API.
func (a *TelegramAdapter) Dispatch(ctx context.Context, m *channel.MessagePayload) (string, error) {
	workspaceID, err := tenant.RequireWorkspaceID(ctx)
	if err != nil {
		return "", channel.NewTerminalError(err)
	}

	credsBytes, err := a.connectionsRepo.GetCredentialsForWorkspace(ctx, workspaceID, m.ConnectionID)
	if err != nil {
		if errors.Is(err, repository.ErrConnectionNotFound) {
			return "", channel.NewTerminalError(fmt.Errorf("connection credentials not found: %w", err))
		}
		return "", err
	}

	var config TelegramConfig
	if err := json.Unmarshal(credsBytes, &config); err != nil {
		return "", channel.NewTerminalError(fmt.Errorf("invalid credentials format: %w", err))
	}

	if config.Token == "" {
		return "", channel.NewTerminalError(errors.New("missing bot token in credentials"))
	}

	if m.Media != nil {
		if a.s3Client == nil {
			return "", channel.NewTerminalError(fmt.Errorf("telegram: media storage client not configured"))
		}

		parts := strings.Split(m.Media.MediaURL, "/")
		if len(parts) < 3 {
			return "", channel.NewTerminalError(fmt.Errorf("telegram: invalid media URL format: %s", m.Media.MediaURL))
		}
		workspaceIDStr := parts[len(parts)-2]
		hashWithExt := parts[len(parts)-1]
		key := workspaceIDStr + "/" + hashWithExt

		bodyRC, _, err := a.s3Client.Download(ctx, key)
		if err != nil {
			return "", fmt.Errorf("telegram media download from S3 failed: %w", err)
		}
		defer func() { _ = bodyRC.Close() }()

		var bodyBuf bytes.Buffer
		writer := multipart.NewWriter(&bodyBuf)

		// Set chat_id
		if err := writer.WriteField("chat_id", m.To); err != nil {
			return "", err
		}

		// Set message_thread_id
		if m.Metadata != nil && m.Metadata["thread_id"] != "" {
			if err := writer.WriteField("message_thread_id", m.Metadata["thread_id"]); err != nil {
				return "", err
			}
		}

		// Set reply_markup
		if m.Interactive != nil && m.Interactive.Type == "button" {
			var keyboard [][]inlineKeyboardButton
			for _, b := range m.Interactive.Action.Buttons {
				keyboard = append(keyboard, []inlineKeyboardButton{
					{
						Text:         b.Reply.Title,
						CallbackData: b.Reply.ID,
					},
				})
			}
			if len(keyboard) > 0 {
				rm := inlineKeyboardMarkup{InlineKeyboard: keyboard}
				rmBytes, _ := json.Marshal(rm)
				if err := writer.WriteField("reply_markup", string(rmBytes)); err != nil {
					return "", err
				}
			}
		}

		// Set caption
		if m.Media.Caption != "" {
			if err := writer.WriteField("caption", m.Media.Caption); err != nil {
				return "", err
			}
		}

		var fieldName string
		var endpoint string
		switch m.Media.MediaType {
		case "image":
			fieldName = "photo"
			endpoint = "sendPhoto"
		case "document":
			fieldName = "document"
			endpoint = "sendDocument"
		case "audio":
			fieldName = "audio"
			endpoint = "sendAudio"
		case "video":
			fieldName = "video"
			endpoint = "sendVideo"
		default:
			return "", channel.NewTerminalError(fmt.Errorf("telegram: unsupported media type %s", m.Media.MediaType))
		}

		filename := m.Media.Filename
		if filename == "" {
			filename = "file"
		}
		part, err := writer.CreateFormFile(fieldName, filename)
		if err != nil {
			return "", err
		}

		if _, err := io.Copy(part, bodyRC); err != nil {
			return "", err
		}

		if err := writer.Close(); err != nil {
			return "", err
		}

		url := fmt.Sprintf("%s/bot%s/%s", a.baseURL, config.Token, endpoint)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, &bodyBuf)
		if err != nil {
			return "", channel.NewTerminalError(errors.New("failed to create Telegram request"))
		}
		req.Header.Set("Content-Type", writer.FormDataContentType())

		return a.executeRequest(req)
	}

	var replyMarkup *inlineKeyboardMarkup
	if m.Interactive != nil && m.Interactive.Type == "button" {
		var keyboard [][]inlineKeyboardButton
		for _, b := range m.Interactive.Action.Buttons {
			keyboard = append(keyboard, []inlineKeyboardButton{
				{
					Text:         b.Reply.Title,
					CallbackData: b.Reply.ID,
				},
			})
		}
		if len(keyboard) > 0 {
			replyMarkup = &inlineKeyboardMarkup{
				InlineKeyboard: keyboard,
			}
		}
	}

	var messageThreadID string
	if m.Metadata != nil {
		messageThreadID = m.Metadata["thread_id"]
	}

	text := m.Body
	if m.Interactive != nil {
		if text != "" {
			text += "\n"
		}
		text += m.Interactive.Body.Text
	}

	reqPayload := telegramMessageRequest{
		ChatID:          m.To,
		MessageThreadID: messageThreadID,
		Text:            text,
		ReplyMarkup:     replyMarkup,
	}

	bodyBytes, err := json.Marshal(reqPayload)
	if err != nil {
		return "", channel.NewTerminalError(fmt.Errorf("marshal request: %w", err))
	}

	url := fmt.Sprintf("%s/bot%s/sendMessage", a.baseURL, config.Token)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", channel.NewTerminalError(errors.New("failed to create Telegram request"))
	}

	req.Header.Set("Content-Type", "application/json")
	return a.executeRequest(req)
}

func (a *TelegramAdapter) executeRequest(req *http.Request) (string, error) {
	resp, err := a.client.Do(req)
	if err != nil {
		// net/http transport errors may embed req.URL, whose path contains the
		// bot token. Preserve uncertainty semantics without exposing the URL.
		return "", channel.NewUncertainError(errors.New("telegram transport response lost"))
	}
	defer func() { _ = resp.Body.Close() }()

	respBytes, readErr := httpresponse.Read(resp)
	if readErr != nil {
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return "", channel.NewUncertainError(errors.New("telegram response body is invalid"))
		}
		return "", classifyTelegramHTTPStatus(resp.StatusCode, 0)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var successResp struct {
			OK     bool `json:"ok"`
			Result struct {
				MessageID json.Number `json:"message_id"`
			} `json:"result"`
		}
		if err := json.Unmarshal(respBytes, &successResp); err != nil ||
			!successResp.OK ||
			successResp.Result.MessageID.String() == "" ||
			successResp.Result.MessageID.String() == "0" {
			return "", channel.NewUncertainError(errors.New("telegram accepted request without a message ID"))
		}
		return successResp.Result.MessageID.String(), nil
	}

	var errorResp TelegramErrorResponse
	if err := json.Unmarshal(respBytes, &errorResp); err != nil {
		return "", classifyTelegramHTTPStatus(resp.StatusCode, 0)
	}

	return "", a.classifyError(resp.StatusCode, &errorResp)
}

func (a *TelegramAdapter) classifyError(statusCode int, errResp *TelegramErrorResponse) error {
	return classifyTelegramHTTPStatus(statusCode, errResp.ErrorCode)
}

func classifyTelegramHTTPStatus(statusCode int, apiCode int) error {
	err := fmt.Errorf("telegram API error (http_status=%d, code=%d)", statusCode, apiCode)

	// Explicit check based on known Telegram error codes
	// 400: Bad Request (chat not found, etc.)
	// 401: Unauthorized (invalid token)
	// 403: Forbidden (bot blocked by user, etc.)
	if apiCode == 400 || apiCode == 401 || apiCode == 403 {
		return channel.NewTerminalError(err)
	}

	// 429: Too Many Requests (Rate limit hit)
	if apiCode == 429 {
		return err
	}

	// General HTTP Status classification
	if statusCode == http.StatusTooManyRequests || statusCode >= 500 {
		return err
	}
	if statusCode >= 400 && statusCode < 500 {
		return channel.NewTerminalError(err)
	}

	return err
}
