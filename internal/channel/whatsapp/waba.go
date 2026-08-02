package whatsapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/pablojhp.pergo/internal/channel"
	"github.com/pablojhp.pergo/internal/platform/httpresponse"
	"github.com/pablojhp.pergo/internal/platform/metaapi"
	"github.com/pablojhp.pergo/internal/platform/postgres/tenant"
	"github.com/pablojhp.pergo/internal/repository"
)

// WindowChecker defines the interface for checking if a customer service window is open.
type WindowChecker interface {
	IsWindowOpen(ctx context.Context, workspaceID uuid.UUID, recipientPhone string, channel string, recipientIdentity string) (bool, error)
}

// WABAAdapter implements channel.Dispatcher for WhatsApp Cloud (WABA) REST API.
type WABAAdapter struct {
	connectionsRepo *repository.ConnectionRepository
	client          *http.Client
	baseURL         string
	windowChecker   WindowChecker
	externalBaseURL string
}

// WABAConfig represents the WABA credentials JSON payload.
type WABAConfig struct {
	PhoneNumberID string `json:"phone_number_id"`
	Token         string `json:"token"`
	WABAAccountID string `json:"waba_account_id"`
	VerifyToken   string `json:"verify_token"`
	AppSecret     string `json:"app_secret"`
}

// ValidateWebhookSecrets applies the minimum policy used by both interactive
// onboarding and the production rotation job. It never includes a submitted
// secret in returned errors.
func ValidateWebhookSecrets(appSecret, verifyToken string) error {
	if err := ValidateAppSecret(appSecret); err != nil {
		return err
	}
	return ValidateVerifyToken(verifyToken)
}

// ValidateAppSecret rejects missing and obviously synthetic Meta application
// secrets without returning the submitted value.
func ValidateAppSecret(appSecret string) error {
	if !acceptableWABASecret(appSecret, "") {
		return errors.New("app_secret must be a non-trivial value of at least 32 characters")
	}
	return nil
}

// ValidateVerifyToken rejects predictable or trivial callback tokens.
func ValidateVerifyToken(verifyToken string) error {
	if !acceptableWABASecret(verifyToken, "pergo_verify_token_") {
		return errors.New("verify_token must be a randomly generated value of at least 32 characters")
	}
	return nil
}

func acceptableWABASecret(value, forbiddenPrefix string) bool {
	value = strings.TrimSpace(value)
	if len(value) < 32 || (forbiddenPrefix != "" && strings.HasPrefix(value, forbiddenPrefix)) {
		return false
	}
	allSame := true
	for i := 1; i < len(value); i++ {
		if value[i] != value[0] {
			allSame = false
			break
		}
	}
	return !allSame
}

type wabaMessageRequest struct {
	MessagingProduct string           `json:"messaging_product"`
	RecipientType    string           `json:"recipient_type"`
	To               string           `json:"to"`
	Type             string           `json:"type"`
	Text             *wabaText        `json:"text,omitempty"`
	Template         *wabaTemplate    `json:"template,omitempty"`
	Interactive      *wabaInteractive `json:"interactive,omitempty"`
}

type wabaText struct {
	PreviewURL bool   `json:"preview_url"`
	Body       string `json:"body"`
}

type wabaTemplate struct {
	Name       string          `json:"name"`
	Language   wabaLanguage    `json:"language"`
	Components []wabaComponent `json:"components,omitempty"`
}

type wabaLanguage struct {
	Code string `json:"code"`
}

type wabaComponent struct {
	Type       string          `json:"type"`
	Parameters []wabaParameter `json:"parameters"`
}

type wabaParameter struct {
	Type     string              `json:"type"`
	Text     string              `json:"text,omitempty"`
	Image    *wabaParameterMedia `json:"image,omitempty"`
	Video    *wabaParameterMedia `json:"video,omitempty"`
	Document *wabaParameterMedia `json:"document,omitempty"`
}

type wabaParameterMedia struct {
	Link string `json:"link"`
}

type wabaInteractive struct {
	Type   string                `json:"type"`
	Header *wabaInteractiveText  `json:"header,omitempty"`
	Body   wabaInteractiveText   `json:"body"`
	Footer *wabaInteractiveText  `json:"footer,omitempty"`
	Action wabaInteractiveAction `json:"action"`
}

type wabaInteractiveText struct {
	Type string `json:"type,omitempty"`
	Text string `json:"text"`
}

type wabaInteractiveAction struct {
	Button   string                       `json:"button,omitempty"`
	Buttons  []wabaInteractiveReplyButton `json:"buttons,omitempty"`
	Sections []wabaInteractiveSection     `json:"sections,omitempty"`
}

type wabaInteractiveReplyButton struct {
	Type  string                     `json:"type"`
	Reply wabaInteractiveButtonReply `json:"reply"`
}

type wabaInteractiveButtonReply struct {
	ID    string `json:"id"`
	Title string `json:"title"`
}

type wabaInteractiveSection struct {
	Title string                      `json:"title,omitempty"`
	Rows  []wabaInteractiveSectionRow `json:"rows"`
}

type wabaInteractiveSectionRow struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Description string `json:"description,omitempty"`
}

// MetaErrorResponse is the structure returned by Meta on API errors.
type MetaErrorResponse struct {
	Error struct {
		Message      string `json:"message"`
		Type         string `json:"type"`
		Code         int    `json:"code"`
		ErrorSubcode int    `json:"error_subcode"`
		FBTraceID    string `json:"fbtrace_id"`
	} `json:"error"`
}

// NewWABAAdapter creates a new WABAAdapter.
func NewWABAAdapter(connectionsRepo *repository.ConnectionRepository, client *http.Client, windowChecker WindowChecker, externalBaseURL string) *WABAAdapter {
	if client == nil {
		client = http.DefaultClient
	}
	return &WABAAdapter{
		connectionsRepo: connectionsRepo,
		client:          client,
		baseURL:         metaapi.BaseURL(metaapi.DefaultVersion),
		windowChecker:   windowChecker,
		externalBaseURL: externalBaseURL,
	}
}

// SetBaseURL overrides the base API URL (useful for testing).
func (a *WABAAdapter) SetBaseURL(url string) {
	a.baseURL = url
}

// Dispatch sends a message through the WhatsApp Cloud REST API.
func (a *WABAAdapter) Dispatch(ctx context.Context, m *channel.MessagePayload) (string, error) {
	workspaceID, err := tenant.RequireWorkspaceID(ctx)
	if err != nil {
		return "", channel.NewTerminalError(err)
	}

	conn, err := a.connectionsRepo.GetByIDForWorkspace(ctx, workspaceID, m.ConnectionID)
	if err != nil {
		if errors.Is(err, repository.ErrConnectionNotFound) {
			return "", channel.NewTerminalError(fmt.Errorf("connection credentials not found: %w", err))
		}
		return "", err
	}

	var config WABAConfig
	if err := json.Unmarshal(conn.Credentials, &config); err != nil {
		return "", channel.NewTerminalError(fmt.Errorf("invalid credentials format: %w", err))
	}

	if config.PhoneNumberID == "" || config.Token == "" {
		return "", channel.NewTerminalError(errors.New("missing phone_number_id or token in credentials"))
	}

	if override, ok := m.ChannelOverrides["whatsapp_cloud"]; ok {
		return a.sendRequest(ctx, config.PhoneNumberID, config.Token, override)
	}

	reqPayload := wabaMessageRequest{
		MessagingProduct: "whatsapp",
		RecipientType:    "individual",
		To:               m.To,
	}

	templateName := m.TemplateName
	if templateName == "" && m.Metadata != nil {
		templateName = m.Metadata["template_name"]
	}

	if templateName == "" {
		if a.windowChecker != nil {
			open, err := a.windowChecker.IsWindowOpen(ctx, workspaceID, m.To, "whatsapp_cloud", conn.SenderIdentity)
			if err != nil {
				return "", err
			}
			if !open {
				return "", channel.NewTerminalError(errors.New("customer service window expired"))
			}
		}
	}

	if templateName != "" {
		langCode := m.Language
		if langCode == "" && m.Metadata != nil {
			langCode = m.Metadata["template_language"]
		}
		if langCode == "" {
			langCode = "en_US"
		}
		tmpl := wabaTemplate{
			Name: templateName,
			Language: wabaLanguage{
				Code: langCode,
			},
		}

		if len(m.Components) > 0 {
			tmpl.Components = make([]wabaComponent, len(m.Components))
			for i, comp := range m.Components {
				tmpl.Components[i] = wabaComponent{
					Type: comp.Type,
				}
				if len(comp.Parameters) > 0 {
					tmpl.Components[i].Parameters = make([]wabaParameter, len(comp.Parameters))
					for j, param := range comp.Parameters {
						var img, video, doc *wabaParameterMedia
						textVal := param.Text
						switch param.Type {
						case "image":
							img = &wabaParameterMedia{Link: param.Text}
							textVal = ""
						case "video":
							video = &wabaParameterMedia{Link: param.Text}
							textVal = ""
						case "document":
							doc = &wabaParameterMedia{Link: param.Text}
							textVal = ""
						}

						tmpl.Components[i].Parameters[j] = wabaParameter{
							Type:     param.Type,
							Text:     textVal,
							Image:    img,
							Video:    video,
							Document: doc,
						}
					}
				}
			}
		} else if m.Metadata != nil {
			var params []wabaParameter
			for i := 1; ; i++ {
				paramKey := fmt.Sprintf("param%d", i)
				val, ok := m.Metadata[paramKey]
				if !ok {
					break
				}
				params = append(params, wabaParameter{
					Type: "text",
					Text: val,
				})
			}

			if len(params) > 0 {
				tmpl.Components = []wabaComponent{
					{
						Type:       "body",
						Parameters: params,
					},
				}
			}
		}

		reqPayload.Type = "template"
		reqPayload.Template = &tmpl
	} else if m.Interactive != nil {
		reqPayload.Type = "interactive"

		var header, footer *wabaInteractiveText
		if m.Interactive.Header != nil {
			header = &wabaInteractiveText{
				Type: "text",
				Text: m.Interactive.Header.Text,
			}
		}
		if m.Interactive.Footer != nil {
			footer = &wabaInteractiveText{
				Text: m.Interactive.Footer.Text,
			}
		}

		action := wabaInteractiveAction{
			Button: m.Interactive.Action.Button,
		}

		for _, b := range m.Interactive.Action.Buttons {
			action.Buttons = append(action.Buttons, wabaInteractiveReplyButton{
				Type: "reply",
				Reply: wabaInteractiveButtonReply{
					ID:    b.Reply.ID,
					Title: b.Reply.Title,
				},
			})
		}

		for _, s := range m.Interactive.Action.Sections {
			section := wabaInteractiveSection{Title: s.Title}
			for _, r := range s.Rows {
				section.Rows = append(section.Rows, wabaInteractiveSectionRow{
					ID:          r.ID,
					Title:       r.Title,
					Description: r.Description,
				})
			}
			action.Sections = append(action.Sections, section)
		}

		reqPayload.Interactive = &wabaInteractive{
			Type:   m.Interactive.Type,
			Header: header,
			Body:   wabaInteractiveText{Text: m.Interactive.Body.Text},
			Footer: footer,
			Action: action,
		}
	} else {
		reqPayload.Type = "text"
		reqPayload.Text = &wabaText{
			PreviewURL: false,
			Body:       m.Body,
		}
	}

	if m.Media != nil {
		mediaURL := m.Media.MediaURL
		if len(mediaURL) > 0 && mediaURL[0] == '/' && a.externalBaseURL != "" {
			base := a.externalBaseURL
			if len(base) > 0 && base[len(base)-1] == '/' {
				base = base[:len(base)-1]
			}
			mediaURL = base + mediaURL
		}

		reqPayload.Type = m.Media.MediaType
		switch m.Media.MediaType {
		case "image":
			type wabaImage struct {
				Link    string  `json:"link"`
				Caption *string `json:"caption,omitempty"`
			}
			type wabaImageRequest struct {
				MessagingProduct string     `json:"messaging_product"`
				RecipientType    string     `json:"recipient_type"`
				To               string     `json:"to"`
				Type             string     `json:"type"`
				Image            *wabaImage `json:"image,omitempty"`
			}
			var caption *string
			if m.Media.Caption != "" {
				caption = &m.Media.Caption
			}
			imgReq := wabaImageRequest{
				MessagingProduct: "whatsapp",
				RecipientType:    "individual",
				To:               m.To,
				Type:             "image",
				Image: &wabaImage{
					Link:    mediaURL,
					Caption: caption,
				},
			}
			bodyBytes, err := json.Marshal(imgReq)
			if err != nil {
				return "", channel.NewTerminalError(fmt.Errorf("marshal request: %w", err))
			}
			return a.sendRequest(ctx, config.PhoneNumberID, config.Token, bodyBytes)

		case "document":
			type wabaDocument struct {
				Link     string  `json:"link"`
				Caption  *string `json:"caption,omitempty"`
				Filename *string `json:"filename,omitempty"`
			}
			type wabaDocumentRequest struct {
				MessagingProduct string        `json:"messaging_product"`
				RecipientType    string        `json:"recipient_type"`
				To               string        `json:"to"`
				Type             string        `json:"type"`
				Document         *wabaDocument `json:"document,omitempty"`
			}
			var caption *string
			if m.Media.Caption != "" {
				caption = &m.Media.Caption
			}
			var filename *string
			if m.Media.Filename != "" {
				filename = &m.Media.Filename
			}
			docReq := wabaDocumentRequest{
				MessagingProduct: "whatsapp",
				RecipientType:    "individual",
				To:               m.To,
				Type:             "document",
				Document: &wabaDocument{
					Link:     mediaURL,
					Caption:  caption,
					Filename: filename,
				},
			}
			bodyBytes, err := json.Marshal(docReq)
			if err != nil {
				return "", channel.NewTerminalError(fmt.Errorf("marshal request: %w", err))
			}
			return a.sendRequest(ctx, config.PhoneNumberID, config.Token, bodyBytes)

		case "audio":
			type wabaAudio struct {
				Link string `json:"link"`
			}
			type wabaAudioRequest struct {
				MessagingProduct string     `json:"messaging_product"`
				RecipientType    string     `json:"recipient_type"`
				To               string     `json:"to"`
				Type             string     `json:"type"`
				Audio            *wabaAudio `json:"audio,omitempty"`
			}
			audReq := wabaAudioRequest{
				MessagingProduct: "whatsapp",
				RecipientType:    "individual",
				To:               m.To,
				Type:             "audio",
				Audio: &wabaAudio{
					Link: mediaURL,
				},
			}
			bodyBytes, err := json.Marshal(audReq)
			if err != nil {
				return "", channel.NewTerminalError(fmt.Errorf("marshal request: %w", err))
			}
			return a.sendRequest(ctx, config.PhoneNumberID, config.Token, bodyBytes)

		case "video":
			type wabaVideo struct {
				Link    string  `json:"link"`
				Caption *string `json:"caption,omitempty"`
			}
			type wabaVideoRequest struct {
				MessagingProduct string     `json:"messaging_product"`
				RecipientType    string     `json:"recipient_type"`
				To               string     `json:"to"`
				Type             string     `json:"type"`
				Video            *wabaVideo `json:"video,omitempty"`
			}
			var caption *string
			if m.Media.Caption != "" {
				caption = &m.Media.Caption
			}
			vidReq := wabaVideoRequest{
				MessagingProduct: "whatsapp",
				RecipientType:    "individual",
				To:               m.To,
				Type:             "video",
				Video: &wabaVideo{
					Link:    mediaURL,
					Caption: caption,
				},
			}
			bodyBytes, err := json.Marshal(vidReq)
			if err != nil {
				return "", channel.NewTerminalError(fmt.Errorf("marshal request: %w", err))
			}
			return a.sendRequest(ctx, config.PhoneNumberID, config.Token, bodyBytes)

		default:
			return "", channel.NewTerminalError(fmt.Errorf("unsupported media type: %s", m.Media.MediaType))
		}
	}

	bodyBytes, err := json.Marshal(reqPayload)
	if err != nil {
		return "", channel.NewTerminalError(fmt.Errorf("marshal request: %w", err))
	}

	return a.sendRequest(ctx, config.PhoneNumberID, config.Token, bodyBytes)
}

func (a *WABAAdapter) sendRequest(ctx context.Context, phoneNumberID, token string, bodyBytes []byte) (string, error) {
	url := fmt.Sprintf("%s/%s/messages", a.baseURL, phoneNumberID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", channel.NewTerminalError(errors.New("failed to create Meta request"))
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.client.Do(req)
	if err != nil {
		return "", channel.NewUncertainError(errors.New("meta transport response lost"))
	}
	defer func() { _ = resp.Body.Close() }()

	respBytes, readErr := httpresponse.Read(resp)
	if readErr != nil {
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return "", channel.NewUncertainError(errors.New("meta response body is invalid"))
		}
		return "", classifyWABAHTTPStatus(resp.StatusCode, 0, 0)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var successResp struct {
			Messages []struct {
				ID string `json:"id"`
			} `json:"messages"`
		}
		if err := json.Unmarshal(respBytes, &successResp); err == nil && len(successResp.Messages) > 0 && successResp.Messages[0].ID != "" {
			return successResp.Messages[0].ID, nil
		}
		return "", channel.NewUncertainError(errors.New("meta accepted request without a message ID"))
	}

	var errorResp MetaErrorResponse
	if err := json.Unmarshal(respBytes, &errorResp); err != nil {
		return "", classifyWABAHTTPStatus(resp.StatusCode, 0, 0)
	}

	return "", a.classifyError(resp.StatusCode, &errorResp)
}

func (a *WABAAdapter) classifyError(statusCode int, errResp *MetaErrorResponse) error {
	metaErr := errResp.Error
	return classifyWABAHTTPStatus(statusCode, metaErr.Code, metaErr.ErrorSubcode)
}

func classifyWABAHTTPStatus(statusCode int, code int, subcode int) error {
	err := fmt.Errorf(
		"meta API error (http_status=%d, code=%d, subcode=%d)",
		statusCode,
		code,
		subcode,
	)

	if code == 131030 ||
		code == 131047 ||
		code == 132000 ||
		code == 190 ||
		(code == 100 && subcode == 33) {
		return channel.NewTerminalError(err)
	}

	if code == 130429 || code == 131052 {
		return err
	}

	if statusCode == http.StatusTooManyRequests || statusCode >= 500 {
		return err
	}
	if statusCode >= 400 && statusCode < 500 {
		return channel.NewTerminalError(err)
	}

	return err
}
