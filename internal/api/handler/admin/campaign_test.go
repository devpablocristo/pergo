package admin_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/nats-io/nats.go"

	"github.com/pablojhp.pergo/internal/api/handler/admin"
	"github.com/pablojhp.pergo/internal/domain"
	"github.com/pablojhp.pergo/internal/platform/crypto"
	"github.com/pablojhp.pergo/internal/platform/messagebus"
	"github.com/pablojhp.pergo/internal/platform/queue"
	"github.com/pablojhp.pergo/internal/repository"
)

func connectNATS(t *testing.T) *nats.Conn {
	t.Helper()
	url := os.Getenv("PERGO_NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Skipf("NATS not available at %s: %v", url, err)
	}
	t.Cleanup(func() {
		nc.Close()
	})
	return nc
}

func TestCampaignCreateRejectsOversizedBatchBeforeRepositoryAccess(t *testing.T) {
	e := echo.New()
	form := url.Values{}
	form.Set("name", "oversized")
	form.Set("channel", "whatsapp_cloud")
	form.Set("batch_size", "1001")

	workspaceID := uuid.New()
	req := httptest.NewRequest(
		http.MethodPost,
		fmt.Sprintf("/admin/workspaces/%s/campaigns", workspaceID),
		strings.NewReader(form.Encode()),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetPath("/admin/workspaces/:workspace_id/campaigns")
	c.SetPathValues(echo.PathValues{{Name: "workspace_id", Value: workspaceID.String()}})

	// Nil repositories are deliberate: validation must reject an abusive batch
	// before any provider or persistence lookup occurs.
	h := &admin.CampaignHandler{}
	if err := h.Create(c); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "maximum") {
		t.Fatalf("response did not explain maximum batch size: %s", rec.Body.String())
	}
}

func TestCampaignCreateRejectsUnboundedDelayBeforeRepositoryAccess(t *testing.T) {
	e := echo.New()
	form := url.Values{}
	form.Set("name", "unbounded delay")
	form.Set("channel", "whatsapp_cloud")
	form.Set("batch_size", "10")
	form.Set("delay_seconds", fmt.Sprint(domain.CampaignMaxDelaySeconds+1))

	workspaceID := uuid.New()
	req := httptest.NewRequest(
		http.MethodPost,
		fmt.Sprintf("/admin/workspaces/%s/campaigns", workspaceID),
		strings.NewReader(form.Encode()),
	)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetPath("/admin/workspaces/:workspace_id/campaigns")
	c.SetPathValues(echo.PathValues{{Name: "workspace_id", Value: workspaceID.String()}})

	h := &admin.CampaignHandler{}
	if err := h.Create(c); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "delay_seconds") {
		t.Fatalf("response did not explain delay bound: %s", rec.Body.String())
	}
}

func TestCampaignCreateRejectsNonCanonicalAudienceBeforeRepositoryAccess(t *testing.T) {
	tests := []struct {
		name       string
		recipients string
	}{
		{name: "malformed JSON", recipients: `[{`},
		{name: "empty audience", recipients: `[]`},
		{name: "invalid phone", recipients: `[{"to":"invalid","variables":null}]`},
		{
			name: "duplicate phone",
			recipients: `[
				{"to":"5511999990001","variables":{}},
				{"to":"+55 (11) 99999-0001","variables":{}}
			]`,
		},
		{
			name:       "unknown field",
			recipients: `[{"to":"5511999990001","variables":{},"admin":true}]`,
		},
		{
			name: "oversized variable",
			recipients: fmt.Sprintf(
				`[{"to":"5511999990001","variables":{"name":%q}}]`,
				strings.Repeat("x", 4097),
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := echo.New()
			form := url.Values{}
			form.Set("name", "canonical validation")
			form.Set("channel", "whatsapp_cloud")
			form.Set("batch_size", "10")
			form.Set("recipients_data", tt.recipients)

			workspaceID := uuid.New()
			req := httptest.NewRequest(
				http.MethodPost,
				fmt.Sprintf("/admin/workspaces/%s/campaigns", workspaceID),
				strings.NewReader(form.Encode()),
			)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			c.SetPathValues(echo.PathValues{{Name: "workspace_id", Value: workspaceID.String()}})

			h := &admin.CampaignHandler{}
			if err := h.Create(c); err != nil {
				t.Fatalf("Create returned error: %v", err)
			}
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), "audience") {
				t.Fatalf("response did not explain audience validation: %s", rec.Body.String())
			}
		})
	}
}

func TestCampaignUploadCSVRejectsOversizeAndTooManyRows(t *testing.T) {
	t.Run("oversized file", func(t *testing.T) {
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)
		part, err := writer.CreateFormFile("csv_file", "oversized.csv")
		if err != nil {
			t.Fatalf("CreateFormFile: %v", err)
		}
		if _, err := part.Write(bytes.Repeat([]byte("x"), 6<<20)); err != nil {
			t.Fatalf("write oversized file: %v", err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("close multipart: %v", err)
		}

		e := echo.New()
		req := httptest.NewRequest(http.MethodPost, "/campaigns/upload", body)
		req.Header.Set("Content-Type", writer.FormDataContentType())
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		h := &admin.CampaignHandler{}
		if err := h.UploadCSV(c); err != nil {
			t.Fatalf("UploadCSV: %v", err)
		}
		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want 413; body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("too many rows", func(t *testing.T) {
		var csvBody strings.Builder
		csvBody.WriteString("phone,name\n")
		for i := 0; i <= 10000; i++ {
			_, _ = fmt.Fprintf(&csvBody, "5511%08d,Recipient %d\n", i, i)
		}

		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)
		part, err := writer.CreateFormFile("csv_file", "too-many.csv")
		if err != nil {
			t.Fatalf("CreateFormFile: %v", err)
		}
		if _, err := part.Write([]byte(csvBody.String())); err != nil {
			t.Fatalf("write CSV: %v", err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("close multipart: %v", err)
		}

		e := echo.New()
		req := httptest.NewRequest(http.MethodPost, "/campaigns/upload", body)
		req.Header.Set("Content-Type", writer.FormDataContentType())
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		h := &admin.CampaignHandler{}
		if err := h.UploadCSV(c); err != nil {
			t.Fatalf("UploadCSV: %v", err)
		}
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "maximum") {
			t.Fatalf("response did not explain row limit: %s", rec.Body.String())
		}
	})
}

func TestCampaignHandlerRejectsCrossWorkspaceReferences(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	wsRepo := repository.NewWorkspaceRepository(pool)
	campaignRepo := repository.NewCampaignRepository(pool)
	enc, err := crypto.NewEncryptor(make([]byte, 32))
	if err != nil {
		t.Fatalf("NewEncryptor: %v", err)
	}
	connectionRepo := repository.NewConnectionRepository(pool, enc)

	owner, err := wsRepo.Create(ctx, "campaign_owner_"+uuid.New().String())
	if err != nil {
		t.Fatalf("create owner workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(context.Background(), owner.ID) }()
	attacker, err := wsRepo.Create(ctx, "campaign_attacker_"+uuid.New().String())
	if err != nil {
		t.Fatalf("create attacker workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(context.Background(), attacker.ID) }()

	connectionID := uuid.New()
	if err := connectionRepo.Create(ctx, &repository.Connection{
		ID:             connectionID,
		WorkspaceID:    owner.ID,
		Name:           "Owner WABA",
		Channel:        "whatsapp_cloud",
		SenderIdentity: "5511999990002",
		Status:         "connected",
		IsDefault:      true,
	}); err != nil {
		t.Fatalf("create owner connection: %v", err)
	}

	channel := "whatsapp_cloud"
	campaign, err := campaignRepo.Create(ctx, &domain.Campaign{
		WorkspaceID:  owner.ID,
		ConnectionID: &connectionID,
		Name:         "Owner campaign",
		Status:       domain.CampaignStatusDraft,
		BatchSize:    10,
		Channel:      &channel,
		Recipients:   []domain.CampaignRecipient{{To: "5511999998888"}},
	})
	if err != nil {
		t.Fatalf("create owner campaign: %v", err)
	}

	h := admin.NewCampaignHandler(campaignRepo, nil, connectionRepo, nil)
	e := echo.New()

	t.Run("connection cannot be borrowed by another workspace", func(t *testing.T) {
		form := url.Values{}
		form.Set("name", "cross-tenant")
		form.Set("channel", connectionID.String())
		form.Set("batch_size", "10")
		form.Set("recipients_data", `[]`)
		req := httptest.NewRequest(
			http.MethodPost,
			fmt.Sprintf("/admin/workspaces/%s/campaigns", attacker.ID),
			strings.NewReader(form.Encode()),
		)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetPath("/admin/workspaces/:workspace_id/campaigns")
		c.SetPathValues(echo.PathValues{{Name: "workspace_id", Value: attacker.ID.String()}})

		if err := h.Create(c); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
		}
		created, err := campaignRepo.ListByWorkspace(ctx, attacker.ID)
		if err != nil {
			t.Fatalf("list attacker campaigns: %v", err)
		}
		if len(created) != 0 {
			t.Fatalf("cross-tenant create persisted %d campaigns", len(created))
		}
	})

	actions := []struct {
		name   string
		method string
		suffix string
		call   func(*echo.Context) error
	}{
		{name: "download", method: http.MethodGet, suffix: "/skipped/download", call: h.DownloadSkipped},
		{name: "start", method: http.MethodPost, suffix: "/start", call: h.Start},
		{name: "cancel", method: http.MethodPost, suffix: "/cancel", call: h.Cancel},
		{name: "delete", method: http.MethodDelete, call: h.Delete},
	}
	for _, action := range actions {
		t.Run(action.name+" hides foreign campaign", func(t *testing.T) {
			req := httptest.NewRequest(
				action.method,
				fmt.Sprintf("/admin/workspaces/%s/campaigns/%s%s", attacker.ID, campaign.ID, action.suffix),
				nil,
			)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			c.SetPathValues(echo.PathValues{
				{Name: "workspace_id", Value: attacker.ID.String()},
				{Name: "id", Value: campaign.ID.String()},
			})
			if err := action.call(c); err != nil {
				t.Fatalf("%s: %v", action.name, err)
			}
			if rec.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want 404; body=%s", rec.Code, rec.Body.String())
			}
		})
	}

	stored, err := campaignRepo.GetByID(ctx, campaign.ID)
	if err != nil {
		t.Fatalf("owner campaign was deleted: %v", err)
	}
	if stored.Status != domain.CampaignStatusDraft {
		t.Fatalf("owner campaign was mutated to %s", stored.Status)
	}
}

func TestCampaignDeleteRejectsActiveAndAllowsSafeStates(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()
	ctx := context.Background()

	wsRepo := repository.NewWorkspaceRepository(pool)
	campaignRepo := repository.NewCampaignRepository(pool)
	workspace, err := wsRepo.Create(ctx, "campaign_delete_states_"+uuid.NewString())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(context.Background(), workspace.ID) }()

	handler := &admin.CampaignHandler{CampaignRepo: campaignRepo}
	tests := []struct {
		status     domain.CampaignStatus
		wantStatus int
		wantDelete bool
	}{
		{status: domain.CampaignStatusDraft, wantStatus: http.StatusOK, wantDelete: true},
		{status: domain.CampaignStatusScheduled, wantStatus: http.StatusConflict},
		{status: domain.CampaignStatusSending, wantStatus: http.StatusConflict},
		{status: domain.CampaignStatusCompleted, wantStatus: http.StatusOK, wantDelete: true},
		{status: domain.CampaignStatusCancelled, wantStatus: http.StatusOK, wantDelete: true},
	}

	for _, tt := range tests {
		t.Run(string(tt.status), func(t *testing.T) {
			campaign, err := campaignRepo.Create(ctx, &domain.Campaign{
				WorkspaceID: workspace.ID,
				Name:        "Delete " + string(tt.status),
				Status:      tt.status,
				BatchSize:   1,
			})
			if err != nil {
				t.Fatalf("create campaign: %v", err)
			}

			e := echo.New()
			req := httptest.NewRequest(
				http.MethodDelete,
				fmt.Sprintf("/admin/workspaces/%s/campaigns/%s", workspace.ID, campaign.ID),
				nil,
			)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			c.SetPathValues(echo.PathValues{
				{Name: "workspace_id", Value: workspace.ID.String()},
				{Name: "id", Value: campaign.ID.String()},
			})
			if err := handler.Delete(c); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, tt.wantStatus, rec.Body.String())
			}

			stored, getErr := campaignRepo.GetByID(ctx, campaign.ID)
			if tt.wantDelete {
				if !errors.Is(getErr, repository.ErrCampaignNotFound) {
					t.Fatalf("deleted campaign GetByID = %#v, %v", stored, getErr)
				}
			} else {
				if getErr != nil || stored.Status != tt.status {
					t.Fatalf("active campaign changed = %#v, %v", stored, getErr)
				}
			}
		})
	}
}

func TestCampaignHandler(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	nc := connectNATS(t)
	pub := queue.NewJetStreamPublisher(nc)

	ctx := context.Background()
	wsRepo := repository.NewWorkspaceRepository(pool)
	campaignRepo := repository.NewCampaignRepository(pool)
	templateRepo := repository.NewWABATemplateRepository(pool)

	kek := make([]byte, 32)
	enc, err := crypto.NewEncryptor(kek)
	if err != nil {
		t.Fatalf("failed to create encryptor: %v", err)
	}
	connectionRepo := repository.NewConnectionRepository(pool, enc)

	ws, err := wsRepo.Create(ctx, "camp_handler_ws_"+uuid.New().String())
	if err != nil {
		t.Fatalf("failed to create workspace: %v", err)
	}
	defer func() {
		_ = wsRepo.Delete(ctx, ws.ID)
	}()

	// Create default connection
	connectionID := uuid.New()
	err = connectionRepo.Create(ctx, &repository.Connection{
		ID:             connectionID,
		WorkspaceID:    ws.ID,
		Name:           "WhatsApp Web",
		Channel:        "whatsapp",
		SenderIdentity: "5511999990002",
		Status:         "connected",
		IsDefault:      true,
	})
	if err != nil {
		t.Fatalf("failed to create default connection: %v", err)
	}

	// Ensure stream exists
	_, err = queue.EnsureCampaignStream(ctx, nc)
	if err != nil {
		t.Fatalf("EnsureCampaignStream failed: %v", err)
	}

	h := admin.NewCampaignHandler(campaignRepo, templateRepo, connectionRepo, pub)
	e := echo.New()

	t.Run("NewForm", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/admin/workspaces/%s/campaigns/new", ws.ID), nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetPath("/admin/workspaces/:workspace_id/campaigns/new")
		c.SetPathValues(echo.PathValues{
			{Name: "workspace_id", Value: ws.ID.String()},
		})

		if err := h.NewForm(c); err != nil {
			t.Fatalf("NewForm failed: %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "Nova Campanha") {
			t.Errorf("expected form title in response, got: %s", rec.Body.String())
		}
	})

	t.Run("Start Rejects Oversized Expanded Message Before Durable Snapshot", func(t *testing.T) {
		channel := "whatsapp"
		template := strings.Repeat("x", messagebus.MaxPayloadBytes)
		campaign, createErr := campaignRepo.Create(ctx, &domain.Campaign{
			WorkspaceID:  ws.ID,
			ConnectionID: &connectionID,
			Name:         "Oversized outbound",
			Status:       domain.CampaignStatusDraft,
			BatchSize:    1,
			TemplateName: &template,
			Channel:      &channel,
			Recipients: []domain.CampaignRecipient{{
				To:        "5511977776666",
				Variables: map[string]string{"name": "Alice"},
			}},
		})
		if createErr != nil {
			t.Fatalf("create oversized campaign: %v", createErr)
		}

		request := httptest.NewRequest(
			http.MethodPost,
			fmt.Sprintf(
				"/admin/workspaces/%s/campaigns/%s/start",
				ws.ID,
				campaign.ID,
			),
			nil,
		)
		response := httptest.NewRecorder()
		echoContext := e.NewContext(request, response)
		echoContext.SetPath("/admin/workspaces/:workspace_id/campaigns/:id/start")
		echoContext.SetPathValues(echo.PathValues{
			{Name: "workspace_id", Value: ws.ID.String()},
			{Name: "id", Value: campaign.ID.String()},
		})
		if err := h.Start(echoContext); err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
		if response.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf(
				"Start status = %d, want %d; body=%s",
				response.Code,
				http.StatusRequestEntityTooLarge,
				response.Body.String(),
			)
		}

		stored, getErr := campaignRepo.GetByID(ctx, campaign.ID)
		if getErr != nil {
			t.Fatalf("reload campaign: %v", getErr)
		}
		if stored.Status != domain.CampaignStatusDraft {
			t.Fatalf("campaign status = %s, want draft", stored.Status)
		}
		var durableBatches int
		if err := pool.QueryRow(
			ctx,
			`SELECT count(*) FROM campaign_batches WHERE campaign_id = $1`,
			campaign.ID,
		).Scan(&durableBatches); err != nil {
			t.Fatalf("count durable campaign batches: %v", err)
		}
		if durableBatches != 0 {
			t.Fatalf("durable campaign batches = %d, want 0", durableBatches)
		}
		if err := campaignRepo.Delete(ctx, campaign.ID, ws.ID); err != nil {
			t.Fatalf("delete rejected draft campaign: %v", err)
		}
	})

	t.Run("Upload CSV Preview", func(t *testing.T) {
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)
		part, _ := writer.CreateFormFile("csv_file", "contacts.csv")

		csvContent := "phone,name\n5511999998888,John\n5511999998888,John\ninvalid-phone,Bad\n5511988887777,Alice\n"
		_, _ = part.Write([]byte(csvContent))
		_ = writer.Close()

		req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/admin/workspaces/%s/campaigns/upload", ws.ID), body)
		req.Header.Set("Content-Type", writer.FormDataContentType())
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetPath("/admin/workspaces/:workspace_id/campaigns/upload")
		c.SetPathValues(echo.PathValues{
			{Name: "workspace_id", Value: ws.ID.String()},
		})

		if err := h.UploadCSV(c); err != nil {
			t.Fatalf("UploadCSV failed: %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", rec.Code)
		}
		bodyStr := rec.Body.String()
		if !strings.Contains(bodyStr, "Resultado da Validação") {
			t.Errorf("expected preview segment in response, got: %s", bodyStr)
		}
		if !strings.Contains(bodyStr, "5511999998888") {
			t.Errorf("expected sanitized E.164 phone in preview, got: %s", bodyStr)
		}
	})

	t.Run("Create Campaign", func(t *testing.T) {
		recipients := []domain.CampaignRecipient{
			{To: "5511999998888", Variables: map[string]string{"name": "John"}},
			{To: "5511988887777", Variables: map[string]string{"name": "Alice"}},
		}
		recipientsJSON, _ := json.Marshal(recipients)

		skipped := []domain.SkippedRow{
			{LineNumber: 3, RawInput: "invalid-phone,Bad", Reason: "numero de telefone invalido (tamanho 13)"},
		}
		skippedJSON, _ := json.Marshal(skipped)

		form := url.Values{}
		form.Set("name", "Campanha Vendas Julho")
		form.Set("channel", "whatsapp")
		form.Set("batch_size", "50")
		form.Set("delay_seconds", "3")
		form.Set("body_template", "Ola {{name}}!")
		form.Set("recipients_data", string(recipientsJSON))
		form.Set("skipped_data", string(skippedJSON))

		req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/admin/workspaces/%s/campaigns", ws.ID), strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetPath("/admin/workspaces/:workspace_id/campaigns")
		c.SetPathValues(echo.PathValues{
			{Name: "workspace_id", Value: ws.ID.String()},
		})

		if err := h.Create(c); err != nil {
			t.Fatalf("Create failed: %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", rec.Code)
		}

		camps, err := campaignRepo.ListByWorkspace(ctx, ws.ID)
		if err != nil {
			t.Fatalf("ListByWorkspace failed: %v", err)
		}
		if len(camps) != 1 {
			t.Fatalf("expected 1 campaign in DB, got %d", len(camps))
		}
		if camps[0].Name != "Campanha Vendas Julho" {
			t.Errorf("expected campaign name 'Campanha Vendas Julho', got '%s'", camps[0].Name)
		}
		if len(camps[0].Recipients) != 2 {
			t.Errorf("expected 2 recipients in DB, got %d", len(camps[0].Recipients))
		}
		if len(camps[0].SkippedRows) != 1 {
			t.Errorf("expected 1 skipped row in DB, got %d", len(camps[0].SkippedRows))
		}

		campaignID := camps[0].ID

		// Test List
		reqList := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/admin/workspaces/%s/campaigns", ws.ID), nil)
		recList := httptest.NewRecorder()
		cList := e.NewContext(reqList, recList)
		cList.SetPath("/admin/workspaces/:workspace_id/campaigns")
		cList.SetPathValues(echo.PathValues{
			{Name: "workspace_id", Value: ws.ID.String()},
		})
		if err := h.List(cList); err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if recList.Code != http.StatusOK {
			t.Errorf("List status expected 200, got %d", recList.Code)
		}
		if !strings.Contains(recList.Body.String(), "Campanha Vendas Julho") {
			t.Errorf("List body expected campaign name, got: %s", recList.Body.String())
		}

		// Test Download Skipped
		reqDownload := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/admin/workspaces/%s/campaigns/%s/skipped/download", ws.ID, campaignID), nil)
		recDownload := httptest.NewRecorder()
		cDownload := e.NewContext(reqDownload, recDownload)
		cDownload.SetPath("/admin/workspaces/:workspace_id/campaigns/:id/skipped/download")
		cDownload.SetPathValues(echo.PathValues{
			{Name: "workspace_id", Value: ws.ID.String()},
			{Name: "id", Value: campaignID.String()},
		})
		if err := h.DownloadSkipped(cDownload); err != nil {
			t.Fatalf("DownloadSkipped failed: %v", err)
		}
		if recDownload.Code != http.StatusOK {
			t.Errorf("DownloadSkipped status expected 200, got %d", recDownload.Code)
		}
		if !strings.Contains(recDownload.Body.String(), "invalid-phone,Bad") {
			t.Errorf("DownloadSkipped CSV body expected skipped row raw input, got: %s", recDownload.Body.String())
		}

		// Test Start. The request persists an outbox snapshot and does not
		// depend on an available broker publisher.
		h.Publisher = nil
		reqStart := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/admin/workspaces/%s/campaigns/%s/start", ws.ID, campaignID), nil)
		recStart := httptest.NewRecorder()
		cStart := e.NewContext(reqStart, recStart)
		cStart.SetPath("/admin/workspaces/:workspace_id/campaigns/:id/start")
		cStart.SetPathValues(echo.PathValues{
			{Name: "workspace_id", Value: ws.ID.String()},
			{Name: "id", Value: campaignID.String()},
		})
		if err := h.Start(cStart); err != nil {
			t.Fatalf("Start failed: %v", err)
		}
		if recStart.Code != http.StatusOK {
			t.Errorf("Start status expected 200, got %d", recStart.Code)
		}
		if !strings.Contains(recStart.Body.String(), "Enviando") {
			t.Errorf("Start response expected status 'Enviando', got: %s", recStart.Body.String())
		}

		// Verify status updated in DB
		updatedCamp, _ := campaignRepo.GetByID(ctx, campaignID)
		if updatedCamp.Status != domain.CampaignStatusSending {
			t.Errorf("expected DB status to be 'sending', got '%s'", updatedCamp.Status)
		}
		var durableBatches int
		if err := pool.QueryRow(
			ctx,
			`SELECT count(*) FROM campaign_batches WHERE campaign_id = $1`,
			campaignID,
		).Scan(&durableBatches); err != nil {
			t.Fatalf("count durable campaign batches: %v", err)
		}
		if durableBatches != 1 {
			t.Fatalf("durable campaign batches = %d, want 1", durableBatches)
		}

		reqStartAgain := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/admin/workspaces/%s/campaigns/%s/start", ws.ID, campaignID), nil)
		recStartAgain := httptest.NewRecorder()
		cStartAgain := e.NewContext(reqStartAgain, recStartAgain)
		cStartAgain.SetPath("/admin/workspaces/:workspace_id/campaigns/:id/start")
		cStartAgain.SetPathValues(echo.PathValues{
			{Name: "workspace_id", Value: ws.ID.String()},
			{Name: "id", Value: campaignID.String()},
		})
		if err := h.Start(cStartAgain); err != nil {
			t.Fatalf("idempotent Start failed: %v", err)
		}
		if recStartAgain.Code != http.StatusOK {
			t.Fatalf("idempotent Start status = %d, want 200", recStartAgain.Code)
		}
		if err := pool.QueryRow(
			ctx,
			`SELECT count(*) FROM campaign_batches WHERE campaign_id = $1`,
			campaignID,
		).Scan(&durableBatches); err != nil {
			t.Fatalf("count idempotent durable batches: %v", err)
		}
		if durableBatches != 1 {
			t.Fatalf("idempotent Start persisted %d batches, want 1", durableBatches)
		}

		// Test Cancel
		reqCancel := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/admin/workspaces/%s/campaigns/%s/cancel", ws.ID, campaignID), nil)
		recCancel := httptest.NewRecorder()
		cCancel := e.NewContext(reqCancel, recCancel)
		cCancel.SetPath("/admin/workspaces/:workspace_id/campaigns/:id/cancel")
		cCancel.SetPathValues(echo.PathValues{
			{Name: "workspace_id", Value: ws.ID.String()},
			{Name: "id", Value: campaignID.String()},
		})
		if err := h.Cancel(cCancel); err != nil {
			t.Fatalf("Cancel failed: %v", err)
		}
		if recCancel.Code != http.StatusOK {
			t.Errorf("Cancel status expected 200, got %d", recCancel.Code)
		}
		if !strings.Contains(recCancel.Body.String(), "Cancelada") {
			t.Errorf("Cancel response expected status 'Cancelada', got: %s", recCancel.Body.String())
		}

		cancelledCamp, _ := campaignRepo.GetByID(ctx, campaignID)
		if cancelledCamp.Status != domain.CampaignStatusCancelled {
			t.Errorf("expected DB status to be 'cancelled', got '%s'", cancelledCamp.Status)
		}

		// Test Delete
		reqDelete := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/admin/workspaces/%s/campaigns/%s", ws.ID, campaignID), nil)
		recDelete := httptest.NewRecorder()
		cDelete := e.NewContext(reqDelete, recDelete)
		cDelete.SetPath("/admin/workspaces/:workspace_id/campaigns/:id")
		cDelete.SetPathValues(echo.PathValues{
			{Name: "workspace_id", Value: ws.ID.String()},
			{Name: "id", Value: campaignID.String()},
		})
		if err := h.Delete(cDelete); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
		if recDelete.Code != http.StatusOK {
			t.Errorf("Delete status expected 200, got %d", recDelete.Code)
		}

		deletedCamp, _ := campaignRepo.GetByID(ctx, campaignID)
		if deletedCamp != nil {
			t.Errorf("expected campaign to be deleted, but still exists")
		}
	})
}
