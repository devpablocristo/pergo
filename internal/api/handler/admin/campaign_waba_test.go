package admin_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/pablojhp.pergo/internal/api/handler/admin"
	"github.com/pablojhp.pergo/internal/domain"
	"github.com/pablojhp.pergo/internal/platform/queue"
	"github.com/pablojhp.pergo/internal/repository"
)

func TestCampaignWABAPreflightFreezesExactApprovedTemplate(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	workspaceRepo := repository.NewWorkspaceRepository(pool)
	connectionRepo := repository.NewConnectionRepository(pool, nil)
	campaignRepo := repository.NewCampaignRepository(pool)
	templateRepo := repository.NewWABATemplateRepository(pool)

	workspace, err := workspaceRepo.Create(ctx, "campaign_waba_"+uuid.NewString())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _ = workspaceRepo.Delete(context.Background(), workspace.ID) }()
	foreignWorkspace, err := workspaceRepo.Create(ctx, "campaign_waba_foreign_"+uuid.NewString())
	if err != nil {
		t.Fatalf("create foreign workspace: %v", err)
	}
	defer func() { _ = workspaceRepo.Delete(context.Background(), foreignWorkspace.ID) }()

	activeConnection := createCampaignTestConnection(
		t,
		ctx,
		connectionRepo,
		workspace.ID,
		"connected",
	)
	disconnectedConnection := createCampaignTestConnection(
		t,
		ctx,
		connectionRepo,
		workspace.ID,
		"disconnected",
	)
	otherActiveConnection := createCampaignTestConnection(
		t,
		ctx,
		connectionRepo,
		workspace.ID,
		"connected",
	)
	foreignConnection := createCampaignTestConnection(
		t,
		ctx,
		connectionRepo,
		foreignWorkspace.ID,
		"connected",
	)

	approved := createCampaignTestTemplate(
		t,
		ctx,
		templateRepo,
		workspace.ID,
		activeConnection.ID,
		"approved_campaign",
		"en_US",
		"APPROVED",
		`[{"type":"BODY","text":"Hello {{1}}"}]`,
	)
	pending := createCampaignTestTemplate(
		t,
		ctx,
		templateRepo,
		workspace.ID,
		activeConnection.ID,
		"pending_campaign",
		"en_US",
		"PENDING",
		`[{"type":"BODY","text":"Hello {{1}}"}]`,
	)
	invalidComponents := createCampaignTestTemplate(
		t,
		ctx,
		templateRepo,
		workspace.ID,
		activeConnection.ID,
		"invalid_components",
		"en_US",
		"APPROVED",
		`[{"type":"HEADER","text":"Order {{1}}"},{"type":"BODY","text":"Hello"}]`,
	)
	disconnectedTemplate := createCampaignTestTemplate(
		t,
		ctx,
		templateRepo,
		workspace.ID,
		disconnectedConnection.ID,
		"disconnected_campaign",
		"en_US",
		"APPROVED",
		`[{"type":"BODY","text":"Hello {{1}}"}]`,
	)
	otherConnectionTemplate := createCampaignTestTemplate(
		t,
		ctx,
		templateRepo,
		workspace.ID,
		otherActiveConnection.ID,
		"other_connection_campaign",
		"en_US",
		"APPROVED",
		`[{"type":"BODY","text":"Hello {{1}}"}]`,
	)
	foreignTemplate := createCampaignTestTemplate(
		t,
		ctx,
		templateRepo,
		foreignWorkspace.ID,
		foreignConnection.ID,
		"foreign_campaign",
		"en_US",
		"APPROVED",
		`[{"type":"BODY","text":"Hello {{1}}"}]`,
	)

	handler := admin.NewCampaignHandler(campaignRepo, templateRepo, connectionRepo, nil)
	assertRejected := func(
		name string,
		connectionID uuid.UUID,
		templateID uuid.UUID,
		includeParameter bool,
	) {
		t.Helper()
		response := createCampaignThroughHandler(
			t,
			handler,
			workspace.ID,
			connectionID,
			templateID.String(),
			"",
			includeParameter,
		)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400; body=%s", name, response.Code, response.Body.String())
		}
	}

	assertRejected("disconnected connection", disconnectedConnection.ID, disconnectedTemplate.ID, true)
	assertRejected("pending template", activeConnection.ID, pending.ID, true)
	assertRejected("template from another connection", activeConnection.ID, otherConnectionTemplate.ID, true)
	assertRejected("foreign template", activeConnection.ID, foreignTemplate.ID, true)
	assertRejected("unsupported components", activeConnection.ID, invalidComponents.ID, false)
	assertRejected("missing parameter", activeConnection.ID, approved.ID, false)

	// The legacy name+language request remains accepted while the UI now posts
	// the opaque template ID.
	response := createCampaignThroughHandler(
		t,
		handler,
		workspace.ID,
		activeConnection.ID,
		approved.Name,
		approved.Language,
		true,
	)
	if response.Code != http.StatusOK {
		t.Fatalf("valid create status = %d, want 200; body=%s", response.Code, response.Body.String())
	}

	campaigns, err := campaignRepo.ListByWorkspace(ctx, workspace.ID)
	if err != nil {
		t.Fatalf("list campaigns: %v", err)
	}
	if len(campaigns) != 1 {
		t.Fatalf("persisted campaigns = %d, want 1", len(campaigns))
	}
	campaign := campaigns[0]
	if campaign.TemplateName == nil || *campaign.TemplateName != approved.Name ||
		campaign.TemplateLanguage == nil || *campaign.TemplateLanguage != approved.Language {
		t.Fatalf(
			"template snapshot = name:%v language:%v",
			campaign.TemplateName,
			campaign.TemplateLanguage,
		)
	}
	if got := campaign.Recipients[0].Variables["1"]; got != "Alice" {
		t.Fatalf("resolved template parameter = %q, want Alice", got)
	}

	if err := connectionRepo.UpdateStatus(ctx, activeConnection.ID, "disconnected"); err != nil {
		t.Fatalf("disconnect campaign connection: %v", err)
	}
	startResponse := startCampaignThroughHandler(t, handler, workspace.ID, campaign.ID)
	if startResponse.Code != http.StatusConflict {
		t.Fatalf(
			"disconnected start status = %d, want 409; body=%s",
			startResponse.Code,
			startResponse.Body.String(),
		)
	}
	if err := connectionRepo.UpdateStatus(ctx, activeConnection.ID, "connected"); err != nil {
		t.Fatalf("reconnect campaign connection: %v", err)
	}
	if err := templateRepo.UpdateStatus(ctx, approved.ID, "REJECTED"); err != nil {
		t.Fatalf("reject template: %v", err)
	}
	startResponse = startCampaignThroughHandler(t, handler, workspace.ID, campaign.ID)
	if startResponse.Code != http.StatusConflict {
		t.Fatalf(
			"rejected-template start status = %d, want 409; body=%s",
			startResponse.Code,
			startResponse.Body.String(),
		)
	}
	if err := templateRepo.UpdateStatus(ctx, approved.ID, "APPROVED"); err != nil {
		t.Fatalf("approve template: %v", err)
	}
	if _, err := templateRepo.Upsert(ctx, &repository.WABATemplate{
		WorkspaceID:    workspace.ID,
		ConnectionID:   activeConnection.ID,
		MetaTemplateID: approved.MetaTemplateID,
		Name:           approved.Name,
		Language:       approved.Language,
		Status:         "APPROVED",
		Category:       approved.Category,
		Components:     json.RawMessage(`[{"type":"BODY","text":"Hello {{1}} {{2}}"}]`),
	}); err != nil {
		t.Fatalf("expand template parameters: %v", err)
	}
	startResponse = startCampaignThroughHandler(t, handler, workspace.ID, campaign.ID)
	if startResponse.Code != http.StatusConflict {
		t.Fatalf(
			"changed-components start status = %d, want 409; body=%s",
			startResponse.Code,
			startResponse.Body.String(),
		)
	}
	if _, err := templateRepo.Upsert(ctx, &repository.WABATemplate{
		WorkspaceID:    workspace.ID,
		ConnectionID:   activeConnection.ID,
		MetaTemplateID: approved.MetaTemplateID,
		Name:           approved.Name,
		Language:       approved.Language,
		Status:         "APPROVED",
		Category:       approved.Category,
		Components:     json.RawMessage(`[{"type":"BODY","text":"Hello {{1}}"}]`),
	}); err != nil {
		t.Fatalf("restore template parameters: %v", err)
	}

	startResponse = startCampaignThroughHandler(t, handler, workspace.ID, campaign.ID)
	if startResponse.Code != http.StatusOK {
		t.Fatalf("valid start status = %d, want 200; body=%s", startResponse.Code, startResponse.Body.String())
	}

	var payload []byte
	if err := pool.QueryRow(
		ctx,
		`SELECT payload
		   FROM campaign_batches
		  WHERE campaign_id = $1 AND batch_index = 1`,
		campaign.ID,
	).Scan(&payload); err != nil {
		t.Fatalf("read durable WABA batch: %v", err)
	}
	var task queue.CampaignBatchTask
	if err := json.Unmarshal(payload, &task); err != nil {
		t.Fatalf("decode durable WABA batch: %v", err)
	}
	if task.TemplateSnapshot == nil ||
		task.TemplateSnapshot.Language != "en_US" ||
		task.TemplateSnapshot.BodyParameterCount != 1 {
		t.Fatalf("durable template snapshot = %#v", task.TemplateSnapshot)
	}
}

func createCampaignTestConnection(
	t *testing.T,
	ctx context.Context,
	repo *repository.ConnectionRepository,
	workspaceID uuid.UUID,
	status string,
) *repository.Connection {
	t.Helper()
	connection := &repository.Connection{
		ID:             uuid.New(),
		WorkspaceID:    workspaceID,
		Name:           "Campaign WABA " + uuid.NewString(),
		Channel:        "whatsapp_cloud",
		SenderIdentity: uuid.NewString(),
		Status:         status,
	}
	if err := repo.Create(ctx, connection); err != nil {
		t.Fatalf("create connection: %v", err)
	}
	return connection
}

func createCampaignTestTemplate(
	t *testing.T,
	ctx context.Context,
	repo *repository.WABATemplateRepository,
	workspaceID uuid.UUID,
	connectionID uuid.UUID,
	name string,
	language string,
	status string,
	components string,
) *repository.WABATemplate {
	t.Helper()
	template, err := repo.Create(ctx, &repository.WABATemplate{
		WorkspaceID:    workspaceID,
		ConnectionID:   connectionID,
		MetaTemplateID: uuid.NewString(),
		Name:           name,
		Language:       language,
		Status:         status,
		Category:       "UTILITY",
		Components:     json.RawMessage(components),
	})
	if err != nil {
		t.Fatalf("create template: %v", err)
	}
	return template
}

func createCampaignThroughHandler(
	t *testing.T,
	handler *admin.CampaignHandler,
	workspaceID uuid.UUID,
	connectionID uuid.UUID,
	templateSelection string,
	templateLanguage string,
	includeParameter bool,
) *httptest.ResponseRecorder {
	t.Helper()
	recipients, err := json.Marshal([]domain.CampaignRecipient{{
		To:        "5511999998888",
		Variables: map[string]string{"name": "Alice"},
	}})
	if err != nil {
		t.Fatalf("marshal recipients: %v", err)
	}
	form := url.Values{
		"name":              {"Campaign " + uuid.NewString()},
		"channel":           {connectionID.String()},
		"batch_size":        {"100"},
		"delay_seconds":     {"0"},
		"template_select":   {templateSelection},
		"template_language": {templateLanguage},
		"recipients_data":   {string(recipients)},
		"skipped_data":      {"[]"},
	}
	if includeParameter {
		form.Set("waba_param_1", "{{name}}")
	}

	request := httptest.NewRequest(
		http.MethodPost,
		fmt.Sprintf("/admin/workspaces/%s/campaigns", workspaceID),
		strings.NewReader(form.Encode()),
	)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	echoServer := echo.New()
	echoContext := echoServer.NewContext(request, response)
	echoContext.SetPathValues(echo.PathValues{{
		Name:  "workspace_id",
		Value: workspaceID.String(),
	}})
	if err := handler.Create(echoContext); err != nil {
		t.Fatalf("Create: %v", err)
	}
	return response
}

func startCampaignThroughHandler(
	t *testing.T,
	handler *admin.CampaignHandler,
	workspaceID uuid.UUID,
	campaignID uuid.UUID,
) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(
		http.MethodPost,
		fmt.Sprintf("/admin/workspaces/%s/campaigns/%s/start", workspaceID, campaignID),
		nil,
	)
	response := httptest.NewRecorder()
	echoServer := echo.New()
	echoContext := echoServer.NewContext(request, response)
	echoContext.SetPathValues(echo.PathValues{
		{Name: "workspace_id", Value: workspaceID.String()},
		{Name: "id", Value: campaignID.String()},
	})
	if err := handler.Start(echoContext); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return response
}
