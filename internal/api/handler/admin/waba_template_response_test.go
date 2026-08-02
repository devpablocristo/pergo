package admin_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/pablojhp.pergo/internal/api/handler/admin"
	"github.com/pablojhp.pergo/internal/platform/crypto"
	"github.com/pablojhp.pergo/internal/platform/httpresponse"
	"github.com/pablojhp.pergo/internal/repository"
)

type adminFailingTransport func(*http.Request) (*http.Response, error)

func (f adminFailingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestWABATemplateRemoteBodiesAreBoundedAndRedacted(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	workspaceRepo := repository.NewWorkspaceRepository(pool)
	templateRepo := repository.NewWABATemplateRepository(pool)
	encryptor, err := crypto.NewEncryptor(make([]byte, 32))
	if err != nil {
		t.Fatalf("NewEncryptor: %v", err)
	}
	connectionRepo := repository.NewConnectionRepository(pool, encryptor)
	workspace, err := workspaceRepo.Create(ctx, "waba_template_response_"+uuid.NewString())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _ = workspaceRepo.Delete(context.Background(), workspace.ID) }()

	const (
		tokenMarker      = "ADMIN_META_TOKEN_MUST_NOT_LEAK"
		accountMarker    = "ADMIN_META_ACCOUNT_MUST_NOT_LEAK"
		templateIDMarker = "ADMIN_META_TEMPLATE_ID_MUST_NOT_LEAK"
		bodyMarker       = "ADMIN_META_BODY_MUST_NOT_LEAK"
		baseMarker       = "ADMIN_META_BASE_MUST_NOT_LEAK"
	)
	credentials, err := json.Marshal(map[string]string{
		"token":           tokenMarker,
		"waba_account_id": accountMarker,
	})
	if err != nil {
		t.Fatalf("marshal credentials: %v", err)
	}
	connection := &repository.Connection{
		ID:             uuid.New(),
		WorkspaceID:    workspace.ID,
		Name:           "WABA",
		Channel:        "whatsapp_cloud",
		SenderIdentity: "phone",
		Status:         "active",
		Credentials:    credentials,
	}
	if err := connectionRepo.Create(ctx, connection); err != nil {
		t.Fatalf("create connection: %v", err)
	}
	template, err := templateRepo.Create(ctx, &repository.WABATemplate{
		WorkspaceID:    workspace.ID,
		ConnectionID:   connection.ID,
		MetaTemplateID: templateIDMarker,
		Name:           "template",
		Language:       "en_US",
		Status:         "PENDING",
		Category:       "UTILITY",
		Components:     json.RawMessage(`[]`),
	})
	if err != nil {
		t.Fatalf("create template: %v", err)
	}

	actions := []struct {
		name string
		call func(*admin.WABATemplateHandler, *httptest.ResponseRecorder)
	}{
		{
			name: "create",
			call: func(handler *admin.WABATemplateHandler, response *httptest.ResponseRecorder) {
				request := httptest.NewRequest(
					http.MethodPost,
					fmt.Sprintf("/admin/workspaces/%s/templates", workspace.ID),
					strings.NewReader(`{"name":"new_template","language":"en_US","category":"UTILITY","components":[]}`),
				)
				request.Header.Set("Content-Type", "application/json")
				echoContext := echo.New().NewContext(request, response)
				echoContext.SetPathValues(echo.PathValues{{
					Name: "workspace_id", Value: workspace.ID.String(),
				}})
				if err := handler.Create(echoContext); err != nil {
					t.Fatalf("Create: %v", err)
				}
			},
		},
		{
			name: "sync",
			call: func(handler *admin.WABATemplateHandler, response *httptest.ResponseRecorder) {
				request := httptest.NewRequest(
					http.MethodPost,
					fmt.Sprintf(
						"/admin/workspaces/%s/templates/%s/sync",
						workspace.ID,
						template.ID,
					),
					nil,
				)
				echoContext := echo.New().NewContext(request, response)
				echoContext.SetPathValues(echo.PathValues{
					{Name: "workspace_id", Value: workspace.ID.String()},
					{Name: "template_id", Value: template.ID.String()},
				})
				if err := handler.Sync(echoContext); err != nil {
					t.Fatalf("Sync: %v", err)
				}
			},
		},
	}

	for _, action := range actions {
		action := action
		for _, status := range []int{http.StatusOK, http.StatusBadRequest} {
			status := status
			t.Run(action.name+"/"+http.StatusText(status), func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(status)
					_, _ = w.Write([]byte(bodyMarker + strings.Repeat("x", int(httpresponse.MaxBodyBytes))))
				}))
				defer server.Close()

				handler := admin.NewWABATemplateHandler(templateRepo, connectionRepo)
				handler.Client = server.Client()
				handler.BaseURL = server.URL
				response := httptest.NewRecorder()
				action.call(handler, response)
				if response.Code != http.StatusBadGateway {
					t.Fatalf(
						"status = %d, want 502; body=%s",
						response.Code,
						response.Body.String(),
					)
				}
				for _, marker := range []string{
					tokenMarker,
					accountMarker,
					templateIDMarker,
					bodyMarker,
				} {
					if strings.Contains(response.Body.String(), marker) {
						t.Fatalf("response leaked %q: %s", marker, response.Body.String())
					}
				}
			})
		}
	}

	for _, action := range actions {
		action := action
		t.Run(action.name+"/transport-error", func(t *testing.T) {
			handler := admin.NewWABATemplateHandler(templateRepo, connectionRepo)
			handler.Client = &http.Client{Transport: adminFailingTransport(
				func(request *http.Request) (*http.Response, error) {
					return nil, errors.New(
						request.URL.String() + " " +
							request.Header.Get("Authorization") + " " +
							bodyMarker,
					)
				},
			)}
			handler.BaseURL = "https://example.invalid/" + baseMarker
			response := httptest.NewRecorder()
			action.call(handler, response)
			if response.Code != http.StatusBadGateway {
				t.Fatalf(
					"status = %d, want 502; body=%s",
					response.Code,
					response.Body.String(),
				)
			}
			for _, marker := range []string{
				baseMarker,
				tokenMarker,
				accountMarker,
				templateIDMarker,
				bodyMarker,
			} {
				if strings.Contains(response.Body.String(), marker) {
					t.Fatalf("response leaked %q: %s", marker, response.Body.String())
				}
			}
		})
	}
}
