package admin_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"

	"github.com/pablojhp.pergo/internal/api/handler/admin"
	"github.com/pablojhp.pergo/internal/api/middleware"
	"github.com/pablojhp.pergo/internal/domain"
	"github.com/pablojhp.pergo/internal/platform/crypto"
	"github.com/pablojhp.pergo/internal/platform/postgres"
	"github.com/pablojhp.pergo/internal/repository"
	"github.com/pablojhp.pergo/internal/session"
)

// TestDeviceHandler_Construction verifies fields are correct.
func TestDeviceHandler_Construction(t *testing.T) {
	h := &admin.DeviceHandler{
		Sessions:      nil,
		Manager:       nil,
		Connections:   nil,
		Publisher:     nil,
		NC:            nil,
		TemplatesRepo: nil,
	}
	if h == nil {
		t.Fatal("expected non-nil DeviceHandler")
	}
}

// TestDeviceHandler_GetQR_MissingPhone verifies BadRequest response when phone param is missing.
func TestDeviceHandler_GetQR_MissingPhone(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/admin/devices/qr", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	h := &admin.DeviceHandler{}
	err := h.GetQR(c)
	if err != nil {
		t.Logf("GetQR returned error (acceptable): %v", err)
	}
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", rec.Code)
	}
}

// TestDeviceHandler_DatabaseFlows runs integration tests against real PostgreSQL.
func TestDeviceHandler_DatabaseFlows(t *testing.T) {
	dsn := os.Getenv("PERGO_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/pergo?sslmode=disable"
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		// Try fallback port 5433 for testing environments
		dsnFallback := "postgres://postgres:postgres@localhost:5433/pergo?sslmode=disable"
		pool, err = pgxpool.New(ctx, dsnFallback)
		if err != nil {
			t.Skip("PostgreSQL not available for testing")
		}
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Skip("PostgreSQL ping failed")
	}

	encryptor, err := crypto.NewEncryptor([]byte("dev-development-key-32-bytes-kek"))
	if err != nil {
		t.Fatalf("failed to initialize encryptor: %v", err)
	}

	connRepo := repository.NewConnectionRepository(pool, encryptor)
	wsRepo := repository.NewWorkspaceRepository(pool)

	// Setup a test workspace
	ws, err := wsRepo.Create(ctx, "Test Workspace Devices")
	if err != nil {
		t.Fatalf("failed to create workspace: %v", err)
	}
	defer func() {
		_ = wsRepo.Delete(ctx, ws.ID)
	}()

	h := &admin.DeviceHandler{
		Connections: connRepo,
	}

	e := echo.New()

	t.Run("List Connections", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/admin/devices", nil)
		// Set workspace cookie
		req.AddCookie(&http.Cookie{Name: "pergo-active-workspace", Value: ws.ID.String()})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		if err := h.List(c); err != nil {
			t.Errorf("List returned error: %v", err)
		}

		if rec.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", rec.Code)
		}
	})

	t.Run("Create Telegram Bot - Bad Token", func(t *testing.T) {
		fValues := make(url.Values)
		fValues.Set("name", "Test TG Bot")
		fValues.Set("channel", "telegram")
		fValues.Set("token", "12345:invalidtoken")

		req := httptest.NewRequest(http.MethodPost, "/admin/devices/create", strings.NewReader(fValues.Encode()))
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationForm)
		req.AddCookie(&http.Cookie{Name: "pergo-active-workspace", Value: ws.ID.String()})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		if err := h.Create(c); err != nil {
			t.Errorf("Create returned error: %v", err)
		}

		// Validation should fail on getMe because token is dummy
		retarget := rec.Header().Get("HX-Retarget")
		if retarget != "#modal-error-container" {
			t.Errorf("expected HX-Retarget header, got %s", retarget)
		}
	})

	t.Run("Delete Connection (Disconnect)", func(t *testing.T) {
		// Manually insert a mock connection
		conn := &repository.Connection{
			WorkspaceID:    ws.ID,
			Name:           "Mock to delete",
			Channel:        "telegram",
			SenderIdentity: "@MockBot",
			Status:         "connected",
		}
		err := connRepo.Create(ctx, conn)
		if err != nil {
			t.Fatalf("failed to insert connection: %v", err)
		}

		req := httptest.NewRequest(http.MethodDelete, "/admin/devices/"+conn.ID.String(), nil)
		req.AddCookie(&http.Cookie{Name: "pergo-active-workspace", Value: ws.ID.String()})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetPath("/admin/devices/:id")
		c.SetPathValues(echo.PathValues{
			{Name: "id", Value: conn.ID.String()},
		})

		if err := h.Disconnect(c); err != nil {
			t.Errorf("Disconnect returned error: %v", err)
		}

		if rec.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", rec.Code)
		}

		// Verify connection is gone
		_, err = connRepo.GetByID(ctx, conn.ID)
		if !errors.Is(err, repository.ErrConnectionNotFound) {
			t.Errorf("expected connection to be deleted, got error: %v", err)
		}
	})
}

// TestDeviceHandler_StartPairing_LimitExceeded checks that the handler returns HTTP 422
// when the WhatsApp connection limit is exceeded.
func TestDeviceHandler_StartPairing_LimitExceeded(t *testing.T) {
	t.Setenv("PERGO_MAX_WHATSAPP_CONNECTIONS", "0")

	dsn := os.Getenv("PERGO_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/pergo?sslmode=disable"
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		dsnFallback := "postgres://postgres:postgres@localhost:5433/pergo?sslmode=disable"
		pool, err = pgxpool.New(ctx, dsnFallback)
		if err != nil {
			t.Skip("PostgreSQL not available for testing")
		}
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Skip("PostgreSQL ping failed")
	}

	sqlDB, err := postgres.NewSQLDB(pool)
	if err != nil {
		t.Fatalf("failed to get sql.DB: %v", err)
	}

	enc, _ := crypto.NewEncryptor(make([]byte, 32))
	repo := repository.NewConnectionRepository(pool, enc)
	registry := session.NewActiveSession()
	manager := session.NewManager(
		sqlDB,
		repo,
		registry,
		nil,
		"2.3000.1025000000",
		nil,
	)

	h := &admin.DeviceHandler{
		Connections: repo,
		Sessions:    registry,
		Manager:     manager,
	}

	e := echo.New()
	fValues := make(url.Values)
	fValues.Set("phone", "5511999990001")
	req := httptest.NewRequest(http.MethodPost, "/admin/devices/pair", strings.NewReader(fValues.Encode()))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationForm)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err = h.StartPairing(c)
	if err != nil {
		t.Errorf("StartPairing returned error: %v", err)
	}

	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("expected status 422, got %d", rec.Code)
	}

	if !strings.Contains(rec.Body.String(), "maximum active WhatsApp connections limit exceeded") {
		t.Errorf("expected body to contain limit exceeded message, got: %s", rec.Body.String())
	}
}

// TestDeviceHandler_WS_RequiresAuth asserts that the WebSocket endpoint /admin/devices/test/ws
// rejects unauthenticated requests.
func TestDeviceHandler_WS_RequiresAuth(t *testing.T) {
	e := echo.New()
	e.Use(middleware.SessionAuthMiddleware())

	h := &admin.DeviceHandler{}
	e.GET("/admin/devices/test/ws", h.WS)

	req := httptest.NewRequest(http.MethodGet, "/admin/devices/test/ws", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	// Requests without session cookie redirect to /admin/login (302)
	if rec.Code != http.StatusFound {
		t.Errorf("expected 302 redirect, got %d", rec.Code)
	}
	location := rec.Header().Get("Location")
	if location != "/admin/login" {
		t.Errorf("expected redirect to /admin/login, got %q", location)
	}
}

type fakeDeviceConnectionStore struct {
	connections map[uuid.UUID]*repository.Connection
	created     []*repository.Connection
}

func newFakeDeviceConnectionStore(connections ...*repository.Connection) *fakeDeviceConnectionStore {
	store := &fakeDeviceConnectionStore{connections: make(map[uuid.UUID]*repository.Connection)}
	for _, connection := range connections {
		store.connections[connection.ID] = connection
	}
	return store
}

func (s *fakeDeviceConnectionStore) ListByWorkspace(_ context.Context, workspaceID uuid.UUID) ([]*repository.Connection, error) {
	var connections []*repository.Connection
	for _, connection := range s.connections {
		if connection.WorkspaceID == workspaceID {
			connections = append(connections, connection)
		}
	}
	return connections, nil
}

func (s *fakeDeviceConnectionStore) GetByID(_ context.Context, id uuid.UUID) (*repository.Connection, error) {
	connection, ok := s.connections[id]
	if !ok {
		return nil, repository.ErrConnectionNotFound
	}
	return connection, nil
}

func (s *fakeDeviceConnectionStore) Create(_ context.Context, connection *repository.Connection) error {
	if connection.ID == uuid.Nil {
		connection.ID = uuid.New()
	}
	s.connections[connection.ID] = connection
	s.created = append(s.created, connection)
	return nil
}

func (s *fakeDeviceConnectionStore) Delete(_ context.Context, id uuid.UUID) error {
	if _, ok := s.connections[id]; !ok {
		return repository.ErrConnectionNotFound
	}
	delete(s.connections, id)
	return nil
}

type fakeDevicePublisher struct {
	calls   int
	subject string
	data    []byte
	traceID string
}

func (p *fakeDevicePublisher) Publish(_ context.Context, subject string, data []byte, traceID string) error {
	p.calls++
	p.subject = subject
	p.data = append([]byte(nil), data...)
	p.traceID = traceID
	return nil
}

func deviceFormRequest(method, target string, values url.Values, workspaceID uuid.UUID) (*echo.Echo, *echo.Context, *httptest.ResponseRecorder) {
	e := echo.New()
	req := httptest.NewRequest(method, target, strings.NewReader(values.Encode()))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationForm)
	if workspaceID != uuid.Nil {
		req.AddCookie(&http.Cookie{Name: "pergo-active-workspace", Value: workspaceID.String()})
	}
	rec := httptest.NewRecorder()
	return e, e.NewContext(req, rec), rec
}

func TestDeviceHandler_WhatsAppMockCreateDisabled(t *testing.T) {
	workspaceID := uuid.New()
	store := newFakeDeviceConnectionStore()
	h := &admin.DeviceHandler{Connections: store}
	values := url.Values{"name": {"Local Mock"}, "channel": {"whatsapp_mock"}}
	_, c, rec := deviceFormRequest(http.MethodPost, "/admin/devices/create", values, workspaceID)

	if err := h.Create(c); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
	if len(store.created) != 0 {
		t.Fatalf("created %d connections while disabled", len(store.created))
	}
}

func TestDeviceHandler_WhatsAppMockCreateEnabled(t *testing.T) {
	workspaceID := uuid.New()
	store := newFakeDeviceConnectionStore()
	h := &admin.DeviceHandler{
		Connections:         store,
		WhatsAppMockEnabled: true,
	}
	values := url.Values{"name": {"Local Mock"}, "channel": {"whatsapp_mock"}}
	_, c, rec := deviceFormRequest(http.MethodPost, "/admin/devices/create", values, workspaceID)

	if err := h.Create(c); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if len(store.created) != 1 {
		t.Fatalf("created %d connections, want 1", len(store.created))
	}

	connection := store.created[0]
	if connection.ID == uuid.Nil {
		t.Fatal("expected generated connection ID")
	}
	if connection.WorkspaceID != workspaceID ||
		connection.Channel != "whatsapp_mock" ||
		connection.Status != "connected" ||
		!connection.IsDefault {
		t.Fatalf("unexpected connection: %+v", connection)
	}
	if connection.SenderIdentity != "whatsapp-mock:"+connection.ID.String() {
		t.Fatalf("sender identity = %q", connection.SenderIdentity)
	}
	if len(connection.Credentials) != 0 {
		t.Fatalf("mock connection stored credentials: %q", connection.Credentials)
	}
}

func TestDeviceHandler_WhatsAppMockRunTestDisabled(t *testing.T) {
	connection := &repository.Connection{
		ID:             uuid.New(),
		WorkspaceID:    uuid.New(),
		Channel:        "whatsapp_mock",
		SenderIdentity: "whatsapp-mock:disabled",
	}
	store := newFakeDeviceConnectionStore(connection)
	publisher := &fakeDevicePublisher{}
	h := &admin.DeviceHandler{Connections: store, Publisher: publisher}
	values := url.Values{
		"connection_id": {connection.ID.String()},
		"to":            {"local-recipient"},
		"body":          {"safe local test"},
	}
	_, c, rec := deviceFormRequest(http.MethodPost, "/admin/devices/test", values, uuid.Nil)

	if err := h.RunTest(c); err != nil {
		t.Fatalf("RunTest: %v", err)
	}
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
	if publisher.calls != 0 {
		t.Fatalf("publisher called %d times while disabled", publisher.calls)
	}
}

func TestDeviceHandler_WhatsAppMockRunTestEnabled(t *testing.T) {
	connection := &repository.Connection{
		ID:             uuid.New(),
		WorkspaceID:    uuid.New(),
		Channel:        "whatsapp_mock",
		SenderIdentity: "whatsapp-mock:" + uuid.New().String(),
	}
	store := newFakeDeviceConnectionStore(connection)
	publisher := &fakeDevicePublisher{}
	h := &admin.DeviceHandler{
		Connections:         store,
		Publisher:           publisher,
		WhatsAppMockEnabled: true,
	}
	values := url.Values{
		"connection_id": {connection.ID.String()},
		"to":            {"local-recipient"},
		"body":          {"safe local test"},
	}
	_, c, rec := deviceFormRequest(http.MethodPost, "/admin/devices/test", values, uuid.Nil)

	if err := h.RunTest(c); err != nil {
		t.Fatalf("RunTest: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if publisher.calls != 1 || publisher.subject != "messages.outbound" {
		t.Fatalf("unexpected publish: calls=%d subject=%q", publisher.calls, publisher.subject)
	}

	var message domain.QueueMessage
	if err := json.Unmarshal(publisher.data, &message); err != nil {
		t.Fatalf("decode queue message: %v", err)
	}
	if message.WorkspaceID != connection.WorkspaceID ||
		message.ConnectionID != connection.ID ||
		message.SenderIdentity != connection.SenderIdentity ||
		message.To != "local-recipient" ||
		message.Body != "safe local test" ||
		message.Channel != "whatsapp_mock" {
		t.Fatalf("unexpected queue message: %+v", message)
	}
	if message.TraceID == "" || message.TraceID != publisher.traceID {
		t.Fatalf("queue trace ID %q does not match publish trace ID %q", message.TraceID, publisher.traceID)
	}
}

func TestDeviceHandler_WhatsAppMockPairForm(t *testing.T) {
	for _, tt := range []struct {
		name    string
		enabled bool
		want    bool
	}{
		{name: "disabled", enabled: false, want: false},
		{name: "enabled", enabled: true, want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, "/admin/devices/pair-form", nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			h := &admin.DeviceHandler{WhatsAppMockEnabled: tt.enabled}

			if err := h.PairForm(c); err != nil {
				t.Fatalf("PairForm: %v", err)
			}
			got := strings.Contains(rec.Body.String(), `value="whatsapp_mock"`)
			if got != tt.want {
				t.Fatalf("mock option present = %v, want %v", got, tt.want)
			}
		})
	}
}
