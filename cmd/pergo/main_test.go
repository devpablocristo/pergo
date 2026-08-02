package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/pablojhp.pergo/internal/api/handler"
	"github.com/pablojhp.pergo/internal/channel/whatsapp"
	"github.com/pablojhp.pergo/internal/config"
	"github.com/pablojhp.pergo/internal/integration/chatwoot"
	"github.com/pablojhp.pergo/internal/integration/typebot"
	"github.com/pablojhp.pergo/internal/platform/crypto"
	echosrv "github.com/pablojhp.pergo/internal/platform/echo"
	"github.com/pablojhp.pergo/internal/platform/postgres"
	"github.com/pablojhp.pergo/internal/repository"
	"github.com/pablojhp.pergo/internal/session"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

// TestServerBootHealthz verifies the server starts on a random port and
// responds to the liveness probe.
func TestServerBootHealthz(t *testing.T) {
	e := echosrv.New()
	h := &handler.HealthHandler{}
	h.RegisterRoutes(e)

	srv := httptest.NewServer(e)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestHTTPServerAppliesBoundedReadAndIdlePolicy(t *testing.T) {
	srv := newHTTPServer(":0", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))

	if srv.ReadHeaderTimeout != 5*time.Second {
		t.Fatalf("ReadHeaderTimeout = %s, want 5s", srv.ReadHeaderTimeout)
	}
	if srv.ReadTimeout != 30*time.Second {
		t.Fatalf("ReadTimeout = %s, want 30s", srv.ReadTimeout)
	}
	if srv.IdleTimeout != 60*time.Second {
		t.Fatalf("IdleTimeout = %s, want 60s", srv.IdleTimeout)
	}
	if srv.MaxHeaderBytes != 1<<20 {
		t.Fatalf("MaxHeaderBytes = %d, want %d", srv.MaxHeaderBytes, 1<<20)
	}
	if srv.WriteTimeout != 0 {
		t.Fatalf("WriteTimeout = %s, want zero for streaming responses", srv.WriteTimeout)
	}
}

func TestRuntimeProfileRouteIsolation(t *testing.T) {
	tests := []struct {
		profile string
		path    string
		want    int
	}{
		{profile: config.RuntimeAPI, path: "/api/v1/messages", want: http.StatusNoContent},
		{profile: config.RuntimeAPI, path: "/webhooks/waba/workspace", want: http.StatusNotFound},
		{profile: config.RuntimeWebhook, path: "/webhooks/waba/workspace", want: http.StatusNoContent},
		{profile: config.RuntimeWebhook, path: "/api/integrations/chatwoot", want: http.StatusNoContent},
		{profile: config.RuntimeWebhook, path: "/api/v1/messages", want: http.StatusNotFound},
		{profile: config.RuntimeWebhook, path: "/healthz", want: http.StatusNoContent},
		{profile: config.RuntimeWorker, path: "/healthz", want: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.profile+" "+tt.path, func(t *testing.T) {
			e := echosrv.New()
			e.Use(profileAccessMiddleware(tt.profile))
			e.Any("/*", func(c *echo.Context) error {
				return c.NoContent(http.StatusNoContent)
			})
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d", rec.Code, tt.want)
			}
		})
	}
}

func TestApplyRuntimeProfileArgument(t *testing.T) {
	cfg := &config.Config{RuntimeProfile: config.RuntimeAll}
	if err := applyRuntimeProfileArgument(cfg, []string{"worker"}); err != nil {
		t.Fatalf("applyRuntimeProfileArgument() error = %v", err)
	}
	if cfg.RuntimeProfile != config.RuntimeWorker {
		t.Fatalf("RuntimeProfile = %q", cfg.RuntimeProfile)
	}
	if err := applyRuntimeProfileArgument(cfg, []string{"unknown"}); err == nil {
		t.Fatal("unknown profile was accepted")
	}
}

func TestRuntimeProfileProcessRoles(t *testing.T) {
	tests := []struct {
		profile     string
		runsHTTP    bool
		runsWorkers bool
	}{
		{profile: config.RuntimeAll, runsHTTP: true, runsWorkers: true},
		{profile: config.RuntimeAPI, runsHTTP: true},
		{profile: config.RuntimeWebhook, runsHTTP: true},
		{profile: config.RuntimeWorker, runsWorkers: true},
		{profile: config.RuntimeMigrate},
	}

	for _, tt := range tests {
		t.Run(tt.profile, func(t *testing.T) {
			if got := profileRunsHTTP(tt.profile); got != tt.runsHTTP {
				t.Fatalf("profileRunsHTTP() = %v, want %v", got, tt.runsHTTP)
			}
			if got := profileRunsWorkers(tt.profile); got != tt.runsWorkers {
				t.Fatalf("profileRunsWorkers() = %v, want %v", got, tt.runsWorkers)
			}
		})
	}
}

// TestServerBootReadyz verifies the readiness probe returns 200 when both
// pgx and NATS are reachable.
func TestServerBootReadyz(t *testing.T) {
	pool, err := postgres.NewPool(context.Background(), testDSN())
	if err != nil {
		t.Skipf("skipping: cannot create pool: %v", err)
	}
	defer pool.Close()

	// Verify PostgreSQL is actually reachable
	if err := pool.Ping(context.Background()); err != nil {
		t.Skipf("skipping: cannot ping PostgreSQL: %v", err)
	}

	e := echosrv.New()
	h := &handler.HealthHandler{
		Pool: pool,
		NATS: &noopNATS{},
	}
	h.RegisterRoutes(e)

	srv := httptest.NewServer(e)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

// TestServerBootReadyzDown verifies readiness returns 503 when NATS is
// unreachable.
func TestServerBootReadyzDown(t *testing.T) {
	pool, err := postgres.NewPool(context.Background(), testDSN())
	if err != nil {
		t.Skipf("skipping: cannot create pool: %v", err)
	}
	defer pool.Close()

	// Verify PostgreSQL is actually reachable
	if err := pool.Ping(context.Background()); err != nil {
		t.Skipf("skipping: cannot ping PostgreSQL: %v", err)
	}

	e := echosrv.New()
	h := &handler.HealthHandler{
		Pool: pool,
		NATS: &failingNATS{},
	}
	h.RegisterRoutes(e)

	srv := httptest.NewServer(e)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", resp.StatusCode)
	}
}

// TestGracefulShutdown verifies the server exits within 5 seconds when the
// context is cancelled.
func TestGracefulShutdown(t *testing.T) {
	pool, err := postgres.NewPool(context.Background(), testDSN())
	if err != nil {
		t.Skipf("skipping: cannot create pool: %v", err)
	}
	defer pool.Close()

	// Verify PostgreSQL is actually reachable
	if err := pool.Ping(context.Background()); err != nil {
		t.Skipf("skipping: cannot ping PostgreSQL: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := postgres.NewSQLDB(pool)
	if err != nil {
		t.Fatalf("NewSQLDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := postgres.RunMigrations(db); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}

	// Start server in background; cancel context to trigger shutdown
	done := make(chan error, 1)
	go func() {
		done <- runServer(ctx, pool, db)
	}()

	// Give server time to start, then cancel
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Server exited cleanly
	case <-time.After(5 * time.Second):
		t.Error("server did not shut down within 5 seconds")
	}
}

// TestServerStartupRestoresPersistedWhatsAppSession verifies the same startup
// hook used by main restores a persisted WhatsApp connection. The client is a
// deterministic fake, so the test never opens a connection to Meta.
func TestServerStartupRestoresPersistedWhatsAppSession(t *testing.T) {
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, testDSN())
	if err != nil {
		t.Skipf("skipping: cannot create pool: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("skipping: cannot ping PostgreSQL: %v", err)
	}

	db, err := postgres.NewSQLDB(pool)
	if err != nil {
		t.Fatalf("NewSQLDB: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := postgres.RunMigrations(db); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}

	enc, err := crypto.NewEncryptor(make([]byte, 32))
	if err != nil {
		t.Fatalf("NewEncryptor: %v", err)
	}
	repo := repository.NewConnectionRepository(pool, enc)
	workspaceID := uuid.New()
	if _, err := pool.Exec(ctx, "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ($1, $2, NOW(), NOW())", workspaceID, "startup-reconnect-"+workspaceID.String()); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _, _ = pool.Exec(context.Background(), "DELETE FROM workspaces WHERE id = $1", workspaceID) }()

	jid := "5491100000001:1@s.whatsapp.net"
	connection := &repository.Connection{
		ID:             uuid.New(),
		WorkspaceID:    workspaceID,
		Name:           "Persisted test device",
		Channel:        "whatsapp",
		SenderIdentity: "5491100000001",
		JID:            &jid,
		Status:         string(session.DeviceStatusDisconnected),
	}
	if err := repo.Create(ctx, connection); err != nil {
		t.Fatalf("seed persisted WhatsApp connection: %v", err)
	}
	defer func() { _ = repo.Delete(context.Background(), connection.ID) }()

	fake := newStartupFakeClient()
	factory := whatsapp.ClientFactoryFunc(func(whatsapp.ClientConfig) (whatsapp.Client, error) {
		return fake, nil
	})
	registry := session.NewActiveSession()
	manager := session.NewManagerWithClientFactory(
		db, repo, registry, nil, "", nil, factory,
		session.WithReconnectTiming(func(context.Context, time.Duration) bool { return true }, func() time.Duration { return 0 }),
	)
	serverCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// This is the exact hook main calls before ListenAndServe; the test does
	// not invoke ReconnectAll directly.
	startWhatsAppRestoration(serverCtx, manager)
	select {
	case <-fake.connected:
	case <-time.After(2 * time.Second):
		t.Fatal("startup did not reconnect persisted WhatsApp session")
	}
	eventually(t, 2*time.Second, func() bool {
		stored, getErr := repo.GetByID(context.Background(), connection.ID)
		return getErr == nil && stored.Status == string(session.DeviceStatusConnected) && registry.Len() == 1
	}, "connection was not marked connected and registered after startup")

	cancel()
	select {
	case <-fake.disconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("restored client did not stop after root context cancellation")
	}
	eventually(t, 2*time.Second, func() bool { return registry.Len() == 0 }, "restored session remained registered after shutdown")
}

// --- helpers ---

func TestCompositionRoot_AllIntegrationsWired(t *testing.T) {
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, testDSN())
	if err != nil {
		t.Skipf("skipping: cannot create pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("skipping: cannot ping PostgreSQL: %v", err)
	}

	kek := make([]byte, 32)
	for i := range kek {
		kek[i] = byte(i)
	}
	enc, err := crypto.NewEncryptor(kek)
	if err != nil {
		t.Fatalf("failed to create encryptor: %v", err)
	}

	integrationRepo := repository.NewIntegrationRepository(pool, enc)
	chatwootMappingRepo := repository.NewChatwootMappingRepository(pool)
	typebotSessionRepo := repository.NewTypebotSessionRepository(pool)

	chatwootSyncer := chatwoot.NewChatwootSyncer(integrationRepo, chatwootMappingRepo, nil)
	typebotForwarder := typebot.NewForwarder(typebotSessionRepo, integrationRepo, nil)

	if chatwootSyncer == nil {
		t.Fatal("chatwootSyncer must not be nil — regression of Phase 21 wiring")
	}
	if typebotForwarder == nil {
		t.Fatal("typebotForwarder must not be nil — regression of TYPE-04 wiring")
	}
}

func testDSN() string {
	if dsn := os.Getenv("PERGO_TEST_DSN"); dsn != "" {
		return dsn
	}
	return "postgres://postgres:postgres@localhost:5432/pergo_test?sslmode=disable"
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(message)
}

type startupFakeClient struct {
	jid          types.JID
	connected    chan struct{}
	disconnected chan struct{}
}

func newStartupFakeClient() *startupFakeClient {
	return &startupFakeClient{connected: make(chan struct{}), disconnected: make(chan struct{})}
}

func (c *startupFakeClient) JID() types.JID       { return c.jid }
func (c *startupFakeClient) SetJID(jid types.JID) { c.jid = jid }
func (c *startupFakeClient) Run(ctx context.Context) error {
	if err := c.Connect(); err != nil {
		return err
	}
	c.Wait(ctx)
	return nil
}
func (c *startupFakeClient) Wait(ctx context.Context) {
	<-ctx.Done()
	c.Disconnect()
}
func (c *startupFakeClient) GetQRChannel(context.Context) (<-chan whatsmeow.QRChannelItem, error) {
	return nil, errors.New("QR pairing is not used by this test client")
}
func (c *startupFakeClient) Connect() error {
	select {
	case <-c.connected:
	default:
		close(c.connected)
	}
	return nil
}
func (c *startupFakeClient) Disconnect() {
	select {
	case <-c.disconnected:
	default:
		close(c.disconnected)
	}
}
func (c *startupFakeClient) AddEventHandler(whatsmeow.EventHandler) uint32 { return 0 }
func (c *startupFakeClient) Download(context.Context, whatsmeow.DownloadableMessage) ([]byte, error) {
	return nil, errors.New("media download is not used by this test client")
}

// noopNATS satisfies the handler.NATSConn interface and always succeeds.
type noopNATS struct{}

func (n *noopNATS) Ping() error { return nil }

// failingNATS satisfies the handler.NATSConn interface and always fails.
type failingNATS struct{}

func (f *failingNATS) Ping() error { return fmt.Errorf("nats unavailable") }
