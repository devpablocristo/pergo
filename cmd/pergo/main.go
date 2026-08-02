// Package main is the composition root for the PerGo server.
// It wires dependencies, starts the HTTP server, and handles graceful shutdown.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
	"github.com/nats-io/nats.go"

	mcpserver "github.com/mark3labs/mcp-go/server"
	"github.com/pablojhp.pergo/internal/api/handler"
	"github.com/pablojhp.pergo/internal/api/handler/admin"
	"github.com/pablojhp.pergo/internal/api/mcp"
	"github.com/pablojhp.pergo/internal/api/middleware"
	"github.com/pablojhp.pergo/internal/channel"
	"github.com/pablojhp.pergo/internal/channel/email"
	"github.com/pablojhp.pergo/internal/channel/instagram"
	"github.com/pablojhp.pergo/internal/channel/telegram"
	"github.com/pablojhp.pergo/internal/channel/whatsapp"
	"github.com/pablojhp.pergo/internal/channel/whatsappmock"
	"github.com/pablojhp.pergo/internal/config"
	"github.com/pablojhp.pergo/internal/inbound"
	"github.com/pablojhp.pergo/internal/integration/chatwoot"
	"github.com/pablojhp.pergo/internal/integration/typebot"
	"github.com/pablojhp.pergo/internal/media"
	"github.com/pablojhp.pergo/internal/outbound"
	"github.com/pablojhp.pergo/internal/platform/audit"
	"github.com/pablojhp.pergo/internal/platform/crypto"
	echosrv "github.com/pablojhp.pergo/internal/platform/echo"
	"github.com/pablojhp.pergo/internal/platform/metaapi"
	"github.com/pablojhp.pergo/internal/platform/obs"
	"github.com/pablojhp.pergo/internal/platform/postgres"
	"github.com/pablojhp.pergo/internal/platform/postgres/tenant"
	"github.com/pablojhp.pergo/internal/platform/queue"
	"github.com/pablojhp.pergo/internal/platform/shutdown"
	"github.com/pablojhp.pergo/internal/platform/storage"
	"github.com/pablojhp.pergo/internal/repository"
	"github.com/pablojhp.pergo/internal/session"
	"github.com/pablojhp.pergo/internal/ui/contextkey"
	"github.com/pablojhp.pergo/internal/webhook"
	"github.com/pablojhp.pergo/templates/pages"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "rotate-waba-webhook-secrets" {
		slog.SetDefault(obs.NewJSONLogger(os.Stderr))
		if err := runWABACredentialRotation(context.Background()); err != nil {
			slog.Error("WABA credential rotation failed", "error", err)
			os.Exit(1)
		}
		slog.Info("WABA webhook credentials rotated successfully",
			"workspace_id", os.Getenv("PERGO_ROTATE_WORKSPACE_ID"),
			"connection_id", os.Getenv("PERGO_ROTATE_CONNECTION_ID"),
		)
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "mcp" {
		slog.SetDefault(obs.NewJSONLogger(os.Stderr))
		cfg := config.Load()
		cfg.RuntimeProfile = config.RuntimeWorker
		if err := cfg.Validate(); err != nil {
			slog.Error("invalid configuration", "error", err)
			os.Exit(1)
		}
		ctx := context.Background()

		pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
		if err != nil {
			slog.Error("failed to create pgxpool", "error", err)
			os.Exit(1)
		}
		defer pool.Close()

		kek := resolvedKEK(cfg)
		encryptor, err := crypto.NewEncryptor(kek)
		if err != nil {
			slog.Error("failed to initialize encryptor", "error", err)
			os.Exit(1)
		}

		nc, err := connectConfiguredNATS(cfg)
		if err != nil {
			slog.Error("failed to connect to NATS", "error", err)
			os.Exit(1)
		}
		defer nc.Close()
		accountLabel := cfg.NATSAccount
		if accountLabel == "" {
			accountLabel = "local"
		}
		if err := queue.VerifyEnvironmentIsolation(ctx, nc, cfg.Environment, accountLabel, cfg.NATSStreamReplicas); err != nil {
			slog.Error("NATS account isolation check failed", "error", err)
			os.Exit(1)
		}
		publisher := queue.NewJetStreamPublisher(nc)

		wsRepo := repository.NewWorkspaceRepository(pool)
		connectionRepo := repository.NewConnectionRepository(pool, encryptor)
		contactRepo := repository.NewContactRepository(pool)
		auditRepo := repository.NewAuditRepository(pool)

		ingestor := outbound.NewProcessor(nil, nil, connectionRepo, publisher)
		mcpServer := mcp.NewServer(wsRepo, connectionRepo, contactRepo, auditRepo, ingestor)

		stdServer := mcpserver.NewStdioServer(mcpServer.MCPServer)
		slog.Info("starting MCP server in stdio mode")
		if err := stdServer.Listen(ctx, os.Stdin, os.Stdout); err != nil {
			slog.Error("MCP stdio server execution failed", "error", err)
			os.Exit(1)
		}
		return
	}

	slog.SetDefault(obs.NewJSONLogger(os.Stdout))

	// --- Config from env vars ---
	cfg := config.Load()
	if err := applyRuntimeProfileArgument(cfg, os.Args[1:]); err != nil {
		slog.Error("invalid command", "error", err)
		os.Exit(2)
	}
	if err := cfg.Validate(); err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	runsWorkers := profileRunsWorkers(cfg.RuntimeProfile)

	// --- PostgreSQL ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	managedShutdown := false

	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("failed to create pgxpool", "error", err)
		os.Exit(1)
	}
	defer func() {
		if !managedShutdown {
			pool.Close()
		}
	}()

	db, err := postgres.NewSQLDB(pool)
	if err != nil {
		slog.Error("failed to create stdlib sql.DB", "error", err)
		os.Exit(1)
	}
	defer func() {
		if !managedShutdown {
			_ = db.Close()
		}
	}()

	// --- Cryptography Encryptor & Credentials Repository ---
	kek := resolvedKEK(cfg)
	if len(cfg.KEKBytes) != 32 {
		slog.Warn("PERGO_KEK_BASE64 is not set or not 32 bytes; using a default development key. DO NOT USE IN PRODUCTION.")
	}
	encryptor, err := crypto.NewEncryptor(kek)
	if err != nil {
		slog.Error("failed to initialize encryptor", "error", err)
		os.Exit(1)
	}

	if cfg.RuntimeProfile == config.RuntimeMigrate {
		if err := postgres.RunMigrations(db); err != nil {
			slog.Error("migrations failed", "error", err)
			os.Exit(1)
		}
		dlqRepo := repository.NewWebhookDLQRepository(pool, encryptor)
		if err := dlqRepo.BackfillLegacyEncryption(ctx); err != nil {
			slog.Error("webhook DLQ encryption backfill failed", "error", err)
			os.Exit(1)
		}
		nc, err := connectConfiguredNATS(cfg)
		if err != nil {
			slog.Error("failed to connect to NATS for bootstrap", "error", err)
			os.Exit(1)
		}
		defer nc.Close()
		accountLabel := cfg.NATSAccount
		if accountLabel == "" {
			accountLabel = "local"
		}
		if cfg.NATSAdoptDrainedLegacy {
			if err := queue.AdoptDrainedLegacyEnvironmentIsolation(
				ctx,
				nc,
				cfg.Environment,
				accountLabel,
				cfg.NATSStreamReplicas,
			); err != nil {
				slog.Error("legacy NATS adoption gate failed", "error", err)
				os.Exit(1)
			}
		}
		if err := queue.BootstrapJetStream(
			ctx,
			nc,
			cfg.Environment,
			accountLabel,
			cfg.NATSStreamReplicas,
		); err != nil {
			slog.Error("NATS bootstrap failed", "error", err)
			os.Exit(1)
		}
		slog.Info("database migrations, encrypted backfill, and NATS bootstrap completed")
		return
	}
	if cfg.RunMigrations {
		if err := postgres.RunMigrations(db); err != nil {
			slog.Error("migrations failed", "error", err)
			os.Exit(1)
		}
		dlqRepo := repository.NewWebhookDLQRepository(pool, encryptor)
		if err := dlqRepo.BackfillLegacyEncryption(ctx); err != nil {
			slog.Error("development webhook DLQ encryption backfill failed", "error", err)
			os.Exit(1)
		}
		slog.Info("development migrations and encrypted backfill applied successfully")
	}

	// --- NATS ---
	nc, err := connectConfiguredNATS(cfg)
	if err != nil {
		slog.Error("failed to connect to NATS", "error", err)
		os.Exit(1)
	}
	defer func() {
		if !managedShutdown {
			nc.Close()
		}
	}()
	slog.Info("connected to NATS", "profile", cfg.RuntimeProfile, "environment", cfg.Environment)
	accountLabel := cfg.NATSAccount
	if accountLabel == "" {
		accountLabel = "local"
	}
	if cfg.IsDevelopment() {
		if err := queue.BootstrapJetStream(
			ctx,
			nc,
			cfg.Environment,
			accountLabel,
			cfg.NATSStreamReplicas,
		); err != nil {
			slog.Error("development NATS bootstrap failed", "error", err)
			os.Exit(1)
		}
	} else if err := queue.VerifyEnvironmentIsolation(
		ctx,
		nc,
		cfg.Environment,
		accountLabel,
		cfg.NATSStreamReplicas,
	); err != nil {
		slog.Error("NATS account isolation check failed", "error", err)
		os.Exit(1)
	}
	publisher := queue.NewJetStreamPublisher(nc)

	// --- Rate limiter (per-workspace token bucket) ---
	rateLimiter := middleware.NewRateLimiter(10, 10) // 10 req/s, burst 10
	loginRateLimiter := middleware.NewIPRateLimiter(0.2, 5)
	// Queue admission is enforced durably by JetStream per workspace subject.
	// A process-local counter is incorrect for split API/worker replicas.
	var queueDepth *middleware.QueueDepthTracker

	// --- Media storage (explicitly disabled outside local/test in this build) ---
	s3Client := storage.NewDisabledS3Client()
	if cfg.MediaMode == config.MediaMemory {
		s3Client, err = storage.NewS3Client(
			cfg.S3Endpoint,
			cfg.S3Region,
			cfg.S3AccessKey,
			cfg.S3SecretKey,
			cfg.S3Bucket,
			cfg.S3UsePathStyle,
		)
		if err != nil {
			slog.Error("failed to initialize development media storage", "error", err)
			os.Exit(1)
		}
		slog.Warn("in-memory media storage enabled; do not use this mode outside development/test")
	} else {
		slog.Info("media storage disabled; media operations fail closed")
	}

	mediaEngine := media.NewDefaultEngine(s3Client)
	metaGraphBaseURL := metaapi.BaseURL(cfg.MetaGraphVersion)

	connectionRepo := repository.NewConnectionRepository(pool, encryptor)
	recipientSessionRepo := repository.NewRecipientSessionRepository(pool)
	contactRepo := repository.NewContactRepository(pool)
	userActionLogRepo := repository.NewUserActionLogRepository(pool)
	windowChecker := session.NewWindowChecker(recipientSessionRepo)

	// --- REST Adapters ---
	wabaAdapter := whatsapp.NewWABAAdapter(connectionRepo, nil, windowChecker, cfg.ExternalURL)
	wabaAdapter.SetBaseURL(metaGraphBaseURL)
	telegramAdapter := telegram.NewTelegramAdapter(connectionRepo, nil, s3Client)
	instagramAdapter := instagram.NewAdapter(connectionRepo, nil, cfg.ExternalURL)
	instagramAdapter.SetBaseURL(metaGraphBaseURL)

	// --- Worker (reads from JetStream, dispatches with retry/TTL/dedup) ---
	sessionRegistry := session.NewActiveSession()

	dispatcherRegistry := channel.NewRegistry(nil) // populated by session manager
	dispatcherRegistry.Register("whatsapp_cloud", wabaAdapter)
	dispatcherRegistry.Register("telegram", telegramAdapter)
	dispatcherRegistry.Register("instagram", instagramAdapter)
	if cfg.IsDevelopment() {
		whatsAppAdapter := whatsapp.NewWhatsAppAdapter(nil, s3Client)
		whatsAppAdapter.SetSessionFinder(sessionRegistry)
		dispatcherRegistry.Register("whatsapp", whatsAppAdapter)

		emailProvider := email.NewSMTPProvider(email.SMTPConfig{
			Host:        "localhost",
			Port:        1025,
			FromAddress: "noreply@pergo.dev",
			FromName:    "PerGo Platform",
		})
		emailAdapter := email.NewEmailAdapter(emailProvider)
		dispatcherRegistry.Register("email", emailAdapter)
		dispatcherRegistry.Register("email_smtp", emailAdapter)
	} else {
		slog.Info("unsupported WhatsApp Web and development SMTP channels disabled")
	}
	if cfg.WhatsAppMockEnabled {
		dispatcherRegistry.Register("whatsapp_mock", whatsappmock.NewAdapter())
		slog.Warn("local WhatsApp mock dispatcher is enabled; simulated sends never contact WhatsApp or Meta")
	}

	// --- Audit writer ---
	auditWriter := audit.NewWriter(pool, 5000, 2)

	dedupRepo := repository.NewInboundDedupRepository(pool)
	wsRepo := repository.NewWorkspaceRepository(pool)
	dispatchRepo := repository.NewMessageDispatchRepository(pool)
	messageIngressLedger := repository.NewMessageIngressLedgerRepository(pool)
	auditRepo := repository.NewAuditRepository(pool)

	integrationRepo := repository.NewIntegrationRepository(pool, encryptor)
	chatwootMappingRepo := repository.NewChatwootMappingRepository(pool)
	var inboundRouter inbound.InboundRouter
	if cfg.IsDevelopment() {
		chatwootSyncer := chatwoot.NewChatwootSyncer(integrationRepo, chatwootMappingRepo, nil)
		typebotSessionRepo := repository.NewTypebotSessionRepository(pool)
		typebotForwarder := typebot.NewForwarder(typebotSessionRepo, integrationRepo, publisher)
		inboundRouter = inbound.NewDefaultRouter(chatwootSyncer, typebotForwarder)
	} else {
		slog.Info("Chatwoot and Typebot routing disabled outside development/test")
	}

	inboundProcessor := inbound.NewInboundProcessor(dedupRepo, wsRepo, mediaEngine, publisher, auditWriter, recipientSessionRepo, contactRepo, dispatchRepo, inboundRouter)
	sessionManager := session.NewManager(db, connectionRepo, sessionRegistry, dispatcherRegistry, "2.3000.1025000000", inboundProcessor)
	orchestrator := queue.NewDispatchOrchestrator(dispatcherRegistry, dispatchRepo, publisher, queueDepth, auditWriter, contactRepo, 5, 60*time.Second)
	orchestrator.SetContactRepository(contactRepo)
	campaignRepo := repository.NewCampaignRepository(pool)
	webhookSubRepo := repository.NewWebhookSubscriptionRepository(pool, encryptor)
	if !cfg.IsDevelopment() {
		if err := webhookSubRepo.RequireSecureActive(ctx); err != nil {
			slog.Error("webhook subscription security gate failed", "error", err)
			os.Exit(1)
		}
	}
	webhookDLQRepo := repository.NewWebhookDLQRepository(pool, encryptor)
	if err := webhookDLQRepo.RequireEncrypted(ctx); err != nil {
		slog.Error("webhook DLQ encryption gate failed", "error", err)
		os.Exit(1)
	}

	var worker *queue.Worker
	var campaignWorker *queue.CampaignWorker
	var webhookWorker *queue.WebhookWorker
	var deliveryReceiptRelay *inbound.DeliveryReceiptRelay
	if runsWorkers {
		stream, err := queue.BindVersionedStream(
			ctx,
			nc,
			queue.StreamName,
			queue.StreamSubject,
			cfg.NATSStreamReplicas,
		)
		if err != nil {
			slog.Error("failed to bind outbound stream", "error", err)
			os.Exit(1)
		}
		consumer, err := queue.BindConsumer(
			ctx,
			stream,
			queue.OutboundConsumerName,
			queue.StreamSubject,
		)
		if err != nil {
			slog.Error("failed to bind outbound consumer", "error", err)
			os.Exit(1)
		}
		worker = queue.NewWorker(ctx, consumer, orchestrator)
		slog.Info("message worker started", "consumer", queue.OutboundConsumerName)

		campStream, err := queue.BindVersionedStream(
			ctx,
			nc,
			queue.CampaignStreamName,
			queue.CampaignSubject,
			cfg.NATSStreamReplicas,
		)
		if err != nil {
			slog.Error("failed to bind campaign stream", "error", err)
			os.Exit(1)
		}
		campConsumer, err := queue.BindConsumer(
			ctx,
			campStream,
			queue.CampaignConsumerName,
			queue.CampaignSubject,
		)
		if err != nil {
			slog.Error("failed to bind campaign consumer", "error", err)
			os.Exit(1)
		}
		campaignWorker = queue.NewCampaignWorker(ctx, campConsumer, campaignRepo, connectionRepo, dispatchRepo, publisher)
		slog.Info("campaign worker started", "consumer", queue.CampaignConsumerName)

		var verbsEngine *webhook.VerbsEngine
		if cfg.IsDevelopment() {
			verbsEngine = webhook.NewVerbsEngine(publisher, contactRepo, userActionLogRepo, connectionRepo)
		} else {
			slog.Info("webhook response verbs disabled outside development/test")
		}
		webhookDispatcher := webhook.NewDefaultDispatcher(webhookSubRepo, webhookDLQRepo, wsRepo, nil, verbsEngine)
		webhookWorker, err = queue.NewWebhookWorker(ctx, nc, webhookDispatcher, webhookSubRepo, cfg.NATSStreamReplicas)
		if err != nil {
			slog.Error("failed to start webhook worker", "error", err)
			os.Exit(1)
		}
		webhookWorker.SetWorkspaceRepository(wsRepo)
		deliveryReceiptRelay = inbound.NewDeliveryReceiptRelay(ctx, dispatchRepo, publisher, time.Second)
	}

	slog.Info("rate limiter configured", "rps", 10, "burst", 10)
	slog.Info("durable workspace queue limit", "max_per_workspace", queue.MaxQueueDepth)
	if cfg.AdminPassword == "pergo-dev-2026" {
		slog.Warn("admin panel running with default password 'pergo-dev-2026'. Change PERGO_ADMIN_PASSWORD in production.")
	} else {
		slog.Info("admin panel password configured from environment")
	}

	// --- Debug server (pprof + expvar) ---
	debugSrv, err := obs.StartDebugServer(net.JoinHostPort("127.0.0.1", cfg.DebugPort))
	if err != nil {
		slog.Warn("debug server unavailable (port in use?), continuing without pprof",
			"addr", net.JoinHostPort("127.0.0.1", cfg.DebugPort),
			"error", err)
	} else {
		slog.Info("debug server started", "addr", debugSrv.Addr())
	}

	// --- Shutdown orchestrator ---
	orch := shutdown.NewOrchestrator()

	// Register cleanup in reverse order of startup. Producers are stopped and
	// awaited before the audit sink is closed.
	// Shutdown order: Echo → debug → sessions → workers → audit → NATS → pgxpool → sqlDB
	orch.Register(func() error {
		slog.Info("closing sql.DB")
		return db.Close()
	})
	orch.Register(func() error {
		slog.Info("closing pgxpool")
		pool.Close()
		return nil
	})
	orch.Register(func() error {
		slog.Info("closing NATS connection")
		nc.Close()
		return nil
	})
	orch.Register(func() error {
		slog.Info("flushing audit buffer")
		err := auditWriter.Close()
		slog.Info("audit buffer flushed")
		return err
	})
	if runsWorkers {
		orch.Register(func() error {
			slog.Info("stopping delivery receipt relay")
			deliveryReceiptRelay.Stop()
			return nil
		})
		// Worker shutdown runs before NATS close — drains the consumer.
		orch.Register(func() error {
			slog.Info("stopping webhook worker")
			webhookWorker.Stop()
			return nil
		})
		orch.Register(func() error {
			slog.Info("stopping campaign worker")
			campaignWorker.Stop()
			return nil
		})
		orch.Register(func() error {
			slog.Info("stopping message worker")
			worker.Stop()
			return nil
		})
		// Session manager stops all device sessions before worker drains.
		orch.Register(func() error {
			slog.Info("stopping all WhatsApp sessions")
			sessionManager.StopAll()
			return nil
		})
	}
	if debugSrv != nil {
		orch.Register(func() error {
			slog.Info("shutting down debug server")
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			return debugSrv.Shutdown(ctx)
		})
	}

	// --- Repositories ---
	apiKeyRepo := repository.NewAPIKeyRepository(pool)
	messageIdempotencyRepo := repository.NewMessageIdempotencyRepository(pool)
	wabaTemplateRepo := repository.NewWABATemplateRepository(pool)
	adminSessionRepo := repository.NewAdminSessionRepository(pool)

	wabaTemplateHandler := admin.NewWABATemplateHandler(wabaTemplateRepo, connectionRepo)
	wabaTemplateHandler.BaseURL = metaGraphBaseURL
	userLogsHandler := admin.NewUserLogsHandler(userActionLogRepo)
	chatwootAdminHandler := admin.NewChatwootAdminHandler(integrationRepo)
	typebotAdminHandler := admin.NewTypebotSettingsHandler(integrationRepo, connectionRepo)

	// --- Echo HTTP server ---
	e := echosrv.New()
	// Do not trust spoofable forwarding headers inside the process. Cloud Run
	// edge controls provide the shared client-aware rate limit; this local
	// limiter intentionally keys by the direct peer as a fail-closed fallback.
	e.IPExtractor = echo.ExtractIPDirect()
	e.Use(profileAccessMiddleware(cfg.RuntimeProfile))

	// Redirect /admin to /admin/ to prevent trailing-slash 404s
	e.GET("/admin", func(c *echo.Context) error {
		return c.Redirect(http.StatusMovedPermanently, "/admin/")
	})

	// Middleware stack: RequestID → Trace → Recover → Auth (on protected routes)
	e.Use(middleware.LocaleMiddleware())
	e.Use(middleware.TraceMiddleware())

	// Auth middleware — protects /api/* routes
	e.Use(middleware.AuthMiddleware(apiKeyRepo))

	// Audit middleware — audits API operations
	e.Use(middleware.AuditMiddleware(userActionLogRepo))

	// Health endpoints
	healthHandler := &handler.HealthHandler{
		Pool: pool,
		NATS: &natsConn{nc: nc},
	}
	healthHandler.RegisterRoutes(e)

	// --- Message handler (POST /messages) ---
	outboundProcessor := outbound.NewProcessor(queueDepth, mediaEngine, connectionRepo, publisher)
	messageHandler := &handler.MessageHandler{
		Ingestor:      outboundProcessor,
		IngressLedger: messageIngressLedger,
		Idempotency:   messageIdempotencyRepo,
	}
	messageHandler.RegisterRoutes(e, middleware.RateLimiterMiddleware(rateLimiter))

	// --- MCP Server (Model Context Protocol) ---
	mcpServer := mcp.NewServer(wsRepo, connectionRepo, contactRepo, auditRepo, outboundProcessor)
	e.Any("/api/mcp/*", echo.WrapHandler(mcpServer.SSEServer))

	// --- Media proxy handler (GET /media/:workspace_id/:hash) ---
	mediaHandler := handler.NewMediaHandler(s3Client)
	e.GET("/media/:workspace_id/:hash", mediaHandler.Handle)

	// --- Telegram Inbound Webhook handler ---
	telegramWebhookHandler := handler.NewTelegramWebhookHandler(connectionRepo, inboundProcessor, mediaEngine)
	e.POST("/webhooks/telegram/:workspace_id", telegramWebhookHandler.Handle)

	// --- WABA Inbound Webhook handler ---
	wabaWebhookHandler := handler.NewWABAWebhookHandler(connectionRepo, inboundProcessor, mediaEngine)
	wabaWebhookHandler.SetBaseURL(metaGraphBaseURL)
	e.GET("/webhooks/waba/:workspace_id", wabaWebhookHandler.HandleGet)
	e.POST("/webhooks/waba/:workspace_id", wabaWebhookHandler.HandlePost)

	if cfg.IsDevelopment() {
		// Chatwoot does not support a custom Authorization header on outgoing
		// webhooks. Keep both experimental integrations out of deployed
		// profiles until each has a dedicated, scoped credential protocol.
		chatwootWebhookHandler := handler.NewChatwootWebhookHandler(pool, chatwootMappingRepo, contactRepo, publisher)
		e.POST("/api/integrations/chatwoot", chatwootWebhookHandler.Handle)
		typebotWebhookHandler := handler.NewTypebotWebhookHandler(pool, publisher)
		e.POST("/api/integrations/typebot", typebotWebhookHandler.Handle)
	}

	// --- Landing Page ---
	e.POST("/locale", handler.LocalePost)
	e.GET("/", func(c *echo.Context) error {
		return middleware.Render(c, http.StatusOK, pages.Landing())
	})

	// --- Admin panel routes ---
	// Repositories for admin dashboard
	auditQuerier := audit.NewQuerier(pool)

	// Public admin routes (no session auth required)
	adminPublic := e.Group("/admin")
	adminPublic.Use(middleware.HTMLLocalizer())
	adminPublic.GET("/login", func(c *echo.Context) error {
		return admin.LoginPage(c, false)
	})
	adminPublic.GET("/login/", func(c *echo.Context) error {
		return admin.LoginPage(c, false)
	})
	adminPublic.POST("/login", func(c *echo.Context) error {
		return admin.LoginPost(c, adminSessionRepo, cfg.AdminPassword)
	}, middleware.IPRateLimiterMiddleware(loginRateLimiter))
	adminPublic.POST("/login/", func(c *echo.Context) error {
		return admin.LoginPost(c, adminSessionRepo, cfg.AdminPassword)
	}, middleware.IPRateLimiterMiddleware(loginRateLimiter))
	adminPublic.POST("/logout", func(c *echo.Context) error {
		return admin.Logout(c, adminSessionRepo)
	})

	// Protected admin routes (session auth required)
	adminGroup := e.Group("/admin")
	adminGroup.Use(middleware.HTMLLocalizer())
	adminGroup.Use(middleware.HTMXMiddleware())
	adminGroup.Use(middleware.SessionAuthMiddleware(adminSessionRepo))
	adminGroup.Use(middleware.DashboardAuditMiddleware(userActionLogRepo))
	adminGroup.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			ctx := c.Request().Context()
			ctx = context.WithValue(ctx, contextkey.ActivePath, c.Request().URL.Path)

			// Get active workspace and workspaces list to avoid HTMX reload flash
			var ws *repository.Workspace
			cookie, err := c.Cookie("pergo-active-workspace")
			if err == nil && cookie != nil && cookie.Value != "" {
				if wsID, parseErr := uuid.Parse(cookie.Value); parseErr == nil {
					ws, _ = wsRepo.GetByID(ctx, wsID)
				}
			}
			if ws == nil {
				list, err := wsRepo.List(ctx, 1)
				if err == nil && len(list) > 0 {
					ws = &list[0]
				}
			}
			workspaces, _ := wsRepo.List(ctx, 50)

			// Inject into context
			if ws != nil {
				ctx = context.WithValue(ctx, contextkey.ActiveWorkspace, ws)
			}
			if len(workspaces) > 0 {
				ctx = context.WithValue(ctx, contextkey.WorkspacesList, workspaces)
			}

			c.SetRequest(c.Request().WithContext(ctx))
			return next(c)
		}
	})

	// Admin dashboard
	dashboardHandler := &admin.DashboardHandler{
		Pool:        pool,
		Workspaces:  wsRepo,
		Audit:       auditQuerier,
		APIKeys:     apiKeyRepo,
		Connections: connectionRepo,
		Publisher:   publisher,
	}
	adminGroup.GET("/", dashboardHandler.Index)
	adminGroup.POST("/webhook/simulate", dashboardHandler.SimulateWebhook)
	adminGroup.POST("/workspaces/active", dashboardHandler.SelectWorkspace)
	adminGroup.GET("/workspaces/selector", dashboardHandler.WorkspaceSelector)

	// Workspace management routes
	workspaceHandler := &admin.WorkspaceHandler{
		Repo:        wsRepo,
		APIKeys:     apiKeyRepo,
		ExternalURL: cfg.ExternalURL,
	}
	adminGroup.GET("/workspaces", workspaceHandler.ActiveWorkspace)
	adminGroup.GET("/workspace", workspaceHandler.ActiveWorkspace)
	adminGroup.POST("/workspaces", workspaceHandler.Create)
	adminGroup.GET("/workspaces/new", func(c *echo.Context) error {
		return middleware.Render(c, http.StatusOK, pages.WorkspaceCreateForm())
	})
	adminGroup.GET("/workspaces/:id", workspaceHandler.Detail)
	adminGroup.GET("/workspace/:id", workspaceHandler.Detail)
	adminGroup.GET("/workspaces/:id/confirm-delete", workspaceHandler.ConfirmDelete)
	adminGroup.DELETE("/workspaces/:id", workspaceHandler.Delete)

	// API key management routes
	apiKeyHandler := &admin.APIKeyHandler{Repo: apiKeyRepo, Workspaces: wsRepo}
	adminGroup.GET("/workspaces/:id/keys", apiKeyHandler.List)
	adminGroup.POST("/workspaces/:id/keys", apiKeyHandler.Generate)
	adminGroup.GET("/workspaces/:id/keys/new", func(c *echo.Context) error {
		idStr, err := echo.PathParam[string](c, "id")
		if err != nil {
			return c.String(http.StatusBadRequest, "invalid workspace ID")
		}
		id, err := uuid.Parse(idStr)
		if err != nil {
			return c.String(http.StatusBadRequest, "invalid workspace ID")
		}
		return middleware.Render(c, http.StatusOK, pages.APIKeyGenerateForm(id))
	})
	adminGroup.GET("/workspaces/:id/keys/:key_id/confirm-revoke", apiKeyHandler.ConfirmRevoke)
	adminGroup.DELETE("/workspaces/:id/keys/:key_id", apiKeyHandler.Revoke)

	// Audit log review routes
	auditHandler := &admin.AuditHandler{Repo: auditRepo, Workspaces: wsRepo}
	adminGroup.GET("/audit", auditHandler.Redirect)
	adminGroup.GET("/logs", func(c *echo.Context) error {
		return c.Redirect(http.StatusMovedPermanently, "/admin/logs/outbound")
	})
	adminGroup.GET("/audit/inbound", auditHandler.ListInbound)
	adminGroup.GET("/logs/inbound", auditHandler.ListInbound)
	adminGroup.GET("/audit/inbound/export", auditHandler.ExportInboundCSV)
	adminGroup.GET("/logs/inbound/export", auditHandler.ExportInboundCSV)
	adminGroup.GET("/audit/outbound", auditHandler.ListOutbound)
	adminGroup.GET("/logs/outbound", auditHandler.ListOutbound)
	adminGroup.GET("/audit/outbound/export", auditHandler.ExportOutboundCSV)
	adminGroup.GET("/logs/outbound/export", auditHandler.ExportOutboundCSV)

	// Inbox routes
	inboxHandler := &admin.InboxHandler{
		Repo:                auditRepo,
		Sessions:            recipientSessionRepo,
		Workspaces:          wsRepo,
		Connections:         connectionRepo,
		Publisher:           publisher,
		Templates:           wabaTemplateRepo,
		ContactRepo:         contactRepo,
		UserActionLogs:      userActionLogRepo,
		InboundProcessor:    inboundProcessor,
		WhatsAppMockEnabled: cfg.WhatsAppMockEnabled,
	}
	adminGroup.GET("/inbox", inboxHandler.View)
	adminGroup.GET("/inbox/conversations/poll", inboxHandler.PollConversations)
	adminGroup.GET("/inbox/chat", inboxHandler.ChatPanel)
	adminGroup.GET("/inbox/messages", inboxHandler.PollMessages)
	adminGroup.POST("/inbox/send", inboxHandler.SendMessage)
	adminGroup.GET("/inbox/new-message-modal", inboxHandler.NewMessageModal)
	adminGroup.POST("/inbox/new-message-send", inboxHandler.NewMessageSend)
	adminGroup.GET("/inbox/mock-inbound-modal", inboxHandler.MockInboundModal)
	adminGroup.POST("/inbox/mock-inbound", inboxHandler.SimulateMockInbound)
	adminGroup.GET("/contacts/search", inboxHandler.SearchContacts)
	adminGroup.POST("/contacts/merge", inboxHandler.MergeContacts)
	adminGroup.POST("/contacts/:id/toggle-bot", inboxHandler.ToggleBot)

	// Device/Connection management routes
	deviceHandler := &admin.DeviceHandler{
		Sessions:              sessionRegistry,
		Manager:               sessionManager,
		SessionControlEnabled: runsWorkers && cfg.IsDevelopment(),
		Connections:           connectionRepo,
		Publisher:             publisher,
		NC:                    nc,
		TemplatesRepo:         wabaTemplateRepo,
		ExternalURL:           cfg.ExternalURL,
		WhatsAppMockEnabled:   cfg.WhatsAppMockEnabled,
		MetaGraphBaseURL:      metaGraphBaseURL,
	}
	adminGroup.GET("/devices", deviceHandler.List)
	adminGroup.GET("/connections", deviceHandler.List)
	adminGroup.GET("/devices/pair-form", deviceHandler.PairForm)
	adminGroup.POST("/devices/pair", deviceHandler.StartPairing)
	adminGroup.GET("/devices/qr", deviceHandler.GetQR)
	adminGroup.DELETE("/devices/:id", deviceHandler.Disconnect)
	adminGroup.POST("/devices/create", deviceHandler.Create)
	adminGroup.GET("/devices/test", deviceHandler.TestForm)
	adminGroup.POST("/devices/test", deviceHandler.RunTest)
	if cfg.IsDevelopment() {
		adminGroup.GET("/devices/test/ws", deviceHandler.WS)
	}

	// Telemetry page (system health: sessions, NATS, uptime)
	telemetryHandler := &admin.TelemetryHandler{
		Manager:    sessionManager,
		Sessions:   sessionRegistry,
		QueueDepth: queueDepth,
		NC:         &natsConn{nc: nc},
		StartTime:  time.Now(),
	}
	adminGroup.GET("/telemetry", telemetryHandler.Index)

	// WABA template routes
	adminGroup.GET("/workspaces/:workspace_id/templates", wabaTemplateHandler.List)
	adminGroup.POST("/workspaces/:workspace_id/templates", wabaTemplateHandler.Create)
	adminGroup.GET("/workspaces/:workspace_id/templates/new", wabaTemplateHandler.NewForm)
	adminGroup.POST("/workspaces/:workspace_id/templates/:template_id/sync", wabaTemplateHandler.Sync)
	adminGroup.DELETE("/workspaces/:workspace_id/templates/:template_id", wabaTemplateHandler.Delete)

	if cfg.IsDevelopment() {
		adminGroup.GET("/workspaces/:workspace_id/integrations/chatwoot", chatwootAdminHandler.GetSettings)
		adminGroup.POST("/workspaces/:workspace_id/integrations/chatwoot", chatwootAdminHandler.PostSettings)
		adminGroup.GET("/workspaces/:workspace_id/integrations/typebot", typebotAdminHandler.GetSettings)
		adminGroup.POST("/workspaces/:workspace_id/integrations/typebot", typebotAdminHandler.PostSettings)
	}

	// Webhooks & DLQ routes
	webhookHandler := admin.NewWebhookDLQHandler(webhookDLQRepo, webhookSubRepo, wsRepo, publisher)
	adminGroup.GET("/webhooks", webhookHandler.GlobalPage)
	adminGroup.GET("/webhooks/dlq/badge", webhookHandler.GetBadgeCount)
	adminGroup.GET("/webhooks/dlq/:dlq_id/details", webhookHandler.GetDetails)
	adminGroup.POST("/webhooks/dlq/:dlq_id/retry", webhookHandler.RetryDLQ)
	adminGroup.DELETE("/webhooks/dlq/:dlq_id", webhookHandler.DeleteDLQ)

	// User action logs routes
	adminGroup.GET("/logs/actions", userLogsHandler.List)
	adminGroup.GET("/logs/actions/:id/metadata", userLogsHandler.GetMetadata)

	adminGroup.GET("/workspaces/:workspace_id/webhooks", webhookHandler.Page)
	adminGroup.GET("/workspaces/:workspace_id/webhooks/subscriptions/new", webhookHandler.GetSubscriptionNewForm)
	adminGroup.GET("/workspaces/:workspace_id/webhooks/subscriptions/:subscription_id/edit", webhookHandler.GetSubscriptionEditForm)
	adminGroup.POST("/workspaces/:workspace_id/webhooks/subscriptions", webhookHandler.CreateSubscription)
	adminGroup.POST("/workspaces/:workspace_id/webhooks/subscriptions/:subscription_id", webhookHandler.UpdateSubscription)
	adminGroup.DELETE("/workspaces/:workspace_id/webhooks/subscriptions/:subscription_id", webhookHandler.DeleteSubscription)
	adminGroup.GET("/workspaces/:workspace_id/webhooks/subscriptions/:subscription_id/test-form", webhookHandler.GetSubscriptionTestForm)
	adminGroup.POST("/workspaces/:workspace_id/webhooks/subscriptions/:subscription_id/test", webhookHandler.TestSubscription)

	// Campaigns routes
	campaignHandler := admin.NewCampaignHandler(campaignRepo, wabaTemplateRepo, connectionRepo, publisher)
	adminGroup.GET("/campaigns", func(c *echo.Context) error {
		ctx := c.Request().Context()
		cookie, err := c.Cookie("pergo-active-workspace")
		var wsID uuid.UUID
		if err == nil && cookie != nil && cookie.Value != "" {
			wsID, _ = uuid.Parse(cookie.Value)
		}
		if wsID == uuid.Nil {
			list, err := wsRepo.List(ctx, 1)
			if err == nil && len(list) > 0 {
				wsID = list[0].ID
			}
		}
		if wsID == uuid.Nil {
			return c.String(http.StatusBadRequest, "nenhum workspace encontrado. Crie um workspace primeiro.")
		}
		return c.Redirect(http.StatusFound, fmt.Sprintf("/admin/workspaces/%s/campaigns", wsID.String()))
	})
	adminGroup.GET("/workspaces/:workspace_id/campaigns", campaignHandler.List)
	adminGroup.GET("/workspaces/:workspace_id/campaigns/new", campaignHandler.NewForm)
	adminGroup.POST("/workspaces/:workspace_id/campaigns/upload", campaignHandler.UploadCSV)
	adminGroup.POST("/workspaces/:workspace_id/campaigns", campaignHandler.Create)
	adminGroup.GET("/workspaces/:workspace_id/campaigns/:id/skipped/download", campaignHandler.DownloadSkipped)
	adminGroup.POST("/workspaces/:workspace_id/campaigns/:id/start", campaignHandler.Start)
	adminGroup.POST("/workspaces/:workspace_id/campaigns/:id/cancel", campaignHandler.Cancel)
	adminGroup.DELETE("/workspaces/:workspace_id/campaigns/:id", campaignHandler.Delete)

	// Static files
	e.Static("/static", "static")

	// Test route: GET /api/v1/me (returns workspace_id from auth context)
	e.GET("/api/v1/me", func(c *echo.Context) error {
		wsID, ok := tenant.WorkspaceIDFrom(c.Request().Context())
		if !ok {
			return c.String(http.StatusUnauthorized, "missing workspace context")
		}
		return c.JSON(http.StatusOK, map[string]string{
			"workspace_id": wsID.String(),
		})
	})

	// Start HTTP server
	srv := newHTTPServer(net.JoinHostPort("", cfg.ServerPort), e)

	runsHTTP := profileRunsHTTP(cfg.RuntimeProfile)
	if runsHTTP {
		// Register Echo shutdown in orchestrator (runs first — stops accepting new requests).
		orch.Register(func() error {
			slog.Info("shutting down HTTP server")
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			return srv.Shutdown(ctx)
		})

		go func() {
			slog.Info("starting server", "port", cfg.ServerPort, "profile", cfg.RuntimeProfile)
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("server error", "error", err)
				os.Exit(1)
			}
		}()
	}
	if runsWorkers && cfg.IsDevelopment() {
		// Restore persisted WhatsApp Web sessions only in worker-capable profiles.
		startWhatsAppRestoration(ctx, sessionManager)
	}

	// --- Graceful shutdown on SIGTERM/SIGINT ---
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	slog.Info("received shutdown signal", "signal", sig)
	managedShutdown = true

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer shutdownCancel()

	if err := orch.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown error", "error", err)
	}

	cancel() // signal background goroutines to exit
	slog.Info("server stopped")
}

// --- helpers ---

// newHTTPServer applies the same bounded header/body-read and idle policies to
// every HTTP-serving runtime profile. WriteTimeout intentionally remains zero:
// admin endpoints may stream responses and are instead bounded by their own
// request contexts.
func newHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
}

// runServer blocks until ctx is cancelled. Used by tests to simulate server lifecycle.
func runServer(ctx context.Context, pool *pgxpool.Pool, db *sql.DB) error {
	_ = pool
	_ = db
	<-ctx.Done()
	return nil
}

// startWhatsAppRestoration is the startup hook for persisted WhatsApp Web
// sessions. Keeping it separate makes the production bootstrap testable with
// a fake client factory and a real connections repository.
func startWhatsAppRestoration(ctx context.Context, manager *session.Manager) {
	go func() {
		if err := manager.ReconnectAll(ctx); err != nil && ctx.Err() == nil {
			slog.Error("failed to restore WhatsApp sessions", "error", err)
		}
	}()
}

func resolvedKEK(cfg *config.Config) []byte {
	if len(cfg.KEKBytes) == 32 {
		return cfg.KEKBytes
	}
	// Config validation guarantees this branch is development/test only.
	kek := make([]byte, 32)
	copy(kek, []byte("dev-development-key-32-bytes-kek"))
	return kek
}

func applyRuntimeProfileArgument(cfg *config.Config, args []string) error {
	if len(args) == 0 {
		return nil
	}
	if len(args) != 1 {
		return fmt.Errorf("usage: pergo [api|webhook|worker|migrate|mcp]")
	}
	switch args[0] {
	case config.RuntimeAPI, config.RuntimeWebhook, config.RuntimeWorker, config.RuntimeMigrate:
		cfg.RuntimeProfile = args[0]
		return nil
	default:
		return fmt.Errorf("unknown runtime profile %q", args[0])
	}
}

func profileRunsHTTP(profile string) bool {
	return profile == config.RuntimeAll ||
		profile == config.RuntimeAPI ||
		profile == config.RuntimeWebhook
}

func profileRunsWorkers(profile string) bool {
	return profile == config.RuntimeAll || profile == config.RuntimeWorker
}

func connectConfiguredNATS(cfg *config.Config) (*nats.Conn, error) {
	return queue.Connect(queue.ConnectionConfig{
		URLs:            cfg.NATSURLs,
		CredentialsFile: cfg.NATSCredentialsFile,
		CAFile:          cfg.NATSCAFile,
		TLSServerName:   cfg.NATSTLSServerName,
	})
}

func profileAccessMiddleware(profile string) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			path := c.Request().URL.Path
			isHealth := path == "/healthz" || path == "/readyz"
			isWebhook := strings.HasPrefix(path, "/webhooks/") ||
				path == "/api/integrations/chatwoot" ||
				path == "/api/integrations/typebot"

			switch profile {
			case config.RuntimeAll:
				return next(c)
			case config.RuntimeAPI:
				if !isWebhook {
					return next(c)
				}
			case config.RuntimeWebhook:
				if isHealth || isWebhook {
					return next(c)
				}
			}
			return c.NoContent(http.StatusNotFound)
		}
	}
}

// natsConn wraps *nats.Conn to satisfy the handler.NATSConn interface.
type natsConn struct {
	nc *nats.Conn
}

func (c *natsConn) Ping() error {
	if !c.nc.IsConnected() {
		return fmt.Errorf("nats not connected")
	}
	return nil
}

// IsConnected returns true if the NATS connection is active.
// Satisfies admin.NATSStatus for the TelemetryHandler.
func (c *natsConn) IsConnected() bool {
	return c.nc.IsConnected()
}
