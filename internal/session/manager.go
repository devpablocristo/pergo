package session

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"

	"github.com/google/uuid"

	"github.com/pablojhp.pergo/internal/channel"
	whatsapp "github.com/pablojhp.pergo/internal/channel/whatsapp"
	"github.com/pablojhp.pergo/internal/inbound"
	"github.com/pablojhp.pergo/internal/repository"
)

const (
	// maxConcurrentReconnect limits how many devices reconnect simultaneously
	// on startup to prevent storming WhatsApp servers.
	maxConcurrentReconnect = 5

	// defaultReconnectBackoff is the base backoff for reconnection attempts.
	defaultReconnectBackoff = 5 * time.Second

	// maxReconnectBackoff caps the exponential backoff.
	maxReconnectBackoff = 5 * time.Minute
)

// Manager coordinates WhatsApp device lifecycle: startup reconnection,
// session registration, and graceful shutdown.
type Manager struct {
	db               *sql.DB
	repo             connectionStore
	registry         *ActiveSession
	dispatchers      *channel.Registry
	waVersion        string
	inboundProcessor *inbound.InboundProcessor
	clientFactory    whatsapp.ClientFactory
	reconnect        func(context.Context, *repository.Connection) error
	wait             func(context.Context, time.Duration) bool
	initialJitter    func() time.Duration
	reconnectMu      sync.Mutex
	reconnectCancel  context.CancelFunc
}

type connectionStore interface {
	Create(context.Context, *repository.Connection) error
	GetByID(context.Context, uuid.UUID) (*repository.Connection, error)
	ListAll(context.Context) ([]*repository.Connection, error)
	ListByWorkspace(context.Context, uuid.UUID) ([]*repository.Connection, error)
	UpdateStatus(context.Context, uuid.UUID, string) error
}

// ReconnectOption configures reconnection behavior. Production uses the
// defaults; tests can provide a deterministic clock without sleeping.
type ReconnectOption func(*Manager)

// WithReconnectTiming replaces the wait and startup-jitter functions. Nil
// functions retain their production defaults.
func WithReconnectTiming(wait func(context.Context, time.Duration) bool, initialJitter func() time.Duration) ReconnectOption {
	return func(m *Manager) {
		if wait != nil {
			m.wait = wait
		}
		if initialJitter != nil {
			m.initialJitter = initialJitter
		}
	}
}

// NewManager creates a session manager.
func NewManager(
	db *sql.DB,
	repo *repository.ConnectionRepository,
	registry *ActiveSession,
	dispatchers *channel.Registry,
	waVersion string,
	inboundProcessor *inbound.InboundProcessor,
) *Manager {
	return NewManagerWithClientFactory(db, repo, registry, dispatchers, waVersion, inboundProcessor, whatsapp.NewClientFactory())
}

// NewManagerWithClientFactory creates a manager with an explicit client
// factory. It is used by process-level tests to restore persisted sessions
// without contacting WhatsApp.
func NewManagerWithClientFactory(
	db *sql.DB,
	repo *repository.ConnectionRepository,
	registry *ActiveSession,
	dispatchers *channel.Registry,
	waVersion string,
	inboundProcessor *inbound.InboundProcessor,
	clientFactory whatsapp.ClientFactory,
	options ...ReconnectOption,
) *Manager {
	if clientFactory == nil {
		clientFactory = whatsapp.NewClientFactory()
	}
	m := &Manager{
		db:               db,
		repo:             repo,
		registry:         registry,
		dispatchers:      dispatchers,
		waVersion:        waVersion,
		inboundProcessor: inboundProcessor,
		clientFactory:    clientFactory,
	}
	m.reconnect = m.reconnectDevice
	m.wait = waitForReconnect
	m.initialJitter = func() time.Duration {
		return time.Duration(rand.Int64N(int64(defaultReconnectBackoff)))
	}
	for _, option := range options {
		if option != nil {
			option(m)
		}
	}
	return m
}

// ReconnectAll reconnects all known devices from the database with
// backoff and storm protection (semaphore cap).
// It blocks until all reconnection attempts complete or ctx is cancelled.
func (m *Manager) ReconnectAll(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	m.reconnectMu.Lock()
	m.reconnectCancel = cancel
	m.reconnectMu.Unlock()
	defer func() {
		m.reconnectMu.Lock()
		if m.reconnectCancel != nil {
			m.reconnectCancel = nil
		}
		m.reconnectMu.Unlock()
	}()

	allConns, err := m.repo.ListAll(ctx)
	if err != nil {
		return fmt.Errorf("session manager: list connections: %w", err)
	}

	var devices []*repository.Connection
	for _, conn := range allConns {
		jid, parseErr := parseJID(derefJID(conn.JID))
		if conn.Channel == "whatsapp" && parseErr == nil && !jid.IsEmpty() && conn.Status != string(DeviceStatusTerminal) && m.registry.Get(jid) == nil {
			devices = append(devices, conn)
		}
	}

	slog.Info("session manager: reconnecting devices", "count", len(devices))

	// Semaphore limits concurrent reconnections
	sem := make(chan struct{}, maxConcurrentReconnect)
	var wg sync.WaitGroup

	for _, d := range devices {
		wg.Add(1)
		go func(d *repository.Connection) {
			defer wg.Done()
			if !m.wait(ctx, m.initialJitter()) {
				return
			}

			for attempt := 0; ctx.Err() == nil; attempt++ {
				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					return
				}
				err := m.reconnect(ctx, d)
				<-sem
				if err == nil {
					return
				}
				slog.Error("session manager: failed to reconnect device",
					"error", err,
					"device_id", d.ID,
				)
				if updateErr := m.repo.UpdateStatus(ctx, d.ID, string(DeviceStatusDisconnected)); updateErr != nil && ctx.Err() == nil {
					slog.Error("session manager: failed to mark device disconnected", "error", updateErr, "device_id", d.ID)
				}
				if !m.wait(ctx, calcBackoff(attempt)) {
					return
				}
			}
		}(d)
	}

	wg.Wait()
	slog.Info("session manager: reconnection complete",
		"reconnected", m.registry.Len(),
	)
	return nil
}

// reconnectDevice creates a whatsmeow client for a persisted device and
// attempts to connect. On success, it registers the session and dispatcher.
func (m *Manager) reconnectDevice(ctx context.Context, d *repository.Connection) error {
	slog.Info("session manager: reconnecting device",
		"device_id", d.ID,
	)

	jid, err := parseJID(*d.JID)
	if err != nil {
		return fmt.Errorf("parse JID: %w", err)
	}
	if m.registry.Get(jid) != nil {
		return nil
	}

	cfg := whatsapp.ClientConfig{
		DB:        m.db,
		WAVersion: m.waVersion,
	}
	if d.ProxyURL != nil {
		cfg.ProxyURL = *d.ProxyURL
	}

	wc, err := m.clientFactory.New(cfg)
	if err != nil {
		return fmt.Errorf("create whatsapp client: %w", err)
	}

	// Set the JID from the persisted device record.
	wc.SetJID(jid)

	if err := wc.Connect(); err != nil {
		wc.Disconnect()
		return fmt.Errorf("connect whatsapp client: %w", err)
	}

	// Create session with cancelable context only after a successful connection.
	sessionCtx, cancel := context.WithCancel(ctx)

	sess := &Session{
		DeviceID: d.ID.String(),
		JID:      jid,
		Client:   wc,
		Cancel:   cancel,
	}

	// Register session atomically
	m.registry.Add(sess)

	// Register event handler to update recipient_sessions on incoming messages
	wc.AddEventHandler(func(evt interface{}) {
		switch v := evt.(type) {
		case *waEvents.LoggedOut:
			slog.Warn("session manager: whatsmeow logged out event received, marking device terminal", "device_id", d.ID)
			if err := m.repo.UpdateStatus(context.Background(), d.ID, string(DeviceStatusTerminal)); err != nil {
				slog.Error("session manager: failed to mark device terminal", "error", err, "device_id", d.ID)
			}
			cancel()
		case *waEvents.Message:
			if v.Info.IsFromMe {
				return
			}

			senderJID := v.Info.Sender.String()
			ctxBg := context.Background()

			// Download media from WhatsApp CDN (needs active whatsmeow client)
			var mediaBytes []byte
			var mediaType string
			var mediaFilename string
			var mediaCaption string
			hasMedia := false

			if imageMsg := v.Message.GetImageMessage(); imageMsg != nil {
				data, err := wc.Download(ctxBg, imageMsg)
				if err == nil {
					mediaBytes = data
				}
				mediaType = "image"
				hasMedia = true
				if imageMsg.Caption != nil {
					mediaCaption = *imageMsg.Caption
				}
			} else if docMsg := v.Message.GetDocumentMessage(); docMsg != nil {
				data, err := wc.Download(ctxBg, docMsg)
				if err == nil {
					mediaBytes = data
				}
				mediaType = "document"
				hasMedia = true
				if docMsg.FileName != nil {
					mediaFilename = *docMsg.FileName
				}
				if docMsg.Caption != nil {
					mediaCaption = *docMsg.Caption
				}
			} else if audioMsg := v.Message.GetAudioMessage(); audioMsg != nil {
				data, err := wc.Download(ctxBg, audioMsg)
				if err == nil {
					mediaBytes = data
				}
				mediaType = "audio"
				hasMedia = true
			} else if videoMsg := v.Message.GetVideoMessage(); videoMsg != nil {
				data, err := wc.Download(ctxBg, videoMsg)
				if err == nil {
					mediaBytes = data
				}
				mediaType = "video"
				hasMedia = true
				if videoMsg.Caption != nil {
					mediaCaption = *videoMsg.Caption
				}
			}

			// Delegate to processor
			if m.inboundProcessor != nil {
				recipientIdentity := d.SenderIdentity
				if recipientIdentity == "" && d.JID != nil {
					recipientIdentity = *d.JID
				}

				var inboundMedia *inbound.InboundMedia
				if hasMedia {
					inboundMedia = &inbound.InboundMedia{
						Bytes:     mediaBytes,
						MediaType: mediaType,
						Filename:  mediaFilename,
						Caption:   mediaCaption,
					}
				}

				var inboundLocation *inbound.InboundLocation
				if locMsg := v.Message.GetLocationMessage(); locMsg != nil {
					inboundLocation = &inbound.InboundLocation{
						Latitude:  *locMsg.DegreesLatitude,
						Longitude: *locMsg.DegreesLongitude,
						Name:      locMsg.GetName(),
						Address:   locMsg.GetAddress(),
					}
				}

				var inboundContacts []inbound.InboundContact
				if contactMsg := v.Message.GetContactMessage(); contactMsg != nil {
					inboundContacts = append(inboundContacts, inbound.InboundContact{
						Name:  contactMsg.GetDisplayName(),
						Phone: contactMsg.GetVcard(),
					})
				}

				event := &inbound.InboundEvent{
					WorkspaceID:  d.WorkspaceID,
					ConnectionID: d.ID,
					MessageID:    v.Info.ID,
					Channel:      "whatsapp",
					From:         senderJID,
					To:           recipientIdentity,
					Body:         extractWhatsAppBody(v),
					Media:        inboundMedia,
					Location:     inboundLocation,
					Contacts:     inboundContacts,
				}

				_ = m.inboundProcessor.Process(ctxBg, event)
			}
		}
	})

	if err := m.repo.UpdateStatus(ctx, d.ID, string(DeviceStatusConnected)); err != nil {
		cancel()
		wc.Disconnect()
		m.registry.Remove(jid)
		return fmt.Errorf("mark device connected: %w", err)
	}

	// Wait for shutdown in a dedicated goroutine.
	go func() {
		wc.Wait(sessionCtx)
		// Update status when goroutine exits
		if err := m.repo.UpdateStatus(context.Background(), d.ID, string(DeviceStatusDisconnected)); err != nil {
			slog.Error("session manager: failed to mark stopped device disconnected", "error", err, "device_id", d.ID)
		}
		m.registry.Remove(jid)
	}()

	return nil
}

func waitForReconnect(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func derefJID(jid *string) string {
	if jid == nil {
		return ""
	}
	return *jid
}

// parseJID is a helper that parses a JID string.
func parseJID(jid string) (types.JID, error) {
	parsed, err := types.ParseJID(jid)
	if err != nil {
		return types.JID{}, err
	}
	return parsed, nil
}

// StopAll gracefully stops all active sessions.
func (m *Manager) StopAll() {
	slog.Info("session manager: stopping all sessions", "count", m.registry.Len())
	m.reconnectMu.Lock()
	cancel := m.reconnectCancel
	m.reconnectMu.Unlock()
	if cancel != nil {
		cancel()
	}
	m.registry.StopAll()
}

// ActiveDevices returns a snapshot of all active sessions.
func (m *Manager) ActiveDevices() []*Session {
	return m.registry.All()
}

// calcBackoff computes exponential backoff with jitter.
func calcBackoff(attempt int) time.Duration {
	backoff := float64(defaultReconnectBackoff) * math.Pow(2, float64(attempt))
	if backoff > float64(maxReconnectBackoff) {
		backoff = float64(maxReconnectBackoff)
	}
	// Add 10% jitter
	jitter := backoff * 0.1 * (rand.Float64()*2 - 1)
	return time.Duration(backoff + jitter)
}

// extractWhatsAppBody pulls the human-readable text from a WhatsApp message.
func extractWhatsAppBody(v *waEvents.Message) string {
	if msgText := v.Message.GetConversation(); msgText != "" {
		return msgText
	}
	if extText := v.Message.GetExtendedTextMessage().GetText(); extText != "" {
		return extText
	}
	if imageMsg := v.Message.GetImageMessage(); imageMsg != nil && imageMsg.Caption != nil {
		return *imageMsg.Caption
	}
	if documentMsg := v.Message.GetDocumentMessage(); documentMsg != nil && documentMsg.Caption != nil {
		return *documentMsg.Caption
	}
	if videoMsg := v.Message.GetVideoMessage(); videoMsg != nil && videoMsg.Caption != nil {
		return *videoMsg.Caption
	}
	return ""
}
