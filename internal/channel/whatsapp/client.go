package whatsapp

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// ClientConfig holds configuration for creating a WhatsApp client.
type ClientConfig struct {
	DB        *sql.DB
	WAVersion string // e.g. "2.3000.1025000000"
	JID       *types.JID
	ProxyURL  string
}

// Client is the lifecycle surface used by the session manager. Keeping this
// small interface in the WhatsApp package lets startup restoration be tested
// with an in-memory client instead of opening a connection to WhatsApp.
type Client interface {
	JID() types.JID
	SetJID(types.JID)
	Run(context.Context) error
	Wait(context.Context)
	GetQRChannel(context.Context) (<-chan whatsmeow.QRChannelItem, error)
	Connect() error
	Disconnect()
	AddEventHandler(whatsmeow.EventHandler) uint32
	Download(context.Context, whatsmeow.DownloadableMessage) ([]byte, error)
}

// ClientFactory creates clients for pairing and persisted-session restoration.
// Production uses NewClientFactory; tests can provide a deterministic fake.
type ClientFactory interface {
	New(ClientConfig) (Client, error)
}

// ClientFactoryFunc adapts a function into a ClientFactory.
type ClientFactoryFunc func(ClientConfig) (Client, error)

func (f ClientFactoryFunc) New(cfg ClientConfig) (Client, error) {
	return f(cfg)
}

// NewClientFactory returns the production whatsmeow-backed client factory.
func NewClientFactory() ClientFactory {
	return ClientFactoryFunc(func(cfg ClientConfig) (Client, error) {
		return NewWhatsAppClient(cfg)
	})
}

// WhatsAppClient wraps a whatsmeow client with event handlers and lifecycle
// management. It provides the Run/Stop goroutine pattern for per-device
// sessions.
type WhatsAppClient struct {
	client *whatsmeow.Client
	jid    types.JID
	log    *slog.Logger
}

// NewWhatsAppClient creates a whatsmeow client with PostgreSQL-backed
// device store. The JID is empty until pairing completes.
func NewWhatsAppClient(cfg ClientConfig) (*WhatsAppClient, error) {
	container := sqlstore.NewWithDB(cfg.DB, "postgres", waLog.Noop)

	var deviceStore *store.Device
	var err error
	if cfg.JID != nil && !cfg.JID.IsEmpty() {
		deviceStore, err = container.GetDevice(context.Background(), *cfg.JID)
	} else {
		deviceStore, err = container.GetFirstDevice(context.Background())
	}
	if err != nil || deviceStore == nil {
		deviceStore = container.NewDevice()
	}

	clientLog := slog.With("component", "whatsapp")

	cli := whatsmeow.NewClient(deviceStore, waLog.Noop)

	if cfg.ProxyURL != "" {
		if err := ConfigureProxy(cli, cfg.ProxyURL); err != nil {
			clientLog.Warn("whatsapp: failed to configure proxy", "url", cfg.ProxyURL, "error", err)
		}
	}

	if cfg.WAVersion != "" {
		if ver, err := store.ParseVersion(cfg.WAVersion); err == nil {
			store.SetWAVersion(ver)
		} else {
			clientLog.Warn("whatsapp: failed to parse WA version", "version", cfg.WAVersion, "error", err)
		}
	}

	wc := &WhatsAppClient{
		client: cli,
		log:    clientLog,
	}

	wc.setupEventHandlers()

	return wc, nil
}

// JID returns the device's JID after pairing. Empty before pairing.
func (wc *WhatsAppClient) JID() types.JID {
	return wc.jid
}

// Client returns the underlying whatsmeow client.
func (wc *WhatsAppClient) Client() *whatsmeow.Client {
	return wc.client
}

// SetJID sets the device JID after pairing.
func (wc *WhatsAppClient) SetJID(jid types.JID) {
	wc.jid = jid
}

// DeviceStore returns the underlying device store for persistence.
func (wc *WhatsAppClient) DeviceStore() *store.Device {
	if wc.client != nil {
		return wc.client.Store
	}
	return nil
}

// setupEventHandlers registers handlers for whatsmeow lifecycle events.
func (wc *WhatsAppClient) setupEventHandlers() {
	wc.client.AddEventHandler(func(evt interface{}) {
		switch v := evt.(type) {
		case *waEvents.LoggedOut:
			wc.log.Warn("whatsapp: logged out",
				"on_connect", v.OnConnect,
				"jid", wc.jid.String(),
			)
		case *waEvents.ClientOutdated:
			wc.log.Warn("whatsapp: client outdated, auto-updating WA version and reconnecting")
			curVer := store.GetWAVersion()
			curVer[2]++ // increment patch
			store.SetWAVersion(curVer)
			go func() {
				wc.client.Disconnect()
				if err := wc.client.Connect(); err != nil {
					wc.log.Error("whatsapp: failed to reconnect after client outdated update", "error", err)
				}
			}()
		case *waEvents.Connected:
			wc.log.Info("whatsapp: connected",
				"jid", wc.jid.String(),
			)
		case *waEvents.Disconnected:
			wc.log.Warn("whatsapp: disconnected",
				"jid", wc.jid.String(),
			)
		}
	})
}

// Run connects the client and blocks until ctx is cancelled.
func (wc *WhatsAppClient) Run(ctx context.Context) error {
	if err := wc.Connect(); err != nil {
		return fmt.Errorf("whatsapp connect: %w", err)
	}

	wc.log.Info("whatsapp: client running", "jid", wc.jid.String())
	wc.Wait(ctx)
	return nil
}

// Wait blocks until ctx is cancelled, then disconnects the client.
// It is used after a synchronous Connect when restoring persisted sessions.
func (wc *WhatsAppClient) Wait(ctx context.Context) {
	<-ctx.Done()
	wc.Disconnect()
}

// GetQRChannel returns the QR code channel for pairing a new device.
// Must be called BEFORE Connect() per whatsmeow API contract.
func (wc *WhatsAppClient) GetQRChannel(ctx context.Context) (<-chan whatsmeow.QRChannelItem, error) {
	return wc.client.GetQRChannel(ctx)
}

// Connect connects the client to WhatsApp WebSocket. For pairing flows,
// call GetQRChannel first, then Connect.
func (wc *WhatsAppClient) Connect() error {
	return wc.client.Connect()
}

// Disconnect disconnects from the WhatsApp WebSocket.
func (wc *WhatsAppClient) Disconnect() {
	wc.client.Disconnect()
}

// AddEventHandler delegates to whatsmeow while keeping the session manager
// independent from the concrete client implementation.
func (wc *WhatsAppClient) AddEventHandler(handler whatsmeow.EventHandler) uint32 {
	return wc.client.AddEventHandler(handler)
}

// Download delegates media retrieval to whatsmeow.
func (wc *WhatsAppClient) Download(ctx context.Context, msg whatsmeow.DownloadableMessage) ([]byte, error) {
	return wc.client.Download(ctx, msg)
}

// ConfigureProxy sets up SOCKS5/HTTP proxy for whatsmeow client connection.
func ConfigureProxy(client *whatsmeow.Client, proxyStr string) error {
	if proxyStr == "" {
		client.SetProxy(nil)
		return nil
	}
	return client.SetProxyAddress(proxyStr)
}
