package handler

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/pablojhp.pergo/internal/api/middleware"
	"github.com/pablojhp.pergo/internal/channel/whatsapp"
	"github.com/pablojhp.pergo/internal/media"
	"github.com/pablojhp.pergo/internal/platform/storage"
	"github.com/pablojhp.pergo/internal/repository"
)

type staticWABAConnectionStore struct {
	connections []*repository.Connection
}

func (s staticWABAConnectionStore) ListByWorkspace(context.Context, uuid.UUID) ([]*repository.Connection, error) {
	return s.connections, nil
}

func TestValidWABASignature(t *testing.T) {
	payload := []byte(`{"object":"whatsapp_business_account"}`)
	const appSecret = "tenant-specific-meta-app-secret"
	valid := signedWABAHeader(payload, appSecret)

	tests := []struct {
		name      string
		body      []byte
		signature string
		secret    string
		want      bool
	}{
		{name: "valid", body: payload, signature: valid, secret: appSecret, want: true},
		{name: "missing header", body: payload, secret: appSecret},
		{name: "wrong prefix", body: payload, signature: "sha1=deadbeef", secret: appSecret},
		{name: "malformed hex", body: payload, signature: "sha256=not-hex", secret: appSecret},
		{name: "wrong secret", body: payload, signature: valid, secret: "other-secret"},
		{name: "tampered payload", body: append(payload, '\n'), signature: valid, secret: appSecret},
		{name: "empty app secret", body: payload, signature: valid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := validWABASignature(tt.body, tt.signature, tt.secret); got != tt.want {
				t.Fatalf("validWABASignature() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractWABAPhoneNumberID(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    string
		wantErr bool
	}{
		{
			name:    "single identity",
			payload: `{"entry":[{"changes":[{"value":{"metadata":{"phone_number_id":"phone-b"}}}]}]}`,
			want:    "phone-b",
		},
		{
			name:    "same identity repeated",
			payload: `{"entry":[{"changes":[{"value":{"metadata":{"phone_number_id":"phone-b"}}},{"value":{"metadata":{"phone_number_id":"phone-b"}}}]}]}`,
			want:    "phone-b",
		},
		{
			name:    "missing identity",
			payload: `{"entry":[{"changes":[{"value":{"metadata":{}}}]}]}`,
			wantErr: true,
		},
		{
			name:    "mixed identities rejected",
			payload: `{"entry":[{"changes":[{"value":{"metadata":{"phone_number_id":"phone-a"}}},{"value":{"metadata":{"phone_number_id":"phone-b"}}}]}]}`,
			wantErr: true,
		},
		{name: "invalid json", payload: `{`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractWABAPhoneNumberID([]byte(tt.payload))
			if (err != nil) != tt.wantErr {
				t.Fatalf("extractWABAPhoneNumberID() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("extractWABAPhoneNumberID() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSelectWABAConnectionUsesExactPhoneNumber(t *testing.T) {
	const (
		secretA = "11111111111111112222222222222222"
		secretB = "33333333333333334444444444444444"
	)
	connA := wabaTestConnection(t, "phone-a", secretA, "verify-a")
	connB := wabaTestConnection(t, "phone-b", secretB, "verify-b")

	selected, creds, err := selectWABAConnection([]*repository.Connection{connA, connB}, "phone-b")
	if err != nil {
		t.Fatalf("selectWABAConnection() error = %v", err)
	}
	if selected.ID != connB.ID {
		t.Fatalf("selected connection = %s, want %s", selected.ID, connB.ID)
	}
	if creds.AppSecret != secretB {
		t.Fatalf("selected app secret = %q, want tenant B secret", creds.AppSecret)
	}

	payload := []byte(`{"entry":[]}`)
	signature := signedWABAHeader(payload, secretB)
	if !validWABASignature(payload, signature, creds.AppSecret) {
		t.Fatal("signature from selected tenant should validate")
	}
	if validWABASignature(payload, signedWABAHeader(payload, secretA), creds.AppSecret) {
		t.Fatal("signature from another tenant must not validate")
	}
}

func TestSelectWABAConnectionFailsClosed(t *testing.T) {
	missingSecret := wabaTestConnection(t, "phone-a", "", "verify-a")
	weakSecret := wabaTestConnection(t, "phone-a", "short-secret", "verify-a")
	duplicateA := wabaTestConnection(t, "phone-a", "55555555555555556666666666666666", "verify-a-2")
	connA := wabaTestConnection(t, "phone-a", "11111111111111112222222222222222", "verify-a")

	tests := []struct {
		name  string
		conns []*repository.Connection
		phone string
	}{
		{name: "unknown phone", conns: []*repository.Connection{connA}, phone: "phone-b"},
		{name: "missing app secret", conns: []*repository.Connection{missingSecret}, phone: "phone-a"},
		{name: "weak app secret", conns: []*repository.Connection{weakSecret}, phone: "phone-a"},
		{name: "duplicate phone identity", conns: []*repository.Connection{connA, duplicateA}, phone: "phone-a"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := selectWABAConnection(tt.conns, tt.phone); err == nil {
				t.Fatal("selectWABAConnection() error = nil, want fail-closed error")
			}
		})
	}
}

func TestMatchesPersistedWABAVerifyTokenRejectsPredictableFallback(t *testing.T) {
	const persisted = "f6H1j9L2p4Q8r3V7x5Z0m2N6c9B1d4K8"
	conn := wabaTestConnection(t, "phone-a", "secret-a", persisted)

	if !matchesPersistedWABAVerifyToken([]*repository.Connection{conn}, persisted) {
		t.Fatal("persisted verify token should match")
	}
	predictable := "pergo_verify_token_" + conn.WorkspaceID.String()
	if matchesPersistedWABAVerifyToken([]*repository.Connection{conn}, predictable) {
		t.Fatal("derived workspace token must not match")
	}
	predictableConn := wabaTestConnection(t, "phone-b", "secret-b", predictable)
	if matchesPersistedWABAVerifyToken([]*repository.Connection{predictableConn}, predictable) {
		t.Fatal("predictable token must be rejected even when persisted")
	}
	if matchesPersistedWABAVerifyToken([]*repository.Connection{conn}, "") {
		t.Fatal("empty verify token must not match")
	}
	repeated := strings.Repeat("x", 32)
	repeatedConn := wabaTestConnection(t, "phone-c", "secret-c", repeated)
	if matchesPersistedWABAVerifyToken([]*repository.Connection{repeatedConn}, repeated) {
		t.Fatal("repeated-character verify token must be rejected")
	}
}

func TestWABAWebhookRejectsOversizedBodyBeforeRepositoryLookup(t *testing.T) {
	e := echo.New()
	workspaceID := uuid.New()
	body := strings.NewReader(strings.Repeat("x", int(maxWABAWebhookBodyBytes)+1))
	req := httptest.NewRequest(http.MethodPost, "/webhooks/waba/"+workspaceID.String(), body)
	rec := httptest.NewRecorder()

	// A nil repository deliberately proves the size gate runs before tenant
	// lookup and HMAC calculation. Running through Echo plus the global audit
	// middleware proves no upstream component buffers the unbounded body first.
	handler := NewWABAWebhookHandler(nil, nil, nil)
	e.POST("/webhooks/waba/:workspace_id", handler.HandlePost, middleware.AuditMiddleware(nil))
	e.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusRequestEntityTooLarge)
	}
}

func TestWABAMediaFailuresAreRetryable(t *testing.T) {
	if !errors.Is(media.ErrDisabled, media.ErrDisabled) {
		t.Fatal("media disabled sentinel must remain stable")
	}
	if errors.Is(errors.New("invalid signature"), whatsapp.ErrWABAMediaRetryable) {
		t.Fatal("authentication failures must not match the media retry sentinel")
	}
}

func TestWABAWebhookMediaDisabledReturnsRetryWithoutAcknowledging(t *testing.T) {
	const (
		phoneID   = "phone-media-disabled"
		appSecret = "0123456789abcdef0123456789abcdef"
	)
	workspaceID := uuid.New()
	connection := wabaTestConnection(t, phoneID, appSecret, "Kj7mQ2vL9xP4sN8dT5zR1cB6hF3wY0uE")
	connection.WorkspaceID = workspaceID
	var credentials whatsapp.WABAConfig
	if err := json.Unmarshal(connection.Credentials, &credentials); err != nil {
		t.Fatalf("unmarshal credentials: %v", err)
	}
	credentials.Token = "meta-token"
	connection.Credentials, _ = json.Marshal(credentials)

	body := []byte(`{"entry":[{"changes":[{"value":{"metadata":{"phone_number_id":"phone-media-disabled"},"messages":[{"from":"5511999999999","id":"wamid.media","type":"image","image":{"id":"media-id"}}]}}]}]}`)
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/webhooks/waba/"+workspaceID.String(), strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", signedWABAHeader(body, appSecret))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetPath("/webhooks/waba/:workspace_id")
	c.SetPathValues(echo.PathValues{{Name: "workspace_id", Value: workspaceID.String()}})

	handler := NewWABAWebhookHandler(
		staticWABAConnectionStore{connections: []*repository.Connection{connection}},
		nil,
		media.NewDefaultEngine(storage.NewDisabledS3Client()),
	)
	if err := handler.HandlePost(c); err != nil {
		t.Fatalf("HandlePost: %v", err)
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if got := rec.Header().Get("Retry-After"); got != "300" {
		t.Fatalf("Retry-After = %q, want 300", got)
	}
}

func signedWABAHeader(payload []byte, appSecret string) string {
	mac := hmac.New(sha256.New, []byte(appSecret))
	_, _ = mac.Write(payload)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func wabaTestConnection(t *testing.T, phoneNumberID, appSecret, verifyToken string) *repository.Connection {
	t.Helper()
	credentials, err := json.Marshal(whatsapp.WABAConfig{
		PhoneNumberID: phoneNumberID,
		AppSecret:     appSecret,
		VerifyToken:   verifyToken,
	})
	if err != nil {
		t.Fatalf("marshal test credentials: %v", err)
	}
	return &repository.Connection{
		ID:          uuid.New(),
		WorkspaceID: uuid.New(),
		Channel:     "whatsapp_cloud",
		Credentials: credentials,
	}
}
