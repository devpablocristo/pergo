package wabarotate

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pablojhp.pergo/internal/channel/whatsapp"
	"github.com/pablojhp.pergo/internal/platform/crypto"
	"github.com/pablojhp.pergo/internal/platform/postgres"
	"github.com/pablojhp.pergo/internal/repository"
)

type fakeStore struct {
	connection *repository.Connection
	savedID    uuid.UUID
	saved      []byte
	saveErr    error
}

func (s *fakeStore) GetByIDForWorkspace(_ context.Context, workspaceID, connectionID uuid.UUID) (*repository.Connection, error) {
	if s.connection == nil || s.connection.WorkspaceID != workspaceID || s.connection.ID != connectionID {
		return nil, repository.ErrConnectionNotFound
	}
	return s.connection, nil
}

func (s *fakeStore) SaveCredentialsForWorkspaceIfRevision(
	_ context.Context,
	_ uuid.UUID,
	id uuid.UUID,
	_ string,
	expectedRevision int64,
	plaintext []byte,
) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	if expectedRevision != s.connection.CredentialRevision {
		return repository.ErrCredentialsChanged
	}
	s.savedID = id
	s.saved = append([]byte(nil), plaintext...)
	return nil
}

func TestRotateFailsInsteadOfClobberingConcurrentCredentialChange(t *testing.T) {
	workspaceID := uuid.New()
	connectionID := uuid.New()
	existing, _ := json.Marshal(whatsapp.WABAConfig{
		PhoneNumberID: "phone-id",
		Token:         "newer-access-token",
		WABAAccountID: "waba-id",
	})
	store := &fakeStore{
		connection: &repository.Connection{
			ID:                 connectionID,
			WorkspaceID:        workspaceID,
			Channel:            "whatsapp_cloud",
			Credentials:        existing,
			CredentialRevision: 7,
		},
		saveErr: repository.ErrCredentialsChanged,
	}

	err := Rotate(context.Background(), store, Input{
		WorkspaceID:  workspaceID,
		ConnectionID: connectionID,
		AppSecret:    "9a8b7c6d5e4f32109a8b7c6d5e4f3210",
		VerifyToken:  "Kj7mQ2vL9xP4sN8dT5zR1cB6hF3wY0uE",
	})
	if !errors.Is(err, repository.ErrCredentialsChanged) {
		t.Fatalf("Rotate error = %v, want ErrCredentialsChanged", err)
	}
	if store.saved != nil {
		t.Fatal("rotation overwrote concurrently changed credentials")
	}
}

func TestRotatePreservesMetaIdentityAndToken(t *testing.T) {
	workspaceID := uuid.New()
	connectionID := uuid.New()
	existing, err := json.Marshal(whatsapp.WABAConfig{
		PhoneNumberID: "phone-id",
		Token:         "access-token",
		WABAAccountID: "waba-id",
		VerifyToken:   "old-old-old-old-old-old-old-token",
		AppSecret:     "old-old-old-old-old-old-old-secret",
	})
	if err != nil {
		t.Fatalf("marshal existing: %v", err)
	}
	store := &fakeStore{connection: &repository.Connection{
		ID:          connectionID,
		WorkspaceID: workspaceID,
		Channel:     "whatsapp_cloud",
		Credentials: existing,
	}}
	input := Input{
		WorkspaceID:  workspaceID,
		ConnectionID: connectionID,
		AppSecret:    "9a8b7c6d5e4f32109a8b7c6d5e4f3210",
		VerifyToken:  "Kj7mQ2vL9xP4sN8dT5zR1cB6hF3wY0uE",
	}

	if err := Rotate(context.Background(), store, input); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if store.savedID != connectionID {
		t.Fatalf("saved connection = %s, want %s", store.savedID, connectionID)
	}
	var got whatsapp.WABAConfig
	if err := json.Unmarshal(store.saved, &got); err != nil {
		t.Fatalf("unmarshal saved: %v", err)
	}
	if got.PhoneNumberID != "phone-id" || got.Token != "access-token" || got.WABAAccountID != "waba-id" {
		t.Fatalf("non-webhook credentials changed: %+v", got)
	}
	if got.AppSecret != input.AppSecret || got.VerifyToken != input.VerifyToken {
		t.Fatal("replacement webhook credentials were not persisted")
	}
}

func TestRotatePreservesUnknownCredentialFields(t *testing.T) {
	workspaceID := uuid.New()
	connectionID := uuid.New()
	store := &fakeStore{connection: &repository.Connection{
		ID:          connectionID,
		WorkspaceID: workspaceID,
		Channel:     "whatsapp_cloud",
		Credentials: []byte(`{
			"phone_number_id":"phone-id",
			"token":"access-token",
			"waba_account_id":"waba-id",
			"future_provider_field":{"nested":true}
		}`),
	}}
	input := Input{
		WorkspaceID:  workspaceID,
		ConnectionID: connectionID,
		AppSecret:    "9a8b7c6d5e4f32109a8b7c6d5e4f3210",
		VerifyToken:  "Kj7mQ2vL9xP4sN8dT5zR1cB6hF3wY0uE",
	}
	if err := Rotate(context.Background(), store, input); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(store.saved, &got); err != nil {
		t.Fatalf("decode saved credentials: %v", err)
	}
	if string(got["future_provider_field"]) != `{"nested":true}` {
		t.Fatalf("unknown credential field was lost or changed: %s", got["future_provider_field"])
	}
}

func TestRotateFailsClosedWithoutWriting(t *testing.T) {
	workspaceID := uuid.New()
	connectionID := uuid.New()
	validCredentials, _ := json.Marshal(whatsapp.WABAConfig{
		PhoneNumberID: "phone-id",
		Token:         "access-token",
		WABAAccountID: "waba-id",
	})
	validInput := Input{
		WorkspaceID:  workspaceID,
		ConnectionID: connectionID,
		AppSecret:    "9a8b7c6d5e4f32109a8b7c6d5e4f3210",
		VerifyToken:  "Kj7mQ2vL9xP4sN8dT5zR1cB6hF3wY0uE",
	}

	tests := []struct {
		name       string
		connection *repository.Connection
		change     func(*Input)
	}{
		{name: "cross tenant", connection: &repository.Connection{ID: connectionID, WorkspaceID: uuid.New(), Channel: "whatsapp_cloud", Credentials: validCredentials}},
		{name: "wrong channel", connection: &repository.Connection{ID: connectionID, WorkspaceID: workspaceID, Channel: "telegram", Credentials: validCredentials}},
		{name: "missing connection"},
		{name: "weak app secret", connection: &repository.Connection{ID: connectionID, WorkspaceID: workspaceID, Channel: "whatsapp_cloud", Credentials: validCredentials}, change: func(input *Input) {
			input.AppSecret = "short"
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := validInput
			if tt.change != nil {
				tt.change(&input)
			}
			store := &fakeStore{connection: tt.connection}
			if err := Rotate(context.Background(), store, input); err == nil {
				t.Fatal("Rotate error = nil")
			}
			if store.saved != nil {
				t.Fatal("credentials were written on failed validation")
			}
		})
	}
}

func TestRotateErrorsNeverContainSubmittedSecrets(t *testing.T) {
	input := Input{
		WorkspaceID:  uuid.New(),
		ConnectionID: uuid.New(),
		AppSecret:    "secret-value-that-must-never-appear",
		VerifyToken:  "another-secret-that-must-never-appear",
	}
	err := Rotate(context.Background(), &fakeStore{}, input)
	if err == nil {
		t.Fatal("Rotate error = nil")
	}
	if errors.Is(err, nil) {
		t.Fatal("unexpected nil error")
	}
	for _, secret := range []string{input.AppSecret, input.VerifyToken} {
		if secret != "" && contains(err.Error(), secret) {
			t.Fatalf("error exposed submitted secret")
		}
	}
}

func contains(value, substring string) bool {
	for i := 0; i+len(substring) <= len(value); i++ {
		if value[i:i+len(substring)] == substring {
			return true
		}
	}
	return false
}

func TestRotateIntegrationPersistsEncryptedTenantScopedCredentials(t *testing.T) {
	dsn := os.Getenv("PERGO_DATABASE_URL")
	if dsn == "" {
		t.Skip("PERGO_DATABASE_URL is required for integration test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect PostgreSQL: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("PostgreSQL unavailable: %v", err)
	}
	db, err := postgres.NewSQLDB(pool)
	if err != nil {
		t.Fatalf("wrap SQL DB: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := postgres.RunMigrations(db); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	kek := sha256.Sum256([]byte("waba-rotation-integration-fixture"))
	encryptor, err := crypto.NewEncryptor(kek[:])
	if err != nil {
		t.Fatalf("encryptor: %v", err)
	}
	workspaceRepo := repository.NewWorkspaceRepository(pool)
	connectionRepo := repository.NewConnectionRepository(pool, encryptor)
	workspace, err := workspaceRepo.Create(ctx, "waba-rotation-"+uuid.NewString())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _ = workspaceRepo.Delete(ctx, workspace.ID) }()

	credentials, _ := json.Marshal(whatsapp.WABAConfig{
		PhoneNumberID: "phone-id",
		Token:         "access-token",
		WABAAccountID: "waba-id",
		VerifyToken:   "old-old-old-old-old-old-old-token",
		AppSecret:     "old-old-old-old-old-old-old-secret",
	})
	connection := &repository.Connection{
		ID:             uuid.New(),
		WorkspaceID:    workspace.ID,
		Name:           "WABA rotation integration",
		Channel:        "whatsapp_cloud",
		SenderIdentity: "phone-id",
		Status:         "active",
		Credentials:    credentials,
	}
	if err := connectionRepo.Create(ctx, connection); err != nil {
		t.Fatalf("create connection: %v", err)
	}

	input := Input{
		WorkspaceID:  workspace.ID,
		ConnectionID: connection.ID,
		AppSecret:    "9a8b7c6d5e4f32109a8b7c6d5e4f3210",
		VerifyToken:  "Kj7mQ2vL9xP4sN8dT5zR1cB6hF3wY0uE",
	}
	if err := Rotate(ctx, connectionRepo, input); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	reloaded, err := connectionRepo.GetByID(ctx, connection.ID)
	if err != nil {
		t.Fatalf("reload connection: %v", err)
	}
	var got whatsapp.WABAConfig
	if err := json.Unmarshal(reloaded.Credentials, &got); err != nil {
		t.Fatalf("decode reloaded credentials: %v", err)
	}
	if got.AppSecret != input.AppSecret || got.VerifyToken != input.VerifyToken {
		t.Fatal("rotated secrets did not round-trip through encrypted storage")
	}
}
