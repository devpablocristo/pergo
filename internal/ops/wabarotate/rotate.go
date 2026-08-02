// Package wabarotate provides the tenant-scoped, non-HTTP operation used to
// backfill or rotate Meta webhook secrets without exposing them in the admin
// UI, command-line arguments, logs, or seed data.
package wabarotate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/pablojhp.pergo/internal/channel/whatsapp"
	"github.com/pablojhp.pergo/internal/repository"
)

// ConnectionStore is the narrow credential persistence contract owned by this
// operation.
type ConnectionStore interface {
	GetByIDForWorkspace(ctx context.Context, workspaceID, id uuid.UUID) (*repository.Connection, error)
	SaveCredentialsForWorkspaceIfRevision(
		ctx context.Context,
		workspaceID, id uuid.UUID,
		channel string,
		expectedRevision int64,
		plaintext []byte,
	) error
}

// Input identifies exactly one tenant connection and supplies its replacement
// secrets. Callers must source secret values from mounted files.
type Input struct {
	WorkspaceID  uuid.UUID
	ConnectionID uuid.UUID
	AppSecret    string
	VerifyToken  string
}

// Rotate atomically replaces only webhook credentials while preserving the
// existing Meta token, WABA account, and phone-number identity.
func Rotate(ctx context.Context, store ConnectionStore, input Input) error {
	if store == nil {
		return errors.New("connection store is required")
	}
	if input.WorkspaceID == uuid.Nil || input.ConnectionID == uuid.Nil {
		return errors.New("workspace_id and connection_id are required")
	}

	appSecret := strings.TrimSpace(input.AppSecret)
	verifyToken := strings.TrimSpace(input.VerifyToken)
	if err := whatsapp.ValidateWebhookSecrets(appSecret, verifyToken); err != nil {
		return err
	}

	connection, err := store.GetByIDForWorkspace(ctx, input.WorkspaceID, input.ConnectionID)
	if err != nil {
		return fmt.Errorf("load connection: %w", err)
	}
	if connection == nil || connection.WorkspaceID != input.WorkspaceID {
		return errors.New("connection does not belong to the requested workspace")
	}
	if connection.Channel != "whatsapp_cloud" {
		return errors.New("connection is not a WhatsApp Cloud connection")
	}

	var rawCredentials map[string]json.RawMessage
	if err := json.Unmarshal(connection.Credentials, &rawCredentials); err != nil {
		return errors.New("existing connection credentials are invalid")
	}
	var credentials whatsapp.WABAConfig
	if err := json.Unmarshal(connection.Credentials, &credentials); err != nil {
		return errors.New("existing connection credentials are invalid")
	}
	if credentials.PhoneNumberID == "" || credentials.Token == "" || credentials.WABAAccountID == "" {
		return errors.New("existing connection is missing required Meta credentials")
	}
	rawCredentials["app_secret"], _ = json.Marshal(appSecret)
	rawCredentials["verify_token"], _ = json.Marshal(verifyToken)

	plaintext, err := json.Marshal(rawCredentials)
	if err != nil {
		return errors.New("encode rotated credentials")
	}
	if err := store.SaveCredentialsForWorkspaceIfRevision(
		ctx,
		input.WorkspaceID,
		connection.ID,
		"whatsapp_cloud",
		connection.CredentialRevision,
		plaintext,
	); err != nil {
		return fmt.Errorf("save rotated credentials: %w", err)
	}
	return nil
}
