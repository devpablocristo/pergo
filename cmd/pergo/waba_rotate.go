package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/google/uuid"
	"github.com/pablojhp.pergo/internal/config"
	"github.com/pablojhp.pergo/internal/ops/wabarotate"
	"github.com/pablojhp.pergo/internal/platform/crypto"
	"github.com/pablojhp.pergo/internal/platform/postgres"
	"github.com/pablojhp.pergo/internal/repository"
)

const maxWABASecretFileBytes = 4096

func runWABACredentialRotation(ctx context.Context) error {
	cfg := config.Load()
	if err := cfg.ValidateCredentialRotation(); err != nil {
		return fmt.Errorf("invalid rotation configuration: %w", err)
	}

	workspaceID, err := uuid.Parse(os.Getenv("PERGO_ROTATE_WORKSPACE_ID"))
	if err != nil {
		return errors.New("PERGO_ROTATE_WORKSPACE_ID must be a UUID")
	}
	connectionID, err := uuid.Parse(os.Getenv("PERGO_ROTATE_CONNECTION_ID"))
	if err != nil {
		return errors.New("PERGO_ROTATE_CONNECTION_ID must be a UUID")
	}
	appSecret, err := readMountedSecret(os.Getenv("PERGO_ROTATE_APP_SECRET_FILE"))
	if err != nil {
		return fmt.Errorf("read app_secret file: %w", err)
	}
	verifyToken, err := readMountedSecret(os.Getenv("PERGO_ROTATE_VERIFY_TOKEN_FILE"))
	if err != nil {
		return fmt.Errorf("read verify_token file: %w", err)
	}

	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("connect PostgreSQL: %w", err)
	}
	defer pool.Close()
	encryptor, err := crypto.NewEncryptor(cfg.KEKBytes)
	if err != nil {
		return errors.New("initialize credential encryption")
	}
	store := repository.NewConnectionRepository(pool, encryptor)
	return wabarotate.Rotate(ctx, store, wabarotate.Input{
		WorkspaceID:  workspaceID,
		ConnectionID: connectionID,
		AppSecret:    appSecret,
		VerifyToken:  verifyToken,
	})
}

func readMountedSecret(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("secret file path is required")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", errors.New("open mounted secret")
	}
	defer func() { _ = file.Close() }()

	buffer, err := io.ReadAll(io.LimitReader(file, maxWABASecretFileBytes+1))
	if err != nil {
		return "", errors.New("read mounted secret")
	}
	if len(buffer) > maxWABASecretFileBytes {
		return "", errors.New("mounted secret exceeds size limit")
	}
	value := strings.TrimSpace(string(buffer))
	for i := range buffer {
		buffer[i] = 0
	}
	if value == "" {
		return "", errors.New("mounted secret is empty")
	}
	return value, nil
}
