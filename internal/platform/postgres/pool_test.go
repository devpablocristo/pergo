package postgres

import (
	"context"
	"strings"
	"testing"
)

func TestNewPoolDoesNotEchoInvalidDSNSecrets(t *testing.T) {
	const secretMarker = "postgres-password-secret-marker"
	_, err := NewPool(
		context.Background(),
		"postgres://user:"+secretMarker+"@%invalid-host/database",
	)
	if err == nil {
		t.Fatal("invalid DSN unexpectedly accepted")
	}
	if strings.Contains(err.Error(), secretMarker) {
		t.Fatal("PostgreSQL password leaked in parse error")
	}
}
