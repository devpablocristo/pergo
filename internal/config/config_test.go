package config

import (
	"crypto/sha256"
	"encoding/base64"
	"strings"
	"testing"
)

func TestWhatsAppMockEnabledIsStrictOptIn(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "unset", value: "", want: false},
		{name: "false", value: "false", want: false},
		{name: "uppercase true", value: "TRUE", want: false},
		{name: "numeric true", value: "1", want: false},
		{name: "exact true", value: "true", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("PERGO_WHATSAPP_MOCK_ENABLED", tt.value)
			if got := Load().WhatsAppMockEnabled; got != tt.want {
				t.Fatalf("WhatsAppMockEnabled = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidateProductionPolicy(t *testing.T) {
	validKey := sha256.Sum256([]byte("production-policy-test-fixture-not-for-deployment"))
	validKEK := base64.StdEncoding.EncodeToString(validKey[:])
	baseEnv := map[string]string{
		"PERGO_ENVIRONMENT":          "production",
		"PERGO_RUNTIME_PROFILE":      "api",
		"PERGO_RUN_MIGRATIONS":       "false",
		"PERGO_DATABASE_URL":         "postgres://pergo:secret@db.internal:5432/pergo?sslmode=verify-full&sslrootcert=/var/run/secrets/postgres-ca.pem",
		"PERGO_EXTERNAL_URL":         "https://pergo.example.test",
		"PERGO_KEK_BASE64":           validKEK,
		"PERGO_SESSION_SECRET":       "production-session-secret-fixture-32-plus",
		"PERGO_META_GRAPH_VERSION":   "v25.0",
		"PERGO_NATS_URLS":            "tls://nats-a.internal:4222,tls://nats-b.internal:4222",
		"PERGO_NATS_CREDS_FILE":      "/var/run/secrets/nats.creds",
		"PERGO_NATS_ACCOUNT":         "pymes-prd",
		"PERGO_NATS_STREAM_REPLICAS": "3",
		"PERGO_MEDIA_MODE":           "disabled",
		"PERGO_ADMIN_PASSWORD":       "not-the-development-password",
	}

	tests := []struct {
		name    string
		change  map[string]string
		wantErr string
	}{
		{name: "valid"},
		{name: "unknown environment", change: map[string]string{"PERGO_ENVIRONMENT": "prod"}, wantErr: "must identify"},
		{name: "monolith forbidden", change: map[string]string{"PERGO_RUNTIME_PROFILE": "all"}, wantErr: "RUNTIME_PROFILE=all"},
		{name: "migration forbidden", change: map[string]string{"PERGO_RUN_MIGRATIONS": "true"}, wantErr: "cannot run migrations"},
		{name: "missing KEK", change: map[string]string{"PERGO_KEK_BASE64": ""}, wantErr: "KEK_BASE64 is required"},
		{name: "plaintext NATS", change: map[string]string{"PERGO_NATS_URLS": "nats://nats:4222"}, wantErr: "must use tls://"},
		{name: "missing account", change: map[string]string{"PERGO_NATS_ACCOUNT": ""}, wantErr: "NATS_ACCOUNT is required"},
		{name: "R1 forbidden", change: map[string]string{"PERGO_NATS_STREAM_REPLICAS": "1"}, wantErr: "at least 3"},
		{name: "memory media forbidden", change: map[string]string{"PERGO_MEDIA_MODE": "memory"}, wantErr: "only permits"},
		{name: "localhost external URL", change: map[string]string{"PERGO_EXTERNAL_URL": "http://localhost:8080"}, wantErr: "EXTERNAL_URL is unsafe"},
		{name: "external URL path", change: map[string]string{"PERGO_EXTERNAL_URL": "https://pergo.example.test/base"}, wantErr: "origin URL"},
		{name: "external URL query", change: map[string]string{"PERGO_EXTERNAL_URL": "https://pergo.example.test?tenant=x"}, wantErr: "origin URL"},
		{name: "default database", change: map[string]string{"PERGO_DATABASE_URL": "postgres://postgres:postgres@localhost:5432/pergo?sslmode=disable"}, wantErr: "local development default"},
		{name: "example admin password", change: map[string]string{"PERGO_ADMIN_PASSWORD": "troque-esta-senha"}, wantErr: "known development value"},
		{name: "short admin password", change: map[string]string{"PERGO_ADMIN_PASSWORD": "short"}, wantErr: "at least 16"},
		{name: "missing session secret", change: map[string]string{"PERGO_SESSION_SECRET": ""}, wantErr: "SESSION_SECRET"},
		{name: "expired Meta Graph version", change: map[string]string{"PERGO_META_GRAPH_VERSION": "v18.0"}, wantErr: "META_GRAPH_VERSION"},
		{name: "unaudited Meta Graph version", change: map[string]string{"PERGO_META_GRAPH_VERSION": "v26.0"}, wantErr: "audited release"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for key, value := range baseEnv {
				t.Setenv(key, value)
			}
			for key, value := range tt.change {
				t.Setenv(key, value)
			}

			err := Load().Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Validate() error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestValidateNeverIncludesNATSURLCredentials(t *testing.T) {
	const secret = "REVIEW_PASSWORD_LEAK"
	validKey := sha256.Sum256([]byte("validation-redaction-fixture-not-for-deployment"))
	t.Setenv("PERGO_ENVIRONMENT", "production")
	t.Setenv("PERGO_RUNTIME_PROFILE", "api")
	t.Setenv("PERGO_RUN_MIGRATIONS", "false")
	t.Setenv("PERGO_DATABASE_URL", "postgres://pergo:secret@db.internal:5432/pergo?sslmode=verify-full&sslrootcert=/var/run/secrets/postgres-ca.pem")
	t.Setenv("PERGO_EXTERNAL_URL", "https://pergo.example.test")
	t.Setenv("PERGO_KEK_BASE64", base64.StdEncoding.EncodeToString(validKey[:]))
	t.Setenv("PERGO_SESSION_SECRET", "production-session-secret-fixture-32-plus")
	t.Setenv("PERGO_NATS_URLS", "nats://operator:"+secret+"@nats.internal:4222")
	t.Setenv("PERGO_NATS_CREDS_FILE", "/var/run/secrets/nats.creds")
	t.Setenv("PERGO_NATS_ACCOUNT", "pymes-prd")
	t.Setenv("PERGO_NATS_STREAM_REPLICAS", "3")
	t.Setenv("PERGO_MEDIA_MODE", "disabled")
	t.Setenv("PERGO_ADMIN_PASSWORD", "not-the-development-password")

	err := Load().Validate()
	if err == nil {
		t.Fatal("Validate accepted URL userinfo and plaintext transport")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("Validate leaked NATS URL credentials: %v", err)
	}
}

func TestValidateRejectsUnsafeDeploymentKEKs(t *testing.T) {
	sequentialZero := make([]byte, 32)
	for i := range sequentialZero {
		sequentialZero[i] = byte(i)
	}
	repeatedHalf := make([]byte, 32)
	for i := range repeatedHalf {
		repeatedHalf[i] = byte(i % 16)
	}
	descending := make([]byte, 32)
	for i := range descending {
		descending[i] = byte(255 - i)
	}
	safeFixture := sha256.Sum256([]byte("safe-validation-control-fixture"))

	tests := []struct {
		name    string
		key     []byte
		wantErr string
	}{
		{name: "all zero", key: make([]byte, 32), wantErr: "repeated-byte"},
		{name: "all repeated", key: bytesOf(0x7f, 32), wantErr: "repeated-byte"},
		{name: "development default", key: []byte("dev-development-key-32-bytes-kek"), wantErr: "known development"},
		{name: "printable passphrase", key: []byte("a-human-readable-key-is-not-safe"), wantErr: "printable passphrases"},
		{name: "repeated half vector", key: repeatedHalf, wantErr: "repeating-pattern"},
		{name: "sequential public vector", key: sequentialZero, wantErr: "arithmetic test vector"},
		{name: "descending public vector", key: descending, wantErr: "arithmetic test vector"},
		{name: "safe control", key: safeFixture[:]},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("PERGO_ENVIRONMENT", "staging")
			t.Setenv("PERGO_RUNTIME_PROFILE", "api")
			t.Setenv("PERGO_RUN_MIGRATIONS", "false")
			t.Setenv("PERGO_DATABASE_URL", "postgres://pergo:secret@db.internal:5432/pergo?sslmode=verify-full&sslrootcert=/var/run/secrets/postgres-ca.pem")
			t.Setenv("PERGO_EXTERNAL_URL", "https://pergo.example.test")
			t.Setenv("PERGO_KEK_BASE64", base64.StdEncoding.EncodeToString(tt.key))
			t.Setenv("PERGO_SESSION_SECRET", "staging-session-secret-fixture-32-plus")
			t.Setenv("PERGO_NATS_URLS", "tls://nats.internal:4222")
			t.Setenv("PERGO_NATS_CREDS_FILE", "/var/run/secrets/nats.creds")
			t.Setenv("PERGO_NATS_ACCOUNT", "pymes-stg")
			t.Setenv("PERGO_NATS_STREAM_REPLICAS", "1")
			t.Setenv("PERGO_MEDIA_MODE", "disabled")
			t.Setenv("PERGO_ADMIN_PASSWORD", "not-the-development-password")

			err := Load().Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Validate() error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func bytesOf(value byte, size int) []byte {
	out := make([]byte, size)
	for i := range out {
		out[i] = value
	}
	return out
}

func TestValidateRejectsMalformedKEKInDevelopment(t *testing.T) {
	t.Setenv("PERGO_ENVIRONMENT", "development")
	t.Setenv("PERGO_KEK_BASE64", "not-base64")

	err := Load().Validate()
	if err == nil || !strings.Contains(err.Error(), "not valid base64") {
		t.Fatalf("Validate() error = %v, want invalid base64", err)
	}
}

func TestValidateProductionMigrationProfileRequiresEncryptedDataAndNATSBootstrap(t *testing.T) {
	safeKey := sha256.Sum256([]byte("migration-profile-test-fixture"))
	t.Setenv("PERGO_ENVIRONMENT", "production")
	t.Setenv("PERGO_RUNTIME_PROFILE", "migrate")
	t.Setenv("PERGO_DATABASE_URL", "postgres://pergo:secret@db.internal:5432/pergo?sslmode=verify-full&sslrootcert=/var/run/secrets/postgres-ca.pem")
	t.Setenv("PERGO_KEK_BASE64", base64.StdEncoding.EncodeToString(safeKey[:]))
	t.Setenv("PERGO_NATS_URLS", "tls://nats.internal:4222")
	t.Setenv("PERGO_NATS_CREDS_FILE", "/var/run/secrets/nats-migrate.creds")
	t.Setenv("PERGO_NATS_ACCOUNT", "pymes-prd")
	t.Setenv("PERGO_NATS_STREAM_REPLICAS", "3")
	t.Setenv("PERGO_ADMIN_PASSWORD", "pergo-dev-2026")

	if err := Load().Validate(); err != nil {
		t.Fatalf("Validate() migration profile error = %v", err)
	}

	t.Setenv("PERGO_NATS_CREDS_FILE", "")
	if err := Load().Validate(); err == nil || !strings.Contains(err.Error(), "NATS_CREDS_FILE") {
		t.Fatalf("migration profile accepted missing NATS bootstrap credentials: %v", err)
	}
}

func TestValidateCredentialRotationRequiresStrongKEKWithoutNATS(t *testing.T) {
	safeKey := sha256.Sum256([]byte("credential-rotation-test-fixture"))
	t.Setenv("PERGO_ENVIRONMENT", "production")
	t.Setenv("PERGO_RUNTIME_PROFILE", "all")
	t.Setenv("PERGO_DATABASE_URL", "postgres://pergo:secret@db.internal:5432/pergo?sslmode=require")
	t.Setenv("PERGO_KEK_BASE64", base64.StdEncoding.EncodeToString(safeKey[:]))
	t.Setenv("PERGO_NATS_URLS", "")
	t.Setenv("PERGO_NATS_CREDS_FILE", "")
	t.Setenv("PERGO_NATS_ACCOUNT", "")

	if err := Load().ValidateCredentialRotation(); err != nil {
		t.Fatalf("ValidateCredentialRotation() error = %v", err)
	}

	t.Setenv("PERGO_KEK_BASE64", base64.StdEncoding.EncodeToString(make([]byte, 32)))
	err := Load().ValidateCredentialRotation()
	if err == nil || !strings.Contains(err.Error(), "repeated-byte") {
		t.Fatalf("ValidateCredentialRotation() error = %v, want unsafe KEK", err)
	}
}

func TestLoadParsesNATSServerList(t *testing.T) {
	t.Setenv("PERGO_NATS_URLS", " nats://one:4222, nats://two:4222 ")

	cfg := Load()
	if len(cfg.NATSURLs) != 2 {
		t.Fatalf("NATSURLs = %#v, want 2 entries", cfg.NATSURLs)
	}
	if cfg.NATSUrl != "nats://one:4222,nats://two:4222" {
		t.Fatalf("NATSUrl = %q", cfg.NATSUrl)
	}
}
