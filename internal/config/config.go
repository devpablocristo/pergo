// Package config provides 12-factor env-var configuration loading for PerGo.
package config

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/pablojhp.pergo/internal/platform/metaapi"
)

const (
	RuntimeAll     = "all"
	RuntimeAPI     = "api"
	RuntimeWebhook = "webhook"
	RuntimeWorker  = "worker"
	RuntimeMigrate = "migrate"

	MediaDisabled = "disabled"
	MediaMemory   = "memory"
)

// Config holds all configuration for the PerGo server.
type Config struct {
	Environment            string
	RuntimeProfile         string
	RunMigrations          bool
	DatabaseURL            string
	NATSUrl                string
	NATSURLs               []string
	NATSCredentialsFile    string
	NATSCAFile             string
	NATSTLSServerName      string
	NATSAccount            string
	NATSStreamReplicas     int
	NATSAdoptDrainedLegacy bool
	ServerPort             string
	DebugPort              string
	KEKBase64              string
	KEKBytes               []byte // decoded from KEKBase64
	SessionSecret          string
	MetaGraphVersion       string
	MediaMode              string
	AdminPassword          string
	S3Endpoint             string
	S3Bucket               string
	S3AccessKey            string
	S3SecretKey            string
	S3Region               string
	S3UsePathStyle         bool
	ExternalURL            string
	WhatsAppMockEnabled    bool
	kekDecodeErr           error
}

// Load reads configuration from environment variables with sensible defaults.
func Load() *Config {
	environment := strings.ToLower(envOrDefault("PERGO_ENVIRONMENT", "development"))
	natsURLs := splitList(envOrDefault("PERGO_NATS_URLS", envOrDefault("PERGO_NATS_URL", "nats://localhost:4222")))
	mediaDefault := MediaDisabled
	runMigrationsDefault := false
	if isDevelopmentEnvironment(environment) {
		mediaDefault = MediaMemory
		runMigrationsDefault = true
	}

	cfg := &Config{
		Environment:            environment,
		RuntimeProfile:         strings.ToLower(envOrDefault("PERGO_RUNTIME_PROFILE", RuntimeAll)),
		RunMigrations:          boolEnv("PERGO_RUN_MIGRATIONS", runMigrationsDefault),
		DatabaseURL:            envOrDefault("PERGO_DATABASE_URL", "postgres://postgres:postgres@localhost:5432/pergo?sslmode=disable"),
		NATSUrl:                strings.Join(natsURLs, ","),
		NATSURLs:               natsURLs,
		NATSCredentialsFile:    os.Getenv("PERGO_NATS_CREDS_FILE"),
		NATSCAFile:             os.Getenv("PERGO_NATS_CA_FILE"),
		NATSTLSServerName:      os.Getenv("PERGO_NATS_TLS_SERVER_NAME"),
		NATSAccount:            os.Getenv("PERGO_NATS_ACCOUNT"),
		NATSStreamReplicas:     intEnv("PERGO_NATS_STREAM_REPLICAS", 1),
		NATSAdoptDrainedLegacy: boolEnv("PERGO_NATS_ADOPT_DRAINED_LEGACY", false),
		ServerPort:             envOrDefault("PERGO_SERVER_PORT", "8080"),
		DebugPort:              envOrDefault("PERGO_DEBUG_PORT", "6060"),
		KEKBase64:              os.Getenv("PERGO_KEK_BASE64"),
		SessionSecret:          os.Getenv("PERGO_SESSION_SECRET"),
		MetaGraphVersion:       envOrDefault("PERGO_META_GRAPH_VERSION", metaapi.DefaultVersion),
		MediaMode:              strings.ToLower(envOrDefault("PERGO_MEDIA_MODE", mediaDefault)),
		AdminPassword:          envOrDefault("PERGO_ADMIN_PASSWORD", "pergo-dev-2026"),
		S3Endpoint:             envOrDefault("PERGO_S3_ENDPOINT", envOrDefault("S3_ENDPOINT", "")),
		S3Bucket:               envOrDefault("PERGO_S3_BUCKET", envOrDefault("S3_BUCKET", "")),
		S3AccessKey:            envOrDefault("PERGO_S3_ACCESS_KEY", envOrDefault("S3_ACCESS_KEY", "")),
		S3SecretKey:            envOrDefault("PERGO_S3_SECRET_KEY", envOrDefault("S3_SECRET_KEY", "")),
		S3Region:               envOrDefault("PERGO_S3_REGION", envOrDefault("S3_REGION", "us-east-1")),
		S3UsePathStyle:         os.Getenv("PERGO_S3_USE_PATH_STYLE") == "true" || os.Getenv("S3_USE_PATH_STYLE") == "true",
		ExternalURL:            envOrDefault("PERGO_EXTERNAL_URL", "http://localhost:8080"),
		WhatsAppMockEnabled:    os.Getenv("PERGO_WHATSAPP_MOCK_ENABLED") == "true",
	}

	if cfg.KEKBase64 != "" {
		kek, err := base64.StdEncoding.DecodeString(cfg.KEKBase64)
		if err != nil {
			cfg.kekDecodeErr = err
		} else {
			cfg.KEKBytes = kek
		}
	}

	return cfg
}

// Validate rejects unsafe or ambiguous deployment configuration before any
// database migration, network connection, or worker is started.
func (c *Config) Validate() error {
	var errs []error
	needsHTTPRuntime := c.RuntimeProfile != RuntimeMigrate

	switch c.Environment {
	case "development", "dev", "local", "test", "staging", "stg", "production", "prd":
	default:
		errs = append(errs, fmt.Errorf("PERGO_ENVIRONMENT must identify development, test, staging, or production"))
	}
	switch c.RuntimeProfile {
	case RuntimeAll, RuntimeAPI, RuntimeWebhook, RuntimeWorker, RuntimeMigrate:
	default:
		errs = append(errs, fmt.Errorf("PERGO_RUNTIME_PROFILE must be one of all, api, webhook, worker, migrate"))
	}
	switch c.MediaMode {
	case MediaDisabled, MediaMemory:
	default:
		errs = append(errs, fmt.Errorf("PERGO_MEDIA_MODE must be disabled or memory"))
	}
	if c.kekDecodeErr != nil {
		errs = append(errs, fmt.Errorf("PERGO_KEK_BASE64 is not valid base64: %w", c.kekDecodeErr))
	} else if len(c.KEKBytes) != 0 && len(c.KEKBytes) != 32 {
		errs = append(errs, fmt.Errorf("PERGO_KEK_BASE64 must decode to exactly 32 bytes"))
	}
	if err := metaapi.ValidateVersion(c.MetaGraphVersion); err != nil {
		errs = append(errs, fmt.Errorf("PERGO_META_GRAPH_VERSION is invalid: %w", err))
	}
	{
		if len(c.NATSURLs) == 0 {
			errs = append(errs, fmt.Errorf("at least one PERGO_NATS_URLS address is required"))
		}
		for _, raw := range c.NATSURLs {
			parsed, err := url.Parse(raw)
			if err != nil || parsed.Host == "" {
				errs = append(errs, fmt.Errorf("invalid NATS URL"))
				continue
			}
			if parsed.User != nil && !c.IsDevelopment() {
				errs = append(errs, fmt.Errorf("NATS credentials must use PERGO_NATS_CREDS_FILE, not URL userinfo"))
			}
			if !c.IsDevelopment() && parsed.Scheme != "tls" && parsed.Scheme != "wss" {
				errs = append(errs, fmt.Errorf("NATS URL must use tls:// or wss:// outside development/test"))
			}
		}
		if c.NATSStreamReplicas < 1 {
			errs = append(errs, fmt.Errorf("PERGO_NATS_STREAM_REPLICAS must be at least 1"))
		}
	}

	if !c.IsDevelopment() {
		if strings.TrimSpace(c.DatabaseURL) == "" || isKnownDevelopmentDatabaseURL(c.DatabaseURL) {
			errs = append(errs, fmt.Errorf("PERGO_DATABASE_URL must not use a local development default outside development/test"))
		}
		if c.RuntimeProfile == RuntimeAll {
			errs = append(errs, fmt.Errorf("PERGO_RUNTIME_PROFILE=all is forbidden outside development/test"))
		}
		if c.RunMigrations && c.RuntimeProfile != RuntimeMigrate {
			errs = append(errs, fmt.Errorf("normal runtimes cannot run migrations outside development/test"))
		}
		if needsHTTPRuntime {
			if err := validateExternalURL(c.ExternalURL); err != nil {
				errs = append(errs, fmt.Errorf("PERGO_EXTERNAL_URL is unsafe outside development/test: %w", err))
			}
			if c.MediaMode != MediaDisabled {
				errs = append(errs, fmt.Errorf("this build only permits PERGO_MEDIA_MODE=disabled outside development/test"))
			}
			if c.WhatsAppMockEnabled {
				errs = append(errs, fmt.Errorf("PERGO_WHATSAPP_MOCK_ENABLED is forbidden outside development/test"))
			}
		}
		if err := validateDeploymentDatabaseURL(c.DatabaseURL); err != nil {
			errs = append(errs, fmt.Errorf("PERGO_DATABASE_URL is unsafe outside development/test: %w", err))
		}
		if len(c.KEKBytes) != 32 {
			errs = append(errs, fmt.Errorf("PERGO_KEK_BASE64 is required outside development/test"))
		} else if err := validateDeploymentKEK(c.KEKBytes); err != nil {
			errs = append(errs, fmt.Errorf("PERGO_KEK_BASE64 is unsafe: %w", err))
		}
		if c.NATSCredentialsFile == "" {
			errs = append(errs, fmt.Errorf("PERGO_NATS_CREDS_FILE is required outside development/test"))
		}
		if c.NATSAccount == "" {
			errs = append(errs, fmt.Errorf("PERGO_NATS_ACCOUNT is required outside development/test"))
		}
		if c.RuntimeProfile == RuntimeAPI && !isDeploymentAdminPassword(c.AdminPassword) {
			errs = append(errs, fmt.Errorf("PERGO_ADMIN_PASSWORD must be at least 16 characters and must not use a known development value"))
		}
		if c.RuntimeProfile == RuntimeAPI && !isDeploymentSessionSecret(c.SessionSecret) {
			errs = append(errs, fmt.Errorf("PERGO_SESSION_SECRET must be a stable secret of at least 32 characters outside development/test"))
		}
	}
	if c.IsProduction() && c.NATSStreamReplicas < 3 {
		errs = append(errs, fmt.Errorf("PERGO_NATS_STREAM_REPLICAS must be at least 3 in production"))
	}
	if c.NATSAdoptDrainedLegacy && c.RuntimeProfile != RuntimeMigrate {
		errs = append(errs, errors.New("PERGO_NATS_ADOPT_DRAINED_LEGACY is allowed only for the migration job"))
	}

	return errors.Join(errs...)
}

// ValidateCredentialRotation validates the minimal, offline operator-job
// configuration. Rotation deliberately has no NATS or HTTP dependency.
func (c *Config) ValidateCredentialRotation() error {
	var errs []error
	switch c.Environment {
	case "development", "dev", "local", "test", "staging", "stg", "production", "prd":
	default:
		errs = append(errs, errors.New("PERGO_ENVIRONMENT must identify development, test, staging, or production"))
	}
	if strings.TrimSpace(c.DatabaseURL) == "" {
		errs = append(errs, errors.New("PERGO_DATABASE_URL is required"))
	} else if !c.IsDevelopment() && isKnownDevelopmentDatabaseURL(c.DatabaseURL) {
		errs = append(errs, errors.New("PERGO_DATABASE_URL must not use a local development default outside development/test"))
	}
	if c.kekDecodeErr != nil {
		errs = append(errs, fmt.Errorf("PERGO_KEK_BASE64 is not valid base64: %w", c.kekDecodeErr))
	} else if err := validateDeploymentKEK(c.KEKBytes); err != nil {
		errs = append(errs, fmt.Errorf("PERGO_KEK_BASE64 is unsafe: %w", err))
	}
	return errors.Join(errs...)
}

func isKnownDevelopmentDatabaseURL(raw string) bool {
	switch strings.TrimSpace(raw) {
	case "postgres://postgres:postgres@localhost:5432/pergo?sslmode=disable",
		"postgres://postgres:postgres@localhost:5433/pergo?sslmode=disable",
		"postgres://postgres:postgres@postgres:5432/pergo?sslmode=disable":
		return true
	default:
		return false
	}
}

func validateDeploymentDatabaseURL(raw string) error {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") {
		return errors.New("must be a PostgreSQL URL")
	}
	socketHost := parsed.Query().Get("host")
	if strings.HasPrefix(socketHost, "/cloudsql/") ||
		strings.HasPrefix(socketHost, "/var/run/postgresql/") {
		return nil
	}
	if parsed.Hostname() == "" {
		return errors.New("must use verified TLS or an approved Unix socket")
	}
	query := parsed.Query()
	if query.Get("sslmode") != "verify-full" {
		return errors.New("TCP connections must use sslmode=verify-full")
	}
	if strings.TrimSpace(query.Get("sslrootcert")) == "" {
		return errors.New("TCP connections must set sslrootcert")
	}
	return nil
}

func validateExternalURL(raw string) error {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return errors.New("must be an absolute https URL")
	}
	host := strings.ToLower(parsed.Hostname())
	if host == "localhost" {
		return errors.New("must not target localhost")
	}
	if ip := net.ParseIP(host); ip != nil && (ip.IsLoopback() || ip.IsUnspecified()) {
		return errors.New("must not target a loopback or unspecified address")
	}
	if parsed.User != nil || parsed.Fragment != "" {
		return errors.New("must not contain userinfo or a fragment")
	}
	if (parsed.Path != "" && parsed.Path != "/") || parsed.RawPath != "" ||
		parsed.RawQuery != "" || parsed.ForceQuery {
		return errors.New("must be an origin URL without a path or query")
	}
	return nil
}

func isDeploymentAdminPassword(password string) bool {
	if len(password) < 16 {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(password)) {
	case "pergo-dev-2026", "troque-esta-senha":
		return false
	default:
		return true
	}
}

func isDeploymentSessionSecret(secret string) bool {
	if len(secret) < 32 {
		return false
	}
	switch secret {
	case "pergo-session-fallback-secret-do-not-use",
		"pergo-dev-session-secret-do-not-use":
		return false
	default:
		return true
	}
}

// validateDeploymentKEK rejects values that are structurally valid AES-256
// keys but are common development defaults, public test vectors, or human
// phrases. Secret provenance still belongs in the deployment secret manager.
func validateDeploymentKEK(kek []byte) error {
	if len(kek) != 32 {
		return errors.New("must contain exactly 32 bytes")
	}

	knownDevelopmentKeys := [][]byte{
		[]byte("dev-development-key-32-bytes-kek"),
		[]byte("0123456789abcdef0123456789abcdef"),
	}
	for _, known := range knownDevelopmentKeys {
		if bytes.Equal(kek, known) {
			return errors.New("known development key is forbidden")
		}
	}

	allSame := true
	printableASCII := true
	distinct := make(map[byte]struct{}, len(kek))
	for i, b := range kek {
		distinct[b] = struct{}{}
		if i > 0 && b != kek[0] {
			allSame = false
		}
		if b < 0x20 || b > 0x7e {
			printableASCII = false
		}
	}
	if allSame {
		return errors.New("repeated-byte key is forbidden")
	}
	if hasRepeatingPeriod(kek) {
		return errors.New("repeating-pattern key is forbidden")
	}
	if printableASCII {
		return errors.New("printable passphrases are forbidden; use 32 random bytes")
	}
	if len(distinct) < 16 {
		return errors.New("insufficient byte diversity; generate 32 random bytes")
	}

	arithmetic := true
	step := kek[1] - kek[0]
	for i := 2; i < len(kek); i++ {
		if kek[i]-kek[i-1] != step {
			arithmetic = false
			break
		}
	}
	if arithmetic {
		return errors.New("arithmetic test vector is forbidden")
	}

	return nil
}

func hasRepeatingPeriod(value []byte) bool {
	for period := 1; period <= len(value)/2; period++ {
		if len(value)%period != 0 {
			continue
		}
		repeated := true
		for i := period; i < len(value); i++ {
			if value[i] != value[i%period] {
				repeated = false
				break
			}
		}
		if repeated {
			return true
		}
	}
	return false
}

// IsDevelopment reports whether insecure local/test defaults are allowed.
func (c *Config) IsDevelopment() bool {
	return isDevelopmentEnvironment(c.Environment)
}

// IsProduction reports whether production-only durability gates apply.
func (c *Config) IsProduction() bool {
	return c.Environment == "production" || c.Environment == "prd"
}

func isDevelopmentEnvironment(environment string) bool {
	switch strings.ToLower(environment) {
	case "development", "dev", "local", "test":
		return true
	default:
		return false
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func splitList(value string) []string {
	var out []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

func boolEnv(key string, fallback bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	return err == nil && parsed
}

func intEnv(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return parsed
}
