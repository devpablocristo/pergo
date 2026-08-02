package queue

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// ConnectionConfig contains transport-only NATS settings. Deployment policy is
// validated by internal/config before this adapter is called.
type ConnectionConfig struct {
	URLs            []string
	CredentialsFile string
	CAFile          string
	TLSServerName   string
}

// Connect establishes a NATS connection with optional credentials and a
// TLS-1.2 minimum. It deliberately does not accept credentials embedded in code.
func Connect(cfg ConnectionConfig) (*nats.Conn, error) {
	if len(cfg.URLs) == 0 {
		return nil, fmt.Errorf("no NATS URLs configured")
	}

	options := []nats.Option{
		nats.Name("pergo"),
		nats.Timeout(5 * time.Second),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	}
	if cfg.CredentialsFile != "" {
		options = append(options, nats.UserCredentials(cfg.CredentialsFile))
	}

	if cfg.CAFile != "" || cfg.TLSServerName != "" || usesTLS(cfg.URLs) {
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		if cfg.CAFile != "" {
			pem, err := os.ReadFile(cfg.CAFile)
			if err != nil {
				return nil, fmt.Errorf("read NATS CA file: %w", err)
			}
			if !roots.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("NATS CA file contains no valid certificates")
			}
		}
		options = append(options, nats.Secure(&tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    roots,
			ServerName: cfg.TLSServerName,
		}))
	}

	nc, err := nats.Connect(strings.Join(cfg.URLs, ","), options...)
	if err != nil {
		return nil, fmt.Errorf("connect to NATS: %s", redactNATSConnectError(err))
	}
	return nc, nil
}

var natsURLUserInfoPattern = regexp.MustCompile(`(?i)\b(nats|tls|ws|wss)://[^\s/@]+@`)

func redactNATSConnectError(err error) string {
	if err == nil {
		return "connection failed"
	}
	return natsURLUserInfoPattern.ReplaceAllString(err.Error(), "$1://[redacted]@")
}

func usesTLS(urls []string) bool {
	for _, raw := range urls {
		if strings.HasPrefix(raw, "tls://") || strings.HasPrefix(raw, "wss://") {
			return true
		}
	}
	return false
}
