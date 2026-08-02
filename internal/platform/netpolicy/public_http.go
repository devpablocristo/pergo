// Package netpolicy provides fail-closed outbound HTTP controls for
// tenant-configured destinations.
package netpolicy

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var ErrUnsafeDestination = errors.New("unsafe outbound destination")

const maxRedirects = 3

// PublicHTTPPolicy resolves and dials only public HTTPS destinations. LookupIP
// is injectable so policy tests never depend on external DNS.
type PublicHTTPPolicy struct {
	LookupIP func(context.Context, string) ([]net.IPAddr, error)
	Dialer   net.Dialer
}

// NewPublicHTTPPolicy returns the production outbound policy.
func NewPublicHTTPPolicy() *PublicHTTPPolicy {
	return &PublicHTTPPolicy{
		LookupIP: net.DefaultResolver.LookupIPAddr,
		Dialer: net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		},
	}
}

// ValidateURL verifies both URL syntax and every currently resolved address.
// The same address policy is enforced again at dial time to prevent DNS
// rebinding between configuration and delivery.
func (p *PublicHTTPPolicy) ValidateURL(ctx context.Context, raw string) error {
	parsed, err := parsePublicHTTPSURL(raw)
	if err != nil {
		return err
	}
	_, err = p.publicAddresses(ctx, parsed.Hostname())
	return err
}

// Client returns an HTTP client that revalidates redirects and dials the
// already-validated public IP directly while preserving TLS SNI for the
// original hostname.
func (p *PublicHTTPPolicy) Client(timeout time.Duration) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialContext = p.dialContext
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}

	return &http.Client{
		Transport: transport,
		Timeout:   timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= maxRedirects {
				return fmt.Errorf("%w: too many redirects", ErrUnsafeDestination)
			}
			return p.ValidateURL(req.Context(), req.URL.String())
		},
	}
}

func (p *PublicHTTPPolicy) dialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid dial address", ErrUnsafeDestination)
	}
	addresses, err := p.publicAddresses(ctx, host)
	if err != nil {
		return nil, err
	}

	var lastErr error
	for _, resolved := range addresses {
		conn, dialErr := p.Dialer.DialContext(ctx, network, net.JoinHostPort(resolved.IP.String(), port))
		if dialErr == nil {
			return conn, nil
		}
		lastErr = dialErr
	}
	if lastErr == nil {
		lastErr = errors.New("destination has no addresses")
	}
	return nil, fmt.Errorf("dial public destination: %w", lastErr)
}

func (p *PublicHTTPPolicy) publicAddresses(ctx context.Context, host string) ([]net.IPAddr, error) {
	host = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
	if host == "" || host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return nil, fmt.Errorf("%w: localhost is forbidden", ErrUnsafeDestination)
	}

	if literal := net.ParseIP(host); literal != nil {
		if !isPublicIP(literal) {
			return nil, fmt.Errorf("%w: non-public IP address", ErrUnsafeDestination)
		}
		return []net.IPAddr{{IP: literal}}, nil
	}
	if p == nil || p.LookupIP == nil {
		return nil, fmt.Errorf("%w: DNS resolver unavailable", ErrUnsafeDestination)
	}
	addresses, err := p.LookupIP(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("resolve outbound destination: %w", err)
	}
	if len(addresses) == 0 {
		return nil, fmt.Errorf("%w: destination has no addresses", ErrUnsafeDestination)
	}
	for _, resolved := range addresses {
		if !isPublicIP(resolved.IP) {
			return nil, fmt.Errorf("%w: destination resolves to a non-public address", ErrUnsafeDestination)
		}
	}
	return addresses, nil
}

func parsePublicHTTPSURL(raw string) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return nil, fmt.Errorf("%w: an absolute HTTPS URL is required", ErrUnsafeDestination)
	}
	if parsed.User != nil || parsed.Fragment != "" {
		return nil, fmt.Errorf("%w: userinfo and fragments are forbidden", ErrUnsafeDestination)
	}
	if parsed.Hostname() == "" {
		return nil, fmt.Errorf("%w: hostname is required", ErrUnsafeDestination)
	}
	return parsed, nil
}

func isPublicIP(ip net.IP) bool {
	if ip == nil ||
		!ip.IsGlobalUnicast() ||
		ip.IsPrivate() ||
		ip.IsLoopback() ||
		ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsMulticast() {
		return false
	}

	for _, raw := range []string{
		"100.64.0.0/10",
		"192.0.0.0/24",
		"192.0.2.0/24",
		"198.18.0.0/15",
		"198.51.100.0/24",
		"203.0.113.0/24",
		"2001:db8::/32",
	} {
		_, reserved, _ := net.ParseCIDR(raw)
		if reserved.Contains(ip) {
			return false
		}
	}
	return true
}
