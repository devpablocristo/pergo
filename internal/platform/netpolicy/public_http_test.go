package netpolicy

import (
	"context"
	"errors"
	"net"
	"testing"
)

func TestPublicHTTPPolicyRejectsUnsafeDestinations(t *testing.T) {
	tests := []struct {
		name string
		url  string
		ips  []net.IPAddr
	}{
		{name: "plain HTTP", url: "http://example.com/hook", ips: publicTestAddresses()},
		{name: "userinfo", url: "https://user:pass@example.com/hook", ips: publicTestAddresses()},
		{name: "loopback literal", url: "https://127.0.0.1/hook"},
		{name: "private DNS", url: "https://example.com/hook", ips: []net.IPAddr{{IP: net.ParseIP("10.0.0.5")}}},
		{name: "mixed DNS", url: "https://example.com/hook", ips: []net.IPAddr{{IP: net.ParseIP("8.8.8.8")}, {IP: net.ParseIP("169.254.169.254")}}},
		{name: "carrier NAT", url: "https://100.64.0.1/hook"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := &PublicHTTPPolicy{
				LookupIP: func(context.Context, string) ([]net.IPAddr, error) {
					return tt.ips, nil
				},
			}
			if err := policy.ValidateURL(context.Background(), tt.url); !errors.Is(err, ErrUnsafeDestination) {
				t.Fatalf("error=%v, want ErrUnsafeDestination", err)
			}
		})
	}
}

func TestPublicHTTPPolicyAcceptsPublicHTTPS(t *testing.T) {
	policy := &PublicHTTPPolicy{
		LookupIP: func(context.Context, string) ([]net.IPAddr, error) {
			return publicTestAddresses(), nil
		},
	}
	if err := policy.ValidateURL(context.Background(), "https://hooks.example.com/pergo"); err != nil {
		t.Fatalf("public HTTPS URL rejected: %v", err)
	}
}

func TestPublicHTTPPolicyRevalidatesDNSAtDialTime(t *testing.T) {
	calls := 0
	policy := &PublicHTTPPolicy{
		LookupIP: func(context.Context, string) ([]net.IPAddr, error) {
			calls++
			if calls == 1 {
				return publicTestAddresses(), nil
			}
			return []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, nil
		},
	}
	if err := policy.ValidateURL(context.Background(), "https://hooks.example.com/pergo"); err != nil {
		t.Fatalf("initial validation failed: %v", err)
	}
	if _, err := policy.dialContext(context.Background(), "tcp", "hooks.example.com:443"); !errors.Is(err, ErrUnsafeDestination) {
		t.Fatalf("dial error=%v, want rebinding rejection", err)
	}
}

func publicTestAddresses() []net.IPAddr {
	return []net.IPAddr{{IP: net.ParseIP("8.8.8.8")}}
}
