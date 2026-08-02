package queue

import (
	"errors"
	"strings"
	"testing"
)

func TestRedactNATSConnectErrorRemovesURLUserInfo(t *testing.T) {
	got := redactNATSConnectError(errors.New(
		"nats: no servers available: nats://alice:topsecret@nats.example:4222, tls://bob:othersecret@nats-2.example:4222",
	))

	for _, secret := range []string{"alice", "topsecret", "bob", "othersecret"} {
		if strings.Contains(got, secret) {
			t.Fatalf("redacted error contains credential %q: %s", secret, got)
		}
	}
	for _, host := range []string{"nats.example:4222", "nats-2.example:4222"} {
		if !strings.Contains(got, host) {
			t.Fatalf("redacted error lost useful host %q: %s", host, got)
		}
	}
}
