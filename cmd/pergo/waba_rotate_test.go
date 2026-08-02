package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadMountedSecretIsBoundedAndTrimsNewline(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "secret")
	if err := os.WriteFile(path, []byte("secret-from-manager\n"), 0o600); err != nil {
		t.Fatalf("write secret fixture: %v", err)
	}
	got, err := readMountedSecret(path)
	if err != nil {
		t.Fatalf("readMountedSecret: %v", err)
	}
	if got != "secret-from-manager" {
		t.Fatalf("secret = %q", got)
	}

	oversized := filepath.Join(dir, "oversized")
	if err := os.WriteFile(oversized, []byte(strings.Repeat("x", maxWABASecretFileBytes+1)), 0o600); err != nil {
		t.Fatalf("write oversized fixture: %v", err)
	}
	if _, err := readMountedSecret(oversized); err == nil {
		t.Fatal("readMountedSecret oversized error = nil")
	}
}
