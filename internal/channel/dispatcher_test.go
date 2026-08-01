package channel

import (
	"errors"
	"testing"
)

func TestTerminalError(t *testing.T) {
	inner := errors.New("account banned")
	term := NewTerminalError(inner)

	if !IsTerminal(term) {
		t.Error("expected IsTerminal to return true")
	}
	if term.Error() != "terminal: account banned" {
		t.Errorf("unexpected error message: %s", term.Error())
	}
	// errors.As should work
	var te *TerminalError
	if !errors.As(term, &te) {
		t.Error("errors.As should unwrap to TerminalError")
	}
	if !te.Terminal() {
		t.Error("Terminal() should return true")
	}
}

func TestIsTerminalNonTerminal(t *testing.T) {
	err := errors.New("regular error")
	if IsTerminal(err) {
		t.Error("regular error should not be terminal")
	}
}

func TestIsTerminalNil(t *testing.T) {
	if IsTerminal(nil) {
		t.Error("nil error should not be terminal")
	}
}

func TestUncertainError(t *testing.T) {
	inner := errors.New("provider response lost at https://example.invalid/bot-secret/send")
	uncertain := NewUncertainError(inner)

	if !IsUncertain(uncertain) {
		t.Fatal("expected IsUncertain to return true")
	}
	if IsTerminal(uncertain) {
		t.Fatal("uncertain error must not be terminal/fallback-safe")
	}
	if uncertain.Error() != "uncertain: provider delivery outcome unknown" {
		t.Fatalf("unexpected error message: %s", uncertain.Error())
	}
	if errors.Is(uncertain, inner) == false {
		t.Fatal("uncertain error must retain its cause for programmatic inspection")
	}
	var target *UncertainError
	if !errors.As(uncertain, &target) {
		t.Fatal("errors.As should unwrap to UncertainError")
	}
}
