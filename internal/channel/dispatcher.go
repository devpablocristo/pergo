// Package channel defines the dispatcher interface and registry for
// message channel adapters. Each adapter (WhatsApp Web, WABA, Telegram)
// implements the Dispatcher interface. The Registry provides lookup by
// channel name.
package channel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/pablojhp.pergo/internal/domain"
)

// MessagePayload is the channel-layer message contract, separate from the
// API's CreateMessageRequest. It carries all fields needed for dispatch.
type MessagePayload struct {
	MessageID        string
	ConnectionID     uuid.UUID
	SenderIdentity   string
	TraceID          string
	To               string
	Channel          string
	Body             string
	Media            *domain.Media
	Metadata         map[string]string
	TemplateName     string
	Language         string
	Components       []domain.TemplateComponent
	Interactive      *domain.Interactive
	ChannelOverrides map[string]json.RawMessage
	FallbackBehavior string
}

// Dispatcher sends a message through a specific channel adapter.
// Implementations must be safe for concurrent use.
type Dispatcher interface {
	Dispatch(ctx context.Context, m *MessagePayload) (string, error)
}

// TerminalError marks an error as non-retryable. The worker and routing
// engine use errors.As to detect terminal errors and skip retries or
// advance fallback channels.
type TerminalError struct {
	Err error
}

func (e *TerminalError) Error() string {
	return fmt.Sprintf("terminal: %v", e.Err)
}

func (e *TerminalError) Unwrap() error {
	return e.Err
}

// Terminal returns true — satisfies the Terminal interface.
func (e *TerminalError) Terminal() bool {
	return true
}

// NewTerminalError wraps an error as terminal (non-retryable).
func NewTerminalError(err error) error {
	return &TerminalError{Err: err}
}

// IsTerminal checks if an error is terminal (non-retryable).
func IsTerminal(err error) bool {
	var t interface{ Terminal() bool }
	return errors.As(err, &t) && t.Terminal()
}

// UncertainError marks a provider call whose request may have been accepted
// even though no authoritative response reached PerGo. Such an error must
// never trigger automatic retry or fallback.
type UncertainError struct {
	Err error
}

func (e *UncertainError) Error() string {
	return "uncertain: provider delivery outcome unknown"
}

func (e *UncertainError) Unwrap() error {
	return e.Err
}

// Uncertain returns true for errors with an unobservable provider outcome.
func (e *UncertainError) Uncertain() bool {
	return true
}

// NewUncertainError wraps an error whose provider outcome cannot be proven.
func NewUncertainError(err error) error {
	return &UncertainError{Err: err}
}

// IsUncertain reports whether retry/fallback could duplicate an external side
// effect.
func IsUncertain(err error) bool {
	var uncertain interface{ Uncertain() bool }
	return errors.As(err, &uncertain) && uncertain.Uncertain()
}
