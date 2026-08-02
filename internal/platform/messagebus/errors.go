// Package messagebus contains transport-neutral messaging boundary errors.
package messagebus

import "errors"

// MaxPayloadBytes is the application-level envelope ceiling. It leaves more
// than 100 KiB below NATS' default 1 MiB max_payload for subjects, headers and
// protocol overhead.
const MaxPayloadBytes = 900 << 10

// ErrWorkspaceQueueCapacity means the durable queue for one workspace reached
// its configured admission limit. Callers may safely translate this to
// backpressure without exposing broker-specific details.
var ErrWorkspaceQueueCapacity = errors.New("workspace queue capacity exceeded")

// ErrPayloadTooLarge means a serialized message cannot fit the supported NATS
// transport contract and must never be retried unchanged.
var ErrPayloadTooLarge = errors.New("message payload exceeds queue transport limit")
