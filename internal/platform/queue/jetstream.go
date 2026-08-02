// Package queue provides JetStream-backed durable message queue primitives.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/pablojhp.pergo/internal/platform/messagebus"
)

const (
	// StreamName is the versioned JetStream stream that holds outbound messages.
	StreamName = "PERGO_V2_OUTBOUND"

	// StreamSubject is the wildcard subject the stream listens on.
	StreamSubject = "pergo.v2.outbound.>"

	WebhookStreamName         = "PERGO_V2_WEBHOOK_EVENTS"
	WebhookStreamSubject      = "pergo.v2.webhook_events.>"
	WebhookDeliveryStreamName = "PERGO_V2_WEBHOOK_DELIVERIES"
	WebhookDeliverySubject    = "pergo.v2.webhook_deliveries.>"
	InboundStreamName         = "PERGO_V2_INBOUND"
	InboundStreamSubject      = "pergo.v2.inbound.>"

	OutboundConsumerName        = "pergo-v2-outbound-worker"
	CampaignConsumerName        = "pergo-v2-campaign-worker"
	WebhookEventConsumerName    = "pergo-v2-webhook-events-worker"
	InboundEventConsumerName    = "pergo-v2-inbound-events-worker"
	WebhookDeliveryConsumerName = "pergo-v2-webhook-deliveries-worker"

	// CampaignStreamName is the versioned JetStream stream that holds campaign
	// batches. Versioning the physical stream prevents rollout from mutating the
	// retention policy or consumers of the legacy CAMPAIGNS stream in place.
	CampaignStreamName = "PERGO_V2_CAMPAIGNS"

	// CampaignSubject is the wildcard subject consumed by CampaignStreamName.
	CampaignSubject = "pergo.v2.campaigns.>"

	// CampaignBatchSubject is the logical subject used by the campaign outbox
	// relay. JetStreamPublisher resolves it to
	// pergo.v2.campaigns.<workspace_id>, preserving tenant capacity isolation.
	CampaignBatchSubject = "pergo.v2.campaigns.batches"

	campaignConsumerAckWait    = 2 * time.Minute
	campaignConsumerMaxPending = 16
	outboundConsumerAckWait    = 2 * time.Minute
	outboundConsumerMaxPending = 100
	webhookConsumerMaxPending  = 128

	// MaxQueueDepth is the durable per-workspace message limit that triggers
	// backpressure. Each workspace is mapped to its own exact NATS subject.
	MaxQueueDepth = 1000

	// MaxEventQueueDepth is the durable per-workspace limit for inbound,
	// webhook-fanout and webhook-delivery queues.
	MaxEventQueueDepth = 10000

	// MessageDuplicateWindow covers PerGo and upstream HTTP retry horizons,
	// including the crash window after JetStream accepted a publish but before
	// the durable HTTP receipt was marked accepted.
	MessageDuplicateWindow = 24 * time.Hour

	// EventDuplicateWindow covers the maximum retained lifetime of inbound and
	// webhook events. Stable event IDs therefore survive a process crash
	// between broker acceptance and database completion.
	EventDuplicateWindow = 7 * 24 * time.Hour

	environmentGuardStream = "PERGO_ENVIRONMENT_GUARD"
)

var perGoDataStreams = map[string]struct{}{
	StreamName:                {},
	WebhookStreamName:         {},
	WebhookDeliveryStreamName: {},
	InboundStreamName:         {},
	CampaignStreamName:        {},
	"MESSAGES":                {},
	"WEBHOOKS":                {},
	"WEBHOOK_DELIVERIES":      {},
	"INBOUND":                 {},
	"CAMPAIGNS":               {},
}

// EnsureEnvironmentIsolation prevents staging and production from accidentally
// sharing one JetStream account. The first workload claims the account with an
// immutable environment/account marker; a conflicting claim fails closed.
func EnsureEnvironmentIsolation(ctx context.Context, nc *nats.Conn, environment, account string, replicas ...int) error {
	if environment == "" || account == "" {
		return fmt.Errorf("environment and NATS account labels are required")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("jetstream.New: %w", err)
	}

	expected := fmt.Sprintf("environment=%s;account=%s", environment, account)
	stream, err := js.Stream(ctx, environmentGuardStream)
	if err == nil {
		info, infoErr := stream.Info(ctx)
		if infoErr != nil {
			return fmt.Errorf("read environment guard: %w", infoErr)
		}
		if info.Config.Description != expected {
			return fmt.Errorf("NATS account isolation conflict: existing %q, requested %q", info.Config.Description, expected)
		}
		desiredReplicas := streamReplicas(replicas)
		if info.Config.Replicas != desiredReplicas {
			cfg := info.Config
			cfg.Replicas = desiredReplicas
			if _, updateErr := js.UpdateStream(ctx, cfg); updateErr != nil {
				return fmt.Errorf("update environment guard replicas to %d: %w", desiredReplicas, updateErr)
			}
		}
		return nil
	}
	if !errors.Is(err, jetstream.ErrStreamNotFound) {
		return fmt.Errorf("read environment guard: %w", err)
	}
	existingStreams, err := existingPerGoStreams(ctx, js)
	if err != nil {
		return fmt.Errorf("inspect unclaimed NATS account: %w", err)
	}
	if len(existingStreams) != 0 {
		return fmt.Errorf(
			"NATS account contains PerGo streams %v but no environment guard; explicit operator migration is required",
			existingStreams,
		)
	}

	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:        environmentGuardStream,
		Description: expected,
		Subjects:    []string{"_pergo.environment.guard"},
		Retention:   jetstream.LimitsPolicy,
		MaxMsgs:     1,
		Storage:     jetstream.FileStorage,
		Replicas:    streamReplicas(replicas),
	})
	if err != nil {
		// A concurrent starter may have won the create race. Re-read and verify.
		stream, readErr := js.Stream(ctx, environmentGuardStream)
		if readErr != nil {
			return fmt.Errorf("create environment guard: %w", err)
		}
		info, infoErr := stream.Info(ctx)
		if infoErr != nil || info.Config.Description != expected {
			return fmt.Errorf("NATS account isolation conflict after concurrent claim")
		}
	}
	return nil
}

// VerifyEnvironmentIsolation is the read-only workload gate. Runtime
// credentials need only stream/consumer read and their role-specific
// publish/consume permissions; only the migration/bootstrap job may call the
// mutating Ensure functions.
func VerifyEnvironmentIsolation(ctx context.Context, nc *nats.Conn, environment, account string, replicas ...int) error {
	if environment == "" || account == "" {
		return fmt.Errorf("environment and NATS account labels are required")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("jetstream.New: %w", err)
	}
	stream, err := js.Stream(ctx, environmentGuardStream)
	if err != nil {
		return fmt.Errorf("environment guard is not bootstrapped: %w", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return fmt.Errorf("read environment guard: %w", err)
	}
	expected := fmt.Sprintf("environment=%s;account=%s", environment, account)
	if info.Config.Description != expected {
		return fmt.Errorf(
			"NATS account isolation conflict: existing %q, requested %q",
			info.Config.Description,
			expected,
		)
	}
	if info.Config.Replicas != streamReplicas(replicas) {
		return fmt.Errorf(
			"environment guard replicas=%d, expected=%d; run bootstrap job",
			info.Config.Replicas,
			streamReplicas(replicas),
		)
	}
	return nil
}

// AdoptDrainedLegacyEnvironmentIsolation creates the guard for a pre-guard
// installation only after every legacy stream is verified empty. Operators
// must quiesce all legacy producers before calling this one-shot gate.
func AdoptDrainedLegacyEnvironmentIsolation(
	ctx context.Context,
	nc *nats.Conn,
	environment string,
	account string,
	replicas ...int,
) error {
	if environment == "" || account == "" {
		return fmt.Errorf("environment and NATS account labels are required")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("jetstream.New: %w", err)
	}
	if _, err := js.Stream(ctx, environmentGuardStream); err == nil {
		return VerifyEnvironmentIsolation(ctx, nc, environment, account, replicas...)
	} else if !errors.Is(err, jetstream.ErrStreamNotFound) {
		return fmt.Errorf("read environment guard: %w", err)
	}

	for _, name := range []string{
		"MESSAGES",
		"WEBHOOKS",
		"WEBHOOK_DELIVERIES",
		"INBOUND",
		"CAMPAIGNS",
	} {
		stream, err := js.Stream(ctx, name)
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			continue
		}
		if err != nil {
			return fmt.Errorf("inspect legacy stream %s: %w", name, err)
		}
		info, err := stream.Info(ctx)
		if err != nil {
			return fmt.Errorf("read legacy stream %s: %w", name, err)
		}
		if info.State.Msgs != 0 {
			return fmt.Errorf(
				"legacy stream %s still has %d messages; drain with old consumers before adoption",
				name,
				info.State.Msgs,
			)
		}
	}
	for _, name := range []string{
		StreamName,
		WebhookStreamName,
		WebhookDeliveryStreamName,
		InboundStreamName,
		CampaignStreamName,
	} {
		if _, err := js.Stream(ctx, name); err == nil {
			return fmt.Errorf("versioned stream %s exists without an environment guard", name)
		} else if !errors.Is(err, jetstream.ErrStreamNotFound) {
			return fmt.Errorf("inspect versioned stream %s: %w", name, err)
		}
	}

	expected := fmt.Sprintf("environment=%s;account=%s", environment, account)
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:        environmentGuardStream,
		Description: expected,
		Subjects:    []string{"_pergo.environment.guard"},
		Retention:   jetstream.LimitsPolicy,
		MaxMsgs:     1,
		Storage:     jetstream.FileStorage,
		Replicas:    streamReplicas(replicas),
	}); err != nil {
		return fmt.Errorf("adopt drained legacy NATS account: %w", err)
	}
	return nil
}

func existingPerGoStreams(ctx context.Context, js jetstream.JetStream) ([]string, error) {
	lister := js.ListStreams(ctx)
	var existing []string
	for info := range lister.Info() {
		if _, owned := perGoDataStreams[info.Config.Name]; owned {
			existing = append(existing, info.Config.Name)
		}
	}
	if err := lister.Err(); err != nil {
		return nil, err
	}
	sort.Strings(existing)
	return existing, nil
}

// EnsureStream creates or updates the versioned outbound WorkQueue stream.
// The stream persists to file storage and rejects new messages only for the
// workspace subject that reached MaxQueueDepth. One noisy tenant therefore
// cannot consume another tenant's queue allowance.
// Safe to call multiple times — CreateOrUpdateStream is idempotent.
func EnsureStream(ctx context.Context, nc *nats.Conn, replicas ...int) (jetstream.Stream, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream.New: %w", err)
	}

	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:                 StreamName,
		Subjects:             []string{StreamSubject},
		Retention:            jetstream.WorkQueuePolicy,
		MaxMsgs:              -1,
		MaxMsgsPerSubject:    MaxQueueDepth,
		Discard:              jetstream.DiscardNew,
		DiscardNewPerSubject: true,
		Storage:              jetstream.FileStorage,
		Replicas:             streamReplicas(replicas),
		MaxAge:               24 * time.Hour,
		Duplicates:           MessageDuplicateWindow,
	})
	if err != nil {
		return nil, fmt.Errorf("create stream %s: %w", StreamName, err)
	}

	slog.Info("jetstream stream ready", "stream", StreamName)
	return stream, nil
}

// EnsureWebhookStream creates or updates the versioned, workspace-isolated
// webhook event WorkQueue stream.
// Safe to call multiple times.
func EnsureWebhookStream(ctx context.Context, nc *nats.Conn, replicas ...int) (jetstream.Stream, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream.New: %w", err)
	}

	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:                 WebhookStreamName,
		Subjects:             []string{WebhookStreamSubject},
		Retention:            jetstream.WorkQueuePolicy,
		MaxMsgs:              -1,
		MaxMsgsPerSubject:    MaxEventQueueDepth,
		Discard:              jetstream.DiscardNew,
		DiscardNewPerSubject: true,
		Storage:              jetstream.FileStorage,
		Replicas:             streamReplicas(replicas),
		MaxAge:               EventDuplicateWindow,
		Duplicates:           EventDuplicateWindow,
	})
	if err != nil {
		return nil, fmt.Errorf("create stream %s: %w", WebhookStreamName, err)
	}

	slog.Info("jetstream webhook stream ready", "stream", WebhookStreamName)
	return stream, nil
}

// EnsureWebhookDeliveryStream creates or updates the versioned webhook
// delivery stream.
func EnsureWebhookDeliveryStream(ctx context.Context, nc *nats.Conn, replicas ...int) (jetstream.Stream, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream.New: %w", err)
	}

	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:                 WebhookDeliveryStreamName,
		Subjects:             []string{WebhookDeliverySubject},
		Retention:            jetstream.WorkQueuePolicy,
		MaxMsgs:              -1,
		MaxMsgsPerSubject:    MaxEventQueueDepth,
		Discard:              jetstream.DiscardNew,
		DiscardNewPerSubject: true,
		Storage:              jetstream.FileStorage,
		Replicas:             streamReplicas(replicas),
		MaxAge:               24 * time.Hour,
		Duplicates:           24 * time.Hour,
	})
	if err != nil {
		return nil, fmt.Errorf("create stream %s: %w", WebhookDeliveryStreamName, err)
	}

	slog.Info("jetstream webhook deliveries stream ready", "stream", WebhookDeliveryStreamName)
	return stream, nil
}

// EnsureInboundStream creates or updates the versioned, workspace-isolated
// inbound event WorkQueue stream.
func EnsureInboundStream(ctx context.Context, nc *nats.Conn, replicas ...int) (jetstream.Stream, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream.New: %w", err)
	}

	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:                 InboundStreamName,
		Subjects:             []string{InboundStreamSubject},
		Retention:            jetstream.WorkQueuePolicy,
		MaxMsgs:              -1,
		MaxMsgsPerSubject:    MaxEventQueueDepth,
		Discard:              jetstream.DiscardNew,
		DiscardNewPerSubject: true,
		Storage:              jetstream.FileStorage,
		Replicas:             streamReplicas(replicas),
		MaxAge:               EventDuplicateWindow,
		Duplicates:           EventDuplicateWindow,
	})
	if err != nil {
		return nil, fmt.Errorf("create stream %s: %w", InboundStreamName, err)
	}

	slog.Info("jetstream inbound stream ready", "stream", InboundStreamName)
	return stream, nil
}

// BootstrapJetStream is the only production entrypoint allowed to mutate
// stream and consumer configuration. It must run from the dedicated migration
// job with JetStream admin credentials before workload rollout.
func BootstrapJetStream(
	ctx context.Context,
	nc *nats.Conn,
	environment string,
	account string,
	replicas ...int,
) error {
	if err := EnsureEnvironmentIsolation(ctx, nc, environment, account, replicas...); err != nil {
		return err
	}
	outbound, err := EnsureStream(ctx, nc, replicas...)
	if err != nil {
		return err
	}
	if _, err := EnsureConsumer(ctx, outbound, OutboundConsumerName); err != nil {
		return err
	}
	webhookEvents, err := EnsureWebhookStream(ctx, nc, replicas...)
	if err != nil {
		return err
	}
	if _, err := ensureConsumer(
		ctx,
		webhookEvents,
		webhookConsumerConfig(
			WebhookEventConsumerName,
			WebhookStreamSubject,
			"PerGo v2 webhook fanout consumer",
		),
	); err != nil {
		return err
	}
	inboundEvents, err := EnsureInboundStream(ctx, nc, replicas...)
	if err != nil {
		return err
	}
	if _, err := ensureConsumer(
		ctx,
		inboundEvents,
		webhookConsumerConfig(
			InboundEventConsumerName,
			InboundStreamSubject,
			"PerGo v2 inbound webhook fanout consumer",
		),
	); err != nil {
		return err
	}
	webhookDeliveries, err := EnsureWebhookDeliveryStream(ctx, nc, replicas...)
	if err != nil {
		return err
	}
	if _, err := ensureConsumer(
		ctx,
		webhookDeliveries,
		webhookConsumerConfig(
			WebhookDeliveryConsumerName,
			WebhookDeliverySubject,
			"PerGo v2 webhook delivery consumer",
		),
	); err != nil {
		return err
	}
	campaigns, err := EnsureCampaignStream(ctx, nc, replicas...)
	if err != nil {
		return err
	}
	if _, err := EnsureCampaignConsumer(ctx, campaigns, CampaignConsumerName); err != nil {
		return err
	}
	return nil
}

func ensureConsumer(
	ctx context.Context,
	stream jetstream.Stream,
	cfg jetstream.ConsumerConfig,
) (jetstream.Consumer, error) {
	consumer, err := createConsumerWithRetry(ctx, stream, cfg)
	if err != nil {
		return nil, fmt.Errorf("bootstrap consumer %s: %w", cfg.Durable, err)
	}
	return consumer, nil
}

func ensureWebhookWorkerResources(
	ctx context.Context,
	nc *nats.Conn,
	replicas ...int,
) error {
	webhookEvents, err := EnsureWebhookStream(ctx, nc, replicas...)
	if err != nil {
		return err
	}
	if _, err := ensureConsumer(
		ctx,
		webhookEvents,
		webhookConsumerConfig(
			WebhookEventConsumerName,
			WebhookStreamSubject,
			"PerGo v2 webhook fanout consumer",
		),
	); err != nil {
		return err
	}
	inboundEvents, err := EnsureInboundStream(ctx, nc, replicas...)
	if err != nil {
		return err
	}
	if _, err := ensureConsumer(
		ctx,
		inboundEvents,
		webhookConsumerConfig(
			InboundEventConsumerName,
			InboundStreamSubject,
			"PerGo v2 inbound webhook fanout consumer",
		),
	); err != nil {
		return err
	}
	deliveries, err := EnsureWebhookDeliveryStream(ctx, nc, replicas...)
	if err != nil {
		return err
	}
	_, err = ensureConsumer(
		ctx,
		deliveries,
		webhookConsumerConfig(
			WebhookDeliveryConsumerName,
			WebhookDeliverySubject,
			"PerGo v2 webhook delivery consumer",
		),
	)
	return err
}

func outboundConsumerConfig(name string) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:       name,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       outboundConsumerAckWait,
		// The orchestrator owns the retry budget and must receive the final
		// delivery so it can persist the terminal state and outbox event.
		MaxDeliver:    -1,
		MaxAckPending: outboundConsumerMaxPending,
		FilterSubject: StreamSubject,
		ReplayPolicy:  jetstream.ReplayInstantPolicy,
	}
}

func campaignConsumerConfig(name string) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:       name,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       campaignConsumerAckWait,
		MaxDeliver:    5,
		MaxAckPending: campaignConsumerMaxPending,
		FilterSubject: CampaignSubject,
		ReplayPolicy:  jetstream.ReplayInstantPolicy,
	}
}

func webhookConsumerConfig(name, filter, description string) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:       name,
		Description:   description,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       webhookAckWait,
		MaxDeliver:    -1,
		MaxAckPending: webhookConsumerMaxPending,
		FilterSubject: filter,
		ReplayPolicy:  jetstream.ReplayInstantPolicy,
	}
}

func consumerConfigFor(name, filter string) (jetstream.ConsumerConfig, error) {
	switch name {
	case OutboundConsumerName:
		cfg := outboundConsumerConfig(name)
		if filter != cfg.FilterSubject {
			return jetstream.ConsumerConfig{}, fmt.Errorf("consumer %s has unexpected filter %q", name, filter)
		}
		return cfg, nil
	case CampaignConsumerName:
		cfg := campaignConsumerConfig(name)
		if filter != cfg.FilterSubject {
			return jetstream.ConsumerConfig{}, fmt.Errorf("consumer %s has unexpected filter %q", name, filter)
		}
		return cfg, nil
	case WebhookEventConsumerName:
		return webhookConsumerConfig(name, WebhookStreamSubject, "PerGo v2 webhook fanout consumer"), nil
	case InboundEventConsumerName:
		return webhookConsumerConfig(name, InboundStreamSubject, "PerGo v2 inbound webhook fanout consumer"), nil
	case WebhookDeliveryConsumerName:
		return webhookConsumerConfig(name, WebhookDeliverySubject, "PerGo v2 webhook delivery consumer"), nil
	default:
		return jetstream.ConsumerConfig{}, fmt.Errorf("consumer %s is not an owned v2 consumer", name)
	}
}

func streamContractFor(name, subject string, replicas int) (jetstream.StreamConfig, error) {
	var expected jetstream.StreamConfig
	switch name {
	case StreamName:
		expected = jetstream.StreamConfig{
			Name:                 name,
			Subjects:             []string{StreamSubject},
			Retention:            jetstream.WorkQueuePolicy,
			MaxMsgs:              -1,
			MaxMsgsPerSubject:    MaxQueueDepth,
			Discard:              jetstream.DiscardNew,
			DiscardNewPerSubject: true,
			Storage:              jetstream.FileStorage,
			Replicas:             replicas,
			MaxAge:               24 * time.Hour,
			Duplicates:           MessageDuplicateWindow,
		}
	case WebhookStreamName:
		expected = jetstream.StreamConfig{
			Name:                 name,
			Subjects:             []string{WebhookStreamSubject},
			Retention:            jetstream.WorkQueuePolicy,
			MaxMsgs:              -1,
			MaxMsgsPerSubject:    MaxEventQueueDepth,
			Discard:              jetstream.DiscardNew,
			DiscardNewPerSubject: true,
			Storage:              jetstream.FileStorage,
			Replicas:             replicas,
			MaxAge:               EventDuplicateWindow,
			Duplicates:           EventDuplicateWindow,
		}
	case InboundStreamName:
		expected = jetstream.StreamConfig{
			Name:                 name,
			Subjects:             []string{InboundStreamSubject},
			Retention:            jetstream.WorkQueuePolicy,
			MaxMsgs:              -1,
			MaxMsgsPerSubject:    MaxEventQueueDepth,
			Discard:              jetstream.DiscardNew,
			DiscardNewPerSubject: true,
			Storage:              jetstream.FileStorage,
			Replicas:             replicas,
			MaxAge:               EventDuplicateWindow,
			Duplicates:           EventDuplicateWindow,
		}
	case WebhookDeliveryStreamName:
		expected = jetstream.StreamConfig{
			Name:                 name,
			Subjects:             []string{WebhookDeliverySubject},
			Retention:            jetstream.WorkQueuePolicy,
			MaxMsgs:              -1,
			MaxMsgsPerSubject:    MaxEventQueueDepth,
			Discard:              jetstream.DiscardNew,
			DiscardNewPerSubject: true,
			Storage:              jetstream.FileStorage,
			Replicas:             replicas,
			MaxAge:               24 * time.Hour,
			Duplicates:           24 * time.Hour,
		}
	case CampaignStreamName:
		expected = jetstream.StreamConfig{
			Name:                 name,
			Subjects:             []string{CampaignSubject},
			Retention:            jetstream.WorkQueuePolicy,
			MaxMsgs:              -1,
			MaxMsgsPerSubject:    MaxQueueDepth,
			Discard:              jetstream.DiscardNew,
			DiscardNewPerSubject: true,
			Storage:              jetstream.FileStorage,
			Replicas:             replicas,
			MaxAge:               24 * time.Hour,
			Duplicates:           MessageDuplicateWindow,
		}
	default:
		return jetstream.StreamConfig{}, fmt.Errorf("stream %s is not an owned v2 stream", name)
	}
	if len(expected.Subjects) != 1 || subject != expected.Subjects[0] {
		return jetstream.StreamConfig{}, fmt.Errorf("stream %s has unexpected subject %q", name, subject)
	}
	return expected, nil
}

func validateStreamContract(actual, expected jetstream.StreamConfig) error {
	if actual.Name != expected.Name ||
		len(actual.Subjects) != 1 ||
		actual.Subjects[0] != expected.Subjects[0] ||
		actual.Retention != expected.Retention ||
		actual.MaxMsgs != expected.MaxMsgs ||
		actual.MaxMsgsPerSubject != expected.MaxMsgsPerSubject ||
		actual.Discard != expected.Discard ||
		actual.DiscardNewPerSubject != expected.DiscardNewPerSubject ||
		actual.Storage != expected.Storage ||
		actual.Replicas != expected.Replicas ||
		actual.MaxAge != expected.MaxAge ||
		actual.Duplicates != expected.Duplicates {
		return errors.New("stream contract mismatch")
	}
	return nil
}

func validateConsumerContract(actual, expected jetstream.ConsumerConfig) error {
	if actual.Durable != expected.Durable ||
		actual.FilterSubject != expected.FilterSubject ||
		actual.DeliverPolicy != expected.DeliverPolicy ||
		actual.AckPolicy != expected.AckPolicy ||
		actual.AckWait != expected.AckWait ||
		actual.MaxDeliver != expected.MaxDeliver ||
		actual.MaxAckPending != expected.MaxAckPending ||
		actual.ReplayPolicy != expected.ReplayPolicy {
		return errors.New("consumer contract mismatch")
	}
	return nil
}

// BindVersionedStream returns an already-bootstrapped stream without mutating
// broker state.
func BindVersionedStream(
	ctx context.Context,
	nc *nats.Conn,
	name string,
	subject string,
	replicas ...int,
) (jetstream.Stream, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream.New: %w", err)
	}
	stream, err := js.Stream(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("stream %s is not bootstrapped: %w", name, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("read stream %s: %w", name, err)
	}
	expected, err := streamContractFor(name, subject, streamReplicas(replicas))
	if err != nil {
		return nil, err
	}
	if err := validateStreamContract(info.Config, expected); err != nil {
		return nil, fmt.Errorf("stream %s configuration drift; run bootstrap job: %w", name, err)
	}
	return stream, nil
}

// BindConsumer returns an existing durable only when its filter and delivery
// safety settings match the bootstrapped contract.
func BindConsumer(
	ctx context.Context,
	stream jetstream.Stream,
	name string,
	filter string,
) (jetstream.Consumer, error) {
	consumer, err := stream.Consumer(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("consumer %s is not bootstrapped: %w", name, err)
	}
	info, err := consumer.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("read consumer %s: %w", name, err)
	}
	expected, err := consumerConfigFor(name, filter)
	if err != nil {
		return nil, err
	}
	if err := validateConsumerContract(info.Config, expected); err != nil {
		return nil, fmt.Errorf("consumer %s configuration drift; run bootstrap job: %w", name, err)
	}
	return consumer, nil
}

// EnsureConsumer creates or gets a durable pull consumer on the given stream.
// It is used only by the bootstrap job and reconciles the complete owned
// consumer contract. It never deletes unrelated consumers.
func EnsureConsumer(ctx context.Context, stream jetstream.Stream, consumerName string) (jetstream.Consumer, error) {
	cfg := outboundConsumerConfig(consumerName)
	cons, err := createConsumerWithRetry(ctx, stream, cfg)
	if err != nil {
		return nil, fmt.Errorf("create consumer %s: %w", consumerName, err)
	}
	slog.Info("jetstream consumer ready", "consumer", consumerName)
	return cons, nil
}

func createConsumerWithRetry(ctx context.Context, stream jetstream.Stream, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	var cons jetstream.Consumer
	var err error
	for i := 0; i < 3; i++ {
		cons, err = stream.CreateOrUpdateConsumer(ctx, cfg)
		if err == nil {
			return cons, nil
		}
		slog.Warn("failed to create/update consumer, retrying after delete", "consumer", cfg.Durable, "attempt", i+1, "error", err)
		_ = stream.DeleteConsumer(ctx, cfg.Durable)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return nil, err
}

const (
	outboundBaseSubject               = "messages.outbound"
	webhookEventBaseSubject           = "webhooks.events"
	inboundEventSubjectPrefix         = "inbound.events."
	webhookDeliverySubjectPrefix      = "webhooks.deliveries."
	maxMessagesPerSubjectExceededCode = jetstream.ErrorCode(10077)
)

// JetStreamPublisher publishes outbound messages to a workspace-scoped
// JetStream subject. Each publish carries a Nats-Msg-Id header set to the
// caller's stable trace ID for publish-side idempotency.
type JetStreamPublisher struct {
	js jetstream.JetStream
}

// NewJetStreamPublisher wraps a JetStream instance for publishing.
func NewJetStreamPublisher(nc *nats.Conn) *JetStreamPublisher {
	js, err := jetstream.New(nc)
	if err != nil {
		// This should never fail if nc is connected and JetStream-enabled.
		slog.Error("failed to create jetstream instance for publisher", "error", err)
	}
	return &JetStreamPublisher{js: js}
}

// Publish sends data to the given subject with a Nats-Msg-Id header set to
// traceID for dedup. Returns an error if the stream is full (DiscardNew) or
// the connection is broken.
func (p *JetStreamPublisher) Publish(ctx context.Context, subject string, data []byte, traceID string) error {
	if len(data) > messagebus.MaxPayloadBytes {
		return messagebus.ErrPayloadTooLarge
	}
	resolvedSubject, workspaceID, err := workspaceScopedSubject(subject, data)
	if err != nil {
		return err
	}

	msg := nats.NewMsg(resolvedSubject)
	msg.Data = data
	if traceID != "" {
		msgID := traceID
		if workspaceID != uuid.Nil {
			msgID = workspaceID.String() + ":" + traceID
		}
		msg.Header.Set("Nats-Msg-Id", msgID)
	}

	_, err = p.js.PublishMsg(ctx, msg)
	if err != nil {
		var apiErr *jetstream.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode == maxMessagesPerSubjectExceededCode {
			return fmt.Errorf("%w: %s", messagebus.ErrWorkspaceQueueCapacity, resolvedSubject)
		}
		return fmt.Errorf("publish to %s: %w", resolvedSubject, err)
	}
	return nil
}

func workspaceScopedSubject(subject string, data []byte) (string, uuid.UUID, error) {
	var physicalPrefix string
	switch {
	case subject == outboundBaseSubject:
		physicalPrefix = "pergo.v2.outbound."
	case subject == webhookEventBaseSubject:
		physicalPrefix = "pergo.v2.webhook_events."
	case strings.HasPrefix(subject, inboundEventSubjectPrefix):
		physicalPrefix = "pergo.v2.inbound."
	case strings.HasPrefix(subject, webhookDeliverySubjectPrefix):
		physicalPrefix = "pergo.v2.webhook_deliveries."
	case subject == CampaignBatchSubject:
		physicalPrefix = "pergo.v2.campaigns."
	default:
		return subject, uuid.Nil, nil
	}

	workspaceID, err := workspaceIDFromPayload(data)
	if err != nil {
		return "", uuid.Nil, err
	}
	return physicalPrefix + workspaceID.String(), workspaceID, nil
}

func workspaceIDFromPayload(data []byte) (uuid.UUID, error) {
	var envelope struct {
		WorkspaceID uuid.UUID `json:"workspace_id"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return uuid.Nil, fmt.Errorf("resolve workspace message subject: %w", err)
	}
	if envelope.WorkspaceID == uuid.Nil {
		return uuid.Nil, fmt.Errorf("resolve workspace message subject: workspace_id is required")
	}
	return envelope.WorkspaceID, nil
}

// EnsureCampaignStream creates or updates the versioned campaign WorkQueuePolicy stream.
func EnsureCampaignStream(ctx context.Context, nc *nats.Conn, replicas ...int) (jetstream.Stream, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream.New: %w", err)
	}

	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:                 CampaignStreamName,
		Subjects:             []string{CampaignSubject},
		Retention:            jetstream.WorkQueuePolicy,
		MaxMsgs:              -1,
		MaxMsgsPerSubject:    MaxQueueDepth,
		Discard:              jetstream.DiscardNew,
		DiscardNewPerSubject: true,
		Storage:              jetstream.FileStorage,
		Replicas:             streamReplicas(replicas),
		MaxAge:               24 * time.Hour,
		Duplicates:           MessageDuplicateWindow,
	})
	if err != nil {
		return nil, fmt.Errorf("create stream %s: %w", CampaignStreamName, err)
	}

	slog.Info("jetstream campaign stream ready", "stream", CampaignStreamName)
	return stream, nil
}

func streamReplicas(values []int) int {
	if len(values) > 0 && values[0] > 0 {
		return values[0]
	}
	return 1
}

// EnsureCampaignConsumer creates or updates the durable pull consumer for the
// versioned campaign stream. Multiple in-flight batches prevent one tenant's
// slow provider call from globally blocking every other tenant.
func EnsureCampaignConsumer(ctx context.Context, stream jetstream.Stream, consumerName string) (jetstream.Consumer, error) {
	cfg := campaignConsumerConfig(consumerName)

	cons, err := createConsumerWithRetry(ctx, stream, cfg)
	if err != nil {
		return nil, fmt.Errorf("create campaigns consumer %s: %w", consumerName, err)
	}
	slog.Info("jetstream campaigns consumer ready", "consumer", consumerName)
	return cons, nil
}
