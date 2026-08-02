package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/pablojhp.pergo/internal/channel"
	"github.com/pablojhp.pergo/internal/domain"
	"github.com/pablojhp.pergo/internal/platform/messagebus"
	"github.com/pablojhp.pergo/internal/repository"
)

// connectNATS dials a local NATS server. Returns nil if unavailable.
func connectNATS(t *testing.T) *nats.Conn {
	t.Helper()
	url := os.Getenv("PERGO_NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}
	nc, err := nats.Connect(url,
		nats.Timeout(2*time.Second),
	)
	if err != nil {
		t.Skipf("NATS not available at %s: %v", url, err)
	}
	t.Cleanup(func() { nc.Close() })
	return nc
}

func TestPublishRejectsOversizedPayloadBeforeBrokerAccess(t *testing.T) {
	publisher := &JetStreamPublisher{}
	err := publisher.Publish(
		context.Background(),
		"messages.outbound",
		make([]byte, messagebus.MaxPayloadBytes+1),
		"oversized",
	)
	if !errors.Is(err, messagebus.ErrPayloadTooLarge) {
		t.Fatalf("error = %v, want ErrPayloadTooLarge", err)
	}
}

func TestCampaignStreamV2DoesNotMutateLegacyProtocol(t *testing.T) {
	nc := connectNATS(t)
	ctx := context.Background()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	_ = js.DeleteStream(ctx, "CAMPAIGNS")
	_ = js.DeleteStream(ctx, CampaignStreamName)
	t.Cleanup(func() {
		_ = js.DeleteStream(context.Background(), "CAMPAIGNS")
		_ = js.DeleteStream(context.Background(), CampaignStreamName)
	})

	legacy, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "CAMPAIGNS",
		Subjects:  []string{"campaigns.>"},
		Retention: jetstream.LimitsPolicy,
		Storage:   jetstream.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("create legacy stream: %v", err)
	}

	versioned, err := EnsureCampaignStream(ctx, nc)
	if err != nil {
		t.Fatalf("EnsureCampaignStream: %v", err)
	}
	versionedInfo, err := versioned.Info(ctx)
	if err != nil {
		t.Fatalf("versioned stream info: %v", err)
	}
	if versionedInfo.Config.Name != CampaignStreamName ||
		fmt.Sprint(versionedInfo.Config.Subjects) != fmt.Sprint([]string{CampaignSubject}) ||
		versionedInfo.Config.Retention != jetstream.WorkQueuePolicy ||
		versionedInfo.Config.MaxMsgs != -1 ||
		versionedInfo.Config.MaxMsgsPerSubject != MaxQueueDepth ||
		!versionedInfo.Config.DiscardNewPerSubject {
		t.Fatalf("versioned campaign config = %+v", versionedInfo.Config)
	}

	legacyInfo, err := legacy.Info(ctx)
	if err != nil {
		t.Fatalf("legacy stream info: %v", err)
	}
	if legacyInfo.Config.Retention != jetstream.LimitsPolicy ||
		fmt.Sprint(legacyInfo.Config.Subjects) != fmt.Sprint([]string{"campaigns.>"}) {
		t.Fatalf("legacy campaign stream was mutated: %+v", legacyInfo.Config)
	}

	consumer, err := EnsureCampaignConsumer(ctx, versioned, "campaign-v2-config-"+uuid.NewString())
	if err != nil {
		t.Fatalf("EnsureCampaignConsumer: %v", err)
	}
	consumerInfo, err := consumer.Info(ctx)
	if err != nil {
		t.Fatalf("campaign consumer info: %v", err)
	}
	if consumerInfo.Config.AckWait != campaignConsumerAckWait {
		t.Fatalf("campaign AckWait = %s, want %s", consumerInfo.Config.AckWait, campaignConsumerAckWait)
	}
	if consumerInfo.Config.MaxAckPending != campaignConsumerMaxPending {
		t.Fatalf(
			"campaign MaxAckPending = %d, want %d",
			consumerInfo.Config.MaxAckPending,
			campaignConsumerMaxPending,
		)
	}
}

func TestEnsureStream(t *testing.T) {
	nc := connectNATS(t)
	ctx := context.Background()

	stream, err := EnsureStream(ctx, nc)
	if err != nil {
		t.Fatalf("EnsureStream failed: %v", err)
	}

	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("stream.Info failed: %v", err)
	}

	if info.Config.Name != StreamName {
		t.Errorf("stream name = %q, want %q", info.Config.Name, StreamName)
	}
	if info.Config.Retention != jetstream.WorkQueuePolicy {
		t.Errorf("retention = %v, want WorkQueuePolicy", info.Config.Retention)
	}
	if info.Config.MaxMsgs != -1 {
		t.Errorf("MaxMsgs = %d, want unlimited (-1)", info.Config.MaxMsgs)
	}
	if info.Config.MaxMsgsPerSubject != MaxQueueDepth {
		t.Errorf("MaxMsgsPerSubject = %d, want %d", info.Config.MaxMsgsPerSubject, MaxQueueDepth)
	}
	if info.Config.Discard != jetstream.DiscardNew {
		t.Errorf("Discard = %v, want DiscardNew", info.Config.Discard)
	}
	if !info.Config.DiscardNewPerSubject {
		t.Error("DiscardNewPerSubject = false, want true")
	}
	if info.Config.Duplicates != MessageDuplicateWindow {
		t.Errorf(
			"Duplicates = %v, want %v",
			info.Config.Duplicates,
			MessageDuplicateWindow,
		)
	}
	if info.Config.Replicas != 1 {
		t.Errorf("Replicas = %d, want 1", info.Config.Replicas)
	}
}

func TestEnsureEnvironmentIsolationRejectsConflictingEnvironment(t *testing.T) {
	nc := connectNATS(t)
	ctx := context.Background()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	deletePerGoStreams(t, js)

	if err := EnsureEnvironmentIsolation(ctx, nc, "staging", "pymes-stg", 1); err != nil {
		t.Fatalf("first claim failed: %v", err)
	}
	if err := EnsureEnvironmentIsolation(ctx, nc, "staging", "pymes-stg", 1); err != nil {
		t.Fatalf("idempotent claim failed: %v", err)
	}
	if err := EnsureEnvironmentIsolation(ctx, nc, "production", "pymes-prd", 1); err == nil {
		t.Fatal("conflicting environment unexpectedly reused the same NATS account")
	}
}

func TestEnsureEnvironmentIsolationRejectsLegacyUnclaimedStreams(t *testing.T) {
	nc := connectNATS(t)
	ctx := context.Background()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	deletePerGoStreams(t, js)
	if _, err := EnsureStream(ctx, nc); err != nil {
		t.Fatalf("create legacy stream: %v", err)
	}

	err = EnsureEnvironmentIsolation(ctx, nc, "production", "pymes-prd", 1)
	if err == nil || !strings.Contains(err.Error(), "explicit operator migration") {
		t.Fatalf("legacy unclaimed account error = %v", err)
	}
}

func TestAdoptLegacyEnvironmentRequiresVerifiedEmptyBacklog(t *testing.T) {
	nc := connectNATS(t)
	ctx := context.Background()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	deletePerGoStreams(t, js)
	legacy, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "MESSAGES",
		Subjects:  []string{"messages.>"},
		Retention: jetstream.WorkQueuePolicy,
		Storage:   jetstream.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("create legacy stream: %v", err)
	}
	if _, err := js.Publish(ctx, "messages.outbound", []byte(`{"legacy":true}`)); err != nil {
		t.Fatalf("publish legacy backlog: %v", err)
	}
	if err := AdoptDrainedLegacyEnvironmentIsolation(
		ctx,
		nc,
		"staging",
		"pymes-stg",
		1,
	); err == nil || !strings.Contains(err.Error(), "drain with old consumers") {
		t.Fatalf("non-empty adoption error=%v", err)
	}
	if err := legacy.Purge(ctx); err != nil {
		t.Fatalf("purge legacy test stream: %v", err)
	}
	if err := AdoptDrainedLegacyEnvironmentIsolation(
		ctx,
		nc,
		"staging",
		"pymes-stg",
		1,
	); err != nil {
		t.Fatalf("adopt empty legacy account: %v", err)
	}
	if err := BootstrapJetStream(ctx, nc, "staging", "pymes-stg", 1); err != nil {
		t.Fatalf("bootstrap versioned streams after adoption: %v", err)
	}
	info, err := legacy.Info(ctx)
	if err != nil {
		t.Fatalf("legacy stream disappeared: %v", err)
	}
	if info.Config.Name != "MESSAGES" ||
		info.Config.Retention != jetstream.WorkQueuePolicy ||
		len(info.Config.Subjects) != 1 ||
		info.Config.Subjects[0] != "messages.>" {
		t.Fatalf("bootstrap mutated legacy stream: %+v", info.Config)
	}
}

func deletePerGoStreams(t *testing.T, js jetstream.JetStream) {
	t.Helper()
	names := []string{
		environmentGuardStream,
		StreamName,
		WebhookStreamName,
		WebhookDeliveryStreamName,
		InboundStreamName,
		CampaignStreamName,
		"MESSAGES",
		"WEBHOOKS",
		"WEBHOOK_DELIVERIES",
		"INBOUND",
		"CAMPAIGNS",
	}
	for _, name := range names {
		_ = js.DeleteStream(context.Background(), name)
	}
	t.Cleanup(func() {
		for _, name := range names {
			_ = js.DeleteStream(context.Background(), name)
		}
	})
}

func TestEnsureStreamIdempotent(t *testing.T) {
	nc := connectNATS(t)
	ctx := context.Background()

	_, err := EnsureStream(ctx, nc)
	if err != nil {
		t.Fatalf("first EnsureStream failed: %v", err)
	}

	// Second call should succeed without error
	_, err = EnsureStream(ctx, nc)
	if err != nil {
		t.Fatalf("second EnsureStream failed (should be idempotent): %v", err)
	}
}

func TestEventStreamsRetainStableDedupWindow(t *testing.T) {
	nc := connectNATS(t)
	ctx := context.Background()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	_ = js.DeleteStream(ctx, InboundStreamName)
	_ = js.DeleteStream(ctx, WebhookStreamName)

	inboundStream, err := EnsureInboundStream(ctx, nc)
	if err != nil {
		t.Fatalf("EnsureInboundStream: %v", err)
	}
	webhookStream, err := EnsureWebhookStream(ctx, nc)
	if err != nil {
		t.Fatalf("EnsureWebhookStream: %v", err)
	}
	for name, stream := range map[string]jetstream.Stream{
		InboundStreamName: inboundStream,
		WebhookStreamName: webhookStream,
	} {
		info, err := stream.Info(ctx)
		if err != nil {
			t.Fatalf("%s info: %v", name, err)
		}
		if info.Config.Duplicates != EventDuplicateWindow {
			t.Fatalf(
				"%s duplicate window=%s, want %s",
				name,
				info.Config.Duplicates,
				EventDuplicateWindow,
			)
		}
	}
}

func TestPublishAndConsume(t *testing.T) {
	nc := connectNATS(t)
	ctx := context.Background()

	// Clean slate: delete and recreate the stream
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	_ = js.DeleteStream(ctx, StreamName)

	stream, err := EnsureStream(ctx, nc)
	if err != nil {
		t.Fatalf("EnsureStream failed: %v", err)
	}

	// Create a consumer to read messages
	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   "test-consumer",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("CreateOrUpdateConsumer failed: %v", err)
	}

	// Publish a message
	publisher := NewJetStreamPublisher(nc)
	workspaceID := uuid.New()
	payload := []byte(fmt.Sprintf(
		`{"workspace_id":%q,"to":"5511999999999","channel":"whatsapp","body":"hello"}`,
		workspaceID,
	))
	traceID := "test-trace-001"

	err = publisher.Publish(ctx, "messages.outbound", payload, traceID)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Consume the message
	msgCtx, err := consumer.Messages()
	if err != nil {
		t.Fatalf("consumer.Messages failed: %v", err)
	}
	defer msgCtx.Stop()

	msg, err := msgCtx.Next()
	if err != nil {
		t.Fatalf("msgCtx.Next failed: %v", err)
	}

	if string(msg.Data()) != string(payload) {
		t.Errorf("message data = %q, want %q", string(msg.Data()), string(payload))
	}
	if got, want := msg.Subject(), "pergo.v2.outbound."+workspaceID.String(); got != want {
		t.Errorf("message subject = %q, want %q", got, want)
	}

	// Verify trace ID in headers
	headers := msg.Headers()
	if headers != nil {
		gotTrace := headers.Get("Nats-Msg-Id")
		wantTrace := workspaceID.String() + ":" + traceID
		if gotTrace != wantTrace {
			t.Errorf("Nats-Msg-Id header = %q, want %q", gotTrace, wantTrace)
		}
	}

	if err := msg.Ack(); err != nil {
		t.Fatalf("msg.Ack failed: %v", err)
	}
}

func TestPublishDedup(t *testing.T) {
	nc := connectNATS(t)
	ctx := context.Background()

	// Delete and recreate the stream to get a clean state (WorkQueuePolicy
	// only allows one non-filtered consumer).
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	_ = js.DeleteStream(ctx, StreamName) // ignore error if not exists

	stream, err := EnsureStream(ctx, nc)
	if err != nil {
		t.Fatalf("EnsureStream failed: %v", err)
	}

	publisher := NewJetStreamPublisher(nc)
	payload := []byte(fmt.Sprintf(
		`{"workspace_id":%q,"to":"5511999999999","channel":"whatsapp","body":"dedup test"}`,
		uuid.New(),
	))
	traceID := "dedup-trace-001"

	// Publish twice with same trace ID — Dedup via Nats-Msg-Id header
	err = publisher.Publish(ctx, "messages.outbound", payload, traceID)
	if err != nil {
		t.Fatalf("first Publish failed: %v", err)
	}
	err = publisher.Publish(ctx, "messages.outbound", payload, traceID)
	if err != nil {
		t.Fatalf("second Publish failed: %v", err)
	}

	// Create a consumer and check only one message arrives
	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   "test-dedup-consumer",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("CreateOrUpdateConsumer failed: %v", err)
	}

	msgCtx, err := consumer.Messages()
	if err != nil {
		t.Fatalf("consumer.Messages failed: %v", err)
	}
	defer msgCtx.Stop()

	// Read first message
	msg, err := msgCtx.Next()
	if err != nil {
		t.Fatalf("msgCtx.Next failed: %v", err)
	}
	if err := msg.Ack(); err != nil {
		t.Fatalf("msg.Ack failed: %v", err)
	}

	// Second message should not arrive (dedup)
	// Use a short timeout to confirm no second message
	_, err = msgCtx.Next(jetstream.NextMaxWait(500 * time.Millisecond))
	if err == nil {
		t.Error("expected no second message (dedup), but got one")
	}
}

func TestPublishDedupIdentityIsScopedByWorkspaceAcrossEveryTenantStream(t *testing.T) {
	tests := []struct {
		name       string
		streamName string
		ensure     func(context.Context, *nats.Conn, ...int) (jetstream.Stream, error)
		subject    func(uuid.UUID) string
	}{
		{
			name:       "outbound",
			streamName: StreamName,
			ensure:     EnsureStream,
			subject:    func(uuid.UUID) string { return "messages.outbound" },
		},
		{
			name:       "inbound",
			streamName: InboundStreamName,
			ensure:     EnsureInboundStream,
			subject: func(workspaceID uuid.UUID) string {
				return "inbound.events." + workspaceID.String()
			},
		},
		{
			name:       "webhook event",
			streamName: WebhookStreamName,
			ensure:     EnsureWebhookStream,
			subject:    func(uuid.UUID) string { return "webhooks.events" },
		},
		{
			name:       "webhook delivery",
			streamName: WebhookDeliveryStreamName,
			ensure:     EnsureWebhookDeliveryStream,
			subject: func(workspaceID uuid.UUID) string {
				return "webhooks.deliveries." + workspaceID.String()
			},
		},
		{
			name:       "campaign batch",
			streamName: CampaignStreamName,
			ensure:     EnsureCampaignStream,
			subject:    func(uuid.UUID) string { return CampaignBatchSubject },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nc := connectNATS(t)
			ctx := context.Background()
			js, err := jetstream.New(nc)
			if err != nil {
				t.Fatalf("jetstream.New: %v", err)
			}
			_ = js.DeleteStream(ctx, tt.streamName)
			stream, err := tt.ensure(ctx, nc)
			if err != nil {
				t.Fatalf("ensure stream: %v", err)
			}
			publisher := NewJetStreamPublisher(nc)
			logicalID := "customer-supplied-stable-id"
			for _, workspaceID := range []uuid.UUID{uuid.New(), uuid.New()} {
				payload := []byte(fmt.Sprintf(`{"workspace_id":%q}`, workspaceID))
				for attempt := 0; attempt < 2; attempt++ {
					if err := publisher.Publish(
						ctx,
						tt.subject(workspaceID),
						payload,
						logicalID,
					); err != nil {
						t.Fatalf("publish workspace=%s attempt=%d: %v", workspaceID, attempt, err)
					}
				}
			}
			info, err := stream.Info(ctx)
			if err != nil {
				t.Fatalf("stream info: %v", err)
			}
			if info.State.Msgs != 2 {
				t.Fatalf("stored messages=%d, want one per workspace (2)", info.State.Msgs)
			}
		})
	}
}

func TestPublishEnforcesDurableCapacityPerWorkspaceSubject(t *testing.T) {
	nc := connectNATS(t)
	ctx := context.Background()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	_ = js.DeleteStream(ctx, StreamName)
	stream, err := EnsureStream(ctx, nc)
	if err != nil {
		t.Fatalf("EnsureStream failed: %v", err)
	}
	cfg := stream.CachedInfo().Config
	cfg.MaxMsgsPerSubject = 1
	if _, err := js.UpdateStream(ctx, cfg); err != nil {
		t.Fatalf("set test workspace capacity: %v", err)
	}

	publisher := NewJetStreamPublisher(nc)
	workspaceA := uuid.New()
	workspaceB := uuid.New()
	payload := func(workspaceID uuid.UUID) []byte {
		return []byte(fmt.Sprintf(
			`{"workspace_id":%q,"to":"recipient","channel":"whatsapp_cloud","body":"hello"}`,
			workspaceID,
		))
	}

	if err := publisher.Publish(ctx, "messages.outbound", payload(workspaceA), "a-1"); err != nil {
		t.Fatalf("first workspace A publish: %v", err)
	}
	err = publisher.Publish(ctx, "messages.outbound", payload(workspaceA), "a-2")
	if !errors.Is(err, messagebus.ErrWorkspaceQueueCapacity) {
		t.Fatalf("second workspace A publish error = %v, want capacity error", err)
	}
	if err := publisher.Publish(ctx, "messages.outbound", payload(workspaceB), "b-1"); err != nil {
		t.Fatalf("workspace B was blocked by workspace A: %v", err)
	}
}

func TestEventQueuesIsolateWorkspaceCapacity(t *testing.T) {
	tests := []struct {
		name       string
		streamName string
		ensure     func(context.Context, *nats.Conn, ...int) (jetstream.Stream, error)
		subject    func(uuid.UUID) string
		payload    func(uuid.UUID) []byte
	}{
		{
			name:       "inbound",
			streamName: InboundStreamName,
			ensure:     EnsureInboundStream,
			subject: func(workspaceID uuid.UUID) string {
				return "inbound.events." + workspaceID.String()
			},
			payload: func(workspaceID uuid.UUID) []byte {
				return []byte(fmt.Sprintf(`{"workspace_id":%q}`, workspaceID))
			},
		},
		{
			name:       "webhook event",
			streamName: WebhookStreamName,
			ensure:     EnsureWebhookStream,
			subject: func(uuid.UUID) string {
				return "webhooks.events"
			},
			payload: func(workspaceID uuid.UUID) []byte {
				return []byte(fmt.Sprintf(`{"workspace_id":%q}`, workspaceID))
			},
		},
		{
			name:       "webhook delivery",
			streamName: WebhookDeliveryStreamName,
			ensure:     EnsureWebhookDeliveryStream,
			subject: func(workspaceID uuid.UUID) string {
				return "webhooks.deliveries." + workspaceID.String()
			},
			payload: func(workspaceID uuid.UUID) []byte {
				return []byte(fmt.Sprintf(`{"workspace_id":%q}`, workspaceID))
			},
		},
		{
			name:       "campaign batch",
			streamName: CampaignStreamName,
			ensure:     EnsureCampaignStream,
			subject: func(uuid.UUID) string {
				return CampaignBatchSubject
			},
			payload: func(workspaceID uuid.UUID) []byte {
				return []byte(fmt.Sprintf(`{"workspace_id":%q}`, workspaceID))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nc := connectNATS(t)
			ctx := context.Background()
			js, err := jetstream.New(nc)
			if err != nil {
				t.Fatalf("jetstream.New: %v", err)
			}
			_ = js.DeleteStream(ctx, tt.streamName)
			stream, err := tt.ensure(ctx, nc)
			if err != nil {
				t.Fatalf("ensure stream: %v", err)
			}
			cfg := stream.CachedInfo().Config
			cfg.MaxMsgsPerSubject = 1
			if _, err := js.UpdateStream(ctx, cfg); err != nil {
				t.Fatalf("set test capacity: %v", err)
			}

			publisher := NewJetStreamPublisher(nc)
			workspaceA := uuid.New()
			workspaceB := uuid.New()
			if err := publisher.Publish(
				ctx,
				tt.subject(workspaceA),
				tt.payload(workspaceA),
				tt.name+"-a-1",
			); err != nil {
				t.Fatalf("workspace A first publish: %v", err)
			}
			if err := publisher.Publish(
				ctx,
				tt.subject(workspaceA),
				tt.payload(workspaceA),
				tt.name+"-a-2",
			); !errors.Is(err, messagebus.ErrWorkspaceQueueCapacity) {
				t.Fatalf("workspace A overflow error=%v", err)
			}
			if err := publisher.Publish(
				ctx,
				tt.subject(workspaceB),
				tt.payload(workspaceB),
				tt.name+"-b-1",
			); err != nil {
				t.Fatalf("workspace B blocked by A: %v", err)
			}
		})
	}
}

func TestPublishRejectsOutboundPayloadWithoutWorkspace(t *testing.T) {
	nc := connectNATS(t)
	publisher := NewJetStreamPublisher(nc)
	err := publisher.Publish(
		context.Background(),
		"messages.outbound",
		[]byte(`{"channel":"whatsapp_cloud","body":"missing tenant"}`),
		"missing-workspace",
	)
	if err == nil || !strings.Contains(err.Error(), "workspace_id is required") {
		t.Fatalf("missing workspace error = %v", err)
	}
}

func TestStreamContractRejectsEverySafetyDrift(t *testing.T) {
	expected, err := streamContractFor(StreamName, StreamSubject, 3)
	if err != nil {
		t.Fatalf("stream contract: %v", err)
	}
	if err := validateStreamContract(expected, expected); err != nil {
		t.Fatalf("valid stream contract rejected: %v", err)
	}

	mutations := map[string]func(*jetstream.StreamConfig){
		"name":                 func(c *jetstream.StreamConfig) { c.Name = "OTHER" },
		"subject":              func(c *jetstream.StreamConfig) { c.Subjects = []string{"pergo.v2.outbound.bad"} },
		"retention":            func(c *jetstream.StreamConfig) { c.Retention = jetstream.LimitsPolicy },
		"global capacity":      func(c *jetstream.StreamConfig) { c.MaxMsgs = 1 },
		"tenant capacity":      func(c *jetstream.StreamConfig) { c.MaxMsgsPerSubject = 1 },
		"discard policy":       func(c *jetstream.StreamConfig) { c.Discard = jetstream.DiscardOld },
		"per-subject discard":  func(c *jetstream.StreamConfig) { c.DiscardNewPerSubject = false },
		"storage":              func(c *jetstream.StreamConfig) { c.Storage = jetstream.MemoryStorage },
		"replicas":             func(c *jetstream.StreamConfig) { c.Replicas = 1 },
		"retention horizon":    func(c *jetstream.StreamConfig) { c.MaxAge = time.Hour },
		"deduplication window": func(c *jetstream.StreamConfig) { c.Duplicates = time.Minute },
		"multiple subject filters": func(c *jetstream.StreamConfig) {
			c.Subjects = append(c.Subjects, "pergo.v2.outbound.extra")
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			actual := expected
			actual.Subjects = append([]string(nil), expected.Subjects...)
			mutate(&actual)
			if err := validateStreamContract(actual, expected); err == nil {
				t.Fatal("unsafe stream drift was accepted")
			}
		})
	}
}

func TestConsumerContractRejectsEveryDeliveryDrift(t *testing.T) {
	expected := outboundConsumerConfig(OutboundConsumerName)
	if err := validateConsumerContract(expected, expected); err != nil {
		t.Fatalf("valid consumer contract rejected: %v", err)
	}

	mutations := map[string]func(*jetstream.ConsumerConfig){
		"durable":        func(c *jetstream.ConsumerConfig) { c.Durable = "other" },
		"filter":         func(c *jetstream.ConsumerConfig) { c.FilterSubject = "pergo.v2.outbound.bad" },
		"deliver policy": func(c *jetstream.ConsumerConfig) { c.DeliverPolicy = jetstream.DeliverLastPolicy },
		"ack policy":     func(c *jetstream.ConsumerConfig) { c.AckPolicy = jetstream.AckNonePolicy },
		"ack wait":       func(c *jetstream.ConsumerConfig) { c.AckWait = time.Second },
		"max deliver":    func(c *jetstream.ConsumerConfig) { c.MaxDeliver = 1 },
		"max pending":    func(c *jetstream.ConsumerConfig) { c.MaxAckPending = 1 },
		"replay policy":  func(c *jetstream.ConsumerConfig) { c.ReplayPolicy = jetstream.ReplayOriginalPolicy },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			actual := expected
			mutate(&actual)
			if err := validateConsumerContract(actual, expected); err == nil {
				t.Fatal("unsafe consumer drift was accepted")
			}
		})
	}
}

func TestBootstrapReconcilesOwnedOutboundConsumerAndRuntimeBindsReadOnly(t *testing.T) {
	nc := connectNATS(t)
	ctx := context.Background()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	_ = js.DeleteStream(ctx, StreamName)
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), StreamName) })
	stream, err := EnsureStream(ctx, nc)
	if err != nil {
		t.Fatalf("ensure stream: %v", err)
	}
	if _, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       OutboundConsumerName,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       time.Second,
		MaxDeliver:    1,
		MaxAckPending: 1,
		FilterSubject: StreamSubject,
		ReplayPolicy:  jetstream.ReplayInstantPolicy,
	}); err != nil {
		t.Fatalf("create drifted consumer: %v", err)
	}

	if _, err := EnsureConsumer(ctx, stream, OutboundConsumerName); err != nil {
		t.Fatalf("bootstrap reconcile consumer: %v", err)
	}
	if _, err := BindVersionedStream(ctx, nc, StreamName, StreamSubject, 1); err != nil {
		t.Fatalf("runtime bind stream after bootstrap: %v", err)
	}
	if _, err := BindConsumer(ctx, stream, OutboundConsumerName, StreamSubject); err != nil {
		t.Fatalf("runtime bind after reconcile: %v", err)
	}
	info, err := stream.Consumer(ctx, OutboundConsumerName)
	if err != nil {
		t.Fatalf("read reconciled consumer: %v", err)
	}
	consumerInfo, err := info.Info(ctx)
	if err != nil {
		t.Fatalf("consumer info: %v", err)
	}
	expected := outboundConsumerConfig(OutboundConsumerName)
	if err := validateConsumerContract(consumerInfo.Config, expected); err != nil {
		t.Fatalf("consumer was not fully reconciled: %v", err)
	}
}

func TestWorkerStopUnblocksIdlePullBeforeAndAfterConsumerStartup(t *testing.T) {
	for _, delay := range []time.Duration{0, 50 * time.Millisecond} {
		t.Run(delay.String(), func(t *testing.T) {
			nc := connectNATS(t)
			ctx := context.Background()
			js, err := jetstream.New(nc)
			if err != nil {
				t.Fatalf("jetstream.New failed: %v", err)
			}
			_ = js.DeleteStream(ctx, StreamName)
			stream, err := EnsureStream(ctx, nc)
			if err != nil {
				t.Fatalf("EnsureStream failed: %v", err)
			}
			consumer, err := EnsureConsumer(ctx, stream, "stop-test-worker-"+uuid.NewString())
			if err != nil {
				t.Fatalf("EnsureConsumer failed: %v", err)
			}

			worker := NewWorker(ctx, consumer, nil)
			time.Sleep(delay)
			stopped := make(chan struct{})
			go func() {
				worker.Stop()
				worker.Stop()
				close(stopped)
			}()

			select {
			case <-stopped:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("Worker.Stop did not unblock an idle pull")
			}
		})
	}
}

func TestWorkerJetStreamMetadataExhaustsTransientRetriesDurably(t *testing.T) {
	nc := connectNATS(t)
	pool := getTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	streamName := "PERGO_TEST_RETRY_" + suffix
	subject := "pergo.test.retry." + suffix
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  []string{subject},
		Retention: jetstream.WorkQueuePolicy,
		Storage:   jetstream.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("create retry test stream: %v", err)
	}
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), streamName) })
	consumer, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "retry-worker",
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       250 * time.Millisecond,
		MaxDeliver:    -1,
		MaxAckPending: 1,
	})
	if err != nil {
		t.Fatalf("create retry consumer: %v", err)
	}

	workspaceRepo := repository.NewWorkspaceRepository(pool)
	dispatchRepo := repository.NewMessageDispatchRepository(pool)
	workspace, err := workspaceRepo.Create(ctx, "retry_metadata_"+suffix)
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	t.Cleanup(func() { _ = workspaceRepo.Delete(context.Background(), workspace.ID) })

	dispatcher := &fakeDispatcher{err: errors.New("provider unavailable")}
	registry := channel.NewRegistry(map[string]channel.Dispatcher{"whatsapp": dispatcher})
	orchestrator := NewDispatchOrchestrator(
		registry,
		dispatchRepo,
		nil,
		nil,
		nil,
		nil,
		2,
		10*time.Millisecond,
	)
	worker := NewWorker(ctx, consumer, orchestrator)
	t.Cleanup(worker.Stop)

	traceID := uuid.NewString()
	payload, err := json.Marshal(domain.QueueMessage{
		WorkspaceID: workspace.ID,
		TraceID:     traceID,
		To:          "+123",
		Channel:     "whatsapp",
		Body:        "always transient",
	})
	if err != nil {
		t.Fatalf("marshal queue message: %v", err)
	}
	if _, err := js.Publish(ctx, subject, payload); err != nil {
		t.Fatalf("publish queue message: %v", err)
	}

	var dispatch *repository.MessageDispatch
	for ctx.Err() == nil {
		dispatch, err = dispatchRepo.GetByTraceID(ctx, workspace.ID, traceID)
		if err == nil && dispatch.Status == "failed" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatalf("terminal dispatch not observed: %v", ctx.Err())
	}
	if dispatcher.calledCount != 3 {
		t.Fatalf("provider calls=%d, want initial plus two retries", dispatcher.calledCount)
	}
	if dispatch.ErrorMessage == nil || *dispatch.ErrorMessage != deliveryRetriesCode {
		t.Fatalf("error_message=%v, want %q", dispatch.ErrorMessage, deliveryRetriesCode)
	}

	for ctx.Err() == nil {
		info, infoErr := consumer.Info(ctx)
		if infoErr != nil {
			t.Fatalf("consumer info: %v", infoErr)
		}
		if info.NumPending == 0 && info.NumAckPending == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatalf("JetStream delivery remained stuck after terminal ACK: %v", ctx.Err())
	}

	events, err := dispatchRepo.ListPendingProviderDeliveryEvents(ctx, 100)
	if err != nil {
		t.Fatalf("list delivery outbox: %v", err)
	}
	found := false
	for _, event := range events {
		if event.DispatchID == dispatch.ID && event.Status == "failed" {
			found = true
		}
	}
	if !found {
		t.Fatal("terminal failure missing from durable delivery outbox")
	}
}

type tenantFairnessDispatcher struct {
	slowEntered   chan struct{}
	releaseSlow   chan struct{}
	fastDelivered chan struct{}
	slowOnce      sync.Once
	fastOnce      sync.Once
}

func (d *tenantFairnessDispatcher) Dispatch(
	ctx context.Context,
	payload *channel.MessagePayload,
) (string, error) {
	if payload.To == "slow" {
		d.slowOnce.Do(func() { close(d.slowEntered) })
		select {
		case <-d.releaseSlow:
			return "slow-provider-id", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	d.fastOnce.Do(func() { close(d.fastDelivered) })
	return "fast-provider-id", nil
}

func TestWorkerSlowTenantDoesNotBlockFastTenant(t *testing.T) {
	nc := connectNATS(t)
	pool := getTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	streamName := "PERGO_TEST_FAIR_" + suffix
	subject := "pergo.test.fair." + suffix
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  []string{subject},
		Retention: jetstream.WorkQueuePolicy,
		Storage:   jetstream.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("create fairness stream: %v", err)
	}
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), streamName) })
	consumer, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "fair-worker",
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       2 * time.Minute,
		MaxDeliver:    -1,
		MaxAckPending: 100,
	})
	if err != nil {
		t.Fatalf("create fairness consumer: %v", err)
	}

	workspaceRepo := repository.NewWorkspaceRepository(pool)
	slowWorkspace, err := workspaceRepo.Create(ctx, "slow_tenant_"+suffix)
	if err != nil {
		t.Fatalf("create slow workspace: %v", err)
	}
	defer func() { _ = workspaceRepo.Delete(context.Background(), slowWorkspace.ID) }()
	fastWorkspace, err := workspaceRepo.Create(ctx, "fast_tenant_"+suffix)
	if err != nil {
		t.Fatalf("create fast workspace: %v", err)
	}
	defer func() { _ = workspaceRepo.Delete(context.Background(), fastWorkspace.ID) }()

	dispatcher := &tenantFairnessDispatcher{
		slowEntered:   make(chan struct{}),
		releaseSlow:   make(chan struct{}),
		fastDelivered: make(chan struct{}),
	}
	registry := channel.NewRegistry(map[string]channel.Dispatcher{"whatsapp": dispatcher})
	orchestrator := NewDispatchOrchestrator(
		registry,
		repository.NewMessageDispatchRepository(pool),
		nil,
		nil,
		nil,
		nil,
		2,
		10*time.Millisecond,
	)
	worker := NewWorker(ctx, consumer, orchestrator)

	publish := func(workspaceID uuid.UUID, to string) {
		t.Helper()
		payload, marshalErr := json.Marshal(domain.QueueMessage{
			WorkspaceID: workspaceID,
			TraceID:     uuid.NewString(),
			To:          to,
			Channel:     "whatsapp",
			Body:        "fairness",
		})
		if marshalErr != nil {
			t.Fatalf("marshal fairness payload: %v", marshalErr)
		}
		if _, publishErr := js.Publish(ctx, subject, payload); publishErr != nil {
			t.Fatalf("publish fairness payload: %v", publishErr)
		}
	}
	for i := 0; i < 20; i++ {
		publish(slowWorkspace.ID, "slow")
	}
	publish(fastWorkspace.ID, "fast")

	select {
	case <-dispatcher.slowEntered:
	case <-ctx.Done():
		t.Fatalf("slow provider never started: %v", ctx.Err())
	}
	select {
	case <-dispatcher.fastDelivered:
	case <-time.After(time.Second):
		t.Fatal("fast tenant was blocked behind the slow tenant queue")
	}

	close(dispatcher.releaseSlow)
	worker.Stop()
}
