package repository

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pablojhp.pergo/internal/domain"
)

func TestPrepareCampaignStartIsAtomicAndIdempotent(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()
	ctx := context.Background()

	wsRepo := NewWorkspaceRepository(pool)
	campRepo := NewCampaignRepository(pool)
	ws, err := wsRepo.Create(ctx, "campaign_batch_start_"+uuid.NewString())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(context.Background(), ws.ID) }()

	campaign, err := campRepo.Create(ctx, &domain.Campaign{
		WorkspaceID: ws.ID,
		Name:        "Atomic start",
		Status:      domain.CampaignStatusDraft,
		BatchSize:   1,
	})
	if err != nil {
		t.Fatalf("create campaign: %v", err)
	}

	invalid := []CampaignBatch{{
		BatchIndex:   1,
		TotalBatches: 2,
		TraceID:      fmt.Sprintf("campaign_%s_batch_1", campaign.ID),
		Payload:      []byte(`{"batch":1}`),
	}}
	if _, err := campRepo.PrepareCampaignStart(ctx, campaign.ID, ws.ID, campaign.UpdatedAt, invalid); !errors.Is(err, ErrCampaignBatchConflict) {
		t.Fatalf("invalid batch set error = %v, want ErrCampaignBatchConflict", err)
	}
	stored, err := campRepo.GetByID(ctx, campaign.ID)
	if err != nil {
		t.Fatalf("get campaign: %v", err)
	}
	if stored.Status != domain.CampaignStatusDraft {
		t.Fatalf("failed preparation changed status to %s", stored.Status)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM campaign_batches WHERE campaign_id = $1`, campaign.ID).Scan(&count); err != nil {
		t.Fatalf("count campaign batches: %v", err)
	}
	if count != 0 {
		t.Fatalf("failed preparation persisted %d partial batches", count)
	}

	valid := []CampaignBatch{{
		BatchIndex:   1,
		TotalBatches: 1,
		TraceID:      fmt.Sprintf("campaign_%s_batch_1", campaign.ID),
		Payload:      []byte(`{"batch":1,"recipients":["one"]}`),
	}}
	if _, err := pool.Exec(
		ctx,
		`UPDATE campaigns SET updated_at = updated_at + interval '1 second' WHERE id = $1`,
		campaign.ID,
	); err != nil {
		t.Fatalf("advance campaign version: %v", err)
	}
	if _, err := campRepo.PrepareCampaignStart(ctx, campaign.ID, ws.ID, campaign.UpdatedAt, valid); !errors.Is(err, ErrCampaignBatchConflict) {
		t.Fatalf("stale campaign snapshot error = %v, want ErrCampaignBatchConflict", err)
	}
	campaign, err = campRepo.GetByID(ctx, campaign.ID)
	if err != nil {
		t.Fatalf("refresh campaign version: %v", err)
	}
	status, err := campRepo.PrepareCampaignStart(ctx, campaign.ID, ws.ID, campaign.UpdatedAt, valid)
	if err != nil {
		t.Fatalf("prepare valid batch: %v", err)
	}
	if status != domain.CampaignStatusSending {
		t.Fatalf("start status = %s, want sending", status)
	}
	status, err = campRepo.PrepareCampaignStart(ctx, campaign.ID, ws.ID, campaign.UpdatedAt, valid)
	if err != nil {
		t.Fatalf("idempotent start: %v", err)
	}
	if status != domain.CampaignStatusSending {
		t.Fatalf("idempotent start status = %s, want sending", status)
	}

	conflicting := append([]CampaignBatch(nil), valid...)
	conflicting[0].Payload = []byte(`{"batch":1,"recipients":["different"]}`)
	if _, err := campRepo.PrepareCampaignStart(ctx, campaign.ID, ws.ID, campaign.UpdatedAt, conflicting); !errors.Is(err, ErrCampaignBatchConflict) {
		t.Fatalf("conflicting replay error = %v, want ErrCampaignBatchConflict", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM campaign_batches WHERE campaign_id = $1`, campaign.ID).Scan(&count); err != nil {
		t.Fatalf("count durable batches: %v", err)
	}
	if count != 1 {
		t.Fatalf("idempotent start persisted %d batches, want 1", count)
	}
	if err := campRepo.UpdateRecipients(
		ctx,
		campaign.ID,
		[]domain.CampaignRecipient{{To: "5511999999999"}},
		nil,
	); !errors.Is(err, ErrCampaignInvalidState) {
		t.Fatalf("recipient mutation after start error = %v, want ErrCampaignInvalidState", err)
	}
}

func TestPrepareCampaignStartRejectsUnpublishablePayloadBeforeStateChange(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()
	ctx := context.Background()

	wsRepo := NewWorkspaceRepository(pool)
	campRepo := NewCampaignRepository(pool)
	ws, err := wsRepo.Create(ctx, "campaign_batch_payload_"+uuid.NewString())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(context.Background(), ws.ID) }()
	campaign, err := campRepo.Create(ctx, &domain.Campaign{
		WorkspaceID: ws.ID,
		Name:        "Oversized durable batch",
		Status:      domain.CampaignStatusDraft,
		BatchSize:   1,
	})
	if err != nil {
		t.Fatalf("create campaign: %v", err)
	}

	_, err = campRepo.PrepareCampaignStart(
		ctx,
		campaign.ID,
		ws.ID,
		campaign.UpdatedAt,
		[]CampaignBatch{{
			BatchIndex:   1,
			TotalBatches: 1,
			TraceID:      "oversized-batch",
			Payload:      make([]byte, MaxCampaignBatchPayloadBytes+1),
		}},
	)
	if !errors.Is(err, ErrCampaignBatchConflict) {
		t.Fatalf("PrepareCampaignStart error = %v, want conflict", err)
	}
	stored, err := campRepo.GetByID(ctx, campaign.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if stored.Status != domain.CampaignStatusDraft {
		t.Fatalf("campaign status = %s, want draft", stored.Status)
	}
}

func TestCampaignConstraintsAreValidated(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	for _, constraintName := range []string{
		"campaigns_delay_seconds_range",
		"campaigns_template_language_shape",
	} {
		var validated bool
		if err := pool.QueryRow(
			context.Background(),
			`SELECT convalidated
			   FROM pg_constraint
			  WHERE conname = $1`,
			constraintName,
		).Scan(&validated); err != nil {
			t.Fatalf("read constraint %s: %v", constraintName, err)
		}
		if !validated {
			t.Fatalf("constraint %s remains NOT VALID", constraintName)
		}
	}
}

func TestCampaignCompletesOnlyAfterEveryDurableBatch(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()
	ctx := context.Background()

	wsRepo := NewWorkspaceRepository(pool)
	campRepo := NewCampaignRepository(pool)
	ws, err := wsRepo.Create(ctx, "campaign_batch_complete_"+uuid.NewString())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(context.Background(), ws.ID) }()

	campaign, err := campRepo.Create(ctx, &domain.Campaign{
		WorkspaceID: ws.ID,
		Name:        "Two batches",
		Status:      domain.CampaignStatusDraft,
		BatchSize:   1,
	})
	if err != nil {
		t.Fatalf("create campaign: %v", err)
	}
	batches := []CampaignBatch{
		{
			BatchIndex:   1,
			TotalBatches: 2,
			TraceID:      fmt.Sprintf("campaign_%s_batch_1", campaign.ID),
			Payload:      []byte(`{"batch":1}`),
		},
		{
			BatchIndex:   2,
			TotalBatches: 2,
			TraceID:      fmt.Sprintf("campaign_%s_batch_2", campaign.ID),
			Payload:      []byte(`{"batch":2}`),
		},
	}
	if _, err := campRepo.PrepareCampaignStart(ctx, campaign.ID, ws.ID, campaign.UpdatedAt, batches); err != nil {
		t.Fatalf("prepare campaign: %v", err)
	}

	completed, err := campRepo.MarkCampaignBatchProcessed(ctx, campaign.ID, ws.ID, 1, batches[0].Payload)
	if err != nil {
		t.Fatalf("process first batch: %v", err)
	}
	if completed {
		t.Fatal("campaign completed with one durable batch still pending")
	}
	stored, err := campRepo.GetByID(ctx, campaign.ID)
	if err != nil {
		t.Fatalf("get campaign after first batch: %v", err)
	}
	if stored.Status != domain.CampaignStatusSending {
		t.Fatalf("campaign after first batch = %s, want sending", stored.Status)
	}

	completed, err = campRepo.MarkCampaignBatchProcessed(ctx, campaign.ID, ws.ID, 2, batches[1].Payload)
	if err != nil {
		t.Fatalf("process final batch: %v", err)
	}
	if !completed {
		t.Fatal("final durable batch did not complete campaign")
	}
	stored, err = campRepo.GetByID(ctx, campaign.ID)
	if err != nil {
		t.Fatalf("get completed campaign: %v", err)
	}
	if stored.Status != domain.CampaignStatusCompleted {
		t.Fatalf("campaign status = %s, want completed", stored.Status)
	}
}

func TestConcurrentFinalCampaignBatchesCannotLeaveCampaignSending(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()
	ctx := context.Background()

	wsRepo := NewWorkspaceRepository(pool)
	campRepo := NewCampaignRepository(pool)
	ws, err := wsRepo.Create(ctx, "campaign_batch_concurrent_"+uuid.NewString())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(context.Background(), ws.ID) }()

	campaign, err := campRepo.Create(ctx, &domain.Campaign{
		WorkspaceID: ws.ID,
		Name:        "Concurrent completion",
		Status:      domain.CampaignStatusDraft,
	})
	if err != nil {
		t.Fatalf("create campaign: %v", err)
	}
	batches := []CampaignBatch{
		{BatchIndex: 1, TotalBatches: 2, TraceID: fmt.Sprintf("campaign_%s_batch_1", campaign.ID), Payload: []byte(`{"batch":1}`)},
		{BatchIndex: 2, TotalBatches: 2, TraceID: fmt.Sprintf("campaign_%s_batch_2", campaign.ID), Payload: []byte(`{"batch":2}`)},
	}
	if _, err := campRepo.PrepareCampaignStart(ctx, campaign.ID, ws.ID, campaign.UpdatedAt, batches); err != nil {
		t.Fatalf("prepare campaign: %v", err)
	}

	start := make(chan struct{})
	errs := make(chan error, len(batches))
	var wg sync.WaitGroup
	for _, batch := range batches {
		batch := batch
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := campRepo.MarkCampaignBatchProcessed(
				ctx,
				campaign.ID,
				ws.ID,
				batch.BatchIndex,
				batch.Payload,
			)
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent batch completion: %v", err)
		}
	}

	stored, err := campRepo.GetByID(ctx, campaign.ID)
	if err != nil {
		t.Fatalf("get campaign: %v", err)
	}
	if stored.Status != domain.CampaignStatusCompleted {
		t.Fatalf("concurrent final batches left campaign %s", stored.Status)
	}
}

func TestCampaignBatchDelayIsDurableAndDoesNotBlockAnotherTenant(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()
	ctx := context.Background()

	wsRepo := NewWorkspaceRepository(pool)
	campRepo := NewCampaignRepository(pool)
	workspaceA, err := wsRepo.Create(ctx, "campaign_delay_a_"+uuid.NewString())
	if err != nil {
		t.Fatalf("create workspace A: %v", err)
	}
	defer func() { _ = wsRepo.Delete(context.Background(), workspaceA.ID) }()
	workspaceB, err := wsRepo.Create(ctx, "campaign_delay_b_"+uuid.NewString())
	if err != nil {
		t.Fatalf("create workspace B: %v", err)
	}
	defer func() { _ = wsRepo.Delete(context.Background(), workspaceB.ID) }()

	campaignA, err := campRepo.Create(ctx, &domain.Campaign{
		WorkspaceID: workspaceA.ID,
		Name:        "Tenant A delayed campaign",
		Status:      domain.CampaignStatusDraft,
		BatchSize:   1,
	})
	if err != nil {
		t.Fatalf("create campaign A: %v", err)
	}
	batchesA := []CampaignBatch{
		{
			BatchIndex:   1,
			TotalBatches: 2,
			TraceID:      fmt.Sprintf("campaign_%s_batch_1", campaignA.ID),
			Payload:      []byte(`{"tenant":"a","batch":1}`),
			DelaySeconds: 60,
		},
		{
			BatchIndex:   2,
			TotalBatches: 2,
			TraceID:      fmt.Sprintf("campaign_%s_batch_2", campaignA.ID),
			Payload:      []byte(`{"tenant":"a","batch":2}`),
			DelaySeconds: 60,
		},
	}
	if _, err := campRepo.PrepareCampaignStart(
		ctx,
		campaignA.ID,
		workspaceA.ID,
		campaignA.UpdatedAt,
		batchesA,
	); err != nil {
		t.Fatalf("prepare campaign A: %v", err)
	}

	campaignB, err := campRepo.Create(ctx, &domain.Campaign{
		WorkspaceID: workspaceB.ID,
		Name:        "Tenant B immediate campaign",
		Status:      domain.CampaignStatusDraft,
		BatchSize:   1,
	})
	if err != nil {
		t.Fatalf("create campaign B: %v", err)
	}
	batchB := CampaignBatch{
		BatchIndex:   1,
		TotalBatches: 1,
		TraceID:      fmt.Sprintf("campaign_%s_batch_1", campaignB.ID),
		Payload:      []byte(`{"tenant":"b","batch":1}`),
	}
	if _, err := campRepo.PrepareCampaignStart(
		ctx,
		campaignB.ID,
		workspaceB.ID,
		campaignB.UpdatedAt,
		[]CampaignBatch{batchB},
	); err != nil {
		t.Fatalf("prepare campaign B: %v", err)
	}

	claims, err := campRepo.ClaimDueCampaignBatches(ctx, 100, 30*time.Second)
	if err != nil {
		t.Fatalf("claim initial batches: %v", err)
	}
	claimed := make(map[uuid.UUID][]int)
	for _, claim := range claims {
		claimed[claim.CampaignID] = append(claimed[claim.CampaignID], claim.BatchIndex)
	}
	if fmt.Sprint(claimed[campaignA.ID]) != fmt.Sprint([]int{1}) {
		t.Fatalf("tenant A initial claims = %v, want only batch 1", claimed[campaignA.ID])
	}
	if fmt.Sprint(claimed[campaignB.ID]) != fmt.Sprint([]int{1}) {
		t.Fatalf("tenant B was blocked by tenant A delay; claims = %v", claimed[campaignB.ID])
	}

	if _, err := campRepo.MarkCampaignBatchProcessed(
		ctx,
		campaignA.ID,
		workspaceA.ID,
		1,
		batchesA[0].Payload,
	); err != nil {
		t.Fatalf("process tenant A batch 1: %v", err)
	}
	var nextBatchDelayed bool
	if err := pool.QueryRow(
		ctx,
		`SELECT next_publish_at >= now() + interval '55 seconds'
		   FROM campaign_batches
		  WHERE campaign_id = $1 AND batch_index = 2`,
		campaignA.ID,
	).Scan(&nextBatchDelayed); err != nil {
		t.Fatalf("read tenant A next schedule: %v", err)
	}
	if !nextBatchDelayed {
		t.Fatal("tenant A inter-batch delay was not durably scheduled")
	}
}

func TestCampaignCancelCannotRegressConcurrentCompletion(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()
	ctx := context.Background()

	wsRepo := NewWorkspaceRepository(pool)
	campRepo := NewCampaignRepository(pool)
	workspace, err := wsRepo.Create(ctx, "campaign_cancel_race_"+uuid.NewString())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(context.Background(), workspace.ID) }()

	for attempt := 0; attempt < 10; attempt++ {
		campaign, err := campRepo.Create(ctx, &domain.Campaign{
			WorkspaceID: workspace.ID,
			Name:        fmt.Sprintf("Cancel race %d", attempt),
			Status:      domain.CampaignStatusDraft,
			BatchSize:   1,
		})
		if err != nil {
			t.Fatalf("create campaign %d: %v", attempt, err)
		}
		batch := CampaignBatch{
			BatchIndex:   1,
			TotalBatches: 1,
			TraceID:      fmt.Sprintf("campaign_%s_batch_1", campaign.ID),
			Payload:      []byte(fmt.Sprintf(`{"attempt":%d}`, attempt)),
		}
		if _, err := campRepo.PrepareCampaignStart(
			ctx,
			campaign.ID,
			workspace.ID,
			campaign.UpdatedAt,
			[]CampaignBatch{batch},
		); err != nil {
			t.Fatalf("prepare campaign %d: %v", attempt, err)
		}

		start := make(chan struct{})
		cancelResult := make(chan error, 1)
		completeResult := make(chan error, 1)
		go func() {
			<-start
			cancelResult <- campRepo.CancelForWorkspace(ctx, campaign.ID, workspace.ID)
		}()
		go func() {
			<-start
			_, completeErr := campRepo.MarkCampaignBatchProcessed(
				ctx,
				campaign.ID,
				workspace.ID,
				1,
				batch.Payload,
			)
			completeResult <- completeErr
		}()
		close(start)

		cancelErr := <-cancelResult
		if completeErr := <-completeResult; completeErr != nil {
			t.Fatalf("complete campaign %d: %v", attempt, completeErr)
		}
		stored, err := campRepo.GetByID(ctx, campaign.ID)
		if err != nil {
			t.Fatalf("get campaign %d: %v", attempt, err)
		}
		switch stored.Status {
		case domain.CampaignStatusCancelled:
			if cancelErr != nil {
				t.Fatalf("cancel won but returned %v", cancelErr)
			}
		case domain.CampaignStatusCompleted:
			if !errors.Is(cancelErr, ErrCampaignInvalidState) {
				t.Fatalf("completion won but cancel returned %v", cancelErr)
			}
			if err := campRepo.CancelForWorkspace(ctx, campaign.ID, workspace.ID); !errors.Is(err, ErrCampaignInvalidState) {
				t.Fatalf("completed campaign was cancellable: %v", err)
			}
			after, err := campRepo.GetByID(ctx, campaign.ID)
			if err != nil || after.Status != domain.CampaignStatusCompleted {
				t.Fatalf("completed campaign regressed to %#v, %v", after, err)
			}
		default:
			t.Fatalf("concurrent cancel/completion left campaign %s", stored.Status)
		}
	}
}
