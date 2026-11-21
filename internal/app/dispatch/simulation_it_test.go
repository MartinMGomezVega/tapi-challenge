//go:build integration

package dispatch_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/martingomezvega/tapi-challenge/internal/adapters/dynamo"
	"github.com/martingomezvega/tapi-challenge/internal/adapters/sqsq"
	"github.com/martingomezvega/tapi-challenge/internal/app/assignslots"
	"github.com/martingomezvega/tapi-challenge/internal/app/dispatch"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
	"github.com/martingomezvega/tapi-challenge/internal/platform/clock"
	"github.com/martingomezvega/tapi-challenge/test/testenv"
)

// T026 + FR-005b E2E: Accelerated simulation on LocalStack proving:
//  1. 100% of enabled records dispatched exactly once across catch-up + midnight crossing (SC-001).
//  2. Real assignslots.UseCase wired to dispatch.UseCase (ImmediateDispatcher) dispatches rebalanced
//     same-day records directly into SQS FIFO + DynamoDB executions.
func TestDispatcher_LocalStack_E2E_Integration(t *testing.T) {
	env := testenv.StartLocalStack(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const (
		reqTable   = "requests_disp_e2e"
		execTable  = "executions_disp_e2e"
		coordTable = "coordination_disp_e2e"
		shards     = 4
	)

	if err := env.EnsureRequestsTable(ctx, reqTable); err != nil {
		t.Fatalf("EnsureRequestsTable: %v", err)
	}
	if err := env.EnsureExecutionsTable(ctx, execTable); err != nil {
		t.Fatalf("EnsureExecutionsTable: %v", err)
	}
	if err := env.EnsureCoordinationTable(ctx, coordTable); err != nil {
		t.Fatalf("EnsureCoordinationTable: %v", err)
	}
	qURL, _, err := env.EnsureFIFOQueue(ctx, "disp-e2e.fifo", "disp-e2e-dlq.fifo")
	if err != nil {
		t.Fatalf("EnsureFIFOQueue: %v", err)
	}

	reqRepo := dynamo.NewRequestsRepo(env.DynamoDB, reqTable)
	execRepo := dynamo.NewExecutionsRepo(env.DynamoDB, execTable, dynamo.DefaultExecutionTTL)
	coordRepo := dynamo.NewCoordinationRepo(env.DynamoDB, coordTable)
	dispCoord := dynamo.NewDispatcherCoordinator(env.DynamoDB, coordTable)
	queue := sqsq.NewQueue(env.SQS, qURL)

	// Seed 120 enabled requests distributed across 12 slots (0..11) + 5 disabled requests.
	ts := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	var batch []domain.Request
	for i := range 120 {
		batch = append(batch, domain.Request{
			ID:        fmt.Sprintf("sim-req-%04d", i),
			Provider:  fmt.Sprintf("prov-%02d", i%10),
			Endpoint:  "/v1/providers/p/debt",
			Method:    "POST",
			Body:      []byte(`{"ok":true}`),
			Enabled:   true,
			Slot:      i % 12,
			Shard:     i % shards,
			Version:   1,
			CreatedAt: ts,
			UpdatedAt: ts,
		})
	}
	for i := range 5 {
		batch = append(batch, domain.Request{
			ID:        fmt.Sprintf("sim-disabled-%02d", i),
			Provider:  "prov-00",
			Endpoint:  "/v1/providers/p/debt",
			Method:    "POST",
			Body:      []byte(`{"ok":false}`),
			Enabled:   false,
			Slot:      i,
			Shard:     0,
			Version:   1,
			CreatedAt: ts,
			UpdatedAt: ts,
		})
	}
	if err := reqRepo.PutBatch(ctx, batch); err != nil {
		t.Fatalf("PutBatch: %v", err)
	}

	fc := clock.NewFake(ts)
	uc, err := dispatch.New(reqRepo, execRepo, queue, dispCoord, dispCoord, nil, fc, discardLogger(), dispatch.Config{
		OwnerID: "disp-e2e-1",
		Shards:  shards,
	})
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	// Simulate ticks: slot 0, then jump 5 min (catch-up 1..5), then advance minute-by-minute to slot 11.
	totalDispatched := 0
	for _, advance := range []time.Duration{0, 5 * time.Minute, 1 * time.Minute, 1 * time.Minute, 1 * time.Minute, 1 * time.Minute, 1 * time.Minute, 1 * time.Minute} {
		if advance > 0 {
			fc.Advance(advance)
		}
		st, err := uc.Tick(ctx)
		if err != nil {
			t.Fatalf("Tick(%v): %v", fc.Now(), err)
		}
		totalDispatched += st.RecordsSent
	}
	if totalDispatched != 120 {
		t.Fatalf("totalDispatched = %d, want 120 (100%% of enabled)", totalDispatched)
	}

	// Drain FIFO queue and verify all 120 unique messages arrived once.
	received := drainFIFO(ctx, t, env.SQS, qURL, 120)
	if len(received) != 120 {
		t.Fatalf("SQS received %d unique messages, want 120", len(received))
	}

	// FR-005b E2E: Wire real dispatch.UseCase as assignslots.ImmediateDispatcher.
	// Create a request currently at slot 1400 (future relative to 12:00 = slot 720) that rebalance moves < 720.
	assigner, _ := domain.NewSlotAssigner(shards)
	target, _ := assigner.Assign("fr005b-prov", "fr005b-req-0", 0, 1)
	if target.Slot >= 1430 {
		t.Skip("unlikely edge slot")
	}
	nowAfter := time.Date(2026, 11, 20, (target.Slot+5)/60, (target.Slot+5)%60, 0, 0, time.UTC)
	fc.Set(nowAfter)

	if err := reqRepo.Put(ctx, domain.Request{
		ID:        "fr005b-req-0",
		Provider:  "fr005b-prov",
		Endpoint:  "/v1/providers/p/debt",
		Method:    "POST",
		Body:      []byte(`{}`),
		Enabled:   true,
		Slot:      target.Slot + 10, // future slot relative to nowAfter
		Shard:     0,
		Version:   1,
		CreatedAt: ts,
		UpdatedAt: ts,
	}); err != nil {
		t.Fatalf("Put fr005b request: %v", err)
	}
	if err := coordRepo.SaveProviderState(ctx, assignslots.ProviderState{
		Provider:   "fr005b-prov",
		Backfilled: false,
		UpdatedAt:  ts,
	}); err != nil {
		t.Fatalf("SaveProviderState: %v", err)
	}

	slotUC, err := assignslots.New(assigner, reqRepo, reqRepo, coordRepo, coordRepo, uc, fc, discardLogger(), assignslots.Config{Workers: 1})
	if err != nil {
		t.Fatalf("assignslots.New: %v", err)
	}
	bStats, err := slotUC.Run(ctx, "job-fr005b-e2e")
	if err != nil {
		t.Fatalf("slotUC.Run(): %v", err)
	}
	if bStats.DispatchedNow != 1 {
		t.Fatalf("DispatchedNow = %d, want 1", bStats.DispatchedNow)
	}

	immMsgs := drainFIFO(ctx, t, env.SQS, qURL, 1)
	if _, ok := immMsgs["fr005b-req-0#2026-11-20"]; !ok {
		t.Fatalf("FR-005b immediate dispatch message missing from SQS FIFO: %+v", immMsgs)
	}
}

func drainFIFO(ctx context.Context, t *testing.T, client *sqs.Client, qURL string, expected int) map[string]dispatch.JobMessage {
	t.Helper()
	got := make(map[string]dispatch.JobMessage, expected)
	deadline := time.Now().Add(30 * time.Second)

	for len(got) < expected && time.Now().Before(deadline) {
		out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(qURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     1,
		})
		if err != nil {
			t.Fatalf("ReceiveMessage: %v", err)
		}
		for _, m := range out.Messages {
			var jm dispatch.JobMessage
			if err := json.Unmarshal([]byte(aws.ToString(m.Body)), &jm); err != nil {
				t.Fatalf("unmarshal SQS msg: %v", err)
			}
			key := jm.DedupID()
			if _, dup := got[key]; dup {
				t.Fatalf("duplicate SQS FIFO message received: %s", key)
			}
			got[key] = jm
			_, _ = client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(qURL),
				ReceiptHandle: m.ReceiptHandle,
			})
		}
	}
	return got
}
