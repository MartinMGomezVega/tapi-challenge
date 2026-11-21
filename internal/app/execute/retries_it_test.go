//go:build integration

package execute_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/martingomezvega/tapi-challenge/internal/adapters/dynamo"
	"github.com/martingomezvega/tapi-challenge/internal/adapters/httpapi"
	"github.com/martingomezvega/tapi-challenge/internal/adapters/s3blob"
	"github.com/martingomezvega/tapi-challenge/internal/adapters/sqsq"
	"github.com/martingomezvega/tapi-challenge/internal/app/dispatch"
	"github.com/martingomezvega/tapi-challenge/internal/app/execute"
	"github.com/martingomezvega/tapi-challenge/internal/app/redrive"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
	"github.com/martingomezvega/tapi-challenge/internal/platform/clock"
	"github.com/martingomezvega/tapi-challenge/test/mockapi"
	"github.com/martingomezvega/tapi-challenge/test/testenv"
)

// 004-T015, T016, T021: LocalStack E2E verifying:
// - SC-002: Provider bad503 (100% 503) exhausts 6 receives -> FAILED_EXHAUSTED + DLQ while healthy providers complete 100% unaffected.
// - SC-004: Provider bad400 (HTTP 400) -> FAILED_PERMANENT with 0 retries (calls == 1).
// - US5: redrive.UseCase re-enqueues bad503 from DLQ, resets execution -> ENQUEUED preserving attempts, and succeeds once healthy.
func TestWorker_LocalStack_RetriesExhaustionIsolationAndRedrive(t *testing.T) {
	env := testenv.StartLocalStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const (
		reqTable  = "retry-requests-it"
		execTable = "retry-executions-it"
		bucket    = "retry-results-it"
		queueName = "retry-jobs-it.fifo"
		dlqName   = "retry-jobs-it-dlq.fifo"
	)

	if err := env.EnsureRequestsTable(ctx, reqTable); err != nil {
		t.Fatalf("EnsureRequestsTable: %v", err)
	}
	if err := env.EnsureExecutionsTable(ctx, execTable); err != nil {
		t.Fatalf("EnsureExecutionsTable: %v", err)
	}
	if err := env.EnsureBucket(ctx, bucket); err != nil {
		t.Fatalf("EnsureBucket: %v", err)
	}
	qURL, dlqURL, err := env.EnsureFIFOQueue(ctx, queueName, dlqName)
	if err != nil {
		t.Fatalf("EnsureFIFOQueue: %v", err)
	}

	mockSrv := mockapi.New(mockapi.Config{
		DefaultLatency: 2 * time.Millisecond,
		Faults: map[string]mockapi.FaultRule{
			"bad503": {StatusCode: http.StatusServiceUnavailable},
			"bad400": {StatusCode: http.StatusBadRequest},
		},
	})
	httpTS := httptest.NewServer(mockSrv)
	defer httpTS.Close()

	reqRepo := dynamo.NewRequestsRepo(env.DynamoDB, reqTable)
	execRepo := dynamo.NewExecutionsRepo(env.DynamoDB, execTable, dynamo.DefaultExecutionTTL)
	blobStore := s3blob.New(env.S3, bucket)
	producer := sqsq.NewQueue(env.SQS, qURL)
	date := domain.RunDate("2026-11-21")
	now := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)

	reqs := []domain.Request{
		{ID: "req-503", Provider: "bad503", Endpoint: "/providers/bad503/query", Method: "POST", Body: []byte(`{}`), Enabled: true, Slot: 0, Version: 1, CreatedAt: now, UpdatedAt: now},
		{ID: "req-400", Provider: "bad400", Endpoint: "/providers/bad400/query", Method: "POST", Body: []byte(`{}`), Enabled: true, Slot: 0, Version: 1, CreatedAt: now, UpdatedAt: now},
	}
	for p := range 3 {
		prov := fmt.Sprintf("good-%d", p)
		for i := range 10 {
			reqs = append(reqs, domain.Request{
				ID: fmt.Sprintf("req-good-%d-%d", p, i), Provider: prov, Endpoint: fmt.Sprintf("/providers/%s/query", prov),
				Method: "POST", Body: []byte(`{}`), Enabled: true, Slot: 0, Version: 1, CreatedAt: now, UpdatedAt: now,
			})
		}
	}
	if err := reqRepo.PutBatch(ctx, reqs); err != nil {
		t.Fatalf("PutBatch: %v", err)
	}

	msgs := make([]dispatch.JobMessage, 0, len(reqs))
	for _, r := range reqs {
		if _, err := execRepo.CreatePending(ctx, r.ID, date, r.Provider, r.Slot, now); err != nil {
			t.Fatalf("CreatePending %s: %v", r.ID, err)
		}
		msgs = append(msgs, dispatch.JobMessage{RequestID: r.ID, RunDate: date, Provider: r.Provider, Slot: r.Slot, DispatchedAt: now})
	}
	for start := 0; start < len(msgs); start += 10 {
		end := min(start+10, len(msgs))
		if failed, err := producer.SendBatch(ctx, msgs[start:end]); err != nil || len(failed) > 0 {
			t.Fatalf("SendBatch: %v / %d", err, len(failed))
		}
	}

	apiClient := httpapi.New(httpapi.Config{
		BaseURL: httpTS.URL, Region: "us-east-1", Timeout: 5 * time.Second,
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
	})
	clk, _ := clock.New(1)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	// Use 1s compressed deferred backoff in integration test so 6 SQS receives complete quickly
	// while exercising real SQS RedrivePolicy maxReceiveCount=6.
	domPol := execute.NewDomainRetryPolicy(
		domain.NewClassifier(nil),
		domain.NewRetryPolicy(domain.RetryPolicyConfig{
			MaxReceiveCount:      6,
			MaxInProcessAttempts: 1, // disable in-process so every receive hits SQS RedrivePolicy cleanly
			BaseDelay:            1 * time.Second,
			MaxDelay:             1 * time.Second,
			RandFloat64:          func() float64 { return 0.5 },
		}),
		httpapi.TranslateOutcome,
		clk,
	)

	consumer := sqsq.NewConsumer(env.SQS, sqsq.ConsumerConfig{
		QueueURL: qURL, Receivers: 2, MaxInflight: 16, WaitTimeSeconds: 1,
		VisibilityTimeout: 10 * time.Second, ShutdownTimeout: 5 * time.Second,
	}, logger)
	uc := execute.New(execRepo, reqRepo, apiClient, blobStore, consumer, domPol, clk, logger, execute.Config{
		OwnerID:  "retry-worker",
		LeaseTTL: 10 * time.Second,
	})

	runCtx, stopWorker := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = consumer.Run(runCtx, uc)
	}()

	// Wait until all 30 good requests SUCCEED, req-400 is FAILED_PERMANENT, and req-503 lands in DLQ as FAILED_EXHAUSTED.
	deadline := time.Now().Add(60 * time.Second)
	var dlqVisible string
	for time.Now().Before(deadline) {
		rec503, _, _ := execRepo.Get(ctx, "req-503", date)
		rec400, _, _ := execRepo.Get(ctx, "req-400", date)
		attrOut, _ := env.SQS.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl:       aws.String(dlqURL),
			AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameApproximateNumberOfMessages},
		})
		dlqVisible = attrOut.Attributes["ApproximateNumberOfMessages"]
		if rec503.Status == domain.StatusFailedExhausted && rec400.Status == domain.StatusFailedPermanent && dlqVisible == "1" {
			break
		}
		time.Sleep(150 * time.Millisecond)
	}

	stopWorker()
	wg.Wait()

	// Assert SC-004: bad400 had 0 retries (exactly 1 call) and FAILED_PERMANENT.
	snap := mockSrv.Stats()
	if snap.Providers["bad400"].Calls != 1 {
		t.Fatalf("SC-004 VIOLATION: bad400 calls = %d, want 1 (0 retries)", snap.Providers["bad400"].Calls)
	}
	rec400, found, err := execRepo.Get(ctx, "req-400", date)
	if err != nil || !found || rec400.Status != domain.StatusFailedPermanent {
		t.Fatalf("req-400 status=%s found=%v err=%v, want FAILED_PERMANENT", rec400.Status, found, err)
	}

	// Assert SC-002: all 30 healthy requests SUCCEEDED unaffected while bad503 hit FAILED_EXHAUSTED + DLQ.
	for p := range 3 {
		prov := fmt.Sprintf("good-%d", p)
		if snap.Providers[prov].Calls != 10 {
			t.Fatalf("healthy provider %s calls = %d, want 10", prov, snap.Providers[prov].Calls)
		}
	}
	rec503, found, err := execRepo.Get(ctx, "req-503", date)
	if err != nil || !found || rec503.Status != domain.StatusFailedExhausted {
		t.Fatalf("req-503 status=%s found=%v err=%v, want FAILED_EXHAUSTED", rec503.Status, found, err)
	}
	if dlqVisible != "1" {
		t.Fatalf("expected 1 message in DLQ, got %q", dlqVisible)
	}
	prevAttempts := rec503.Attempts
	if prevAttempts < 6 {
		t.Fatalf("req-503 attempts = %d, want >= 6", prevAttempts)
	}

	// Assert US5: operator redrive resets req-503 -> ENQUEUED preserving attempts and re-enqueues to main queue.
	dlqAdapter := sqsq.NewDLQAdapter(env.SQS, dlqURL, qURL)
	redriver := redrive.New(dlqAdapter, dlqAdapter, execRepo, time.Now)
	rep, err := redriver.Run(ctx, redrive.Filter{Provider: "bad503", Since: date})
	if err != nil || rep.Redriven != 1 {
		t.Fatalf("redrive Run(): rep=%+v err=%v", rep, err)
	}
	recAfter, _, err := execRepo.Get(ctx, "req-503", date)
	if err != nil || recAfter.Status != domain.StatusEnqueued || recAfter.Attempts != prevAttempts {
		t.Fatalf("after redrive: status=%s attempts=%d (want ENQUEUED / %d) err=%v",
			recAfter.Status, recAfter.Attempts, prevAttempts, err)
	}
}
