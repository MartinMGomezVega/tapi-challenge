//go:build integration

package reconcile_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/martingomezvega/tapi-challenge/internal/adapters/dynamo"
	"github.com/martingomezvega/tapi-challenge/internal/adapters/httpapi"
	"github.com/martingomezvega/tapi-challenge/internal/adapters/s3blob"
	"github.com/martingomezvega/tapi-challenge/internal/adapters/sqsq"
	"github.com/martingomezvega/tapi-challenge/internal/app/dispatch"
	"github.com/martingomezvega/tapi-challenge/internal/app/execute"
	"github.com/martingomezvega/tapi-challenge/internal/app/reconcile"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
	"github.com/martingomezvega/tapi-challenge/internal/platform/clock"
	"github.com/martingomezvega/tapi-challenge/test/mockapi"
	"github.com/martingomezvega/tapi-challenge/test/testenv"
)

// 005-T016: Fault injection (missing + stuck ENQUEUED > 2h) -> reconciler detects, re-dispatches,
// 2nd run re-dispatches 0, worker completes all re-dispatched -> 100% coverage (SC-001) + S3 JSON/CSV reports.
func TestReconcile_LocalStack_FaultInjectionTo100PercentCoverage(t *testing.T) {
	env := testenv.StartLocalStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const (
		reqTable  = "rec-requests-it"
		execTable = "rec-executions-it"
		bucket    = "rec-reports-it"
		queueName = "rec-jobs-it.fifo"
		dlqName   = "rec-jobs-it-dlq.fifo"
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
	qURL, _, err := env.EnsureFIFOQueue(ctx, queueName, dlqName)
	if err != nil {
		t.Fatalf("EnsureFIFOQueue: %v", err)
	}

	reqRepo := dynamo.NewRequestsRepo(env.DynamoDB, reqTable)
	execRepo := dynamo.NewExecutionsRepo(env.DynamoDB, execTable, dynamo.DefaultExecutionTTL)
	blobStore := s3blob.New(env.S3, bucket)
	queue := sqsq.NewQueue(env.SQS, qURL)

	date := domain.RunDate("2026-11-21")
	dayBefore := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	runTime := time.Date(2026, 11, 22, 0, 30, 0, 0, time.UTC)
	staleTime := runTime.Add(-4 * time.Hour)

	const (
		totalReqs  = 100
		missingCnt = 10 // fault injected: no execution row
		stuckCnt   = 5  // fault injected: left ENQUEUED > 2h
	)

	reqs := make([]domain.Request, 0, totalReqs)
	for i := range totalReqs {
		prov := fmt.Sprintf("prov-%d", i%5)
		slot := i % 10 // concentrate in 10 slots so test runs fast while still scanning all 1440 slots
		reqs = append(reqs, domain.Request{
			ID:        fmt.Sprintf("rec-%03d", i),
			Provider:  prov,
			Endpoint:  fmt.Sprintf("/providers/%s/query", prov),
			Method:    "POST",
			Body:      []byte(`{}`),
			Enabled:   true,
			Slot:      slot,
			Shard:     0,
			Version:   1,
			CreatedAt: dayBefore,
			UpdatedAt: dayBefore,
		})
	}
	if err := reqRepo.PutBatch(ctx, reqs); err != nil {
		t.Fatalf("PutBatch: %v", err)
	}

	// Populate executions: [0..84] SUCCEEDED, [85..89] stuck ENQUEUED (>2h), [90..99] missing!
	for i, r := range reqs {
		if i >= totalReqs-missingCnt {
			continue // fault: missing execution
		}
		if _, err := execRepo.CreatePending(ctx, r.ID, date, r.Provider, r.Slot, staleTime); err != nil {
			t.Fatalf("CreatePending %s: %v", r.ID, err)
		}
		if err := execRepo.MarkEnqueued(ctx, []dispatch.ExecKey{{RequestID: r.ID, RunDate: date}}, staleTime); err != nil {
			t.Fatalf("MarkEnqueued %s: %v", r.ID, err)
		}
		if i < totalReqs-missingCnt-stuckCnt {
			if _, _, err := execRepo.Acquire(ctx, execute.ExecKey{RequestID: r.ID, RunDate: date}, "seed-w", time.Minute, staleTime); err != nil {
				t.Fatalf("Acquire %s: %v", r.ID, err)
			}
			if err := execRepo.Complete(ctx, execute.ExecKey{RequestID: r.ID, RunDate: date}, "seed-w", execute.Result{
				HTTPStatus: 200, ResponseBody: `{}`, LatencyMs: 5, CompletedAt: staleTime.Add(time.Second),
			}); err != nil {
				t.Fatalf("Complete %s: %v", r.ID, err)
			}
		}
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	uc := reconcile.New(reqRepo, execRepo, queue, blobStore, nil, func() time.Time { return runTime }, logger, reconcile.Config{
		Shards:      1,
		Parallelism: 32,
		StaleAfter:  2 * time.Hour,
		Location:    time.UTC,
		Redispatch:  true,
	})

	// Run #1: detects 10 missing + 5 stuck and re-dispatches all 15.
	sum1, err := uc.Run(ctx, date)
	if err != nil {
		t.Fatalf("Reconcile #1: %v", err)
	}
	if sum1.Expected != totalReqs || sum1.Missing != missingCnt || sum1.Stuck != stuckCnt || sum1.Redispatched != missingCnt+stuckCnt {
		t.Fatalf("Reconcile #1 mismatch: %+v", sum1)
	}

	// Verify S3 JSON and CSV reports exist.
	jsonBytes, err := blobStore.Get(ctx, sum1.JSONReportKey)
	if err != nil || !strings.Contains(string(jsonBytes), `"expected":100`) {
		t.Fatalf("S3 JSON report check failed: err=%v body=%s", err, string(jsonBytes))
	}
	csvBytes, err := blobStore.Get(ctx, sum1.CSVReportKey)
	if err != nil || !strings.Contains(string(csvBytes), "prov-0") {
		t.Fatalf("S3 CSV report check failed: err=%v body=%s", err, string(csvBytes))
	}

	// Run #2 immediately: 0 re-dispatches (idempotent).
	sum2, err := uc.Run(ctx, date)
	if err != nil {
		t.Fatalf("Reconcile #2: %v", err)
	}
	if sum2.Redispatched != 0 {
		t.Fatalf("Reconcile #2 redispatched = %d, want 0", sum2.Redispatched)
	}

	// Run worker to execute the 15 re-dispatched messages against mockapi.
	mockSrv := mockapi.New(mockapi.Config{DefaultLatency: 1 * time.Millisecond})
	httpTS := httptest.NewServer(mockSrv)
	defer httpTS.Close()

	apiClient := httpapi.New(httpapi.Config{
		BaseURL: httpTS.URL, Region: "us-east-1", Timeout: 5 * time.Second,
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
	})
	clk, _ := clock.New(1)
	consumer := sqsq.NewConsumer(env.SQS, sqsq.ConsumerConfig{
		QueueURL: qURL, Receivers: 2, MaxInflight: 16, WaitTimeSeconds: 1,
		VisibilityTimeout: 30 * time.Second, ShutdownTimeout: 5 * time.Second,
	}, logger)
	execUC := execute.New(execRepo, reqRepo, apiClient, blobStore, consumer, nil, clk, logger, execute.Config{OwnerID: "rec-worker"})

	wCtx, stopW := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = consumer.Run(wCtx, execUC)
	}()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if mockSrv.Stats().TotalCalls >= missingCnt+stuckCnt {
			time.Sleep(150 * time.Millisecond)
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	stopW()
	wg.Wait()

	// Run #3 after worker drains: coverage MUST be 100% (SC-001)!
	sum3, err := uc.Run(ctx, date)
	if err != nil {
		t.Fatalf("Reconcile #3: %v", err)
	}
	if sum3.Coverage != 1.0 || sum3.Terminal != totalReqs || sum3.Missing != 0 || sum3.Stuck != 0 {
		t.Fatalf("SC-001 FAILED: expected 100%% coverage after redispatch, got %+v", sum3)
	}
}
