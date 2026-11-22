//go:build integration

package execute_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"golang.org/x/sync/errgroup"

	"github.com/martingomezvega/tapi-challenge/internal/adapters/dynamo"
	"github.com/martingomezvega/tapi-challenge/internal/adapters/httpapi"
	"github.com/martingomezvega/tapi-challenge/internal/adapters/s3blob"
	"github.com/martingomezvega/tapi-challenge/internal/adapters/sqsq"
	"github.com/martingomezvega/tapi-challenge/internal/app/dispatch"
	"github.com/martingomezvega/tapi-challenge/internal/app/execute"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
	"github.com/martingomezvega/tapi-challenge/internal/platform/clock"
	"github.com/martingomezvega/tapi-challenge/test/mockapi"
	"github.com/martingomezvega/tapi-challenge/test/testenv"
)

// T018 + T020: LocalStack FIFO + real mockapi across multiple concurrent workers verifying:
// - SC-001: max_inflight == 1 per provider (invariant IV.1)
// - SC-002: 0 duplicate API calls by Idempotency-Key despite 5% injected duplicate messages.
func TestWorker_LocalStack_ExclusivityAndIdempotency(t *testing.T) {
	env := testenv.StartLocalStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	const (
		reqTable  = "worker-requests-it"
		execTable = "worker-executions-it"
		bucket    = "worker-results-it"
		queueName = "worker-jobs-it.fifo"
		dlqName   = "worker-jobs-it-dlq.fifo"
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

	numProviders := 20
	perProvider := 15 // 300 messages in -short
	numWorkers := 4
	inflightPerWorker := 8
	latency := 3 * time.Millisecond
	if !testing.Short() {
		perProvider = 500 // exact T018 scale: 20 providers x 500 = 10,000 messages, 4 workers x 16
		inflightPerWorker = 16
		latency = 1 * time.Millisecond
	}

	mockSrv := mockapi.New(mockapi.Config{DefaultLatency: latency})
	httpTS := httptest.NewServer(mockSrv)
	defer httpTS.Close()

	reqRepo := dynamo.NewRequestsRepo(env.DynamoDB, reqTable)
	execRepo := dynamo.NewExecutionsRepo(env.DynamoDB, execTable, dynamo.DefaultExecutionTTL)
	blobStore := s3blob.New(env.S3, bucket)
	producer := sqsq.NewQueue(env.SQS, qURL)
	date := domain.RunDate("2026-11-21")
	now := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)

	total := numProviders * perProvider
	reqs := make([]domain.Request, 0, total)
	msgs := make([]dispatch.JobMessage, 0, total)
	for p := range numProviders {
		prov := fmt.Sprintf("prov-%02d", p)
		for i := range perProvider {
			id := fmt.Sprintf("req-%02d-%04d", p, i)
			reqs = append(reqs, domain.Request{
				ID:        id,
				Provider:  prov,
				Endpoint:  fmt.Sprintf("/providers/%s/query", prov),
				Method:    "POST",
				Body:      []byte(`{"ok":true}`),
				Enabled:   true,
				Slot:      i % domain.SlotsPerDay,
				Shard:     0,
				Version:   1,
				CreatedAt: now,
				UpdatedAt: now,
			})
			msgs = append(msgs, dispatch.JobMessage{
				RequestID:    id,
				RunDate:      date,
				Provider:     prov,
				Slot:         i % domain.SlotsPerDay,
				DispatchedAt: now,
			})
		}
	}

	if err := reqRepo.PutBatch(ctx, reqs); err != nil {
		t.Fatalf("PutBatch requests: %v", err)
	}
	for _, m := range msgs {
		if _, err := execRepo.CreatePending(ctx, m.RequestID, m.RunDate, m.Provider, m.Slot, now); err != nil {
			t.Fatalf("CreatePending %s: %v", m.RequestID, err)
		}
	}
	for start := 0; start < len(msgs); start += 10 {
		end := min(start+10, len(msgs))
		failed, err := producer.SendBatch(ctx, msgs[start:end])
		if err != nil || len(failed) > 0 {
			t.Fatalf("SendBatch: err=%v failed=%d", err, len(failed))
		}
	}

	// Inject 5% duplicate messages with distinct SQS dedup IDs (bypassing SQS dedup window to test DynamoDB layer, SC-002).
	dupCount := max(total/20, 1)
	for i := range dupCount {
		m := msgs[i]
		body, _ := json.Marshal(m)
		_, err := env.SQS.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:               aws.String(qURL),
			MessageBody:            aws.String(string(body)),
			MessageGroupId:         aws.String(m.Provider),
			MessageDeduplicationId: aws.String(fmt.Sprintf("%s-dup-%d", m.DedupID(), i)),
		})
		if err != nil {
			t.Fatalf("inject duplicate %d: %v", i, err)
		}
	}

	// Launch 4 concurrent workers with real SigV4 HTTP client.
	apiClient := httpapi.New(httpapi.Config{
		BaseURL:             httpTS.URL,
		Region:              "us-east-1",
		Timeout:             10 * time.Second,
		MaxIdleConnsPerHost: inflightPerWorker * numWorkers,
		Credentials:         credentials.NewStaticCredentialsProvider("test", "test", ""),
	})

	runCtx, stopWorkers := context.WithCancel(ctx)
	var wg sync.WaitGroup
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	clk, _ := clock.New(1)

	for w := range numWorkers {
		consumer := sqsq.NewConsumer(env.SQS, sqsq.ConsumerConfig{
			QueueURL:          qURL,
			Receivers:         2,
			MaxInflight:       inflightPerWorker,
			WaitTimeSeconds:   1,
			VisibilityTimeout: 60 * time.Second,
			ShutdownTimeout:   10 * time.Second,
		}, logger)
		uc := execute.New(execRepo, reqRepo, apiClient, blobStore, consumer, nil, clk, logger, execute.Config{
			OwnerID: fmt.Sprintf("worker-%d", w),
		})
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = consumer.Run(runCtx, uc)
		}()
	}

	// Wait until all total calls complete and queue drains.
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		snap := mockSrv.Stats()
		if snap.TotalCalls >= int64(total) {
			// Give duplicate acks a moment to drain.
			attrOut, _ := env.SQS.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
				QueueUrl: aws.String(qURL),
				AttributeNames: []sqstypes.QueueAttributeName{
					sqstypes.QueueAttributeNameApproximateNumberOfMessages,
					sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
				},
			})
			vis := attrOut.Attributes["ApproximateNumberOfMessages"]
			notVis := attrOut.Attributes["ApproximateNumberOfMessagesNotVisible"]
			if vis == "0" && notVis == "0" {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	stopWorkers()
	wg.Wait()

	snap := mockSrv.Stats()
	t.Logf("SC-001/SC-002 stats: total_calls=%d expected=%d dup_idem=%d max_provider_inflight=%d global_max_inflight=%d",
		snap.TotalCalls, total, snap.DuplicateIdemKeys, snap.MaxProviderInflight, snap.GlobalMaxInflight)

	if snap.MaxProviderInflight != 1 {
		t.Fatalf("SC-001 VIOLATION: MaxProviderInflight = %d, want 1 (providers=%+v)", snap.MaxProviderInflight, snap.Providers)
	}
	if snap.DuplicateIdemKeys != 0 {
		t.Fatalf("SC-002 VIOLATION: DuplicateIdemKeys = %d, want 0 (injected %d duplicates)", snap.DuplicateIdemKeys, dupCount)
	}
	if snap.TotalCalls != int64(total) {
		t.Fatalf("TotalCalls = %d, want %d", snap.TotalCalls, total)
	}

	// Spot-check persisted DynamoDB execution states.
	for _, sample := range []int{0, total / 2, total - 1} {
		rec, found, err := execRepo.Get(ctx, msgs[sample].RequestID, date)
		if err != nil || !found || rec.Status != domain.StatusSucceeded {
			t.Fatalf("execution %s: found=%v status=%s err=%v", msgs[sample].RequestID, found, rec.Status, err)
		}
	}
}

// T027: Graceful shutdown (SIGTERM equivalent via context cancel) during active load -> 0 messages lost (SC-004).
func TestWorker_LocalStack_GracefulShutdown_ZeroLoss(t *testing.T) {
	env := testenv.StartLocalStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const (
		reqTable  = "worker-shutdown-reqs"
		execTable = "worker-shutdown-execs"
		bucket    = "worker-shutdown-bucket"
		queueName = "worker-shutdown.fifo"
		dlqName   = "worker-shutdown-dlq.fifo"
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

	mockSrv := mockapi.New(mockapi.Config{DefaultLatency: 15 * time.Millisecond})
	httpTS := httptest.NewServer(mockSrv)
	defer httpTS.Close()

	reqRepo := dynamo.NewRequestsRepo(env.DynamoDB, reqTable)
	execRepo := dynamo.NewExecutionsRepo(env.DynamoDB, execTable, dynamo.DefaultExecutionTTL)
	blobStore := s3blob.New(env.S3, bucket)
	producer := sqsq.NewQueue(env.SQS, qURL)
	date := domain.RunDate("2026-11-21")
	now := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)

	const total = 60
	reqs := make([]domain.Request, 0, total)
	msgs := make([]dispatch.JobMessage, 0, total)
	for i := range total {
		prov := fmt.Sprintf("prov-%d", i%6)
		id := fmt.Sprintf("sd-req-%03d", i)
		reqs = append(reqs, domain.Request{
			ID: id, Provider: prov, Endpoint: fmt.Sprintf("/providers/%s/query", prov),
			Method: "POST", Body: []byte(`{}`), Enabled: true, Slot: 0, Version: 1, CreatedAt: now, UpdatedAt: now,
		})
		msgs = append(msgs, dispatch.JobMessage{RequestID: id, RunDate: date, Provider: prov, Slot: 0, DispatchedAt: now})
	}
	if err := reqRepo.PutBatch(ctx, reqs); err != nil {
		t.Fatalf("PutBatch: %v", err)
	}
	for _, m := range msgs {
		if _, err := execRepo.CreatePending(ctx, m.RequestID, m.RunDate, m.Provider, m.Slot, now); err != nil {
			t.Fatalf("CreatePending: %v", err)
		}
	}
	for start := 0; start < len(msgs); start += 10 {
		end := min(start+10, len(msgs))
		if failed, err := producer.SendBatch(ctx, msgs[start:end]); err != nil || len(failed) > 0 {
			t.Fatalf("SendBatch: %v / %d", err, len(failed))
		}
	}

	apiClient := httpapi.New(httpapi.Config{
		BaseURL: httpTS.URL, Region: "us-east-1", Timeout: 10 * time.Second,
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
	})
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	clk, _ := clock.New(1)

	w1Ctx, stopW1 := context.WithCancel(ctx)
	w2Ctx, stopW2 := context.WithCancel(ctx)
	var wg sync.WaitGroup

	startWorker := func(wCtx context.Context, id string) {
		c := sqsq.NewConsumer(env.SQS, sqsq.ConsumerConfig{
			QueueURL: qURL, Receivers: 1, MaxInflight: 8, WaitTimeSeconds: 1,
			VisibilityTimeout: 60 * time.Second, ShutdownTimeout: 10 * time.Second,
		}, logger)
		uc := execute.New(execRepo, reqRepo, apiClient, blobStore, c, nil, clk, logger, execute.Config{OwnerID: id})
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.Run(wCtx, uc)
		}()
	}

	startWorker(w1Ctx, "worker-1")
	startWorker(w2Ctx, "worker-2")

	// Wait until partial progress, then send SIGTERM (cancel) to worker-1 while under load.
	for mockSrv.Stats().TotalCalls < 10 {
		time.Sleep(10 * time.Millisecond)
	}
	stopW1()

	// Wait for worker-2 to finish all remaining items.
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if mockSrv.Stats().TotalCalls >= total {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	stopW2()
	wg.Wait()

	snap := mockSrv.Stats()
	if snap.TotalCalls != total || snap.MaxProviderInflight != 1 || snap.DuplicateIdemKeys != 0 {
		t.Fatalf("SC-004 stats mismatch: total=%d max_inflight=%d dup=%d", snap.TotalCalls, snap.MaxProviderInflight, snap.DuplicateIdemKeys)
	}
	for _, m := range msgs {
		rec, found, err := execRepo.Get(ctx, m.RequestID, date)
		if err != nil || !found || rec.Status != domain.StatusSucceeded {
			t.Fatalf("lost or incomplete message %s: found=%v status=%s err=%v", m.RequestID, found, rec.Status, err)
		}
	}
}

// T030 (Review Finding 1): Real LocalStack End-to-End Throughput & Exclusivity test.
// Uses real SQS FIFO + sqsq.Consumer + DynamoDB RequestsRepo/ExecutionsRepo + S3 + SigV4 httpapi + mockapi
// across 50 Zipf providers, 2,000 messages, and 4 concurrent workers x 16 inflight.
// Exclusivity (max_provider_inflight == 1) is enforced purely by SQS FIFO + Consumer + DynamoDB lease
// and measured externally by mockapi.
func TestWorker_LocalStack_EndToEndThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 50-provider / 2,000-message LocalStack E2E throughput test in -short mode")
	}

	env := testenv.StartLocalStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	const (
		reqTable          = "e2e-throughput-reqs"
		execTable         = "e2e-throughput-execs"
		bucket            = "e2e-throughput-bucket"
		queueName         = "e2e-throughput.fifo"
		dlqName           = "e2e-throughput-dlq.fifo"
		numProviders      = 50
		totalMsgs         = 2000
		numWorkers        = 4
		inflightPerWorker = 16
		apiLatency        = 2 * time.Millisecond
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

	// Build 50 Zipf-distributed providers (alpha = 1.1) summing to exactly 2,000 messages.
	weights := make([]float64, numProviders)
	var sumW float64
	for i := range numProviders {
		w := 1.0 / math.Pow(float64(i+1), 1.1)
		weights[i] = w
		sumW += w
	}
	counts := make([]int, numProviders)
	assigned := 0
	for p := range numProviders {
		c := max(int(math.Round(float64(totalMsgs)*weights[p]/sumW)), 1)
		counts[p] = c
		assigned += c
	}
	counts[0] += totalMsgs - assigned

	date := domain.RunDate("2026-11-21")
	now := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	reqs := make([]domain.Request, 0, totalMsgs)
	msgs := make([]dispatch.JobMessage, 0, totalMsgs)
	for p, cnt := range counts {
		prov := fmt.Sprintf("zipf-%02d", p)
		for i := range cnt {
			id := fmt.Sprintf("e2e-%02d-%04d", p, i)
			reqs = append(reqs, domain.Request{
				ID: id, Provider: prov, Endpoint: fmt.Sprintf("/providers/%s/query", prov),
				Method: "POST", Body: []byte(`{"q":1}`), Enabled: true,
				Slot: i % domain.SlotsPerDay, Shard: 0, Version: 1, CreatedAt: now, UpdatedAt: now,
			})
			msgs = append(msgs, dispatch.JobMessage{
				RequestID: id, RunDate: date, Provider: prov, Slot: i % domain.SlotsPerDay, DispatchedAt: now,
			})
		}
	}

	reqRepo := dynamo.NewRequestsRepo(env.DynamoDB, reqTable)
	execRepo := dynamo.NewExecutionsRepo(env.DynamoDB, execTable, dynamo.DefaultExecutionTTL)
	blobStore := s3blob.New(env.S3, bucket)
	producer := sqsq.NewQueue(env.SQS, qURL)

	// Seed requests, executions (PENDING), and enqueue all 2,000 messages onto LocalStack SQS FIFO before starting timer.
	if err := reqRepo.PutBatch(ctx, reqs); err != nil {
		t.Fatalf("PutBatch requests: %v", err)
	}
	seedGrp, seedCtx := errgroup.WithContext(ctx)
	seedGrp.SetLimit(32)
	for _, m := range msgs {
		seedGrp.Go(func() error {
			_, err := execRepo.CreatePending(seedCtx, m.RequestID, m.RunDate, m.Provider, m.Slot, now)
			return err
		})
	}
	if err := seedGrp.Wait(); err != nil {
		t.Fatalf("CreatePending seed: %v", err)
	}

	enqGrp, enqCtx := errgroup.WithContext(ctx)
	enqGrp.SetLimit(16)
	for start := 0; start < len(msgs); start += 10 {
		chunk := msgs[start:min(start+10, len(msgs))]
		enqGrp.Go(func() error {
			failed, err := producer.SendBatch(enqCtx, chunk)
			if err != nil {
				return err
			}
			if len(failed) > 0 {
				return fmt.Errorf("SendBatch had %d failed entries", len(failed))
			}
			return nil
		})
	}
	if err := enqGrp.Wait(); err != nil {
		t.Fatalf("SendBatch seed: %v", err)
	}

	mockSrv := mockapi.New(mockapi.Config{DefaultLatency: apiLatency})
	httpTS := httptest.NewServer(mockSrv)
	defer httpTS.Close()

	apiClient := httpapi.New(httpapi.Config{
		BaseURL:             httpTS.URL,
		Region:              "us-east-1",
		Timeout:             10 * time.Second,
		MaxIdleConnsPerHost: numWorkers * inflightPerWorker,
		Credentials:         credentials.NewStaticCredentialsProvider("test", "test", ""),
	})

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	clk, _ := clock.New(1)
	domPol := execute.NewDomainRetryPolicy(
		domain.NewClassifier(nil),
		domain.NewRetryPolicy(domain.RetryPolicyConfig{}),
		httpapi.TranslateOutcome,
		clk,
	)

	runCtx, stopWorkers := context.WithCancel(ctx)
	var wg sync.WaitGroup
	start := time.Now()

	for w := range numWorkers {
		consumer := sqsq.NewConsumer(env.SQS, sqsq.ConsumerConfig{
			QueueURL:          qURL,
			Receivers:         2,
			MaxInflight:       inflightPerWorker,
			WaitTimeSeconds:   1,
			VisibilityTimeout: 60 * time.Second,
			ShutdownTimeout:   10 * time.Second,
		}, logger)
		uc := execute.New(execRepo, reqRepo, apiClient, blobStore, consumer, domPol, clk, logger, execute.Config{
			OwnerID: fmt.Sprintf("e2e-worker-%d", w),
		})
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = consumer.Run(runCtx, uc)
		}()
	}

	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		if mockSrv.Stats().TotalCalls >= totalMsgs {
			attrOut, _ := env.SQS.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
				QueueUrl: aws.String(qURL),
				AttributeNames: []sqstypes.QueueAttributeName{
					sqstypes.QueueAttributeNameApproximateNumberOfMessages,
					sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
				},
			})
			if attrOut.Attributes["ApproximateNumberOfMessages"] == "0" &&
				attrOut.Attributes["ApproximateNumberOfMessagesNotVisible"] == "0" {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	wall := time.Since(start)

	stopWorkers()
	wg.Wait()

	snap := mockSrv.Stats()
	rps := float64(snap.TotalCalls) / wall.Seconds()
	t.Logf("E2E_LOCALSTACK_REPORT: providers=%d msgs=%d workers=%d inflight_per_worker=%d top_provider_msgs=%d wall=%v throughput=%.1f req/s max_provider_inflight=%d global_max_inflight=%d dup_idem=%d",
		numProviders, totalMsgs, numWorkers, inflightPerWorker, counts[0], wall.Round(time.Millisecond), rps,
		snap.MaxProviderInflight, snap.GlobalMaxInflight, snap.DuplicateIdemKeys)

	if snap.TotalCalls != totalMsgs {
		t.Fatalf("TotalCalls = %d, want %d", snap.TotalCalls, totalMsgs)
	}
	if snap.MaxProviderInflight != 1 {
		t.Fatalf("SC-001 VIOLATION: real LocalStack FIFO MaxProviderInflight = %d, want 1", snap.MaxProviderInflight)
	}
	if snap.DuplicateIdemKeys != 0 {
		t.Fatalf("SC-002 VIOLATION: DuplicateIdemKeys = %d, want 0", snap.DuplicateIdemKeys)
	}
}
