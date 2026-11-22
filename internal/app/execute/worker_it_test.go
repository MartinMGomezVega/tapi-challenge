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
	"slices"
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

// T030: Sustained load test >= 120 req/s across 200 Zipf providers measuring worker overhead p99 < 50 ms (SC-001..SC-003).
func TestWorker_LoadBenchmark_Zipf200Providers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 200-provider Zipf load test in -short mode")
	}

	const (
		numProviders = 200
		totalMsgs    = 2400 // >= 120 req/s target
		apiLatency   = 5 * time.Millisecond
	)

	mockSrv := mockapi.New(mockapi.Config{DefaultLatency: apiLatency})
	httpTS := httptest.NewServer(mockSrv)
	defer httpTS.Close()

	// Build 200 Zipf-distributed providers (alpha=1.1).
	weights := make([]float64, numProviders)
	var sumW float64
	for i := range numProviders {
		w := 1.0 / math.Pow(float64(i+1), 1.1)
		weights[i] = w
		sumW += w
	}

	date := domain.RunDate("2026-11-21")
	reqsMap := make(map[string]domain.Request, totalMsgs)
	byProvider := make(map[string][]execute.Message, numProviders)
	assigned := 0
	for p := range numProviders {
		count := max(int(math.Round(float64(totalMsgs)*weights[p]/sumW)), 1)
		if assigned+count > totalMsgs {
			count = totalMsgs - assigned
		}
		if count <= 0 {
			break
		}
		prov := fmt.Sprintf("zipf-%03d", p)
		for i := range count {
			id := fmt.Sprintf("z-%03d-%04d", p, i)
			reqsMap[id] = domain.Request{
				ID: id, Provider: prov, Endpoint: fmt.Sprintf("/providers/%s/query", prov),
				Method: "POST", Body: []byte(`{"q":1}`), Enabled: true,
			}
			byProvider[prov] = append(byProvider[prov], execute.Message{
				RequestID: id, RunDate: date, Provider: prov, ReceiveCount: 1, MessageGroupID: prov,
			})
		}
		assigned += count
	}

	apiClient := httpapi.New(httpapi.Config{
		BaseURL:             httpTS.URL,
		Region:              "us-east-1",
		Timeout:             10 * time.Second,
		MaxIdleConnsPerHost: 64,
		Credentials:         credentials.NewStaticCredentialsProvider("test", "test", ""),
	})

	store := &memExecStore{items: make(map[string]domain.ExecutionStatus, assigned)}
	reqs := &memReqGetter{items: reqsMap}
	acker := &noopAcker{}
	clk, _ := clock.New(1)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	uc := execute.New(store, reqs, apiClient, nil, acker, nil, clk, logger, execute.Config{OwnerID: "load-worker"})

	sem := make(chan struct{}, 64)
	var (
		wg        sync.WaitGroup
		ovMu      sync.Mutex
		overheads = make([]time.Duration, 0, assigned)
	)

	start := time.Now()
	for _, group := range byProvider {
		wg.Add(1)
		go func(msgs []execute.Message) {
			defer wg.Done()
			for _, m := range msgs {
				sem <- struct{}{}
				t0 := time.Now()
				_, err := uc.Execute(context.Background(), m)
				elapsed := time.Since(t0)
				<-sem
				if err != nil {
					t.Errorf("Execute %s: %v", m.RequestID, err)
					return
				}
				ov := max(elapsed-apiLatency, 0)
				ovMu.Lock()
				overheads = append(overheads, ov)
				ovMu.Unlock()
			}
		}(group)
	}
	wg.Wait()
	wall := time.Since(start)

	slices.Sort(overheads)
	p50 := overheads[len(overheads)/2]
	p95 := overheads[int(float64(len(overheads))*0.95)]
	p99 := overheads[int(float64(len(overheads))*0.99)]
	rps := float64(assigned) / wall.Seconds()
	snap := mockSrv.Stats()

	t.Logf("LOAD REPORT (Zipf 200 providers): msgs=%d wall=%v throughput=%.1f req/s overhead_p50=%v overhead_p95=%v overhead_p99=%v max_provider_inflight=%d dup_idem=%d",
		assigned, wall.Round(time.Millisecond), rps, p50, p95, p99, snap.MaxProviderInflight, snap.DuplicateIdemKeys)

	if snap.MaxProviderInflight != 1 {
		t.Fatalf("SC-001 failed: max_provider_inflight=%d", snap.MaxProviderInflight)
	}
	if snap.DuplicateIdemKeys != 0 {
		t.Fatalf("SC-002 failed: dup_idem=%d", snap.DuplicateIdemKeys)
	}
	if rps < 120.0 {
		t.Fatalf("SC-003 failed: throughput=%.1f req/s < 120 req/s", rps)
	}
	if p99 >= 50*time.Millisecond {
		t.Fatalf("SC-003 failed: overhead p99=%v >= 50ms", p99)
	}
}

type memExecStore struct {
	mu    sync.Mutex
	items map[string]domain.ExecutionStatus
}

func (m *memExecStore) Acquire(_ context.Context, key execute.ExecKey, owner string, ttl time.Duration, now time.Time) (execute.Execution, execute.AcquireOutcome, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := key.RequestID + "#" + string(key.RunDate)
	if st, ok := m.items[k]; ok && st.IsTerminal() {
		return execute.Execution{Status: st}, execute.AcquireAlreadyTerminal, nil
	}
	m.items[k] = domain.StatusInProgress
	return execute.Execution{RequestID: key.RequestID, RunDate: key.RunDate, Status: domain.StatusInProgress, Attempts: 1, LeaseOwner: owner, LeaseExpiresAt: now.Add(ttl)}, execute.AcquireAcquired, nil
}
func (m *memExecStore) Complete(_ context.Context, key execute.ExecKey, _ string, _ execute.Result) error {
	m.mu.Lock()
	m.items[key.RequestID+"#"+string(key.RunDate)] = domain.StatusSucceeded
	m.mu.Unlock()
	return nil
}
func (m *memExecStore) ScheduleRetry(_ context.Context, _ execute.ExecKey, _ string, _ execute.ErrInfo) error {
	return nil
}
func (m *memExecStore) FailPermanent(_ context.Context, _ execute.ExecKey, _ string, _ execute.ErrInfo) error {
	return nil
}
func (m *memExecStore) FailExhausted(_ context.Context, _ execute.ExecKey, _ string, _ execute.ErrInfo) error {
	return nil
}
func (m *memExecStore) MarkSkipped(_ context.Context, _ execute.ExecKey, _ string, _ time.Time) error {
	return nil
}

type memReqGetter struct{ items map[string]domain.Request }

func (m *memReqGetter) Get(_ context.Context, id string) (domain.Request, error) {
	r, ok := m.items[id]
	if !ok {
		return domain.Request{}, domain.ErrRequestNotFound
	}
	return r, nil
}

type noopAcker struct{}

func (noopAcker) Ack(_ context.Context, _ execute.Message) error                     { return nil }
func (noopAcker) Nack(_ context.Context, _ execute.Message, _ time.Duration) error   { return nil }
func (noopAcker) Extend(_ context.Context, _ execute.Message, _ time.Duration) error { return nil }
