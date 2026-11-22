package execute_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/martingomezvega/tapi-challenge/internal/adapters/httpapi"
	"github.com/martingomezvega/tapi-challenge/internal/app/execute"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
	"github.com/martingomezvega/tapi-challenge/internal/platform/clock"
	"github.com/martingomezvega/tapi-challenge/test/mockapi"
)

type benchHandlerTransport struct{ h http.Handler }

func (t benchHandlerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	t.h.ServeHTTP(rec, req)
	return rec.Result(), nil
}

type benchExecStore struct{}

func (benchExecStore) Acquire(_ context.Context, key execute.ExecKey, owner string, ttl time.Duration, now time.Time) (execute.Execution, execute.AcquireOutcome, error) {
	return execute.Execution{
		RequestID:      key.RequestID,
		RunDate:        key.RunDate,
		Status:         domain.StatusInProgress,
		Attempts:       1,
		LeaseOwner:     owner,
		LeaseExpiresAt: now.Add(ttl),
	}, execute.AcquireAcquired, nil
}
func (benchExecStore) Complete(_ context.Context, _ execute.ExecKey, _ string, _ execute.Result) error {
	return nil
}
func (benchExecStore) ScheduleRetry(_ context.Context, _ execute.ExecKey, _ string, _ execute.ErrInfo) error {
	return nil
}
func (benchExecStore) FailPermanent(_ context.Context, _ execute.ExecKey, _ string, _ execute.ErrInfo) error {
	return nil
}
func (benchExecStore) FailExhausted(_ context.Context, _ execute.ExecKey, _ string, _ execute.ErrInfo) error {
	return nil
}
func (benchExecStore) MarkSkipped(_ context.Context, _ execute.ExecKey, _ string, _ time.Time) error {
	return nil
}

type benchReqGetter struct{ req domain.Request }

func (g benchReqGetter) Get(_ context.Context, _ string) (domain.Request, error) { return g.req, nil }

type benchNoopAcker struct{}

func (benchNoopAcker) Ack(_ context.Context, _ execute.Message) error                     { return nil }
func (benchNoopAcker) Nack(_ context.Context, _ execute.Message, _ time.Duration) error   { return nil }
func (benchNoopAcker) Extend(_ context.Context, _ execute.Message, _ time.Duration) error { return nil }

// BenchmarkExecuteUseCase_Overhead measures ONLY the pure Go overhead and throughput of
// ExecuteRequest + SigV4 signing + JSON response handling (in-memory store & noop acker).
// It does NOT test or assert provider exclusivity — see TestWorker_LocalStack_EndToEndThroughput
// for real SQS FIFO + DynamoDB exclusivity and E2E throughput.
func BenchmarkExecuteUseCase_Overhead(b *testing.B) {
	mockSrv := mockapi.New(mockapi.Config{DefaultLatency: 0})
	apiClient := httpapi.New(httpapi.Config{
		BaseURL:     "http://mockapi.local",
		Region:      "us-east-1",
		Timeout:     5 * time.Second,
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
		Transport:   benchHandlerTransport{h: mockSrv},
	})
	clk, _ := clock.New(1)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	req := domain.Request{
		ID:       "bench-req",
		Provider: "prov-001",
		Endpoint: "/providers/prov-001/query",
		Method:   "POST",
		Body:     []byte(`{"account":"123"}`),
		Enabled:  true,
	}
	domPol := execute.NewDomainRetryPolicy(
		domain.NewClassifier(nil),
		domain.NewRetryPolicy(domain.RetryPolicyConfig{}),
		httpapi.TranslateOutcome,
		clk,
	)
	uc := execute.New(benchExecStore{}, benchReqGetter{req: req}, apiClient, nil, benchNoopAcker{}, domPol, clk, logger, execute.Config{
		OwnerID: "bench-worker",
	})
	msg := execute.Message{
		RequestID:      "bench-req",
		RunDate:        domain.RunDate("2026-11-21"),
		Provider:       "prov-001",
		ReceiveCount:   1,
		MessageGroupID: "prov-001",
	}

	var (
		mu      sync.Mutex
		samples []time.Duration
	)
	b.ResetTimer()
	start := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		local := make([]time.Duration, 0, 256)
		for pb.Next() {
			t0 := time.Now()
			if _, err := uc.Execute(context.Background(), msg); err != nil {
				b.Errorf("Execute: %v", err)
				return
			}
			local = append(local, time.Since(t0))
		}
		mu.Lock()
		samples = append(samples, local...)
		mu.Unlock()
	})
	elapsed := time.Since(start)
	b.StopTimer()

	if len(samples) > 0 {
		slices.Sort(samples)
		p50 := samples[len(samples)/2]
		p95 := samples[int(float64(len(samples))*0.95)]
		p99 := samples[int(float64(len(samples))*0.99)]
		rps := float64(len(samples)) / elapsed.Seconds()
		b.ReportMetric(float64(p50.Microseconds())/1000.0, "p50_ms")
		b.ReportMetric(float64(p95.Microseconds())/1000.0, "p95_ms")
		b.ReportMetric(float64(p99.Microseconds())/1000.0, "p99_ms")
		b.ReportMetric(rps, "req/s")
		b.Logf("ExecuteUseCase pure overhead (in-memory, no exclusivity claim): n=%d throughput=%.1f req/s p50=%v p95=%v p99=%v",
			len(samples), rps, p50, p95, p99)
	}
}
