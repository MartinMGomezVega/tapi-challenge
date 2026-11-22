package execute_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/app/execute"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
	"github.com/martingomezvega/tapi-challenge/internal/platform/clock"
)

// T011 + T012 + T018 + T020 + T024: ExecuteRequest wired with real DomainRetryPolicy & ProviderBreaker.
func TestExecute_DomainRetryPolicy_And_RealBreaker(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	clk := clock.NewFake(now)
	domPol := execute.NewDomainRetryPolicy(
		domain.NewClassifier(nil),
		domain.NewRetryPolicy(domain.RetryPolicyConfig{
			RandFloat64:       func() float64 { return 0.5 },
			InProcessMinDelay: time.Millisecond,
			InProcessMaxDelay: 2 * time.Millisecond,
		}),
		nil,
		clk,
	)

	t.Run("409 ALREADY_PROCESSED completes cleanly", func(t *testing.T) {
		t.Parallel()
		store := &fakeExecStore{outcome: execute.AcquireAcquired}
		api := &fakeProviderAPI{resp: execute.Response{StatusCode: 409, Body: []byte(`{"status":"duplicate"}`)}}
		acker := &fakeAcker{}
		uc := execute.New(store, &fakeReqGetter{req: sampleReq()}, api, &fakeBlobStore{}, acker, domPol, clk, discardLogger(), execute.Config{OwnerID: "w1"})
		if _, err := uc.Execute(context.Background(), sampleMsg()); err != nil {
			t.Fatalf("Execute 409: %v", err)
		}
		if store.completeCalls != 1 || acker.acks != 1 {
			t.Fatalf("expected Complete+Ack on 409, got complete=%d acks=%d", store.completeCalls, acker.acks)
		}
	})

	t.Run("429 respects Retry-After header and schedules retry", func(t *testing.T) {
		t.Parallel()
		store := &fakeExecStore{outcome: execute.AcquireAcquired}
		api := &fakeProviderAPI{resp: execute.Response{StatusCode: 429, RetryAfter: "180"}}
		acker := &fakeAcker{}
		uc := execute.New(store, &fakeReqGetter{req: sampleReq()}, api, &fakeBlobStore{}, acker, domPol, clk, discardLogger(), execute.Config{OwnerID: "w1"})
		if _, err := uc.Execute(context.Background(), sampleMsg()); err != nil {
			t.Fatalf("Execute 429: %v", err)
		}
		if store.scheduleCalls != 1 || store.scheduledInfo.ErrorClass != "THROTTLED" || acker.lastDelay != 3*time.Minute {
			t.Fatalf("schedule=%d info=%+v delay=%v", store.scheduleCalls, store.scheduledInfo, acker.lastDelay)
		}
	})

	t.Run("401 AUTH fails permanent with sanitized error message and 0 retries", func(t *testing.T) {
		t.Parallel()
		store := &fakeExecStore{outcome: execute.AcquireAcquired}
		api := &fakeProviderAPI{resp: execute.Response{StatusCode: 401}}
		acker := &fakeAcker{}
		uc := execute.New(store, &fakeReqGetter{req: sampleReq()}, api, &fakeBlobStore{}, acker, domPol, clk, discardLogger(), execute.Config{OwnerID: "w1"})
		if _, err := uc.Execute(context.Background(), sampleMsg()); err != nil {
			t.Fatalf("Execute 401: %v", err)
		}
		if api.calls != 1 || store.failPermCalls != 1 || store.failedInfo.ErrorClass != "AUTH" || acker.acks != 1 {
			t.Fatalf("calls=%d failPerm=%d class=%s acks=%d", api.calls, store.failPermCalls, store.failedInfo.ErrorClass, acker.acks)
		}
	})

	t.Run("transport error with bearer token is sanitized before persisting", func(t *testing.T) {
		t.Parallel()
		store := &fakeExecStore{outcome: execute.AcquireAcquired}
		api := &fakeProviderAPI{err: errors.New("failed Authorization: Bearer supersecret.jwt.token user=a@b.com")}
		acker := &fakeAcker{}
		uc := execute.New(store, &fakeReqGetter{req: sampleReq()}, api, &fakeBlobStore{}, acker, domPol, clk, discardLogger(), execute.Config{OwnerID: "w1"})
		if _, err := uc.Execute(context.Background(), sampleMsg()); err != nil {
			t.Fatalf("Execute transport err: %v", err)
		}
		if strings.Contains(store.scheduledInfo.ErrorMessage, "supersecret") || strings.Contains(store.scheduledInfo.ErrorMessage, "a@b.com") {
			t.Fatalf("PII/token leaked in ErrorMessage: %q", store.scheduledInfo.ErrorMessage)
		}
	})

	t.Run("T018+T020 real ProviderBreaker opens after 50% failures and defers without calling API or Acquire", func(t *testing.T) {
		t.Parallel()
		brkClk := clock.NewFake(now)
		brk := domain.NewProviderBreaker(domain.BreakerConfig{
			WindowSize:       4,
			FailureThreshold: 0.50,
			Cooldown:         60 * time.Second,
			Now:              brkClk.Now,
		})
		// Trip breaker with 4 retryable failures.
		for range 4 {
			brk.Record("edenor", true)
		}
		store := &fakeExecStore{outcome: execute.AcquireAcquired}
		api := &fakeProviderAPI{resp: execute.Response{StatusCode: 200}}
		acker := &fakeAcker{}
		met := &fakeMetrics{}

		uc := execute.New(store, &fakeReqGetter{req: sampleReq()}, api, &fakeBlobStore{}, acker, domPol, brkClk, discardLogger(), execute.Config{OwnerID: "w1"}).
			WithBreaker(brk).WithMetrics(met)
		if _, err := uc.Execute(context.Background(), sampleMsg()); err != nil {
			t.Fatalf("Execute open breaker: %v", err)
		}
		if api.calls != 0 || len(store.events) != 0 || acker.nacks != 1 || acker.lastDelay != 60*time.Second {
			t.Fatalf("expected breaker short-circuit without Acquire/Call: api=%d events=%v nacks=%d delay=%v",
				api.calls, store.events, acker.nacks, acker.lastDelay)
		}
		if len(met.breakerOpens) != 1 || !met.breakerOpens[0] {
			t.Fatalf("expected BreakerOpen=true metric, got %v", met.breakerOpens)
		}
	})

	t.Run("T013 DynamoDB persistence failure nacks without calling provider API", func(t *testing.T) {
		t.Parallel()
		store := &fakeExecStore{acquireErr: errors.New("ProvisionedThroughputExceededException")}
		api := &fakeProviderAPI{}
		acker := &fakeAcker{}
		uc := execute.New(store, &fakeReqGetter{req: sampleReq()}, api, &fakeBlobStore{}, acker, domPol, clk, discardLogger(), execute.Config{OwnerID: "w1"})
		if _, err := uc.Execute(context.Background(), sampleMsg()); err == nil {
			t.Fatalf("expected error on DynamoDB acquire failure")
		}
		if api.calls != 0 || acker.nacks != 1 {
			t.Fatalf("expected 0 provider calls and 1 nack, got calls=%d nacks=%d", api.calls, acker.nacks)
		}
	})
}
