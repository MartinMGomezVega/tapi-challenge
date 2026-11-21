package execute_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/app/execute"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
	"github.com/martingomezvega/tapi-challenge/internal/platform/clock"
)

type fakeExecStore struct {
	mu             sync.Mutex
	outcome        execute.AcquireOutcome
	exec           execute.Execution
	acquireErr     error
	completeErr    error
	scheduleErr    error
	failPermErr    error
	failExhErr     error
	skipErr        error
	events         []string
	completedRes   execute.Result
	failedInfo     execute.ErrInfo
	scheduledInfo  execute.ErrInfo
	exhaustedInfo  execute.ErrInfo
	skippedCalls   int
	completeCalls  int
	failPermCalls  int
	failExhCalls   int
	scheduleCalls  int
}

func (f *fakeExecStore) Acquire(_ context.Context, key execute.ExecKey, owner string, ttl time.Duration, now time.Time) (execute.Execution, execute.AcquireOutcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, "acquire")
	if f.acquireErr != nil {
		return execute.Execution{}, 0, f.acquireErr
	}
	e := f.exec
	if e.RequestID == "" {
		e.RequestID = key.RequestID
		e.RunDate = key.RunDate
	}
	if e.LeaseOwner == "" {
		e.LeaseOwner = owner
	}
	if e.LeaseExpiresAt.IsZero() {
		e.LeaseExpiresAt = now.Add(ttl)
	}
	return e, f.outcome, nil
}

func (f *fakeExecStore) Complete(_ context.Context, _ execute.ExecKey, _ string, res execute.Result) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, "complete")
	f.completeCalls++
	f.completedRes = res
	return f.completeErr
}

func (f *fakeExecStore) ScheduleRetry(_ context.Context, _ execute.ExecKey, _ string, info execute.ErrInfo) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, "schedule_retry")
	f.scheduleCalls++
	f.scheduledInfo = info
	return f.scheduleErr
}

func (f *fakeExecStore) FailPermanent(_ context.Context, _ execute.ExecKey, _ string, info execute.ErrInfo) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, "fail_permanent")
	f.failPermCalls++
	f.failedInfo = info
	return f.failPermErr
}

func (f *fakeExecStore) FailExhausted(_ context.Context, _ execute.ExecKey, _ string, info execute.ErrInfo) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, "fail_exhausted")
	f.failExhCalls++
	f.exhaustedInfo = info
	return f.failExhErr
}

func (f *fakeExecStore) MarkSkipped(_ context.Context, _ execute.ExecKey, _ string, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, "mark_skipped")
	f.skippedCalls++
	return f.skipErr
}

type fakeReqGetter struct {
	req domain.Request
	err error
}

func (f *fakeReqGetter) Get(_ context.Context, _ string) (domain.Request, error) {
	return f.req, f.err
}

type fakeProviderAPI struct {
	mu           sync.Mutex
	calls        int
	lastReq      domain.Request
	lastIdemKey  string
	lastTrace    string
	resp         execute.Response
	err          error
	fn           func(call int) (execute.Response, error)
}

func (f *fakeProviderAPI) Call(_ context.Context, req domain.Request, idemKey, traceParent string) (execute.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastReq = req
	f.lastIdemKey = idemKey
	f.lastTrace = traceParent
	if f.fn != nil {
		return f.fn(f.calls)
	}
	return f.resp, f.err
}

type fakeBlobStore struct {
	mu      sync.Mutex
	events  *[]string
	lastKey string
	lastLen int
	puts    int
	err     error
}

func (f *fakeBlobStore) Put(_ context.Context, key string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.events != nil {
		*f.events = append(*f.events, "blob_put")
	}
	f.puts++
	f.lastKey = key
	f.lastLen = len(data)
	return f.err
}

type fakeAcker struct {
	mu          sync.Mutex
	acks        int
	nacks       int
	extends     int
	lastDelay   time.Duration
	lastExtend  time.Duration
	events      *[]string
}

func (f *fakeAcker) Ack(_ context.Context, _ execute.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.events != nil {
		*f.events = append(*f.events, "ack")
	}
	f.acks++
	return nil
}

func (f *fakeAcker) Nack(_ context.Context, _ execute.Message, delay time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.events != nil {
		*f.events = append(*f.events, "nack")
	}
	f.nacks++
	f.lastDelay = delay
	return nil
}

func (f *fakeAcker) Extend(_ context.Context, _ execute.Message, d time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.events != nil {
		*f.events = append(*f.events, "extend")
	}
	f.extends++
	f.lastExtend = d
	return nil
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func sampleReq() domain.Request {
	return domain.Request{
		ID:       "req-001",
		Provider: "edenor",
		Endpoint: "/providers/edenor/query",
		Method:   "POST",
		Body:     []byte(`{"account":"123"}`),
		Enabled:  true,
		Slot:     10,
	}
}

func sampleMsg() execute.Message {
	return execute.Message{
		RequestID:      "req-001",
		RunDate:        domain.RunDate("2026-11-21"),
		Provider:       "edenor",
		Slot:           10,
		TraceContext:   "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		ReceiptHandle:  "rh-1",
		ReceiveCount:   1,
		MessageGroupID: "edenor",
	}
}

// T005: ExecuteRequest success -> Acquire -> Call -> Complete -> Ack.
func TestExecute_SuccessInlineResult(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	clk := clock.NewFake(now)
	store := &fakeExecStore{outcome: execute.AcquireAcquired, exec: execute.Execution{Attempts: 1}}
	reqs := &fakeReqGetter{req: sampleReq()}
	api := &fakeProviderAPI{resp: execute.Response{StatusCode: 200, Body: []byte(`{"debt":100}`)}}
	blobs := &fakeBlobStore{events: &store.events}
	acker := &fakeAcker{events: &store.events}

	uc := execute.New(store, reqs, api, blobs, acker, nil, clk, discardLogger(), execute.Config{OwnerID: "worker-1"})
	if err := uc.Execute(context.Background(), sampleMsg()); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if api.calls != 1 {
		t.Fatalf("api calls = %d, want 1", api.calls)
	}
	if api.lastIdemKey != "req-001#2026-11-21" {
		t.Fatalf("Idempotency-Key = %q, want req-001#2026-11-21", api.lastIdemKey)
	}
	if api.lastTrace != sampleMsg().TraceContext {
		t.Fatalf("traceparent = %q, want %q", api.lastTrace, sampleMsg().TraceContext)
	}
	if store.completeCalls != 1 || store.completedRes.HTTPStatus != 200 || store.completedRes.ResponseBody != `{"debt":100}` {
		t.Fatalf("unexpected complete result: calls=%d res=%+v", store.completeCalls, store.completedRes)
	}
	if blobs.puts != 0 {
		t.Fatalf("blob puts = %d, want 0 for inline response", blobs.puts)
	}
	if acker.acks != 1 || acker.nacks != 0 {
		t.Fatalf("acks=%d nacks=%d, want 1/0", acker.acks, acker.nacks)
	}
}

// T006: NotFound -> FAILED_PERMANENT/REQUEST_NOT_FOUND; Disabled -> SKIPPED.
func TestExecute_NotFoundAndDisabled(t *testing.T) {
	t.Parallel()

	t.Run("request not found", func(t *testing.T) {
		t.Parallel()
		clk := clock.NewFake(time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC))
		store := &fakeExecStore{outcome: execute.AcquireAcquired}
		reqs := &fakeReqGetter{err: domain.ErrRequestNotFound}
		api := &fakeProviderAPI{}
		acker := &fakeAcker{}

		uc := execute.New(store, reqs, api, &fakeBlobStore{}, acker, nil, clk, discardLogger(), execute.Config{OwnerID: "w1"})
		if err := uc.Execute(context.Background(), sampleMsg()); err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		if api.calls != 0 {
			t.Fatalf("api calls = %d, want 0", api.calls)
		}
		if store.failPermCalls != 1 || store.failedInfo.ErrorClass != execute.ReasonNotFound {
			t.Fatalf("failPermCalls=%d info=%+v, want 1 / REQUEST_NOT_FOUND", store.failPermCalls, store.failedInfo)
		}
		if acker.acks != 1 {
			t.Fatalf("acks = %d, want 1", acker.acks)
		}
	})

	t.Run("request disabled", func(t *testing.T) {
		t.Parallel()
		clk := clock.NewFake(time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC))
		store := &fakeExecStore{outcome: execute.AcquireAcquired}
		req := sampleReq()
		req.Enabled = false
		reqs := &fakeReqGetter{req: req}
		api := &fakeProviderAPI{}
		acker := &fakeAcker{}

		uc := execute.New(store, reqs, api, &fakeBlobStore{}, acker, nil, clk, discardLogger(), execute.Config{OwnerID: "w1"})
		if err := uc.Execute(context.Background(), sampleMsg()); err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		if api.calls != 0 {
			t.Fatalf("api calls = %d, want 0", api.calls)
		}
		if store.skippedCalls != 1 {
			t.Fatalf("skippedCalls = %d, want 1", store.skippedCalls)
		}
		if acker.acks != 1 {
			t.Fatalf("acks = %d, want 1", acker.acks)
		}
	})
}

// T007: Response > 350 KiB goes to BlobStore BEFORE Complete with deterministic key.
func TestExecute_OversizedResponseToS3BeforeComplete(t *testing.T) {
	t.Parallel()

	clk := clock.NewFake(time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC))
	store := &fakeExecStore{outcome: execute.AcquireAcquired}
	reqs := &fakeReqGetter{req: sampleReq()}
	bigBody := bytes.Repeat([]byte("a"), domain.MaxInlineResponseBytes+1)
	api := &fakeProviderAPI{resp: execute.Response{StatusCode: 200, Body: bigBody}}
	blobs := &fakeBlobStore{events: &store.events}
	acker := &fakeAcker{events: &store.events}

	uc := execute.New(store, reqs, api, blobs, acker, nil, clk, discardLogger(), execute.Config{OwnerID: "w1"})
	if err := uc.Execute(context.Background(), sampleMsg()); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	wantKey := "results/run_date=2026-11-21/provider=edenor/req-001.json"
	if blobs.puts != 1 || blobs.lastKey != wantKey || blobs.lastLen != len(bigBody) {
		t.Fatalf("blob put mismatch: puts=%d key=%q len=%d", blobs.puts, blobs.lastKey, blobs.lastLen)
	}
	if store.completedRes.ResponseS3Key != wantKey || store.completedRes.ResponseBody != "" {
		t.Fatalf("complete should store s3 key only: %+v", store.completedRes)
	}

	order := strings.Join(store.events, ",")
	if order != "acquire,blob_put,complete,ack" {
		t.Fatalf("event order = %q, want acquire,blob_put,complete,ack", order)
	}
}

// T016: Acquire LeasedByOther extends visibility and does NOT call API.
func TestExecute_LeasedByOtherExtendsVisibilityWithoutCall(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	clk := clock.NewFake(now)
	store := &fakeExecStore{
		outcome: execute.AcquireLeasedByOther,
		exec: execute.Execution{
			Status:         domain.StatusInProgress,
			LeaseOwner:     "worker-other",
			LeaseExpiresAt: now.Add(42 * time.Second),
		},
	}
	api := &fakeProviderAPI{}
	acker := &fakeAcker{}

	uc := execute.New(store, &fakeReqGetter{req: sampleReq()}, api, &fakeBlobStore{}, acker, nil, clk, discardLogger(), execute.Config{OwnerID: "worker-1"})
	if err := uc.Execute(context.Background(), sampleMsg()); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if api.calls != 0 {
		t.Fatalf("api calls = %d, want 0", api.calls)
	}
	if acker.extends != 1 || acker.lastExtend != 42*time.Second {
		t.Fatalf("extends=%d lastExtend=%v, want 1 / 42s", acker.extends, acker.lastExtend)
	}
	if acker.acks != 0 || acker.nacks != 0 {
		t.Fatalf("expected neither ack nor nack, got ack=%d nack=%d", acker.acks, acker.nacks)
	}
}

// T019: Duplicate on SUCCEEDED or FAILED_PERMANENT -> Ack without Call.
func TestExecute_AlreadyTerminalAcksWithoutCall(t *testing.T) {
	t.Parallel()

	for _, status := range []domain.ExecutionStatus{
		domain.StatusSucceeded,
		domain.StatusFailedPermanent,
		domain.StatusFailedExhausted,
		domain.StatusSkipped,
	} {
		t.Run(string(status), func(t *testing.T) {
			t.Parallel()
			clk := clock.NewFake(time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC))
			store := &fakeExecStore{
				outcome: execute.AcquireAlreadyTerminal,
				exec:    execute.Execution{Status: status},
			}
			api := &fakeProviderAPI{}
			acker := &fakeAcker{}

			uc := execute.New(store, &fakeReqGetter{req: sampleReq()}, api, &fakeBlobStore{}, acker, nil, clk, discardLogger(), execute.Config{OwnerID: "w1"})
			if err := uc.Execute(context.Background(), sampleMsg()); err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if api.calls != 0 {
				t.Fatalf("api calls = %d, want 0", api.calls)
			}
			if acker.acks != 1 {
				t.Fatalf("acks = %d, want 1", acker.acks)
			}
		})
	}
}

// T021: ValidateConcurrencyBudget enforces k * t <= LAMBDA_BUDGET / 2.
func TestValidateConcurrencyBudget(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		k, t, b int
		wantErr bool
	}{
		{"within budget", 32, 10, 1000, false},
		{"exact boundary", 25, 20, 1000, false},
		{"exceeds half lambda budget", 32, 20, 1000, true},
		{"zero k", 0, 10, 1000, true},
		{"zero t", 10, 0, 1000, true},
		{"zero budget", 10, 1, 0, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := execute.ValidateConcurrencyBudget(tc.k, tc.t, tc.b)
			if tc.wantErr && !errors.Is(err, execute.ErrInvalidBudget) {
				t.Fatalf("err = %v, want ErrInvalidBudget", err)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected err = %v", err)
			}
		})
	}
}

// T024: Optional per-provider rate limiter isolates providers.
func TestProviderRateLimiter(t *testing.T) {
	t.Parallel()

	lim := execute.NewProviderRateLimiter(50, 1) // 1 burst, 20ms refill
	if err := lim.Wait(context.Background(), "provA"); err != nil {
		t.Fatalf("first provA wait: %v", err)
	}
	// provB should not be blocked by provA's empty bucket.
	start := time.Now()
	if err := lim.Wait(context.Background(), "provB"); err != nil {
		t.Fatalf("first provB wait: %v", err)
	}
	if time.Since(start) > 15*time.Millisecond {
		t.Fatalf("provB was unexpectedly delayed by provA")
	}
	// Second provA call respects context cancellation when bucket is empty.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
	defer cancel()
	if err := lim.Wait(ctx, "provA"); err == nil {
		t.Fatalf("expected provA rate limit context error")
	}

	// Disabled limiter (rps <= 0) returns nil immediately.
	if err := execute.NewProviderRateLimiter(0, 0).Wait(context.Background(), "provA"); err != nil {
		t.Fatalf("disabled limiter error = %v", err)
	}
}

type fakeBreaker struct {
	allow       bool
	cooldown    time.Duration
	recorded    []bool
	recordCalls int
}

func (f *fakeBreaker) Allow(_ string) (bool, time.Duration) { return f.allow, f.cooldown }
func (f *fakeBreaker) Record(_ string, retryableFailure bool) {
	f.recordCalls++
	f.recorded = append(f.recorded, retryableFailure)
}

type fakeMetrics struct {
	statuses     []domain.ExecutionStatus
	breakerOpens []bool
}

func (f *fakeMetrics) RecordExecution(_ string, s domain.ExecutionStatus, _ string, _ time.Duration) {
	f.statuses = append(f.statuses, s)
}
func (f *fakeMetrics) RecordBreakerOpen(_ string, open bool) {
	f.breakerOpens = append(f.breakerOpens, open)
}

type staticPolicy struct {
	fn func(err error, resp execute.Response, attempts, receiveCount int) execute.RetryDecision
}

func (p staticPolicy) Decide(err error, resp execute.Response, attempts, receiveCount int) execute.RetryDecision {
	return p.fn(err, resp, attempts, receiveCount)
}

func TestExecute_RetryPermanentExhaustedAndInProcess(t *testing.T) {
	t.Parallel()

	t.Run("5xx schedules retry and nacks", func(t *testing.T) {
		t.Parallel()
		clk := clock.NewFake(time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC))
		store := &fakeExecStore{outcome: execute.AcquireAcquired}
		api := &fakeProviderAPI{resp: execute.Response{StatusCode: 503}}
		acker := &fakeAcker{}
		brk := &fakeBreaker{allow: true}
		met := &fakeMetrics{}

		uc := execute.New(store, &fakeReqGetter{req: sampleReq()}, api, &fakeBlobStore{}, acker, nil, clk, discardLogger(), execute.Config{OwnerID: "w1"}).
			WithBreaker(brk).WithMetrics(met)
		if err := uc.Execute(context.Background(), sampleMsg()); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if store.scheduleCalls != 1 || acker.nacks != 1 || acker.lastDelay != 30*time.Second {
			t.Fatalf("schedule=%d nacks=%d delay=%v", store.scheduleCalls, acker.nacks, acker.lastDelay)
		}
		if len(brk.recorded) != 1 || !brk.recorded[0] {
			t.Fatalf("expected breaker retryable failure record, got %v", brk.recorded)
		}
	})

	t.Run("4xx fails permanent and acks", func(t *testing.T) {
		t.Parallel()
		clk := clock.NewFake(time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC))
		store := &fakeExecStore{outcome: execute.AcquireAcquired}
		api := &fakeProviderAPI{resp: execute.Response{StatusCode: 400}}
		acker := &fakeAcker{}
		met := &fakeMetrics{}

		uc := execute.New(store, &fakeReqGetter{req: sampleReq()}, api, &fakeBlobStore{}, acker, nil, clk, discardLogger(), execute.Config{OwnerID: "w1"}).
			WithMetrics(met)
		if err := uc.Execute(context.Background(), sampleMsg()); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if store.failPermCalls != 1 || acker.acks != 1 {
			t.Fatalf("failPerm=%d acks=%d", store.failPermCalls, acker.acks)
		}
	})

	t.Run("exhausted on receiveCount 6 marks FAILED_EXHAUSTED and nacks(0) for DLQ", func(t *testing.T) {
		t.Parallel()
		clk := clock.NewFake(time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC))
		store := &fakeExecStore{outcome: execute.AcquireAcquired}
		api := &fakeProviderAPI{err: errors.New(strings.Repeat("x", 1500))}
		acker := &fakeAcker{}

		msg := sampleMsg()
		msg.ReceiveCount = 6
		uc := execute.New(store, &fakeReqGetter{req: sampleReq()}, api, &fakeBlobStore{}, acker, nil, clk, discardLogger(), execute.Config{OwnerID: "w1"})
		if err := uc.Execute(context.Background(), msg); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if store.failExhCalls != 1 || acker.nacks != 1 || acker.lastDelay != 0 {
			t.Fatalf("failExh=%d nacks=%d delay=%v", store.failExhCalls, acker.nacks, acker.lastDelay)
		}
		if len(store.exhaustedInfo.ErrorMessage) > 1024 {
			t.Fatalf("error message not truncated: len=%d", len(store.exhaustedInfo.ErrorMessage))
		}
	})

	t.Run("fast in-process retry succeeds on second attempt", func(t *testing.T) {
		t.Parallel()
		clk := clock.NewFake(time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC))
		store := &fakeExecStore{outcome: execute.AcquireAcquired}
		api := &fakeProviderAPI{
			fn: func(call int) (execute.Response, error) {
				if call == 1 {
					return execute.Response{StatusCode: 503}, nil
				}
				return execute.Response{StatusCode: 200, Body: []byte(`{"ok":true}`)}, nil
			},
		}
		acker := &fakeAcker{}
		pol := staticPolicy{fn: func(_ error, resp execute.Response, attempts, _ int) execute.RetryDecision {
			if resp.StatusCode == 200 {
				return execute.RetryDecision{Action: execute.ActionComplete}
			}
			return execute.RetryDecision{Action: execute.ActionRetryInProcess, Backoff: time.Millisecond}
		}}
		brk := &fakeBreaker{allow: true}
		met := &fakeMetrics{}

		uc := execute.New(store, &fakeReqGetter{req: sampleReq()}, api, &fakeBlobStore{}, acker, pol, clk, discardLogger(), execute.Config{OwnerID: "w1"}).
			WithBreaker(brk).WithMetrics(met).WithLimiter(execute.NewProviderRateLimiter(1000, 10))
		if err := uc.Execute(context.Background(), sampleMsg()); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if api.calls != 2 || store.completeCalls != 1 || acker.acks != 1 {
			t.Fatalf("calls=%d complete=%d acks=%d", api.calls, store.completeCalls, acker.acks)
		}
	})

	t.Run("open circuit breaker nacks without calling Acquire or API", func(t *testing.T) {
		t.Parallel()
		clk := clock.NewFake(time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC))
		store := &fakeExecStore{outcome: execute.AcquireAcquired}
		api := &fakeProviderAPI{}
		acker := &fakeAcker{}
		brk := &fakeBreaker{allow: false, cooldown: 60 * time.Second}
		met := &fakeMetrics{}

		uc := execute.New(store, &fakeReqGetter{req: sampleReq()}, api, &fakeBlobStore{}, acker, nil, clk, discardLogger(), execute.Config{OwnerID: "w1"}).
			WithBreaker(brk).WithMetrics(met)
		if err := uc.Execute(context.Background(), sampleMsg()); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if api.calls != 0 || len(store.events) != 0 || acker.nacks != 1 || acker.lastDelay != 60*time.Second {
			t.Fatalf("expected breaker short-circuit: api=%d events=%v nacks=%d delay=%v", api.calls, store.events, acker.nacks, acker.lastDelay)
		}
	})
}

func TestExecute_PersistenceAndLeaseLostErrors(t *testing.T) {
	t.Parallel()

	t.Run("acquire error nacks and returns error", func(t *testing.T) {
		t.Parallel()
		clk := clock.NewFake(time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC))
		store := &fakeExecStore{acquireErr: errors.New("ddb throttle")}
		acker := &fakeAcker{}
		uc := execute.New(store, &fakeReqGetter{req: sampleReq()}, &fakeProviderAPI{}, &fakeBlobStore{}, acker, nil, clk, discardLogger(), execute.Config{OwnerID: "w1"})
		if err := uc.Execute(context.Background(), sampleMsg()); err == nil {
			t.Fatalf("expected error on acquire failure")
		}
		if acker.nacks != 1 {
			t.Fatalf("expected nack on acquire failure")
		}
	})

	t.Run("get request error nacks and returns error", func(t *testing.T) {
		t.Parallel()
		clk := clock.NewFake(time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC))
		store := &fakeExecStore{outcome: execute.AcquireAcquired}
		acker := &fakeAcker{}
		uc := execute.New(store, &fakeReqGetter{err: errors.New("ddb timeout")}, &fakeProviderAPI{}, &fakeBlobStore{}, acker, nil, clk, discardLogger(), execute.Config{OwnerID: "w1"})
		if err := uc.Execute(context.Background(), sampleMsg()); err == nil {
			t.Fatalf("expected error on get failure")
		}
		if acker.nacks != 1 {
			t.Fatalf("expected nack on get failure")
		}
	})

	t.Run("blob put error prevents Complete and nacks", func(t *testing.T) {
		t.Parallel()
		clk := clock.NewFake(time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC))
		store := &fakeExecStore{outcome: execute.AcquireAcquired}
		bigBody := bytes.Repeat([]byte("b"), domain.MaxInlineResponseBytes+10)
		api := &fakeProviderAPI{resp: execute.Response{StatusCode: 200, Body: bigBody}}
		blobs := &fakeBlobStore{err: errors.New("s3 down")}
		acker := &fakeAcker{}

		uc := execute.New(store, &fakeReqGetter{req: sampleReq()}, api, blobs, acker, nil, clk, discardLogger(), execute.Config{OwnerID: "w1"})
		if err := uc.Execute(context.Background(), sampleMsg()); err == nil {
			t.Fatalf("expected error on s3 put failure")
		}
		if store.completeCalls != 0 || acker.acks != 0 || acker.nacks != 1 {
			t.Fatalf("complete=%d acks=%d nacks=%d", store.completeCalls, acker.acks, acker.nacks)
		}
	})

	t.Run("Complete ErrLeaseLost returns nil without Ack", func(t *testing.T) {
		t.Parallel()
		clk := clock.NewFake(time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC))
		store := &fakeExecStore{outcome: execute.AcquireAcquired, completeErr: execute.ErrLeaseLost}
		api := &fakeProviderAPI{resp: execute.Response{StatusCode: 200, Body: []byte(`{}`)}}
		acker := &fakeAcker{}

		uc := execute.New(store, &fakeReqGetter{req: sampleReq()}, api, &fakeBlobStore{}, acker, nil, clk, discardLogger(), execute.Config{OwnerID: "w1"})
		if err := uc.Execute(context.Background(), sampleMsg()); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if acker.acks != 0 || acker.nacks != 0 {
			t.Fatalf("expected no ack/nack when lease lost, got ack=%d nack=%d", acker.acks, acker.nacks)
		}
	})

	t.Run("Complete transient error nacks and returns error", func(t *testing.T) {
		t.Parallel()
		clk := clock.NewFake(time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC))
		store := &fakeExecStore{outcome: execute.AcquireAcquired, completeErr: errors.New("ddb 500")}
		api := &fakeProviderAPI{resp: execute.Response{StatusCode: 200, Body: []byte(`{}`)}}
		acker := &fakeAcker{}

		uc := execute.New(store, &fakeReqGetter{req: sampleReq()}, api, &fakeBlobStore{}, acker, nil, clk, discardLogger(), execute.Config{OwnerID: "w1"})
		if err := uc.Execute(context.Background(), sampleMsg()); err == nil {
			t.Fatalf("expected error on Complete failure")
		}
		if acker.nacks != 1 {
			t.Fatalf("expected nack on Complete failure")
		}
	})

	t.Run("transport error retries, store errors and lease lost on non-2xx paths", func(t *testing.T) {
		t.Parallel()
		clk := clock.NewFake(time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC))

		// Transport error on receiveCount 1 -> ScheduleRetry.
		store := &fakeExecStore{outcome: execute.AcquireAcquired}
		acker := &fakeAcker{}
		uc := execute.New(store, &fakeReqGetter{req: sampleReq()}, &fakeProviderAPI{err: errors.New("conn reset")}, &fakeBlobStore{}, acker, nil, clk, nil, execute.Config{OwnerID: "w1"})
		if err := uc.Execute(context.Background(), sampleMsg()); err != nil {
			t.Fatalf("transport retry: %v", err)
		}
		if store.scheduleCalls != 1 || acker.nacks != 1 {
			t.Fatalf("expected retry on transport err")
		}

		// ScheduleRetry ErrLeaseLost returns nil; other error returns err.
		storeLost := &fakeExecStore{outcome: execute.AcquireAcquired, scheduleErr: execute.ErrLeaseLost}
		ucLost := execute.New(storeLost, &fakeReqGetter{req: sampleReq()}, &fakeProviderAPI{resp: execute.Response{StatusCode: 503}}, &fakeBlobStore{}, &fakeAcker{}, nil, clk, discardLogger(), execute.Config{OwnerID: "w1"})
		if err := ucLost.Execute(context.Background(), sampleMsg()); err != nil {
			t.Fatalf("schedule lease lost: %v", err)
		}
		storeErr := &fakeExecStore{outcome: execute.AcquireAcquired, scheduleErr: errors.New("ddb err")}
		ucErr := execute.New(storeErr, &fakeReqGetter{req: sampleReq()}, &fakeProviderAPI{resp: execute.Response{StatusCode: 503}}, &fakeBlobStore{}, &fakeAcker{}, nil, clk, discardLogger(), execute.Config{OwnerID: "w1"})
		if err := ucErr.Execute(context.Background(), sampleMsg()); err == nil {
			t.Fatalf("expected schedule error")
		}

		// FailPermanent ErrLeaseLost & transient error.
		storePermLost := &fakeExecStore{outcome: execute.AcquireAcquired, failPermErr: execute.ErrLeaseLost}
		ucPermLost := execute.New(storePermLost, &fakeReqGetter{req: sampleReq()}, &fakeProviderAPI{resp: execute.Response{StatusCode: 400}}, &fakeBlobStore{}, &fakeAcker{}, nil, clk, discardLogger(), execute.Config{OwnerID: "w1"})
		if err := ucPermLost.Execute(context.Background(), sampleMsg()); err != nil {
			t.Fatalf("failPerm lease lost: %v", err)
		}
		storePermErr := &fakeExecStore{outcome: execute.AcquireAcquired, failPermErr: errors.New("ddb err")}
		ucPermErr := execute.New(storePermErr, &fakeReqGetter{req: sampleReq()}, &fakeProviderAPI{resp: execute.Response{StatusCode: 400}}, &fakeBlobStore{}, &fakeAcker{}, nil, clk, discardLogger(), execute.Config{OwnerID: "w1"})
		if err := ucPermErr.Execute(context.Background(), sampleMsg()); err == nil {
			t.Fatalf("expected failPerm error")
		}

		// FailExhausted ErrLeaseLost & transient error.
		msgExh := sampleMsg()
		msgExh.ReceiveCount = 6
		storeExhLost := &fakeExecStore{outcome: execute.AcquireAcquired, failExhErr: execute.ErrLeaseLost}
		ucExhLost := execute.New(storeExhLost, &fakeReqGetter{req: sampleReq()}, &fakeProviderAPI{resp: execute.Response{StatusCode: 503}}, &fakeBlobStore{}, &fakeAcker{}, nil, clk, discardLogger(), execute.Config{OwnerID: "w1"})
		if err := ucExhLost.Execute(context.Background(), msgExh); err != nil {
			t.Fatalf("failExh lease lost: %v", err)
		}
		storeExhErr := &fakeExecStore{outcome: execute.AcquireAcquired, failExhErr: errors.New("ddb err")}
		ucExhErr := execute.New(storeExhErr, &fakeReqGetter{req: sampleReq()}, &fakeProviderAPI{resp: execute.Response{StatusCode: 503}}, &fakeBlobStore{}, &fakeAcker{}, nil, clk, discardLogger(), execute.Config{OwnerID: "w1"})
		if err := ucExhErr.Execute(context.Background(), msgExh); err == nil {
			t.Fatalf("expected failExh error")
		}

		// NotFound / Disabled store error & oversized response with nil BlobStore.
		ucNF := execute.New(&fakeExecStore{outcome: execute.AcquireAcquired, failPermErr: errors.New("ddb")}, &fakeReqGetter{err: domain.ErrRequestNotFound}, &fakeProviderAPI{}, nil, &fakeAcker{}, nil, clk, discardLogger(), execute.Config{OwnerID: "w1"})
		if err := ucNF.Execute(context.Background(), sampleMsg()); err == nil {
			t.Fatalf("expected not-found persist error")
		}
		disReq := sampleReq()
		disReq.Enabled = false
		ucSkip := execute.New(&fakeExecStore{outcome: execute.AcquireAcquired, skipErr: errors.New("ddb")}, &fakeReqGetter{req: disReq}, &fakeProviderAPI{}, nil, &fakeAcker{}, nil, clk, discardLogger(), execute.Config{OwnerID: "w1"})
		if err := ucSkip.Execute(context.Background(), sampleMsg()); err == nil {
			t.Fatalf("expected skip persist error")
		}
		big := bytes.Repeat([]byte("z"), domain.MaxInlineResponseBytes+1)
		ucNoBlob := execute.New(&fakeExecStore{outcome: execute.AcquireAcquired}, &fakeReqGetter{req: sampleReq()}, &fakeProviderAPI{resp: execute.Response{StatusCode: 200, Body: big}}, nil, &fakeAcker{}, nil, clk, discardLogger(), execute.Config{OwnerID: "w1"})
		if err := ucNoBlob.Execute(context.Background(), sampleMsg()); err == nil {
			t.Fatalf("expected error when BlobStore is nil for oversized response")
		}

		// RetryInProcess exceeding maxInProcessAttempts falls back to ActionRetryLater.
		alwaysInProc := staticPolicy{fn: func(_ error, _ execute.Response, _, _ int) execute.RetryDecision {
			return execute.RetryDecision{Action: execute.ActionRetryInProcess, ErrorClass: "FAST"}
		}}
		storeFallback := &fakeExecStore{outcome: execute.AcquireAcquired}
		ucFB := execute.New(storeFallback, &fakeReqGetter{req: sampleReq()}, &fakeProviderAPI{resp: execute.Response{StatusCode: 503}}, &fakeBlobStore{}, &fakeAcker{}, alwaysInProc, clk, discardLogger(), execute.Config{OwnerID: "w1"})
		if err := ucFB.Execute(context.Background(), sampleMsg()); err != nil {
			t.Fatalf("in-process fallback: %v", err)
		}
		if storeFallback.scheduleCalls != 1 {
			t.Fatalf("expected ScheduleRetry after max in-process attempts")
		}
	})
}
