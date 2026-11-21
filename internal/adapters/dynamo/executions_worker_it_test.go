//go:build integration

package dynamo_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/adapters/dynamo"
	"github.com/martingomezvega/tapi-challenge/internal/app/dispatch"
	"github.com/martingomezvega/tapi-challenge/internal/app/execute"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
	"github.com/martingomezvega/tapi-challenge/test/testenv"
)

// T009: Conditional Acquire (including expired lease takeover), Complete, ScheduleRetry,
// FailPermanent, FailExhausted, MarkSkipped, and ErrLeaseLost fencing against LocalStack.
func TestExecutionsRepo_Worker_Integration(t *testing.T) {
	env := testenv.StartLocalStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	const table = "executions-worker-test"
	if err := env.EnsureExecutionsTable(ctx, table); err != nil {
		t.Fatalf("EnsureExecutionsTable: %v", err)
	}

	repo := dynamo.NewExecutionsRepo(env.DynamoDB, table, 90*24*time.Hour)
	date := domain.RunDate("2026-11-21")
	t0 := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)

	// 1. CreatePending -> MarkEnqueued -> Acquire by worker-1 -> LeasedByOther for worker-2 -> Expired lease takeover by worker-2 -> Complete.
	if _, err := repo.CreatePending(ctx, "req-1", date, "edenor", 5, t0); err != nil {
		t.Fatalf("CreatePending: %v", err)
	}
	if err := repo.MarkEnqueued(ctx, []dispatch.ExecKey{{RequestID: "req-1", RunDate: date}}, t0.Add(time.Second)); err != nil {
		t.Fatalf("MarkEnqueued: %v", err)
	}

	key1 := execute.ExecKey{RequestID: "req-1", RunDate: date}
	ex, outcome, err := repo.Acquire(ctx, key1, "worker-1", 60*time.Second, t0.Add(2*time.Second))
	if err != nil || outcome != execute.AcquireAcquired || ex.Attempts != 1 || ex.LeaseOwner != "worker-1" {
		t.Fatalf("Acquire #1: outcome=%v ex=%+v err=%v", outcome, ex, err)
	}

	// Active lease held by worker-1 -> worker-2 sees LeasedByOther.
	ex2, outcome2, err := repo.Acquire(ctx, key1, "worker-2", 60*time.Second, t0.Add(10*time.Second))
	if err != nil || outcome2 != execute.AcquireLeasedByOther || ex2.LeaseOwner != "worker-1" {
		t.Fatalf("Acquire while active: outcome=%v ex=%+v err=%v", outcome2, ex2, err)
	}

	// After lease expiry (+70s), worker-2 takes over the lease and increments attempts.
	ex3, outcome3, err := repo.Acquire(ctx, key1, "worker-2", 60*time.Second, t0.Add(70*time.Second))
	if err != nil || outcome3 != execute.AcquireAcquired || ex3.Attempts != 2 || ex3.LeaseOwner != "worker-2" {
		t.Fatalf("Acquire after expiry: outcome=%v ex=%+v err=%v", outcome3, ex3, err)
	}

	// Stale owner worker-1 attempting Complete must fail with ErrLeaseLost.
	err = repo.Complete(ctx, key1, "worker-1", execute.Result{HTTPStatus: 200, ResponseBody: `{}`, LatencyMs: 10, CompletedAt: t0.Add(71 * time.Second)})
	if !errors.Is(err, execute.ErrLeaseLost) {
		t.Fatalf("stale Complete error = %v, want ErrLeaseLost", err)
	}

	// Current owner worker-2 completes successfully.
	if err := repo.Complete(ctx, key1, "worker-2", execute.Result{
		HTTPStatus:   200,
		ResponseBody: `{"debt":0}`,
		LatencyMs:    15,
		CompletedAt:  t0.Add(72 * time.Second),
	}); err != nil {
		t.Fatalf("Complete by worker-2: %v", err)
	}

	// Duplicate Acquire on SUCCEEDED returns AcquireAlreadyTerminal.
	exTerm, outTerm, err := repo.Acquire(ctx, key1, "worker-3", 60*time.Second, t0.Add(73*time.Second))
	if err != nil || outTerm != execute.AcquireAlreadyTerminal || exTerm.Status != domain.StatusSucceeded {
		t.Fatalf("Acquire terminal: outcome=%v ex=%+v err=%v", outTerm, exTerm, err)
	}

	// 2. ScheduleRetry -> re-Acquire from RETRYING -> FailExhausted -> terminal.
	if _, err := repo.CreatePending(ctx, "req-2", date, "edesur", 5, t0); err != nil {
		t.Fatalf("CreatePending req-2: %v", err)
	}
	key2 := execute.ExecKey{RequestID: "req-2", RunDate: date}
	if _, outcome, err := repo.Acquire(ctx, key2, "worker-1", 60*time.Second, t0); err != nil || outcome != execute.AcquireAcquired {
		t.Fatalf("Acquire req-2: outcome=%v err=%v", outcome, err)
	}
	if err := repo.ScheduleRetry(ctx, key2, "worker-1", execute.ErrInfo{
		ErrorClass:   "HTTP_503",
		ErrorMessage: "unavailable",
		HTTPStatus:   503,
		LatencyMs:    20,
		NextRetryAt:  t0.Add(30 * time.Second),
	}); err != nil {
		t.Fatalf("ScheduleRetry: %v", err)
	}
	if exRetry, outRetry, err := repo.Acquire(ctx, key2, "worker-2", 60*time.Second, t0.Add(31*time.Second)); err != nil || outRetry != execute.AcquireAcquired || exRetry.Attempts != 2 {
		t.Fatalf("Re-acquire from RETRYING: outcome=%v ex=%+v err=%v", outRetry, exRetry, err)
	}
	if err := repo.FailExhausted(ctx, key2, "worker-2", execute.ErrInfo{
		ErrorClass:   "HTTP_503",
		ErrorMessage: "max retries",
		HTTPStatus:   503,
		CompletedAt:  t0.Add(35 * time.Second),
	}); err != nil {
		t.Fatalf("FailExhausted: %v", err)
	}

	// 3. FailPermanent and MarkSkipped.
	if _, err := repo.CreatePending(ctx, "req-3", date, "aysa", 5, t0); err != nil {
		t.Fatalf("CreatePending req-3: %v", err)
	}
	key3 := execute.ExecKey{RequestID: "req-3", RunDate: date}
	if _, _, err := repo.Acquire(ctx, key3, "w1", 60*time.Second, t0); err != nil {
		t.Fatalf("Acquire req-3: %v", err)
	}
	if err := repo.FailPermanent(ctx, key3, "w1", execute.ErrInfo{ErrorClass: "HTTP_400", ErrorMessage: "bad", HTTPStatus: 400, CompletedAt: t0}); err != nil {
		t.Fatalf("FailPermanent: %v", err)
	}

	if _, err := repo.CreatePending(ctx, "req-4", date, "aysa", 5, t0); err != nil {
		t.Fatalf("CreatePending req-4: %v", err)
	}
	key4 := execute.ExecKey{RequestID: "req-4", RunDate: date}
	if _, _, err := repo.Acquire(ctx, key4, "w1", 60*time.Second, t0); err != nil {
		t.Fatalf("Acquire req-4: %v", err)
	}
	if err := repo.MarkSkipped(ctx, key4, "w1", t0); err != nil {
		t.Fatalf("MarkSkipped: %v", err)
	}
}
