//go:build integration

package dynamo_test

import (
	"context"
	"testing"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/adapters/dynamo"
	"github.com/martingomezvega/tapi-challenge/internal/app/dispatch"
	"github.com/martingomezvega/tapi-challenge/test/testenv"
)

// T010: Conditional CreatePending and MarkEnqueued in LocalStack.
func TestExecutionsRepo_Dispatcher_Integration(t *testing.T) {
	env := testenv.StartLocalStack(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const tableName = "executions_dispatch_it"
	if err := env.EnsureExecutionsTable(ctx, tableName); err != nil {
		t.Fatalf("EnsureExecutionsTable: %v", err)
	}

	repo := dynamo.NewExecutionsRepo(env.DynamoDB, tableName, 90*24*time.Hour)
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

	res1, err := repo.CreatePending(ctx, "req-1", "2026-11-20", "edenor", 720, now)
	if err != nil {
		t.Fatalf("first CreatePending(): %v", err)
	}
	if !res1.Created || res1.Status != dispatch.StatusPending {
		t.Fatalf("first CreatePending() = %+v, want Created=true Status=PENDING", res1)
	}

	// Second CreatePending for same (request_id, run_date) must return Created=false with existing metadata.
	res2, err := repo.CreatePending(ctx, "req-1", "2026-11-20", "edenor", 720, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("second CreatePending(): %v", err)
	}
	if res2.Created || res2.Status != dispatch.StatusPending || !res2.CreatedAt.Equal(now) {
		t.Fatalf("second CreatePending() = %+v, want Created=false Status=PENDING CreatedAt=%v", res2, now)
	}

	// MarkEnqueued transitions PENDING -> ENQUEUED.
	if err := repo.MarkEnqueued(ctx, []dispatch.ExecKey{{RequestID: "req-1", RunDate: "2026-11-20"}}, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("MarkEnqueued(): %v", err)
	}

	// Subsequent CreatePending observes Status=ENQUEUED.
	res3, err := repo.CreatePending(ctx, "req-1", "2026-11-20", "edenor", 720, now.Add(3*time.Minute))
	if err != nil {
		t.Fatalf("third CreatePending(): %v", err)
	}
	if res3.Created || res3.Status != dispatch.StatusEnqueued {
		t.Fatalf("third CreatePending() = %+v, want Created=false Status=ENQUEUED", res3)
	}

	// Different run_date for same request_id creates a new independent PENDING execution.
	resNextDay, err := repo.CreatePending(ctx, "req-1", "2026-11-21", "edenor", 720, now.Add(24*time.Hour))
	if err != nil {
		t.Fatalf("next day CreatePending(): %v", err)
	}
	if !resNextDay.Created {
		t.Fatalf("next day CreatePending() Created=false, want true")
	}
}
