package reconcile_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/app/dispatch"
	"github.com/martingomezvega/tapi-challenge/internal/app/reconcile"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

type synthCatalog struct {
	perSlot int
	created time.Time
}

func (s *synthCatalog) ListBySlotShard(_ context.Context, slot, shard int, _ string) ([]domain.RequestKey, string, error) {
	items := make([]domain.RequestKey, s.perSlot)
	for i := range s.perSlot {
		items[i] = domain.RequestKey{
			ID:        fmt.Sprintf("r-%d-%d-%d", slot, shard, i),
			Provider:  fmt.Sprintf("p-%d", i%50),
			Enabled:   true,
			CreatedAt: s.created,
		}
	}
	return items, "", nil
}

type synthExecStore struct {
	updated time.Time
}

func (s *synthExecStore) BatchGet(_ context.Context, reqIDs []string, _ domain.RunDate) (map[string]domain.ExecutionSnapshot, error) {
	out := make(map[string]domain.ExecutionSnapshot, len(reqIDs))
	for _, id := range reqIDs {
		out[id] = domain.ExecutionSnapshot{
			RequestID: id,
			Status:    domain.StatusSucceeded,
			UpdatedAt: s.updated,
		}
	}
	return out, nil
}
func (s *synthExecStore) CreatePending(_ context.Context, _ string, _ domain.RunDate, _ string, _ int, _ time.Time) (dispatch.CreatePendingResult, error) {
	return dispatch.CreatePendingResult{}, nil
}
func (s *synthExecStore) MarkEnqueued(_ context.Context, _ []dispatch.ExecKey, _ time.Time) error {
	return nil
}
func (s *synthExecStore) ResetStale(_ context.Context, _ string, _ domain.RunDate, _, _ time.Time) (bool, error) {
	return false, nil
}

// T017: Benchmark reconciling 1M records across 1440 x 4 shards (CPU + aggregation overhead).
func BenchmarkReconcile1M(b *testing.B) {
	date := domain.RunDate("2026-11-21")
	now := time.Date(2026, 11, 22, 0, 30, 0, 0, time.UTC)
	// 1440 slots * 4 shards * 174 items/partition = 1,002,240 records (~1M)
	cat := &synthCatalog{perSlot: 174, created: now.Add(-48 * time.Hour)}
	store := &synthExecStore{updated: now.Add(-5 * time.Hour)}
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	uc := reconcile.New(cat, store, nil, nil, nil, func() time.Time { return now }, logger, reconcile.Config{
		Shards:      4,
		Parallelism: 64,
		StaleAfter:  2 * time.Hour,
		Location:    time.UTC,
		Redispatch:  false,
	})

	b.ResetTimer()
	for b.Loop() {
		sum, err := uc.Run(context.Background(), date)
		if err != nil {
			b.Fatalf("Run: %v", err)
		}
		if sum.Expected != 1002240 || sum.Coverage != 1.0 {
			b.Fatalf("unexpected summary: expected=%d cov=%v", sum.Expected, sum.Coverage)
		}
	}
}
