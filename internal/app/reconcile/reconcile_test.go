package reconcile_test

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/app/dispatch"
	"github.com/martingomezvega/tapi-challenge/internal/app/reconcile"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

type fakeCatalog struct {
	bySlotShard map[[2]int][]domain.RequestKey
}

func (f *fakeCatalog) ListBySlotShard(_ context.Context, slot, shard int, _ string) ([]domain.RequestKey, string, error) {
	return f.bySlotShard[[2]int{slot, shard}], "", nil
}

type fakeExecStore struct {
	mu    sync.Mutex
	items map[string]domain.ExecutionSnapshot
}

func (f *fakeExecStore) BatchGet(_ context.Context, reqIDs []string, _ domain.RunDate) (map[string]domain.ExecutionSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]domain.ExecutionSnapshot)
	for _, id := range reqIDs {
		if snap, ok := f.items[id]; ok {
			out[id] = snap
		}
	}
	return out, nil
}

func (f *fakeExecStore) CreatePending(_ context.Context, reqID string, _ domain.RunDate, _ string, _ int, now time.Time) (dispatch.CreatePendingResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if snap, ok := f.items[reqID]; ok {
		return dispatch.CreatePendingResult{Created: false, Status: string(snap.Status), CreatedAt: snap.CreatedAt}, nil
	}
	f.items[reqID] = domain.ExecutionSnapshot{
		RequestID: reqID,
		Status:    domain.StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}
	return dispatch.CreatePendingResult{Created: true, Status: dispatch.StatusPending, CreatedAt: now}, nil
}

func (f *fakeExecStore) MarkEnqueued(_ context.Context, keys []dispatch.ExecKey, now time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, k := range keys {
		if snap, ok := f.items[k.RequestID]; ok {
			snap.Status = domain.StatusEnqueued
			snap.UpdatedAt = now
			f.items[k.RequestID] = snap
		}
	}
	return nil
}

func (f *fakeExecStore) ResetStale(_ context.Context, reqID string, _ domain.RunDate, staleBefore, now time.Time) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	snap, ok := f.items[reqID]
	if !ok || snap.Status.IsTerminal() || snap.UpdatedAt.After(staleBefore) {
		return false, nil
	}
	snap.Status = domain.StatusEnqueued
	snap.UpdatedAt = now
	f.items[reqID] = snap
	return true, nil
}

type fakeQueue struct {
	mu   sync.Mutex
	sent []dispatch.JobMessage
}

func (f *fakeQueue) SendBatch(_ context.Context, msgs []dispatch.JobMessage) ([]dispatch.JobMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, msgs...)
	return nil, nil
}

type fakeReportStore struct {
	mu    sync.Mutex
	blobs map[string]string
}

func (f *fakeReportStore) PutWithContentType(_ context.Context, key string, data []byte, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.blobs == nil {
		f.blobs = make(map[string]string)
	}
	f.blobs[key] = string(data)
	return nil
}

type fakeMetrics struct {
	calls    int
	coverage float64
	missing  int
	stuck    int
}

func (f *fakeMetrics) RecordReconcile(_ domain.RunDate, cov float64, missing, stuck int) {
	f.calls++
	f.coverage = cov
	f.missing = missing
	f.stuck = stuck
}

// T010 & T011: Reconcile detects missing & stuck(>2h), excludes disabled & created-after-slot,
// re-dispatches via JobQueue, writes JSON+CSV reports, and 2nd consecutive run re-dispatches 0 (idempotent).
func TestReconcile_RedispatchAndIdempotency(t *testing.T) {
	t.Parallel()

	date := domain.RunDate("2026-11-21")
	now := time.Date(2026, 11, 22, 0, 30, 0, 0, time.UTC)
	dayBefore := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	afterSlot10 := time.Date(2026, 11, 21, 0, 15, 0, 0, time.UTC) // slot 10 = 00:10 UTC -> created after slot!

	cat := &fakeCatalog{
		bySlotShard: map[[2]int][]domain.RequestKey{
			{10, 0}: {
				{ID: "r-ok", Provider: "edenor", Enabled: true, CreatedAt: dayBefore},
				{ID: "r-skip", Provider: "edenor", Enabled: true, CreatedAt: dayBefore},
				{ID: "r-missing", Provider: "edenor", Enabled: true, CreatedAt: dayBefore},
				{ID: "r-stuck", Provider: "edesur", Enabled: true, CreatedAt: dayBefore},
				{ID: "r-disabled", Provider: "edesur", Enabled: false, CreatedAt: dayBefore},
				{ID: "r-late-create", Provider: "edesur", Enabled: true, CreatedAt: afterSlot10},
			},
		},
	}

	store := &fakeExecStore{
		items: map[string]domain.ExecutionSnapshot{
			"r-ok":    {RequestID: "r-ok", Status: domain.StatusSucceeded, UpdatedAt: now.Add(-10 * time.Hour)},
			"r-skip":  {RequestID: "r-skip", Status: domain.StatusSkipped, UpdatedAt: now.Add(-10 * time.Hour)},
			"r-stuck": {RequestID: "r-stuck", Status: domain.StatusEnqueued, UpdatedAt: now.Add(-4 * time.Hour)},
		},
	}
	queue := &fakeQueue{}
	reports := &fakeReportStore{}
	metrics := &fakeMetrics{}
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	uc := reconcile.New(cat, store, queue, reports, metrics, func() time.Time { return now }, logger, reconcile.Config{
		Shards:      1,
		Parallelism: 4,
		StaleAfter:  2 * time.Hour,
		Location:    time.UTC,
		Redispatch:  true,
	})

	// Run #1: expects 4 (r-ok, r-skip, r-missing, r-stuck); 2 terminal (ok+skip), 1 missing, 1 stuck -> 2 re-dispatched.
	sum1, err := uc.Run(context.Background(), date)
	if err != nil {
		t.Fatalf("Run #1 error = %v", err)
	}
	if sum1.Expected != 4 || sum1.Terminal != 2 || sum1.Missing != 1 || sum1.Stuck != 1 || sum1.Redispatched != 2 {
		t.Fatalf("Run #1 summary mismatch: %+v", sum1)
	}
	if sum1.Coverage != 0.5 {
		t.Fatalf("Run #1 coverage = %v, want 0.5", sum1.Coverage)
	}
	if len(queue.sent) != 2 {
		t.Fatalf("Run #1 queue sent = %d, want 2", len(queue.sent))
	}
	if !strings.Contains(reports.blobs["reports/run_date=2026-11-21/summary.json"], `"expected":4`) {
		t.Fatalf("missing json report: %v", reports.blobs)
	}
	if !strings.Contains(reports.blobs["reports/run_date=2026-11-21/summary.csv"], "edenor") {
		t.Fatalf("missing csv report: %v", reports.blobs)
	}
	if metrics.calls != 1 {
		t.Fatalf("metrics calls = %d, want 1", metrics.calls)
	}

	// Run #2 immediately after: both re-dispatched items now have fresh UpdatedAt=now (<2h) -> 0 re-dispatched!
	sum2, err := uc.Run(context.Background(), date)
	if err != nil {
		t.Fatalf("Run #2 error = %v", err)
	}
	if sum2.Redispatched != 0 || len(queue.sent) != 2 {
		t.Fatalf("Run #2 must be idempotent (0 redispatches), got redispatched=%d total_sent=%d", sum2.Redispatched, len(queue.sent))
	}
}
