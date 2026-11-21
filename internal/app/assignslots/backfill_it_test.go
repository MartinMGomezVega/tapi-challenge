//go:build integration

package assignslots_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/martingomezvega/tapi-challenge/internal/adapters/dynamo"
	"github.com/martingomezvega/tapi-challenge/internal/app/assignslots"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
	"github.com/martingomezvega/tapi-challenge/test/testenv"
)

type interruptingSlotWriter struct {
	inner     assignslots.SlotWriter
	limit     int64
	calls     atomic.Int64
	injectErr error
}

func (w *interruptingSlotWriter) SetSlot(ctx context.Context, id string, slot, shard int, expectedVersion int64) error {
	if w.calls.Add(1) > w.limit {
		return w.injectErr
	}
	return w.inner.SetSlot(ctx, id, slot, shard, expectedVersion)
}

type heapSamplingRegistry struct {
	inner    assignslots.ProviderRegistry
	peakHeap atomic.Uint64
}

func (h *heapSamplingRegistry) ListProviders(ctx context.Context) ([]assignslots.ProviderState, error) {
	return h.inner.ListProviders(ctx)
}

func (h *heapSamplingRegistry) SaveProviderState(ctx context.Context, state assignslots.ProviderState) error {
	err := h.inner.SaveProviderState(ctx, state)
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	for {
		cur := h.peakHeap.Load()
		if ms.HeapAlloc <= cur || h.peakHeap.CompareAndSwap(cur, ms.HeapAlloc) {
			break
		}
	}
	return err
}

// T020: Full LocalStack integration test for AssignSlots:
//  1. Interrupt mid-backfill and resume from DynamoDB checkpoint -> 100% slotted, idempotent 2nd run,
//     histograms persisted in coordination.
//  2. End-to-end live heap boundedness across app/assignslots + real DynamoDB adapters (1x vs 4x N).
func TestBackfill_LocalStack_Integration(t *testing.T) {
	env := testenv.StartLocalStack(t)

	t.Run("interrupt, resume, 100% slot coverage, idempotency, and histogram persistence", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		const (
			reqTable   = "requests_backfill_it"
			coordTable = "coordination_backfill_it"
			numProvs   = 6
			perProv    = 40
			shards     = 4
		)

		if err := env.EnsureRequestsTable(ctx, reqTable); err != nil {
			t.Fatalf("EnsureRequestsTable: %v", err)
		}
		if err := env.EnsureCoordinationTable(ctx, coordTable); err != nil {
			t.Fatalf("EnsureCoordinationTable: %v", err)
		}

		reqRepo := dynamo.NewRequestsRepo(env.DynamoDB, reqTable, dynamo.WithPageSize(10))
		coordRepo := dynamo.NewCoordinationRepo(env.DynamoDB, coordTable)
		assigner, err := domain.NewSlotAssigner(shards)
		if err != nil {
			t.Fatalf("NewSlotAssigner: %v", err)
		}

		ts := time.Date(2026, 11, 20, 3, 0, 0, 0, time.UTC)
		seedCatalog(ctx, t, reqRepo, coordRepo, numProvs, perProv, ts)

		// Run 1: inject interruption after 90 writes (mid-job across providers & pages).
		injectedErr := errors.New("simulated pod SIGKILL")
		faultWriter := &interruptingSlotWriter{inner: reqRepo, limit: 90, injectErr: injectedErr}
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

		uc1, err := assignslots.New(
			assigner, reqRepo, faultWriter, coordRepo, coordRepo,
			assignslots.NoopImmediateDispatcher{}, fakeClock{now: ts}, logger,
			assignslots.Config{Workers: 2, WCULimit: 500},
		)
		if err != nil {
			t.Fatalf("assignslots.New (run 1): %v", err)
		}

		if _, err := uc1.Run(ctx, "job-it-001"); !errors.Is(err, injectedErr) {
			t.Fatalf("interrupted Run() error = %v, want %v", err, injectedErr)
		}

		// Run 2: resume same job ID with real writer.
		uc2, err := assignslots.New(
			assigner, reqRepo, reqRepo, coordRepo, coordRepo,
			assignslots.NoopImmediateDispatcher{}, fakeClock{now: ts.Add(time.Minute)}, logger,
			assignslots.Config{Workers: 2, WCULimit: 500},
		)
		if err != nil {
			t.Fatalf("assignslots.New (run 2): %v", err)
		}

		stats2, err := uc2.Run(ctx, "job-it-001")
		if err != nil {
			t.Fatalf("resumed Run(): %v", err)
		}
		if stats2.ProvidersSkipped == 0 {
			t.Errorf("expected at least 1 already-completed provider to be skipped on resume")
		}

		// Verify 100% of records are slotted and histograms in coordination match DynamoDB exactly.
		states, err := coordRepo.ListProviders(ctx)
		if err != nil {
			t.Fatalf("ListProviders: %v", err)
		}
		if len(states) != numProvs {
			t.Fatalf("ListProviders len = %d, want %d", len(states), numProvs)
		}

		totalSlotted := 0
		for _, st := range states {
			if !st.Backfilled || st.Count != perProv || st.NewSinceBackfill != 0 {
				t.Errorf("provider %s registry state unexpected: Backfilled=%t Count=%d New=%d",
					st.Provider, st.Backfilled, st.Count, st.NewSinceBackfill)
			}

			var actualHist [domain.SlotsPerDay]int
			page := ""
			for {
				items, next, err := reqRepo.ListByProvider(ctx, st.Provider, page)
				if err != nil {
					t.Fatalf("ListByProvider(%s): %v", st.Provider, err)
				}
				for _, it := range items {
					if it.Slot < 0 || it.Slot >= domain.SlotsPerDay || it.Shard < 0 || it.Shard >= shards {
						t.Errorf("record %s has invalid slot/shard (%d,%d)", it.ID, it.Slot, it.Shard)
					} else {
						actualHist[it.Slot]++
						totalSlotted++
					}
				}
				if next == "" {
					break
				}
				page = next
			}
			if st.Histogram != actualHist {
				t.Errorf("provider %s persisted histogram does not match actual DynamoDB slots", st.Provider)
			}
		}
		if totalSlotted != numProvs*perProv {
			t.Fatalf("totalSlotted = %d, want %d (100%%)", totalSlotted, numProvs*perProv)
		}

		// Run 3: fresh job ID must be 100% idempotent (0 writes).
		stats3, err := uc2.Run(ctx, "job-it-002")
		if err != nil {
			t.Fatalf("idempotent Run(): %v", err)
		}
		if stats3.SlotsUpdated != 0 {
			t.Errorf("idempotent Run() SlotsUpdated = %d, want 0", stats3.SlotsUpdated)
		}
	})

	t.Run("real app+dynamo live heap stays bounded when N scales 4x with fixed k_max", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()

		kMax := 5_000
		pageSize := int32(500)
		numProvs1x, numProvs4x := 4, 16 // 20,000 vs 80,000 records (k_max=5,000)
		if testing.Short() {
			kMax = 100
			pageSize = 25
			numProvs1x, numProvs4x = 2, 8 // 200 vs 800 records for fast PR integration gate
		}
		const shards = 4
		assigner, _ := domain.NewSlotAssigner(shards)
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
		ts := time.Date(2026, 11, 20, 4, 0, 0, 0, time.UTC)

		measure := func(suffix string, numProvs int) uint64 {
			reqTable := "requests_mem_" + suffix
			coordTable := "coordination_mem_" + suffix
			if err := env.EnsureRequestsTable(ctx, reqTable); err != nil {
				t.Fatalf("EnsureRequestsTable(%s): %v", reqTable, err)
			}
			if err := env.EnsureCoordinationTable(ctx, coordTable); err != nil {
				t.Fatalf("EnsureCoordinationTable(%s): %v", coordTable, err)
			}

			reqRepo := dynamo.NewRequestsRepo(env.DynamoDB, reqTable, dynamo.WithPageSize(pageSize))
			coordRepo := dynamo.NewCoordinationRepo(env.DynamoDB, coordTable)
			seedCatalog(ctx, t, reqRepo, coordRepo, numProvs, kMax, ts)

			sampler := &heapSamplingRegistry{inner: coordRepo}
			uc, err := assignslots.New(
				assigner, reqRepo, reqRepo, sampler, coordRepo,
				assignslots.NoopImmediateDispatcher{}, fakeClock{now: ts}, logger,
				assignslots.Config{Workers: 4},
			)
			if err != nil {
				t.Fatalf("assignslots.New(%s): %v", suffix, err)
			}
			stats, err := uc.Run(ctx, "job-mem-"+suffix)
			if err != nil {
				t.Fatalf("Run(%s): %v", suffix, err)
			}
			if stats.SlotsUpdated != numProvs*kMax {
				t.Fatalf("Run(%s) SlotsUpdated = %d, want %d", suffix, stats.SlotsUpdated, numProvs*kMax)
			}
			return sampler.peakHeap.Load()
		}

		peak1x := measure("1x", numProvs1x)
		peak4x := measure("4x", numProvs4x)

		t.Logf("real app+dynamo live heap (k_max=%d): %d recs peak=%d KiB | %d recs peak=%d KiB",
			kMax, numProvs1x*kMax, peak1x/1024, numProvs4x*kMax, peak4x/1024)

		const maxAllowedDeltaBytes = 8 * 1024 * 1024
		if peak4x > peak1x+maxAllowedDeltaBytes {
			t.Fatalf("real app+dynamo live heap grew excessively: 1x=%d KiB, 4x=%d KiB", peak1x/1024, peak4x/1024)
		}
	})
}

func seedCatalog(
	ctx context.Context,
	t *testing.T,
	reqRepo *dynamo.RequestsRepo,
	coordRepo *dynamo.CoordinationRepo,
	numProvs, perProv int,
	ts time.Time,
) {
	t.Helper()
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)

	for p := range numProvs {
		provider := fmt.Sprintf("prov-%02d", p)
		g.Go(func() error {
			const chunkSize = 250
			batch := make([]domain.Request, 0, chunkSize)
			for i := range perProv {
				batch = append(batch, domain.Request{
					ID:        fmt.Sprintf("%s-req-%05d", provider, i),
					Provider:  provider,
					Endpoint:  fmt.Sprintf("/v1/providers/%s/debt", provider),
					Method:    "POST",
					Body:      []byte(`{"account":"masked"}`),
					Enabled:   true,
					Slot:      -1,
					Shard:     -1,
					Version:   1,
					CreatedAt: ts,
					UpdatedAt: ts,
				})
				if len(batch) == chunkSize {
					if err := reqRepo.PutBatch(gctx, batch); err != nil {
						return fmt.Errorf("PutBatch(%s): %w", provider, err)
					}
					batch = batch[:0]
				}
			}
			if len(batch) > 0 {
				if err := reqRepo.PutBatch(gctx, batch); err != nil {
					return fmt.Errorf("PutBatch(%s): %w", provider, err)
				}
			}
			if err := coordRepo.SaveProviderState(gctx, assignslots.ProviderState{
				Provider:   provider,
				Backfilled: false,
				UpdatedAt:  ts,
			}); err != nil {
				return fmt.Errorf("SaveProviderState(%s): %w", provider, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatalf("seedCatalog: %v", err)
	}
}
