//go:build integration

package dynamo_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/adapters/dynamo"
	"github.com/martingomezvega/tapi-challenge/internal/app/assignslots"
	"github.com/martingomezvega/tapi-challenge/internal/app/dispatch"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
	"github.com/martingomezvega/tapi-challenge/test/testenv"
)

func TestCoordinationRepo_Integration(t *testing.T) {
	env := testenv.StartLocalStack(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const tableName = "coordination_it"
	if err := env.EnsureCoordinationTable(ctx, tableName); err != nil {
		t.Fatalf("EnsureCoordinationTable: %v", err)
	}

	repo := dynamo.NewCoordinationRepo(env.DynamoDB, tableName)

	t.Run("SaveProviderState and ListProviders round-trip with 1440-slot histogram", func(t *testing.T) {
		ts := time.Date(2026, 11, 20, 14, 0, 0, 0, time.UTC)
		var histEdenor [domain.SlotsPerDay]int
		histEdenor[0] = 5
		histEdenor[742] = 12
		histEdenor[1439] = 3

		s1 := assignslots.ProviderState{
			Provider:         "metrogas",
			Count:            10,
			NewSinceBackfill: 2,
			Backfilled:       true,
			UpdatedAt:        ts,
		}
		s2 := assignslots.ProviderState{
			Provider:         "edenor",
			Count:            20,
			Histogram:        histEdenor,
			NewSinceBackfill: 0,
			Backfilled:       true,
			UpdatedAt:        ts,
		}

		if err := repo.SaveProviderState(ctx, s1); err != nil {
			t.Fatalf("SaveProviderState(metrogas): %v", err)
		}
		if err := repo.SaveProviderState(ctx, s2); err != nil {
			t.Fatalf("SaveProviderState(edenor): %v", err)
		}

		// Save a checkpoint item too and verify ListProviders excludes non-provider items.
		if err := repo.Save(ctx, "job-x", "edenor", assignslots.BackfillCheckpoint{
			Status:    assignslots.CheckpointDone,
			Rank:      20,
			Count:     20,
			UpdatedAt: ts,
		}); err != nil {
			t.Fatalf("Save checkpoint: %v", err)
		}

		list, err := repo.ListProviders(ctx)
		if err != nil {
			t.Fatalf("ListProviders(): %v", err)
		}
		if len(list) != 2 {
			t.Fatalf("ListProviders() len = %d, want 2", len(list))
		}
		if list[0].Provider != "edenor" || list[1].Provider != "metrogas" {
			t.Fatalf("ListProviders() order = [%s, %s], want [edenor, metrogas]", list[0].Provider, list[1].Provider)
		}
		if list[0].Histogram != histEdenor || list[0].Count != 20 || !list[0].Backfilled {
			t.Fatalf("edenor state mismatch: %+v", list[0])
		}
		if list[1].Count != 10 || list[1].NewSinceBackfill != 2 || !list[1].UpdatedAt.Equal(ts) {
			t.Fatalf("metrogas state mismatch: %+v", list[1])
		}
	})

	t.Run("Checkpoint Load missing returns false and Save/Load round-trips", func(t *testing.T) {
		_, found, err := repo.Load(ctx, "job-missing", "aysa")
		if err != nil {
			t.Fatalf("Load(missing): %v", err)
		}
		if found {
			t.Fatal("Load(missing) found = true, want false")
		}

		ts := time.Date(2026, 11, 20, 15, 30, 0, 0, time.UTC)
		var hist [domain.SlotsPerDay]int
		hist[10] = 7
		hist[1200] = 4

		want := assignslots.BackfillCheckpoint{
			Status:      assignslots.CheckpointPending,
			Incremental: false,
			Cursor:      "eyJpZCI6InJlcS01MCIsInByb3ZpZGVyIjoiYXlzYSJ9",
			Rank:        50,
			Count:       200,
			Histogram:   hist,
			UpdatedAt:   ts,
		}
		if err := repo.Save(ctx, "job-cp", "aysa", want); err != nil {
			t.Fatalf("Save(): %v", err)
		}

		got, found, err := repo.Load(ctx, "job-cp", "aysa")
		if err != nil {
			t.Fatalf("Load(): %v", err)
		}
		if !found {
			t.Fatal("Load() found = false, want true")
		}
		if got.Status != want.Status ||
			got.Incremental != want.Incremental ||
			got.Cursor != want.Cursor ||
			got.Rank != want.Rank ||
			got.Count != want.Count ||
			got.Histogram != want.Histogram ||
			!got.UpdatedAt.Equal(want.UpdatedAt) {
			t.Fatalf("Load() mismatch:\ngot  %+v\nwant %+v", got, want)
		}
	})

	// 002-T020: Conditional leader lease with 2 concurrent goroutines (-race) + fenced watermark.
	t.Run("DispatcherCoordinator concurrent lease election and fenced watermark", func(t *testing.T) {
		dc := dynamo.NewDispatcherCoordinator(env.DynamoDB, tableName)
		now := time.Date(2026, 11, 20, 16, 0, 0, 0, time.UTC)

		var (
			wg       sync.WaitGroup
			winners  atomic.Int32
			winOwner string
			winToken int64
			mu       sync.Mutex
		)

		for _, owner := range []string{"dispatcher-az1", "dispatcher-az2"} {
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				tok, ok, err := dc.TryAcquire(ctx, id, 30*time.Second, now)
				if err != nil {
					t.Errorf("TryAcquire(%s): %v", id, err)
					return
				}
				if ok {
					winners.Add(1)
					mu.Lock()
					winOwner = id
					winToken = tok
					mu.Unlock()
				}
			}(owner)
		}
		wg.Wait()

		if winners.Load() != 1 {
			t.Fatalf("expected exactly 1 lease winner, got %d", winners.Load())
		}

		// Winner advances watermark cleanly.
		c1 := domain.SlotCoord{Date: "2026-11-20", Slot: 960}
		if err := dc.Advance(ctx, c1, winToken, now); err != nil {
			t.Fatalf("winner Advance(): %v", err)
		}
		gotWM, found, err := dc.Load(ctx)
		if err != nil || !found || gotWM != c1 {
			t.Fatalf("Load() = (%+v, %t, %v), want (%+v, true, nil)", gotWM, found, err, c1)
		}

		// After lease expiry, other replica takes over with higher token; ex-leader's old token is fenced out.
		other := "dispatcher-az2"
		if winOwner == other {
			other = "dispatcher-az1"
		}
		newToken, ok, err := dc.TryAcquire(ctx, other, 30*time.Second, now.Add(35*time.Second))
		if err != nil || !ok || newToken <= winToken {
			t.Fatalf("failover TryAcquire(%s) = (tok=%d, ok=%t, err=%v), want ok=true tok>%d", other, newToken, ok, err, winToken)
		}

		if err := dc.Advance(ctx, domain.SlotCoord{Date: "2026-11-20", Slot: 961}, winToken, now.Add(36*time.Second)); !errors.Is(err, dispatch.ErrFencingMismatch) {
			t.Errorf("stale leader Advance() error = %v, want %v", err, dispatch.ErrFencingMismatch)
		}
		if err := dc.Advance(ctx, domain.SlotCoord{Date: "2026-11-20", Slot: 961}, newToken, now.Add(36*time.Second)); err != nil {
			t.Errorf("new leader Advance() error = %v, want nil", err)
		}

		// Release allows immediate takeover without waiting for TTL.
		if err := dc.Release(ctx, other); err != nil {
			t.Fatalf("Release(%s): %v", other, err)
		}
		if _, ok, err := dc.TryAcquire(ctx, winOwner, 30*time.Second, now.Add(37*time.Second)); err != nil || !ok {
			t.Fatalf("re-acquire after Release = (ok=%t, err=%v), want (true, nil)", ok, err)
		}
	})
}
