//go:build integration

package dynamo_test

import (
	"context"
	"testing"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/adapters/dynamo"
	"github.com/martingomezvega/tapi-challenge/internal/app/assignslots"
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
}
