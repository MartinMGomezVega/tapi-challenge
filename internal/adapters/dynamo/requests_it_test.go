//go:build integration

package dynamo_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/adapters/dynamo"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
	"github.com/martingomezvega/tapi-challenge/test/testenv"
)

func TestRequestsRepo_Integration(t *testing.T) {
	env := testenv.StartLocalStack(t)

	t.Run("Put and Get round-trip and NotFound", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		const tableName = "requests_put_get"
		if err := env.EnsureRequestsTable(ctx, tableName); err != nil {
			t.Fatalf("EnsureRequestsTable: %v", err)
		}

		repo := dynamo.NewRequestsRepo(env.DynamoDB, tableName)
		createdAt := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)

		want := domain.Request{
			ID:        "req-100",
			Provider:  "edenor",
			Endpoint:  "/v1/providers/edenor/debt",
			Method:    "POST",
			Body:      []byte(`{"account":"998877"}`),
			Enabled:   true,
			Slot:      742,
			Shard:     3,
			Version:   1,
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
		}

		if err := repo.Put(ctx, want); err != nil {
			t.Fatalf("Put(): %v", err)
		}

		got, err := repo.Get(ctx, want.ID)
		if err != nil {
			t.Fatalf("Get(%s): %v", want.ID, err)
		}

		if got.ID != want.ID ||
			got.Provider != want.Provider ||
			got.Endpoint != want.Endpoint ||
			got.Method != want.Method ||
			!bytes.Equal(got.Body, want.Body) ||
			got.Enabled != want.Enabled ||
			got.Slot != want.Slot ||
			got.Shard != want.Shard ||
			got.Version != want.Version ||
			!got.CreatedAt.Equal(want.CreatedAt) ||
			!got.UpdatedAt.Equal(want.UpdatedAt) {
			t.Fatalf("Get() mismatch:\ngot  %+v\nwant %+v", got, want)
		}

		if _, err := repo.Get(ctx, "non-existent"); !errors.Is(err, domain.ErrRequestNotFound) {
			t.Fatalf("Get(non-existent) error = %v, want %v", err, domain.ErrRequestNotFound)
		}
	})

	// T014: ListBySlotShard returns only enabled items of the target (slot, shard), paginated (FR-004).
	t.Run("ListBySlotShard returns only enabled items of target slot and shard with pagination", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		const tableName = "requests_gsi_list"
		if err := env.EnsureRequestsTable(ctx, tableName); err != nil {
			t.Fatalf("EnsureRequestsTable: %v", err)
		}

		repo := dynamo.NewRequestsRepo(env.DynamoDB, tableName, dynamo.WithPageSize(2))
		baseTime := time.Date(2026, 11, 20, 8, 30, 0, 0, time.UTC)

		seed := []domain.Request{
			// 5 enabled items in target (slot=742, shard=3)
			makeReq("r-01", "aysa", 742, 3, true, baseTime),
			makeReq("r-02", "edenor", 742, 3, true, baseTime.Add(time.Minute)),
			makeReq("r-03", "edenor", 742, 3, true, baseTime.Add(2*time.Minute)),
			makeReq("r-04", "metrogas", 742, 3, true, baseTime.Add(3*time.Minute)),
			makeReq("r-05", "telecom", 742, 3, true, baseTime.Add(4*time.Minute)),
			// 2 disabled items in SAME (slot=742, shard=3) -> must be omitted
			makeReq("r-disabled-1", "edenor", 742, 3, false, baseTime),
			makeReq("r-disabled-2", "metrogas", 742, 3, false, baseTime),
			// 2 enabled items in DIFFERENT slot (743, 3) -> must be omitted
			makeReq("r-other-slot-1", "edenor", 743, 3, true, baseTime),
			makeReq("r-other-slot-2", "aysa", 743, 3, true, baseTime),
			// 2 enabled items in DIFFERENT shard (742, 2) -> must be omitted
			makeReq("r-other-shard-1", "edenor", 742, 2, true, baseTime),
			makeReq("r-other-shard-2", "aysa", 742, 2, true, baseTime),
			// 1 unslotted enabled item -> sparse GSI, must be omitted
			makeReq("r-unslotted", "edenor", -1, -1, true, baseTime),
		}

		for _, req := range seed {
			if err := repo.Put(ctx, req); err != nil {
				t.Fatalf("Put(%s): %v", req.ID, err)
			}
		}

		var gotKeys []domain.RequestKey
		page := ""
		pages := 0
		for {
			items, nextPage, err := repo.ListBySlotShard(ctx, 742, 3, page)
			if err != nil {
				t.Fatalf("ListBySlotShard(page=%d): %v", pages, err)
			}
			pages++
			gotKeys = append(gotKeys, items...)
			if nextPage == "" {
				break
			}
			page = nextPage
			if pages > 20 {
				t.Fatal("pagination loop exceeded 20 pages")
			}
		}

		if pages < 2 {
			t.Errorf("expected multiple pages with pageSize=2, got %d page(s)", pages)
		}

		gotIDs := make([]string, len(gotKeys))
		for i, k := range gotKeys {
			gotIDs[i] = k.ID
			if !k.Enabled {
				t.Errorf("returned disabled key: %+v", k)
			}
			if k.Provider == "" {
				t.Errorf("returned key with empty provider: %+v", k)
			}
			if k.CreatedAt.IsZero() {
				t.Errorf("returned key with zero CreatedAt: %+v", k)
			}
		}
		slices.Sort(gotIDs)
		wantIDs := []string{"r-01", "r-02", "r-03", "r-04", "r-05"}
		if !slices.Equal(gotIDs, wantIDs) {
			t.Fatalf("ListBySlotShard IDs = %v, want %v", gotIDs, wantIDs)
		}

		if _, _, err := repo.ListBySlotShard(ctx, 742, 3, "!!!not-base64!!!"); !errors.Is(err, dynamo.ErrInvalidPageToken) {
			t.Errorf("ListBySlotShard(invalid token) error = %v, want %v", err, dynamo.ErrInvalidPageToken)
		}
	})

	// T015: SetSlot updates slot/shard conditionally on version (optimistic locking).
	t.Run("SetSlot updates conditionally by version and projects into GSI", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		const tableName = "requests_set_slot"
		if err := env.EnsureRequestsTable(ctx, tableName); err != nil {
			t.Fatalf("EnsureRequestsTable: %v", err)
		}

		t0 := time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC)
		t1 := t0.Add(5 * time.Minute)
		repo := dynamo.NewRequestsRepo(env.DynamoDB, tableName, dynamo.WithClock(func() time.Time { return t1 }))

		req := makeReq("req-slot-1", "edenor", -1, -1, true, t0)
		if err := repo.Put(ctx, req); err != nil {
			t.Fatalf("Put(): %v", err)
		}

		// Wrong version must fail with ErrVersionConflict.
		if err := repo.SetSlot(ctx, req.ID, 120, 1, 999); !errors.Is(err, domain.ErrVersionConflict) {
			t.Fatalf("SetSlot(wrong version) error = %v, want %v", err, domain.ErrVersionConflict)
		}

		// Matching version succeeds and increments version to 2.
		if err := repo.SetSlot(ctx, req.ID, 120, 1, 1); err != nil {
			t.Fatalf("SetSlot(version=1): %v", err)
		}

		// Replaying with stale version=1 must fail.
		if err := repo.SetSlot(ctx, req.ID, 121, 1, 1); !errors.Is(err, domain.ErrVersionConflict) {
			t.Fatalf("SetSlot(stale version=1) error = %v, want %v", err, domain.ErrVersionConflict)
		}

		updated, err := repo.Get(ctx, req.ID)
		if err != nil {
			t.Fatalf("Get(%s): %v", req.ID, err)
		}
		if updated.Slot != 120 || updated.Shard != 1 || updated.Version != 2 || !updated.UpdatedAt.Equal(t1) {
			t.Fatalf("updated request = %+v, want slot=120 shard=1 version=2 updatedAt=%v", updated, t1)
		}

		keys, nextPage, err := repo.ListBySlotShard(ctx, 120, 1, "")
		if err != nil {
			t.Fatalf("ListBySlotShard(120, 1): %v", err)
		}
		if nextPage != "" || len(keys) != 1 || keys[0].ID != req.ID {
			t.Fatalf("ListBySlotShard(120, 1) = (%+v, %q), want single key %s", keys, nextPage, req.ID)
		}

		// Invalid slot rejected before DynamoDB call.
		if err := repo.SetSlot(ctx, req.ID, 1440, 1, 2); !errors.Is(err, domain.ErrInvalidSlot) {
			t.Errorf("SetSlot(slot=1440) error = %v, want %v", err, domain.ErrInvalidSlot)
		}
		if err := repo.SetSlot(ctx, req.ID, 10, -1, 2); !errors.Is(err, domain.ErrInvalidShard) {
			t.Errorf("SetSlot(shard=-1) error = %v, want %v", err, domain.ErrInvalidShard)
		}
	})

	// T016c: gsi_provider CountByProvider + ListByProvider ordered by SK with projected fields (FR-004b).
	t.Run("CountByProvider and ListByProvider ordered by SK with projected attrs and pagination", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		const tableName = "requests_gsi_provider"
		if err := env.EnsureRequestsTable(ctx, tableName); err != nil {
			t.Fatalf("EnsureRequestsTable: %v", err)
		}

		repo := dynamo.NewRequestsRepo(env.DynamoDB, tableName, dynamo.WithPageSize(2))
		ts := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

		// Insert in intentionally scrambled ID order to verify DynamoDB SK sorting.
		seed := []domain.Request{
			{ID: "id-05", Provider: "edenor", Endpoint: "/v1/providers/edenor/debt", Method: "POST", Body: []byte(`{}`), Enabled: true, Slot: 500, Shard: 2, Version: 3, CreatedAt: ts, UpdatedAt: ts},
			{ID: "id-01", Provider: "edenor", Endpoint: "/v1/providers/edenor/debt", Method: "POST", Body: []byte(`{}`), Enabled: true, Slot: -1, Shard: -1, Version: 1, CreatedAt: ts, UpdatedAt: ts},
			{ID: "id-04", Provider: "edenor", Endpoint: "/v1/providers/edenor/debt", Method: "POST", Body: []byte(`{}`), Enabled: false, Slot: 400, Shard: 1, Version: 2, CreatedAt: ts, UpdatedAt: ts},
			{ID: "id-02", Provider: "edenor", Endpoint: "/v1/providers/edenor/debt", Method: "POST", Body: []byte(`{}`), Enabled: true, Slot: 100, Shard: 0, Version: 4, CreatedAt: ts, UpdatedAt: ts},
			{ID: "id-03", Provider: "edenor", Endpoint: "/v1/providers/edenor/debt", Method: "POST", Body: []byte(`{}`), Enabled: true, Slot: -1, Shard: -1, Version: 1, CreatedAt: ts, UpdatedAt: ts},
			// Other provider -> must not appear for edenor
			makeReq("other-1", "metrogas", 10, 0, true, ts),
			makeReq("other-2", "metrogas", 20, 1, true, ts),
		}
		for _, req := range seed {
			if err := repo.Put(ctx, req); err != nil {
				t.Fatalf("Put(%s): %v", req.ID, err)
			}
		}

		count, err := repo.CountByProvider(ctx, "edenor")
		if err != nil {
			t.Fatalf("CountByProvider(edenor): %v", err)
		}
		if count != 5 {
			t.Fatalf("CountByProvider(edenor) = %d, want 5", count)
		}

		emptyCount, err := repo.CountByProvider(ctx, "unknown-provider")
		if err != nil {
			t.Fatalf("CountByProvider(unknown): %v", err)
		}
		if emptyCount != 0 {
			t.Fatalf("CountByProvider(unknown) = %d, want 0", emptyCount)
		}

		var got []domain.ProviderRequestItem
		page := ""
		pages := 0
		for {
			items, nextPage, err := repo.ListByProvider(ctx, "edenor", page)
			if err != nil {
				t.Fatalf("ListByProvider(page=%d): %v", pages, err)
			}
			pages++
			got = append(got, items...)
			if nextPage == "" {
				break
			}
			page = nextPage
			if pages > 20 {
				t.Fatal("pagination loop exceeded 20 pages")
			}
		}

		if pages < 2 {
			t.Errorf("expected multiple pages with pageSize=2, got %d", pages)
		}

		want := []domain.ProviderRequestItem{
			{ID: "id-01", Provider: "edenor", Version: 1, Slot: -1, Shard: -1, Enabled: true},
			{ID: "id-02", Provider: "edenor", Version: 4, Slot: 100, Shard: 0, Enabled: true},
			{ID: "id-03", Provider: "edenor", Version: 1, Slot: -1, Shard: -1, Enabled: true},
			{ID: "id-04", Provider: "edenor", Version: 2, Slot: 400, Shard: 1, Enabled: false},
			{ID: "id-05", Provider: "edenor", Version: 3, Slot: 500, Shard: 2, Enabled: true},
		}
		if !slices.Equal(got, want) {
			t.Fatalf("ListByProvider() mismatch:\ngot  %+v\nwant %+v", got, want)
		}

		if _, _, err := repo.ListByProvider(ctx, "edenor", "!!!bad-token!!!"); !errors.Is(err, dynamo.ErrInvalidPageToken) {
			t.Errorf("ListByProvider(bad token) error = %v, want %v", err, dynamo.ErrInvalidPageToken)
		}
	})
}

func makeReq(id, provider string, slot, shard int, enabled bool, ts time.Time) domain.Request {
	return domain.Request{
		ID:        id,
		Provider:  provider,
		Endpoint:  fmt.Sprintf("/v1/providers/%s/debt", provider),
		Method:    "POST",
		Body:      []byte(`{"ok":true}`),
		Enabled:   enabled,
		Slot:      slot,
		Shard:     shard,
		Version:   1,
		CreatedAt: ts,
		UpdatedAt: ts,
	}
}
