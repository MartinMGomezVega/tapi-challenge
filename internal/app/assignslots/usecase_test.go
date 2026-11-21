package assignslots_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/app/assignslots"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

type fakeClock struct {
	now time.Time
}

func (f fakeClock) Now() time.Time { return f.now }

type fakeCatalog struct {
	mu            sync.Mutex
	items         map[string][]domain.ProviderRequestItem // provider -> sorted by ID
	countOverride map[string]int                          // optional stale COUNT for rank >= k test
	pageSize      int
	setCalls      int
	failAfterSet  int // if > 0, return error after N SetSlot calls
	failErr       error
}

func newFakeCatalog(pageSize int) *fakeCatalog {
	return &fakeCatalog{
		items:         make(map[string][]domain.ProviderRequestItem),
		countOverride: make(map[string]int),
		pageSize:      pageSize,
	}
}

func (c *fakeCatalog) addProvider(provider string, count int) {
	list := make([]domain.ProviderRequestItem, count)
	for i := range count {
		list[i] = domain.ProviderRequestItem{
			ID:       fmt.Sprintf("%s-%06d", provider, i),
			Provider: provider,
			Version:  1,
			Slot:     -1,
			Shard:    -1,
			Enabled:  true,
		}
	}
	c.items[provider] = list
}

func (c *fakeCatalog) CountByProvider(_ context.Context, provider string) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ov, ok := c.countOverride[provider]; ok {
		return ov, nil
	}
	return len(c.items[provider]), nil
}

func (c *fakeCatalog) ListByProvider(_ context.Context, provider, page string) ([]domain.ProviderRequestItem, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	all := c.items[provider]
	start := 0
	if page != "" {
		var err error
		start, err = strconv.Atoi(page)
		if err != nil {
			return nil, "", fmt.Errorf("bad cursor %q: %w", page, err)
		}
	}
	if start >= len(all) {
		return nil, "", nil
	}
	end := min(start+c.pageSize, len(all))
	out := slices.Clone(all[start:end])
	next := ""
	if end < len(all) {
		next = strconv.Itoa(end)
	}
	return out, next, nil
}

func (c *fakeCatalog) SetSlot(_ context.Context, id string, slot, shard int, expectedVersion int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.failAfterSet > 0 && c.setCalls >= c.failAfterSet {
		return c.failErr
	}
	for p, list := range c.items {
		for i := range list {
			if list[i].ID == id {
				if list[i].Version != expectedVersion {
					return domain.ErrVersionConflict
				}
				list[i].Slot = slot
				list[i].Shard = shard
				list[i].Version++
				c.items[p] = list
				c.setCalls++
				return nil
			}
		}
	}
	return domain.ErrRequestNotFound
}

func (c *fakeCatalog) snapshot(provider string) []domain.ProviderRequestItem {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.items[provider])
}

type fakeRegistry struct {
	mu     sync.Mutex
	states map[string]assignslots.ProviderState
}

func newFakeRegistry(states ...assignslots.ProviderState) *fakeRegistry {
	m := make(map[string]assignslots.ProviderState, len(states))
	for _, s := range states {
		m[s.Provider] = s
	}
	return &fakeRegistry{states: m}
}

func (r *fakeRegistry) ListProviders(_ context.Context) ([]assignslots.ProviderState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]assignslots.ProviderState, 0, len(r.states))
	for _, s := range r.states {
		out = append(out, s)
	}
	slices.SortFunc(out, func(a, b assignslots.ProviderState) int {
		if a.Provider < b.Provider {
			return -1
		}
		if a.Provider > b.Provider {
			return 1
		}
		return 0
	})
	return out, nil
}

func (r *fakeRegistry) SaveProviderState(_ context.Context, state assignslots.ProviderState) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.states[state.Provider] = state
	return nil
}

func (r *fakeRegistry) get(provider string) assignslots.ProviderState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.states[provider]
}

type fakeCheckpointer struct {
	mu  sync.Mutex
	cps map[string]assignslots.BackfillCheckpoint
}

func newFakeCheckpointer() *fakeCheckpointer {
	return &fakeCheckpointer{cps: make(map[string]assignslots.BackfillCheckpoint)}
}

func (c *fakeCheckpointer) Load(_ context.Context, job, provider string) (assignslots.BackfillCheckpoint, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp, ok := c.cps[job+"#"+provider]
	return cp, ok, nil
}

func (c *fakeCheckpointer) Save(_ context.Context, job, provider string, cp assignslots.BackfillCheckpoint) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cps[job+"#"+provider] = cp
	return nil
}

type dispatchedEvent struct {
	reqID    string
	provider string
}

type fakeDispatcher struct {
	mu     sync.Mutex
	events []dispatchedEvent
}

func (d *fakeDispatcher) DispatchNow(_ context.Context, reqID, provider string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.events = append(d.events, dispatchedEvent{reqID: reqID, provider: provider})
	return nil
}

func (d *fakeDispatcher) list() []dispatchedEvent {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.events)
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// T017: Idempotency (2 runs = exact same state, 0 writes on 2nd run).
func TestUseCase_IdempotentTwoRuns(t *testing.T) {
	t.Parallel()

	assigner, err := domain.NewSlotAssigner(4)
	if err != nil {
		t.Fatalf("NewSlotAssigner: %v", err)
	}

	cat := newFakeCatalog(5)
	cat.addProvider("edenor", 12)
	cat.addProvider("metrogas", 20)

	reg := newFakeRegistry(
		assignslots.ProviderState{Provider: "edenor", Backfilled: false},
		assignslots.ProviderState{Provider: "metrogas", Backfilled: false},
	)
	cps := newFakeCheckpointer()
	clk := fakeClock{now: time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)}

	uc, err := assignslots.New(assigner, cat, cat, reg, cps, assignslots.NoopImmediateDispatcher{}, clk, discardLogger(), assignslots.Config{Workers: 2})
	if err != nil {
		t.Fatalf("assignslots.New: %v", err)
	}

	stats1, err := uc.Run(context.Background(), "job-1")
	if err != nil {
		t.Fatalf("first Run(): %v", err)
	}
	if stats1.SlotsUpdated != 32 {
		t.Fatalf("first Run SlotsUpdated = %d, want 32", stats1.SlotsUpdated)
	}

	after1Edenor := cat.snapshot("edenor")
	after1Metrogas := cat.snapshot("metrogas")
	writesAfter1 := cat.setCalls

	// Second run with a brand-new job ID must produce identical slots and zero writes.
	stats2, err := uc.Run(context.Background(), "job-2")
	if err != nil {
		t.Fatalf("second Run(): %v", err)
	}
	if stats2.SlotsUpdated != 0 {
		t.Errorf("second Run SlotsUpdated = %d, want 0 (idempotent)", stats2.SlotsUpdated)
	}
	if cat.setCalls != writesAfter1 {
		t.Errorf("second Run performed %d extra SetSlot calls, want 0", cat.setCalls-writesAfter1)
	}
	if !slices.Equal(cat.snapshot("edenor"), after1Edenor) {
		t.Error("edenor state changed on second run")
	}
	if !slices.Equal(cat.snapshot("metrogas"), after1Metrogas) {
		t.Error("metrogas state changed on second run")
	}
}

// T017: Threshold policy (> 1440 new records -> full rebalance; <= 1440 -> incremental by histogram).
func TestUseCase_RebalanceVsIncrementalThreshold(t *testing.T) {
	t.Parallel()

	assigner, err := domain.NewSlotAssigner(4)
	if err != nil {
		t.Fatalf("NewSlotAssigner: %v", err)
	}

	cat := newFakeCatalog(50)
	// Provider "inc" already backfilled with 10 slotted items + 2 new unslotted items (<= 1440).
	var incHist [domain.SlotsPerDay]int
	for i := range 10 {
		slot := i * 100
		incHist[slot]++
		cat.items["inc"] = append(cat.items["inc"], domain.ProviderRequestItem{
			ID: fmt.Sprintf("inc-%04d", i), Provider: "inc", Version: 2, Slot: slot, Shard: 0, Enabled: true,
		})
	}
	cat.items["inc"] = append(cat.items["inc"],
		domain.ProviderRequestItem{ID: "inc-new-1", Provider: "inc", Version: 1, Slot: -1, Shard: -1, Enabled: true},
		domain.ProviderRequestItem{ID: "inc-new-2", Provider: "inc", Version: 1, Slot: -1, Shard: -1, Enabled: true},
	)

	// Provider "reb" already backfilled but accumulated 1441 new records (> 1440 -> full rebalance).
	cat.items["reb"] = []domain.ProviderRequestItem{
		{ID: "reb-0001", Provider: "reb", Version: 2, Slot: 0, Shard: 0, Enabled: true},
		{ID: "reb-0002", Provider: "reb", Version: 2, Slot: 0, Shard: 0, Enabled: true}, // collides at slot 0; rebalance must separate it
		{ID: "reb-0003", Provider: "reb", Version: 1, Slot: -1, Shard: -1, Enabled: true},
	}

	reg := newFakeRegistry(
		assignslots.ProviderState{Provider: "inc", Count: 10, Histogram: incHist, NewSinceBackfill: 2, Backfilled: true},
		assignslots.ProviderState{Provider: "reb", Count: 2, NewSinceBackfill: 1441, Backfilled: true},
	)
	cps := newFakeCheckpointer()
	clk := fakeClock{now: time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)}

	uc, err := assignslots.New(assigner, cat, cat, reg, cps, assignslots.NoopImmediateDispatcher{}, clk, discardLogger(), assignslots.Config{Workers: 2})
	if err != nil {
		t.Fatalf("assignslots.New: %v", err)
	}

	stats, err := uc.Run(context.Background(), "job-threshold")
	if err != nil {
		t.Fatalf("Run(): %v", err)
	}
	if stats.ProvidersIncrement != 1 || stats.ProvidersRebalanced != 1 {
		t.Fatalf("stats = %+v, want 1 incremental and 1 rebalanced", stats)
	}

	// Incremental provider must preserve existing 10 items untouched (Version == 2) and slot the 2 new ones.
	incItems := cat.snapshot("inc")
	for i := range 10 {
		if incItems[i].Version != 2 || incItems[i].Slot != i*100 {
			t.Errorf("incremental mode altered existing item %s: %+v", incItems[i].ID, incItems[i])
		}
	}
	for i := 10; i < 12; i++ {
		if incItems[i].Slot < 0 || incItems[i].Version != 2 {
			t.Errorf("incremental mode failed to slot new item %s: %+v", incItems[i].ID, incItems[i])
		}
	}

	// Rebalanced provider must have 3 distinct equispaced slots.
	rebItems := cat.snapshot("reb")
	seen := make(map[int]bool)
	for _, it := range rebItems {
		if it.Slot < 0 || seen[it.Slot] {
			t.Errorf("rebalanced provider has invalid or colliding slot: %+v", it)
		}
		seen[it.Slot] = true
	}
}

// T017: Concurrent inserts during rebalance (rank >= k) are assigned via AssignIncremental without error.
func TestUseCase_ConcurrentInsertsRankGeK(t *testing.T) {
	t.Parallel()

	assigner, err := domain.NewSlotAssigner(4)
	if err != nil {
		t.Fatalf("NewSlotAssigner: %v", err)
	}

	cat := newFakeCatalog(4)
	cat.addProvider("aysa", 10)
	// Simulate COUNT returning 7 while streaming yields 10 items (3 concurrent inserts -> ranks 7, 8, 9 >= k).
	cat.countOverride["aysa"] = 7

	reg := newFakeRegistry(assignslots.ProviderState{Provider: "aysa", Backfilled: false})
	cps := newFakeCheckpointer()
	clk := fakeClock{now: time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)}

	uc, err := assignslots.New(assigner, cat, cat, reg, cps, assignslots.NoopImmediateDispatcher{}, clk, discardLogger(), assignslots.Config{Workers: 1})
	if err != nil {
		t.Fatalf("assignslots.New: %v", err)
	}

	stats, err := uc.Run(context.Background(), "job-concurrent")
	if err != nil {
		t.Fatalf("Run() failed on rank >= k: %v", err)
	}
	if stats.ConcurrentOverflow != 3 {
		t.Errorf("ConcurrentOverflow = %d, want 3", stats.ConcurrentOverflow)
	}
	if stats.SlotsUpdated != 10 {
		t.Errorf("SlotsUpdated = %d, want 10", stats.SlotsUpdated)
	}

	state := reg.get("aysa")
	if state.Count != 10 {
		t.Errorf("persisted Count = %d, want 10", state.Count)
	}
	sumHist := 0
	for _, c := range state.Histogram {
		sumHist += c
	}
	if sumHist != 10 {
		t.Errorf("persisted Histogram sum = %d, want 10", sumHist)
	}
}

// T017b: Same-day safety (FR-005b / R1): enabled item moved from slot_old >= slot_now to slot_new < slot_now
// is dispatched immediately; inverse case (already executed -> future slot), disabled, and unslotted are NOT dispatched.
func TestUseCase_SameDaySafetyDispatch(t *testing.T) {
	t.Parallel()

	assigner, err := domain.NewSlotAssigner(4)
	if err != nil {
		t.Fatalf("NewSlotAssigner: %v", err)
	}

	// Precompute exact target slots for provider "edenor" with k=4 so we can position slot_now deterministically.
	targetSlots := make([]int, 4)
	for r := range 4 {
		a, err := assigner.Assign("edenor", fmt.Sprintf("edenor-%06d", r), r, 4)
		if err != nil {
			t.Fatalf("Assign: %v", err)
		}
		targetSlots[r] = a.Slot
	}
	// Find an index with slot_new < 1000 so we can set slot_now = targetSlots[i] + 10 (still <= 1430).
	// Pick slot_now right between the minimum and maximum target slots.
	minSlot := slices.Min(targetSlots)
	maxSlot := slices.Max(targetSlots)
	slotNow := (minSlot + maxSlot) / 2
	if slotNow <= minSlot || slotNow >= maxSlot {
		t.Fatalf("unexpected slot spread: min=%d max=%d", minSlot, maxSlot)
	}

	cat := newFakeCatalog(10)
	var expectedDispatched []string
	for r := range 4 {
		id := fmt.Sprintf("edenor-%06d", r)
		var oldSlot int
		enabled := true

		switch {
		case targetSlots[r] < slotNow && len(expectedDispatched) == 0:
			// Case 1: enabled, slot_new < slot_now <= slot_old -> MUST dispatch immediately!
			oldSlot = slotNow + 20
			expectedDispatched = append(expectedDispatched, id)
		case targetSlots[r] < slotNow:
			// Case 2: disabled, slot_new < slot_now <= slot_old -> must NOT dispatch.
			oldSlot = slotNow + 20
			enabled = false
		default:
			// Case 3: inverse (already executed earlier today: slot_old < slot_now <= slot_new) -> must NOT dispatch.
			oldSlot = max(0, slotNow-20)
		}

		cat.items["edenor"] = append(cat.items["edenor"], domain.ProviderRequestItem{
			ID:       id,
			Provider: "edenor",
			Version:  2,
			Slot:     oldSlot,
			Shard:    0,
			Enabled:  enabled,
		})
	}
	if len(expectedDispatched) != 1 {
		t.Fatalf("test setup expected 1 dispatch candidate, got %d", len(expectedDispatched))
	}

	reg := newFakeRegistry(assignslots.ProviderState{Provider: "edenor", Backfilled: false})
	cps := newFakeCheckpointer()
	disp := &fakeDispatcher{}
	now := time.Date(2026, 11, 20, slotNow/60, slotNow%60, 0, 0, time.UTC)

	uc, err := assignslots.New(assigner, cat, cat, reg, cps, disp, fakeClock{now: now}, discardLogger(), assignslots.Config{Workers: 1})
	if err != nil {
		t.Fatalf("assignslots.New: %v", err)
	}

	stats, err := uc.Run(context.Background(), "job-sameday")
	if err != nil {
		t.Fatalf("Run(): %v", err)
	}
	if stats.DispatchedNow != 1 {
		t.Fatalf("DispatchedNow = %d, want 1", stats.DispatchedNow)
	}
	events := disp.list()
	if len(events) != 1 || events[0].reqID != expectedDispatched[0] || events[0].provider != "edenor" {
		t.Fatalf("dispatched events = %+v, want [{reqID:%s provider:edenor}]", events, expectedDispatched[0])
	}
}

// T018: Resumes from (provider, cursor, rank) checkpoint after mid-provider interruption.
func TestUseCase_ResumeFromCheckpointAfterInterruption(t *testing.T) {
	t.Parallel()

	assigner, err := domain.NewSlotAssigner(4)
	if err != nil {
		t.Fatalf("NewSlotAssigner: %v", err)
	}

	cat := newFakeCatalog(3) // 3 items per page -> multiple checkpoints per provider
	cat.addProvider("alpha", 6)
	cat.addProvider("beta", 9)
	injectedErr := errors.New("simulated network outage")
	cat.failAfterSet = 10 // alpha (6) completes, beta fails during page 2 (after 4 writes)
	cat.failErr = injectedErr

	reg := newFakeRegistry(
		assignslots.ProviderState{Provider: "alpha", Backfilled: false},
		assignslots.ProviderState{Provider: "beta", Backfilled: false},
	)
	cps := newFakeCheckpointer()
	clk := fakeClock{now: time.Date(2026, 11, 20, 1, 0, 0, 0, time.UTC)}

	// Workers=1 so alpha runs deterministically before beta.
	uc, err := assignslots.New(assigner, cat, cat, reg, cps, assignslots.NoopImmediateDispatcher{}, clk, discardLogger(), assignslots.Config{Workers: 1})
	if err != nil {
		t.Fatalf("assignslots.New: %v", err)
	}

	_, err = uc.Run(context.Background(), "job-resumable")
	if !errors.Is(err, injectedErr) {
		t.Fatalf("interrupted Run() error = %v, want %v", err, injectedErr)
	}

	// Verify alpha is marked DONE and beta is PENDING with a non-empty cursor.
	alphaCP, ok, _ := cps.Load(context.Background(), "job-resumable", "alpha")
	if !ok || alphaCP.Status != assignslots.CheckpointDone {
		t.Fatalf("alpha checkpoint = (%+v, %t), want DONE", alphaCP, ok)
	}
	betaCP, ok, _ := cps.Load(context.Background(), "job-resumable", "beta")
	if !ok || betaCP.Status != assignslots.CheckpointPending || betaCP.Cursor == "" || betaCP.Rank == 0 {
		t.Fatalf("beta checkpoint = (%+v, %t), want PENDING with non-empty cursor and rank > 0", betaCP, ok)
	}

	// Clear injected fault and resume same jobID.
	cat.failAfterSet = 0
	stats2, err := uc.Run(context.Background(), "job-resumable")
	if err != nil {
		t.Fatalf("resumed Run() failed: %v", err)
	}
	if stats2.ProvidersSkipped != 1 {
		t.Errorf("resumed ProvidersSkipped = %d, want 1 (alpha already DONE)", stats2.ProvidersSkipped)
	}

	// Every record of alpha and beta must match exact deterministic offset+rank assignment.
	for _, provider := range []string{"alpha", "beta"} {
		items := cat.snapshot(provider)
		k := len(items)
		for rank, it := range items {
			want, err := assigner.Assign(provider, it.ID, rank, k)
			if err != nil {
				t.Fatalf("Assign: %v", err)
			}
			if it.Slot != want.Slot || it.Shard != want.Shard {
				t.Errorf("%s[%d] = (%d,%d), want (%d,%d)", provider, rank, it.Slot, it.Shard, want.Slot, want.Shard)
			}
		}
	}
}

// T018b: Backfill persists provider count and 1440-slot histogram in coordination registry.
func TestUseCase_PersistsProviderHistogramInRegistry(t *testing.T) {
	t.Parallel()

	assigner, err := domain.NewSlotAssigner(4)
	if err != nil {
		t.Fatalf("NewSlotAssigner: %v", err)
	}

	cat := newFakeCatalog(100)
	cat.addProvider("telecom", 2500)

	reg := newFakeRegistry(assignslots.ProviderState{Provider: "telecom", NewSinceBackfill: 2500, Backfilled: false})
	cps := newFakeCheckpointer()
	ts := time.Date(2026, 11, 20, 6, 30, 0, 0, time.UTC)

	uc, err := assignslots.New(assigner, cat, cat, reg, cps, assignslots.NoopImmediateDispatcher{}, fakeClock{now: ts}, discardLogger(), assignslots.Config{Workers: 2})
	if err != nil {
		t.Fatalf("assignslots.New: %v", err)
	}

	if _, err := uc.Run(context.Background(), "job-hist"); err != nil {
		t.Fatalf("Run(): %v", err)
	}

	saved := reg.get("telecom")
	if !saved.Backfilled {
		t.Error("saved.Backfilled = false, want true")
	}
	if saved.Count != 2500 {
		t.Errorf("saved.Count = %d, want 2500", saved.Count)
	}
	if saved.NewSinceBackfill != 0 {
		t.Errorf("saved.NewSinceBackfill = %d, want 0", saved.NewSinceBackfill)
	}
	if !saved.UpdatedAt.Equal(ts) {
		t.Errorf("saved.UpdatedAt = %v, want %v", saved.UpdatedAt, ts)
	}

	// Verify saved histogram matches the exact slot counts of telecom's catalog records.
	var wantHist [domain.SlotsPerDay]int
	for _, it := range cat.snapshot("telecom") {
		wantHist[it.Slot]++
	}
	if saved.Histogram != wantHist {
		t.Error("persisted Histogram does not match assigned slot distribution")
	}

	// Verify persisted histogram can immediately drive AssignIncremental into a minimum-occupancy slot.
	next, err := assigner.AssignIncremental("telecom", "telecom-new-1", saved.Histogram[:])
	if err != nil {
		t.Fatalf("AssignIncremental: %v", err)
	}
	minOcc := slices.Min(saved.Histogram[:])
	if saved.Histogram[next.Slot] != minOcc {
		t.Errorf("AssignIncremental picked slot %d with count %d, want min %d", next.Slot, saved.Histogram[next.Slot], minOcc)
	}
}

func TestUseCase_ValidationDefaultsAndEmptyProvider(t *testing.T) {
	t.Parallel()

	assigner, _ := domain.NewSlotAssigner(4)
	cat := newFakeCatalog(5)
	reg := newFakeRegistry(assignslots.ProviderState{Provider: "empty-prov", Backfilled: false})
	cps := newFakeCheckpointer()
	clk := fakeClock{now: time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)}

	if _, err := assignslots.New(nil, cat, cat, reg, cps, nil, clk, nil, assignslots.Config{}); !errors.Is(err, assignslots.ErrInvalidConfig) {
		t.Errorf("New(nil assigner) error = %v, want %v", err, assignslots.ErrInvalidConfig)
	}

	// Nil dispatcher & nil logger & zero workers/threshold/location use safe defaults.
	uc, err := assignslots.New(assigner, cat, cat, reg, cps, nil, clk, nil, assignslots.Config{WCULimit: 1000})
	if err != nil {
		t.Fatalf("New() unexpected error: %v", err)
	}

	if _, err := uc.Run(context.Background(), "   "); !errors.Is(err, assignslots.ErrInvalidConfig) {
		t.Errorf("Run(empty jobID) error = %v, want %v", err, assignslots.ErrInvalidConfig)
	}

	stats, err := uc.Run(context.Background(), "job-empty")
	if err != nil {
		t.Fatalf("Run(empty-prov): %v", err)
	}
	if stats.ProvidersRebalanced != 1 || stats.SlotsUpdated != 0 {
		t.Errorf("empty provider stats = %+v, want 1 rebalanced, 0 updated", stats)
	}
	if saved := reg.get("empty-prov"); !saved.Backfilled || saved.Count != 0 {
		t.Errorf("empty provider saved state = %+v, want Backfilled=true Count=0", saved)
	}
}

func TestUseCase_IncrementalResumeAndWCULimit(t *testing.T) {
	t.Parallel()

	assigner, _ := domain.NewSlotAssigner(4)
	cat := newFakeCatalog(2) // forces multi-page checkpoints in incremental mode
	var hist [domain.SlotsPerDay]int
	for i := range 2 {
		hist[i] = 1
		cat.items["incp"] = append(cat.items["incp"], domain.ProviderRequestItem{
			ID: fmt.Sprintf("incp-%03d", i), Provider: "incp", Version: 2, Slot: i, Shard: 0, Enabled: true,
		})
	}
	for i := 2; i < 6; i++ {
		cat.items["incp"] = append(cat.items["incp"], domain.ProviderRequestItem{
			ID: fmt.Sprintf("incp-%03d", i), Provider: "incp", Version: 1, Slot: -1, Shard: -1, Enabled: true,
		})
	}

	injectedErr := errors.New("wcu throttle")
	cat.failAfterSet = 2
	cat.failErr = injectedErr

	reg := newFakeRegistry(assignslots.ProviderState{
		Provider:         "incp",
		Count:            2,
		Histogram:        hist,
		NewSinceBackfill: 4,
		Backfilled:       true,
	})
	cps := newFakeCheckpointer()
	clk := fakeClock{now: time.Date(2026, 11, 20, 2, 0, 0, 0, time.UTC)}

	uc, err := assignslots.New(assigner, cat, cat, reg, cps, assignslots.NoopImmediateDispatcher{}, clk, discardLogger(), assignslots.Config{
		Workers:  1,
		WCULimit: 10_000,
	})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}

	if _, err := uc.Run(context.Background(), "job-inc-resume"); !errors.Is(err, injectedErr) {
		t.Fatalf("first Run() error = %v, want %v", err, injectedErr)
	}

	// Resume after transient failure.
	cat.failAfterSet = 0
	stats, err := uc.Run(context.Background(), "job-inc-resume")
	if err != nil {
		t.Fatalf("resumed Run() failed: %v", err)
	}
	if stats.ProvidersIncrement != 1 {
		t.Errorf("ProvidersIncrement = %d, want 1", stats.ProvidersIncrement)
	}

	saved := reg.get("incp")
	if saved.Count != 6 || saved.NewSinceBackfill != 0 {
		t.Errorf("saved state = (Count=%d New=%d), want (6, 0)", saved.Count, saved.NewSinceBackfill)
	}
	for _, it := range cat.snapshot("incp") {
		if it.Slot < 0 || it.Shard < 0 {
			t.Errorf("item left unslotted after incremental resume: %+v", it)
		}
	}

	// Verify canceled context stops rate limiter wait cleanly.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cat.addProvider("cancel-prov", 3)
	reg2 := newFakeRegistry(assignslots.ProviderState{Provider: "cancel-prov", Backfilled: false})
	uc2, _ := assignslots.New(assigner, cat, cat, reg2, newFakeCheckpointer(), nil, clk, discardLogger(), assignslots.Config{WCULimit: 1})
	if _, err := uc2.Run(ctx, "job-cancel"); !errors.Is(err, context.Canceled) {
		t.Errorf("Run(canceled ctx) error = %v, want context.Canceled", err)
	}
}
