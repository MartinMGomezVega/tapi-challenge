package domain_test

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"testing"

	"pgregory.net/rapid"

	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

// T007: Assign is deterministic (FR-001, US1 Scenario 2).
func TestSlotAssigner_Deterministic(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		shards := rapid.IntRange(1, 64).Draw(t, "shards")
		provider := rapid.StringMatching(`[a-z][a-z0-9_-]{1,20}`).Draw(t, "provider")
		id := rapid.StringMatching(`[a-z0-9-]{4,24}`).Draw(t, "id")
		total := rapid.IntRange(1, 500_000).Draw(t, "total")
		rank := rapid.IntRange(0, total-1).Draw(t, "rank")

		assigner, err := domain.NewSlotAssigner(shards)
		if err != nil {
			t.Fatalf("NewSlotAssigner(%d): %v", shards, err)
		}

		first, err := assigner.Assign(provider, id, rank, total)
		if err != nil {
			t.Fatalf("first Assign() failed: %v", err)
		}
		second, err := assigner.Assign(provider, id, rank, total)
		if err != nil {
			t.Fatalf("second Assign() failed: %v", err)
		}
		if first != second {
			t.Fatalf("Assign not deterministic: first=%+v second=%+v", first, second)
		}
	})
}

func TestSlotAssigner_AssignProviderOrderIndependent(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		shards := rapid.IntRange(1, 32).Draw(t, "shards")
		provider := rapid.StringMatching(`[a-z]{3,12}`).Draw(t, "provider")
		n := rapid.IntRange(1, 200).Draw(t, "n")

		ids := make([]string, n)
		for i := range n {
			ids[i] = fmt.Sprintf("req-%06d", i)
		}
		reversed := slices.Clone(ids)
		slices.Reverse(reversed)

		assigner, err := domain.NewSlotAssigner(shards)
		if err != nil {
			t.Fatalf("NewSlotAssigner(%d): %v", shards, err)
		}

		a1, err := assigner.AssignProvider(provider, ids)
		if err != nil {
			t.Fatalf("AssignProvider(ids): %v", err)
		}
		a2, err := assigner.AssignProvider(provider, reversed)
		if err != nil {
			t.Fatalf("AssignProvider(reversed): %v", err)
		}
		if len(a1) != len(a2) {
			t.Fatalf("assignment count mismatch: %d vs %d", len(a1), len(a2))
		}
		for _, id := range ids {
			if a1[id] != a2[id] {
				t.Fatalf("order changed assignment for %s: %+v vs %+v", id, a1[id], a2[id])
			}
		}
	})
}

// T008: slot in [0, 1440) and shard in [0, S) (FR-001, FR-003, US1 Scenario 1).
func TestSlotAssigner_Bounds(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		shards := rapid.IntRange(1, 128).Draw(t, "shards")
		provider := rapid.StringMatching(`[a-z][a-z0-9_-]{1,24}`).Draw(t, "provider")
		id := rapid.StringMatching(`[a-zA-Z0-9_-]{1,36}`).Draw(t, "id")
		total := rapid.IntRange(1, 1_000_000).Draw(t, "total")
		rank := rapid.IntRange(0, total-1).Draw(t, "rank")

		assigner, err := domain.NewSlotAssigner(shards)
		if err != nil {
			t.Fatalf("NewSlotAssigner(%d): %v", shards, err)
		}

		got, err := assigner.Assign(provider, id, rank, total)
		if err != nil {
			t.Fatalf("Assign(): %v", err)
		}
		if got.Slot < 0 || got.Slot >= domain.SlotsPerDay {
			t.Fatalf("slot %d out of range [0, %d)", got.Slot, domain.SlotsPerDay)
		}
		if got.Shard < 0 || got.Shard >= shards {
			t.Fatalf("shard %d out of range [0, %d)", got.Shard, shards)
		}
	})
}

func TestSlotAssigner_InvalidInputs(t *testing.T) {
	t.Parallel()

	if _, err := domain.NewSlotAssigner(0); !errors.Is(err, domain.ErrInvalidShardCount) {
		t.Errorf("NewSlotAssigner(0) err = %v, want %v", err, domain.ErrInvalidShardCount)
	}
	if _, err := domain.NewSlotAssigner(-2); !errors.Is(err, domain.ErrInvalidShardCount) {
		t.Errorf("NewSlotAssigner(-2) err = %v, want %v", err, domain.ErrInvalidShardCount)
	}

	assigner, err := domain.NewSlotAssigner(4)
	if err != nil {
		t.Fatalf("NewSlotAssigner(4): %v", err)
	}

	cases := []struct {
		name     string
		provider string
		id       string
		rank     int
		total    int
		wantErr  error
	}{
		{name: "empty provider", provider: "", id: "id-1", rank: 0, total: 1, wantErr: domain.ErrInvalidProvider},
		{name: "provider with hash", provider: "a#b", id: "id-1", rank: 0, total: 1, wantErr: domain.ErrInvalidProvider},
		{name: "empty id", provider: "edenor", id: "", rank: 0, total: 1, wantErr: domain.ErrEmptyID},
		{name: "negative rank", provider: "edenor", id: "id-1", rank: -1, total: 10, wantErr: domain.ErrInvalidRank},
		{name: "rank equals total", provider: "edenor", id: "id-1", rank: 10, total: 10, wantErr: domain.ErrInvalidRank},
		{name: "zero total", provider: "edenor", id: "id-1", rank: 0, total: 0, wantErr: domain.ErrInvalidRank},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := assigner.Assign(tc.provider, tc.id, tc.rank, tc.total)
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("Assign() error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

// T009: Global uniformity <= 1.10 x average with 1M records (FR-002, SC-002, US1 Scenario 3).
func TestSlotAssigner_GlobalUniformity1M(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping 1M uniformity test in -short mode; run make test-full")
	}

	const (
		totalRecords = 1_000_000
		numProviders = 50
		shards       = 4
	)

	scenarios := []struct {
		name   string
		counts []int
	}{
		{
			name:   "uniform 50 providers (20k each)",
			counts: uniformProviderCounts(totalRecords, numProviders),
		},
		{
			name:   "skewed Zipf-like 50 providers summing to 1M",
			counts: zipfProviderCounts(totalRecords, numProviders),
		},
	}

	assigner, err := domain.NewSlotAssigner(shards)
	if err != nil {
		t.Fatalf("NewSlotAssigner(%d): %v", shards, err)
	}

	avg := float64(totalRecords) / float64(domain.SlotsPerDay)
	maxAllowed := avg * 1.10

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()

			var globalHist [domain.SlotsPerDay]int
			var shardHist [shards]int
			maxProviderExcess := 0

			for pIdx, k := range sc.counts {
				provider := fmt.Sprintf("provider-%03d", pIdx)
				var provHist [domain.SlotsPerDay]int

				for r := range k {
					id := provider + "-" + strconv.Itoa(r)
					got, err := assigner.Assign(provider, id, r, k)
					if err != nil {
						t.Fatalf("Assign(%s, %s, %d, %d): %v", provider, id, r, k, err)
					}
					globalHist[got.Slot]++
					provHist[got.Slot]++
					shardHist[got.Shard]++
				}

				provLimit := (k+domain.SlotsPerDay-1)/domain.SlotsPerDay + 1
				provMax := slices.Max(provHist[:])
				if provMax > provLimit {
					t.Fatalf("provider %s (k=%d) max/min = %d > limit %d", provider, k, provMax, provLimit)
				}
				provCeil := (k + domain.SlotsPerDay - 1) / domain.SlotsPerDay
				if excess := provMax - provCeil; excess > maxProviderExcess {
					maxProviderExcess = excess
				}
			}

			globalMin := slices.Min(globalHist[:])
			globalMax := slices.Max(globalHist[:])
			maxDevPct := (math.Max(float64(globalMax)-avg, avg-float64(globalMin)) / avg) * 100

			t.Logf("1M uniformity (%s): avg=%.2f min=%d max=%d maxDev=%.3f%% maxProviderExcessOverCeil=%d shards=%v",
				sc.name, avg, globalMin, globalMax, maxDevPct, maxProviderExcess, shardHist)

			if float64(globalMax) > maxAllowed {
				t.Errorf("global max per minute = %d exceeds 1.10*avg (%.2f)", globalMax, maxAllowed)
			}
		})
	}
}

func TestSlotAssigner_GlobalUniformityProperty(t *testing.T) {
	t.Parallel()

	assigner, err := domain.NewSlotAssigner(4)
	if err != nil {
		t.Fatalf("NewSlotAssigner(4): %v", err)
	}

	// Universal property across 10..60 providers: each provider contributes at most +1
	// above floor(k/1440) to any minute, so global spread (max - min) <= numProviders.
	rapid.Check(t, func(t *rapid.T) {
		numProviders := rapid.IntRange(10, 60).Draw(t, "numProviders")
		var globalHist [domain.SlotsPerDay]int
		total := 0

		for p := range numProviders {
			k := rapid.IntRange(100, 3_000).Draw(t, fmt.Sprintf("k_%d", p))
			provider := "prov-" + strconv.Itoa(p)
			total += k
			for r := range k {
				got, err := assigner.Assign(provider, "id", r, k)
				if err != nil {
					t.Fatalf("Assign(): %v", err)
				}
				globalHist[got.Slot]++
			}
		}

		minMinute := slices.Min(globalHist[:])
		maxMinute := slices.Max(globalHist[:])
		if spread := maxMinute - minMinute; spread > numProviders {
			t.Fatalf("total=%d numProviders=%d spread=%d (min=%d max=%d) exceeds numProviders",
				total, numProviders, spread, minMinute, maxMinute)
		}

		avg := float64(total) / float64(domain.SlotsPerDay)
		if avg >= 10*float64(numProviders) && float64(maxMinute) > avg*1.10 {
			t.Fatalf("total=%d avg=%.2f maxMinute=%d exceeds 1.10*avg", total, avg, maxMinute)
		}
	})
}

func TestSlotAssigner_GlobalUniformityScaleProperty1M(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping 1M scale property test in -short mode; run make test-full")
	}

	assigner, err := domain.NewSlotAssigner(4)
	if err != nil {
		t.Fatalf("NewSlotAssigner(4): %v", err)
	}

	const totalRecords = 1_000_000
	avg := float64(totalRecords) / float64(domain.SlotsPerDay)

	// Random provider counts in [10, 60] with random weights scaled to 1M records must
	// satisfy SC-002 (max minute <= 1.10 * avg).
	rapid.Check(t, func(t *rapid.T) {
		numProviders := rapid.IntRange(10, 60).Draw(t, "numProviders")
		weights := make([]int, numProviders)
		weightSum := 0
		for p := range numProviders {
			w := rapid.IntRange(1, 100).Draw(t, fmt.Sprintf("w_%d", p))
			weights[p] = w
			weightSum += w
		}

		counts := make([]int, numProviders)
		assigned := 0
		for p, w := range weights {
			c := max(1, (totalRecords*w)/weightSum)
			counts[p] = c
			assigned += c
		}
		counts[0] += totalRecords - assigned

		var globalHist [domain.SlotsPerDay]int
		for p, k := range counts {
			provider := "prov-" + strconv.Itoa(p)
			for r := range k {
				got, err := assigner.Assign(provider, "id", r, k)
				if err != nil {
					t.Fatalf("Assign(): %v", err)
				}
				globalHist[got.Slot]++
			}
		}

		maxMinute := slices.Max(globalHist[:])
		if float64(maxMinute) > avg*1.10 {
			t.Fatalf("numProviders=%d avg=%.2f maxMinute=%d exceeds 1.10*avg", numProviders, avg, maxMinute)
		}
	})
}

// T010: Per-provider uniformity (<= ceil(k/1440)+1 for k >= 1440, equispaced for k < 1440) (FR-002, US1 Scenario 4).
func TestSlotAssigner_PerProviderUniformity(t *testing.T) {
	t.Parallel()

	assigner, err := domain.NewSlotAssigner(4)
	if err != nil {
		t.Fatalf("NewSlotAssigner(4): %v", err)
	}

	t.Run("k >= 1440 bounded by ceil(k/1440)+1", func(t *testing.T) {
		t.Parallel()

		maxK := 500_000
		if testing.Short() {
			maxK = 10_000
		}

		rapid.Check(t, func(t *rapid.T) {
			provider := rapid.StringMatching(`[a-z][a-z0-9]{2,12}`).Draw(t, "provider")
			k := rapid.IntRange(domain.SlotsPerDay, maxK).Draw(t, "k")

			var hist [domain.SlotsPerDay]int
			for r := range k {
				got, err := assigner.Assign(provider, "req-id", r, k)
				if err != nil {
					t.Fatalf("Assign(): %v", err)
				}
				hist[got.Slot]++
			}

			limit := (k+domain.SlotsPerDay-1)/domain.SlotsPerDay + 1
			for slot, count := range hist {
				if count > limit {
					t.Fatalf("provider=%s k=%d slot=%d count=%d > limit=%d", provider, k, slot, count, limit)
				}
			}
		})
	})

	t.Run("k < 1440 equispaced across the day without collisions", func(t *testing.T) {
		t.Parallel()

		rapid.Check(t, func(t *rapid.T) {
			provider := rapid.StringMatching(`[a-z][a-z0-9]{2,12}`).Draw(t, "provider")
			k := rapid.IntRange(2, domain.SlotsPerDay-1).Draw(t, "k")

			slots := make([]int, k)
			seen := make(map[int]struct{}, k)
			for r := range k {
				got, err := assigner.Assign(provider, fmt.Sprintf("r-%d", r), r, k)
				if err != nil {
					t.Fatalf("Assign(): %v", err)
				}
				if _, dup := seen[got.Slot]; dup {
					t.Fatalf("provider=%s k=%d had duplicate slot %d", provider, k, got.Slot)
				}
				seen[got.Slot] = struct{}{}
				slots[r] = got.Slot
			}

			slices.Sort(slots)
			minGap := domain.SlotsPerDay / k
			maxGap := (domain.SlotsPerDay + k - 1) / k

			for i := range k {
				var gap int
				if i+1 < k {
					gap = slots[i+1] - slots[i]
				} else {
					gap = (domain.SlotsPerDay - slots[i]) + slots[0]
				}
				if gap < minGap || gap > maxGap {
					t.Fatalf("provider=%s k=%d non-equispaced circular gap=%d (expected in [%d, %d])",
						provider, k, gap, minGap, maxGap)
				}
			}
		})
	})
}

func uniformProviderCounts(total, n int) []int {
	counts := make([]int, n)
	base := total / n
	rem := total % n
	for i := range n {
		counts[i] = base
		if i < rem {
			counts[i]++
		}
	}
	return counts
}

func zipfProviderCounts(total, n int) []int {
	weights := make([]float64, n)
	var sum float64
	for i := range n {
		w := 1.0 / math.Pow(float64(i+1), 1.05)
		weights[i] = w
		sum += w
	}

	counts := make([]int, n)
	assigned := 0
	for i := range n {
		c := max(1, int(math.Round(float64(total)*weights[i]/sum)))
		counts[i] = c
		assigned += c
	}
	// Adjust largest provider so total matches exact target.
	counts[0] += total - assigned
	return counts
}

// T012: Incremental assignment selects the minimum-occupancy minute (plan.md, FR-001, FR-002).
func TestSlotAssigner_AssignIncremental_PicksUniqueMinimum(t *testing.T) {
	t.Parallel()

	assigner, err := domain.NewSlotAssigner(4)
	if err != nil {
		t.Fatalf("NewSlotAssigner(4): %v", err)
	}

	hist := make([]int, domain.SlotsPerDay)
	for i := range hist {
		hist[i] = 10
	}
	hist[742] = 9
	snapshot := slices.Clone(hist)

	got, err := assigner.AssignIncremental("edenor", "req-new-1", hist)
	if err != nil {
		t.Fatalf("AssignIncremental(): %v", err)
	}
	if got.Slot != 742 {
		t.Errorf("Slot = %d, want 742 (unique minimum)", got.Slot)
	}
	if got.Shard < 0 || got.Shard >= 4 {
		t.Errorf("Shard = %d, want in [0, 4)", got.Shard)
	}
	if !slices.Equal(hist, snapshot) {
		t.Error("AssignIncremental mutated input histogram")
	}
}

func TestSlotAssigner_AssignIncremental_TieBreakDeterministic(t *testing.T) {
	t.Parallel()

	assigner, err := domain.NewSlotAssigner(8)
	if err != nil {
		t.Fatalf("NewSlotAssigner(8): %v", err)
	}

	hist := make([]int, domain.SlotsPerDay)
	for i := range hist {
		hist[i] = 5
	}
	tied := []int{100, 500, 1200}
	for _, s := range tied {
		hist[s] = 2
	}

	first, err := assigner.AssignIncremental("metrogas", "req-tie-99", hist)
	if err != nil {
		t.Fatalf("first AssignIncremental(): %v", err)
	}
	if !slices.Contains(tied, first.Slot) {
		t.Fatalf("Slot = %d, want one of tied minimums %v", first.Slot, tied)
	}

	second, err := assigner.AssignIncremental("metrogas", "req-tie-99", hist)
	if err != nil {
		t.Fatalf("second AssignIncremental(): %v", err)
	}
	if first != second {
		t.Errorf("tie-break not deterministic: first=%+v second=%+v", first, second)
	}
}

func TestSlotAssigner_AssignIncremental_SequentialUniformity(t *testing.T) {
	t.Parallel()

	assigner, err := domain.NewSlotAssigner(4)
	if err != nil {
		t.Fatalf("NewSlotAssigner(4): %v", err)
	}

	hist := make([]int, domain.SlotsPerDay)
	for i := range 3000 {
		got, err := assigner.AssignIncremental("aysa", "req-"+strconv.Itoa(i), hist)
		if err != nil {
			t.Fatalf("AssignIncremental(%d): %v", i, err)
		}
		hist[got.Slot]++

		if i == domain.SlotsPerDay-1 {
			if slices.Min(hist) != 1 || slices.Max(hist) != 1 {
				t.Fatalf("after 1440 inserts expected every slot=1, got min=%d max=%d",
					slices.Min(hist), slices.Max(hist))
			}
		}
	}

	if diff := slices.Max(hist) - slices.Min(hist); diff > 1 {
		t.Errorf("after 3000 inserts max-min = %d > 1 (min=%d max=%d)",
			diff, slices.Min(hist), slices.Max(hist))
	}
}

func TestSlotAssigner_AssignIncremental_Property(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		shards := rapid.IntRange(1, 32).Draw(t, "shards")
		provider := rapid.StringMatching(`[a-z][a-z0-9]{2,10}`).Draw(t, "provider")
		id := rapid.StringMatching(`[a-z0-9-]{3,16}`).Draw(t, "id")
		base := rapid.IntRange(0, 500).Draw(t, "base")
		numLows := rapid.IntRange(1, 20).Draw(t, "numLows")

		hist := make([]int, domain.SlotsPerDay)
		for i := range hist {
			hist[i] = base + 5
		}
		minVal := base
		for j := range numLows {
			slot := rapid.IntRange(0, domain.SlotsPerDay-1).Draw(t, fmt.Sprintf("low_%d", j))
			hist[slot] = minVal
		}

		assigner, err := domain.NewSlotAssigner(shards)
		if err != nil {
			t.Fatalf("NewSlotAssigner(%d): %v", shards, err)
		}

		got, err := assigner.AssignIncremental(provider, id, hist)
		if err != nil {
			t.Fatalf("AssignIncremental(): %v", err)
		}
		if got.Slot < 0 || got.Slot >= domain.SlotsPerDay {
			t.Fatalf("slot %d out of bounds", got.Slot)
		}
		if got.Shard < 0 || got.Shard >= shards {
			t.Fatalf("shard %d out of bounds", got.Shard)
		}
		if hist[got.Slot] != minVal {
			t.Fatalf("chosen slot %d has occupancy %d, want minimum %d", got.Slot, hist[got.Slot], minVal)
		}
	})
}

func TestSlotAssigner_AssignIncremental_InvalidInputs(t *testing.T) {
	t.Parallel()

	assigner, err := domain.NewSlotAssigner(4)
	if err != nil {
		t.Fatalf("NewSlotAssigner(4): %v", err)
	}

	validHist := make([]int, domain.SlotsPerDay)
	negHist := make([]int, domain.SlotsPerDay)
	negHist[10] = -1

	cases := []struct {
		name     string
		provider string
		id       string
		hist     []int
		wantErr  error
	}{
		{name: "nil histogram", provider: "edenor", id: "r1", hist: nil, wantErr: domain.ErrInvalidHistogram},
		{name: "short histogram", provider: "edenor", id: "r1", hist: make([]int, 1439), wantErr: domain.ErrInvalidHistogram},
		{name: "long histogram", provider: "edenor", id: "r1", hist: make([]int, 1441), wantErr: domain.ErrInvalidHistogram},
		{name: "negative count", provider: "edenor", id: "r1", hist: negHist, wantErr: domain.ErrInvalidHistogram},
		{name: "invalid provider", provider: "", id: "r1", hist: validHist, wantErr: domain.ErrInvalidProvider},
		{name: "empty id", provider: "edenor", id: "", hist: validHist, wantErr: domain.ErrEmptyID},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := assigner.AssignIncremental(tc.provider, tc.id, tc.hist)
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("AssignIncremental() error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}
