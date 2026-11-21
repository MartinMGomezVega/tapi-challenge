package domain_test

import (
	"fmt"
	"math/rand/v2"
	"runtime"
	"strconv"
	"testing"

	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

var (
	benchSlotSink      domain.SlotAssignment
	benchBackfillSink  int
	benchHistogramSink [domain.SlotsPerDay]int
)

// T016: Benchmark assigning 1M pre-ranked records across 50 Zipf providers (pure Assign hot path).
func BenchmarkAssign1M(b *testing.B) {
	const (
		totalRecords = 1_000_000
		numProviders = 50
		shards       = 4
	)

	assigner, err := domain.NewSlotAssigner(shards)
	if err != nil {
		b.Fatalf("NewSlotAssigner(%d): %v", shards, err)
	}

	counts := zipfProviderCounts(totalRecords, numProviders)
	providers := make([]string, numProviders)
	ids := make([]string, totalRecords)
	idx := 0
	for pIdx, k := range counts {
		p := fmt.Sprintf("provider-%03d", pIdx)
		providers[pIdx] = p
		for r := range k {
			ids[idx] = p + "-" + strconv.Itoa(r)
			idx++
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		var last domain.SlotAssignment
		pos := 0
		for pIdx, k := range counts {
			p := providers[pIdx]
			for r := range k {
				got, err := assigner.Assign(p, ids[pos], r, k)
				if err != nil {
					b.Fatalf("Assign(): %v", err)
				}
				last = got
				pos++
			}
		}
		benchSlotSink = last
	}
}

type rawCatalogRecord struct {
	provider string
	id       string
}

// BenchmarkBackfillAssign1M measures the superseded O(N) in-memory grouping + sorting baseline
// (~162 MiB/1M -> ~16 GB at 100M), retained as a regression comparison against gsi_provider streaming.
func BenchmarkBackfillAssign1M(b *testing.B) {
	const (
		totalRecords = 1_000_000
		numProviders = 50
		shards       = 4
	)

	assigner, err := domain.NewSlotAssigner(shards)
	if err != nil {
		b.Fatalf("NewSlotAssigner(%d): %v", shards, err)
	}

	counts := zipfProviderCounts(totalRecords, numProviders)
	records := make([]rawCatalogRecord, 0, totalRecords)
	for pIdx, k := range counts {
		p := fmt.Sprintf("provider-%03d", pIdx)
		for r := range k {
			records = append(records, rawCatalogRecord{
				provider: p,
				id:       p + "-" + strconv.Itoa(r),
			})
		}
	}

	// Deterministic shuffle to simulate unsorted DynamoDB scan order.
	rng := rand.New(rand.NewPCG(42, 1337))
	rng.Shuffle(len(records), func(i, j int) {
		records[i], records[j] = records[j], records[i]
	})

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		byProvider := make(map[string][]string, numProviders)
		for _, rec := range records {
			byProvider[rec.provider] = append(byProvider[rec.provider], rec.id)
		}

		totalAssigned := 0
		for provider, ids := range byProvider {
			assigned, err := assigner.AssignProvider(provider, ids)
			if err != nil {
				b.Fatalf("AssignProvider(%s): %v", provider, err)
			}
			totalAssigned += len(assigned)
		}
		benchBackfillSink = totalAssigned
	}
}

const (
	streamingKMax     = 20_000
	streamingPageSize = 1_000
)

// runStreamingBackfill simulates the FR-004b gsi_provider streaming backfill:
// one provider at a time, IDs streamed in DynamoDB SK ascending order (rank = stream position,
// no in-memory sort or global catalog map), holding only one page + 1440-slot histogram in memory.
func runStreamingBackfill(
	tb testing.TB,
	assigner *domain.SlotAssigner,
	numProviders int,
	sampleHeap bool,
) (totalAssigned int, peakLiveHeap uint64) {
	tb.Helper()

	page := make([]string, streamingPageSize)
	idBuf := make([]byte, 0, 32)

	for p := range numProviders {
		provider := "provider-" + strconv.Itoa(p)
		var hist [domain.SlotsPerDay]int

		for baseRank := 0; baseRank < streamingKMax; baseRank += streamingPageSize {
			n := min(streamingPageSize, streamingKMax-baseRank)
			// Simulate DynamoDB gsi_provider page already ordered by SK=id.
			for i := range n {
				idBuf = append(idBuf[:0], "req-"...)
				idBuf = strconv.AppendInt(idBuf, int64(baseRank+i), 10)
				page[i] = string(idBuf)
			}
			for i := range n {
				got, err := assigner.Assign(provider, page[i], baseRank+i, streamingKMax)
				if err != nil {
					tb.Fatalf("Assign(%s, rank=%d): %v", provider, baseRank+i, err)
				}
				hist[got.Slot]++
				totalAssigned++
			}
		}

		benchHistogramSink = hist
		if sampleHeap && (p%25 == 0 || p == numProviders-1) {
			runtime.GC()
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			if ms.HeapAlloc > peakLiveHeap {
				peakLiveHeap = ms.HeapAlloc
			}
		}
	}

	return totalAssigned, peakLiveHeap
}

// BenchmarkStreamingBackfill1M benchmarks FR-004b gsi_provider streaming backfill for 1M records
// (50 providers x 20k fixed k_max).
func BenchmarkStreamingBackfill1M(b *testing.B) {
	assigner, err := domain.NewSlotAssigner(4)
	if err != nil {
		b.Fatalf("NewSlotAssigner(4): %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		total, _ := runStreamingBackfill(b, assigner, 50, false)
		benchBackfillSink = total
	}
}

// BenchmarkStreamingBackfill4M benchmarks FR-004b gsi_provider streaming backfill for 4M records
// (200 providers x 20k fixed k_max) to verify linear CPU and constant live heap vs 1M.
func BenchmarkStreamingBackfill4M(b *testing.B) {
	assigner, err := domain.NewSlotAssigner(4)
	if err != nil {
		b.Fatalf("NewSlotAssigner(4): %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		total, _ := runStreamingBackfill(b, assigner, 200, false)
		benchBackfillSink = total
	}
}

// TestStreamingBackfill_MemoryInvariantWithN verifies Constitution §V: live heap memory
// during gsi_provider streaming backfill stays bounded by O(k_max) / O(page_size) and
// does NOT grow proportionally with N (1M vs 4M with fixed k_max = 20,000).
// Intentionally not t.Parallel() so runtime.ReadMemStats measures isolated heap usage.
func TestStreamingBackfill_MemoryInvariantWithN(t *testing.T) {
	providers1x := 50  // 1,000,000 records
	providers4x := 200 // 4,000,000 records
	if testing.Short() {
		providers1x = 10 // 200,000 records
		providers4x = 40 // 800,000 records
	}

	assigner, err := domain.NewSlotAssigner(4)
	if err != nil {
		t.Fatalf("NewSlotAssigner(4): %v", err)
	}

	total1x, peak1x := runStreamingBackfill(t, assigner, providers1x, true)
	total4x, peak4x := runStreamingBackfill(t, assigner, providers4x, true)

	if total1x != providers1x*streamingKMax || total4x != providers4x*streamingKMax {
		t.Fatalf("unexpected totals: 1x=%d 4x=%d", total1x, total4x)
	}

	t.Logf("streaming backfill live heap (k_max=%d): %d records peak=%d KiB | %d records peak=%d KiB",
		streamingKMax, total1x, peak1x/1024, total4x, peak4x/1024)

	// If memory scaled with O(N), 4x records would grow peak live heap by ~4x (hundreds of MiB).
	// With streaming gsi_provider, peak live heap must stay within a small constant margin (< 4 MiB delta).
	const maxAllowedDeltaBytes = 4 * 1024 * 1024
	if peak4x > peak1x+maxAllowedDeltaBytes {
		t.Fatalf("live heap grew with N: 1x=%d bytes (%d KiB), 4x=%d bytes (%d KiB), delta=%d > %d",
			peak1x, peak1x/1024, peak4x, peak4x/1024, peak4x-peak1x, maxAllowedDeltaBytes)
	}
}
