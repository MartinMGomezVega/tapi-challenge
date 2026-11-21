package domain_test

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

var (
	benchSlotSink     domain.SlotAssignment
	benchBackfillSink int
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

// BenchmarkBackfillAssign1M measures full backfill assignment for 1M unsorted records:
// grouping by provider + sorting IDs per provider for stable rank + computing (slot, shard).
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
