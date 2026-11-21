package domain_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

var benchSlotSink domain.SlotAssignment

// T016: Benchmark assigning 1M records across 50 providers (target < 2 s).
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
