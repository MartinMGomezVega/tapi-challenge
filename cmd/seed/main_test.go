package main

import (
	"testing"
)

func TestZipfCounts(t *testing.T) {
	t.Parallel()

	counts, err := zipfCounts(1_000_000, 50, 1.05)
	if err != nil {
		t.Fatalf("zipfCounts(1M, 50, 1.05): %v", err)
	}
	if len(counts) != 50 {
		t.Fatalf("len(counts) = %d, want 50", len(counts))
	}

	sum := 0
	for i, c := range counts {
		if c < 1 {
			t.Errorf("provider %d has count %d < 1", i, c)
		}
		if i > 0 && c > counts[i-1] {
			t.Errorf("zipf counts not monotonically non-increasing at %d: %d > %d", i, c, counts[i-1])
		}
		sum += c
	}
	if sum != 1_000_000 {
		t.Errorf("sum(counts) = %d, want 1000000", sum)
	}

	if _, err := zipfCounts(10, 50, 1.05); err == nil {
		t.Error("expected error when providers > total")
	}
	if _, err := zipfCounts(100, 10, 0); err == nil {
		t.Error("expected error when exponent <= 0")
	}
}
