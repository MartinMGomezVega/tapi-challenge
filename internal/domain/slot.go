package domain

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

const (
	fnvOffset64 uint64 = 14695981039346656037
	fnvPrime64  uint64 = 1099511628211

	fnvOffset32 uint32 = 2166136261
	fnvPrime32  uint32 = 16777619
)

var (
	// ErrInvalidShardCount indicates shard count S < 1.
	ErrInvalidShardCount = errors.New("shard count must be >= 1")
	// ErrInvalidRank indicates rank/total bounds are invalid (requires 0 <= rank < total).
	ErrInvalidRank = errors.New("rank must satisfy 0 <= rank < total")
	// ErrInvalidHistogram indicates a provider slot histogram does not have 1440 non-negative entries.
	ErrInvalidHistogram = errors.New("histogram must have 1440 non-negative entries")
)

// SlotAssignment holds the computed (slot, shard) pair for a request.
type SlotAssignment struct {
	Slot  int
	Shard int
}

// SlotAssigner is a pure domain policy that deterministically assigns (slot, shard) pairs
// uniformly globally and per provider (ADR-004, FR-001..FR-003).
type SlotAssigner struct {
	shards uint32
}

// NewSlotAssigner creates a SlotAssigner configured for S shards (S >= 1).
func NewSlotAssigner(shards int) (*SlotAssigner, error) {
	if shards < 1 || shards > 1<<30 {
		return nil, fmt.Errorf("%w: got %d", ErrInvalidShardCount, shards)
	}
	return &SlotAssigner{shards: uint32(shards)}, nil // #nosec G115 -- bounds checked above
}

// Shards returns the configured number of shards S.
func (a *SlotAssigner) Shards() int {
	return int(a.shards)
}

// Assign computes a deterministic (slot, shard) for a request at position rank of total
// within provider:
//
//	offset(provider) = fnv64a(provider) mod 1440
//	slot             = (offset + floor(rank * 1440 / total)) mod 1440
//	shard            = fnv32a(id) mod S
func (a *SlotAssigner) Assign(provider, id string, rank, total int) (SlotAssignment, error) {
	if err := validateProvider(provider); err != nil {
		return SlotAssignment{}, err
	}
	if strings.TrimSpace(id) == "" {
		return SlotAssignment{}, ErrEmptyID
	}
	if total < 1 || rank < 0 || rank >= total {
		return SlotAssignment{}, fmt.Errorf("%w: rank=%d total=%d", ErrInvalidRank, rank, total)
	}

	offset := int(fnv64a(provider) % SlotsPerDay) // #nosec G115 -- result in [0, 1440)
	step := int((int64(rank) * SlotsPerDay) / int64(total))
	slot := (offset + step) % SlotsPerDay
	shard := int(fnv32a(id) % a.shards)

	return SlotAssignment{
		Slot:  slot,
		Shard: shard,
	}, nil
}

// AssignProvider deterministically assigns (slot, shard) to all request IDs of a provider,
// independent of the input slice order.
func (a *SlotAssigner) AssignProvider(provider string, ids []string) (map[string]SlotAssignment, error) {
	if err := validateProvider(provider); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return map[string]SlotAssignment{}, nil
	}

	sorted := slices.Clone(ids)
	slices.Sort(sorted)

	out := make(map[string]SlotAssignment, len(sorted))
	total := len(sorted)
	for rank, id := range sorted {
		assigned, err := a.Assign(provider, id, rank, total)
		if err != nil {
			return nil, err
		}
		out[id] = assigned
	}
	return out, nil
}

// AssignIncremental selects the least-occupied slot in the provider's 1440-minute histogram,
// breaking ties deterministically by hashing (provider, id). Does not mutate counts.
func (a *SlotAssigner) AssignIncremental(provider, id string, counts []int) (SlotAssignment, error) {
	if err := validateProvider(provider); err != nil {
		return SlotAssignment{}, err
	}
	if strings.TrimSpace(id) == "" {
		return SlotAssignment{}, ErrEmptyID
	}
	if len(counts) != SlotsPerDay {
		return SlotAssignment{}, fmt.Errorf("%w: got length %d", ErrInvalidHistogram, len(counts))
	}

	var minSlots [SlotsPerDay]int
	minCount := counts[0]
	nMin := 0

	for slot, c := range counts {
		if c < 0 {
			return SlotAssignment{}, fmt.Errorf("%w: negative count %d at slot %d", ErrInvalidHistogram, c, slot)
		}
		switch {
		case c < minCount:
			minCount = c
			minSlots[0] = slot
			nMin = 1
		case c == minCount:
			minSlots[nMin] = slot
			nMin++
		}
	}

	idx := int(fnv64aPair(provider, id) % uint64(nMin)) // #nosec G115 -- nMin in [1, 1440]
	return SlotAssignment{
		Slot:  minSlots[idx],
		Shard: int(fnv32a(id) % a.shards),
	}, nil
}

func fnv64a(s string) uint64 {
	h := fnvOffset64
	for i := range len(s) {
		h ^= uint64(s[i])
		h *= fnvPrime64
	}
	return h
}

func fnv64aPair(a, b string) uint64 {
	h := fnvOffset64
	for i := range len(a) {
		h ^= uint64(a[i])
		h *= fnvPrime64
	}
	h ^= '#'
	h *= fnvPrime64
	for i := range len(b) {
		h ^= uint64(b[i])
		h *= fnvPrime64
	}
	return h
}

func fnv32a(s string) uint32 {
	h := fnvOffset32
	for i := range len(s) {
		h ^= uint32(s[i])
		h *= fnvPrime32
	}
	return h
}
