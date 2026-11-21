package domain

import "errors"

var (
	// ErrInvalidShardCount indicates shard count S < 1.
	ErrInvalidShardCount = errors.New("shard count must be >= 1")
	// ErrInvalidRank indicates rank/total bounds are invalid (requires 0 <= rank < total).
	ErrInvalidRank = errors.New("rank must satisfy 0 <= rank < total")
	// ErrInvalidHistogram indicates a provider slot histogram does not have 1440 non-negative entries.
	ErrInvalidHistogram = errors.New("histogram must have 1440 non-negative entries")

	errNotImplemented = errors.New("not implemented")
)

// SlotAssignment holds the computed (slot, shard) pair for a request.
type SlotAssignment struct {
	Slot  int
	Shard int
}

// SlotAssigner is a pure domain policy that deterministically assigns (slot, shard) pairs.
type SlotAssigner struct {
	shards int
}

// NewSlotAssigner creates a SlotAssigner configured for S shards.
func NewSlotAssigner(shards int) (*SlotAssigner, error) {
	return &SlotAssigner{shards: shards}, nil
}

// Shards returns the configured number of shards S.
func (a *SlotAssigner) Shards() int {
	return a.shards
}

// Assign computes a deterministic (slot, shard) for a request at position rank of total within provider.
func (a *SlotAssigner) Assign(_, _ string, _, _ int) (SlotAssignment, error) {
	return SlotAssignment{Slot: -1, Shard: -1}, errNotImplemented
}

// AssignProvider deterministically assigns (slot, shard) to all request IDs of a provider,
// independent of the input slice order.
func (a *SlotAssigner) AssignProvider(_ string, _ []string) (map[string]SlotAssignment, error) {
	return nil, errNotImplemented
}
