// Package assignslots implements the resumable, rate-limited streaming backfill use case (001-US2).
package assignslots

import (
	"context"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

// Clock supplies current UTC time for timestamps and current-slot calculation (FR-005b).
type Clock interface {
	Now() time.Time
}

// ProviderIndexReader reads catalog items ordered by SK=id per provider over gsi_provider (FR-004b).
type ProviderIndexReader interface {
	CountByProvider(ctx context.Context, provider string) (int, error)
	ListByProvider(ctx context.Context, provider, page string) ([]domain.ProviderRequestItem, string, error)
}

// SlotWriter atomically assigns (slot, shard) on a request conditioned on expectedVersion.
type SlotWriter interface {
	SetSlot(ctx context.Context, id string, slot, shard int, expectedVersion int64) error
}

// ImmediateDispatcher dispatches a request immediately through the idempotent dispatch path
// when a rebalance moves it from a future/current slot to an already-passed slot today (FR-005b).
type ImmediateDispatcher interface {
	DispatchNow(ctx context.Context, reqID, provider string) error
}

// NoopImmediateDispatcher is a stub ImmediateDispatcher used until feature 002 wires the real dispatcher.
type NoopImmediateDispatcher struct{}

// DispatchNow is a no-op stub.
func (NoopImmediateDispatcher) DispatchNow(_ context.Context, _, _ string) error {
	return nil
}

// ProviderState represents a provider's persisted registry entry in the coordination table,
// shared between US2 (backfill & incremental assignment) and US3 (capacity validation).
type ProviderState struct {
	Provider         string
	Count            int
	Histogram        [domain.SlotsPerDay]int
	NewSinceBackfill int
	Backfilled       bool
	UpdatedAt        time.Time
}

// ProviderRegistry stores and lists per-provider metadata and slot histograms in coordination.
type ProviderRegistry interface {
	ListProviders(ctx context.Context) ([]ProviderState, error)
	SaveProviderState(ctx context.Context, state ProviderState) error
}

// CheckpointStatus indicates whether a provider's backfill work for a job is still in progress or complete.
type CheckpointStatus string

const (
	// CheckpointPending indicates the provider still has remaining pages to process.
	CheckpointPending CheckpointStatus = "PENDING"
	// CheckpointDone indicates the provider completed backfill for the job.
	CheckpointDone CheckpointStatus = "DONE"
)

// BackfillCheckpoint tracks resumable per-provider backfill progress in coordination.
type BackfillCheckpoint struct {
	Status      CheckpointStatus
	Incremental bool
	Cursor      string
	Rank        int
	Count       int
	Histogram   [domain.SlotsPerDay]int
	UpdatedAt   time.Time
}

// Checkpointer persists and restores per-provider backfill checkpoints.
type Checkpointer interface {
	Load(ctx context.Context, job, provider string) (BackfillCheckpoint, bool, error)
	Save(ctx context.Context, job, provider string, cp BackfillCheckpoint) error
}
