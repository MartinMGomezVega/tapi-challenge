// Package dispatch implements the minute-by-minute slot dispatcher with catch-up,
// leader election with fencing, orphan PENDING recovery, and immediate dispatch (002, 001 FR-005b).
package dispatch

import (
	"context"
	"errors"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

const (
	// StatusPending is the initial execution status created before enqueuing to SQS.
	StatusPending = "PENDING"
	// StatusEnqueued is the execution status set after successful SQS FIFO enqueue.
	StatusEnqueued = "ENQUEUED"
)

var (
	// ErrFencingMismatch indicates a stale leader attempted to advance the watermark.
	ErrFencingMismatch = errors.New("leader fencing token mismatch")
	// ErrInvalidConfig indicates invalid dispatcher use case configuration.
	ErrInvalidConfig = errors.New("invalid dispatch configuration")
)

// Clock supplies current UTC wall-clock time.
type Clock interface {
	Now() time.Time
}

// RequestReader queries enabled requests in a (slot, shard) via gsi_slot_shard.
type RequestReader interface {
	ListBySlotShard(ctx context.Context, slot, shard int, page string) ([]domain.RequestKey, string, error)
}

// ExecKey identifies a single execution record (request_id, run_date).
type ExecKey struct {
	RequestID string
	RunDate   domain.RunDate
}

// CreatePendingResult captures the outcome of a conditional PENDING PutItem.
type CreatePendingResult struct {
	Created   bool
	Status    string
	CreatedAt time.Time
}

// ExecutionCreator creates conditional PENDING executions and transitions them to ENQUEUED.
type ExecutionCreator interface {
	CreatePending(ctx context.Context, reqID string, date domain.RunDate, provider string, slot int, now time.Time) (CreatePendingResult, error)
	MarkEnqueued(ctx context.Context, keys []ExecKey, now time.Time) error
}

// JobMessage is the payload dispatched to SQS FIFO (FR-003).
type JobMessage struct {
	RequestID    string         `json:"request_id"`
	RunDate      domain.RunDate `json:"run_date"`
	Provider     string         `json:"provider"`
	Slot         int            `json:"slot"`
	DispatchedAt time.Time      `json:"dispatched_at"`
	TraceContext string         `json:"trace_context,omitempty"`
}

// DedupID returns the SQS FIFO MessageDeduplicationId ("request_id#run_date").
func (m JobMessage) DedupID() string {
	return domain.IdempotencyKey(m.RequestID, m.RunDate)
}

// JobQueue sends batches of up to 10 job messages to SQS FIFO.
type JobQueue interface {
	SendBatch(ctx context.Context, msgs []JobMessage) (failed []JobMessage, err error)
}

// LeaderLease coordinates single-leader election with a monotonically increasing fencing token.
type LeaderLease interface {
	TryAcquire(ctx context.Context, owner string, ttl time.Duration, now time.Time) (token int64, acquired bool, err error)
	Release(ctx context.Context, owner string) error
}

// WatermarkStore loads and advances the last fully dispatched SlotCoord conditioned on fencing token.
type WatermarkStore interface {
	Load(ctx context.Context) (domain.SlotCoord, bool, error)
	Advance(ctx context.Context, coord domain.SlotCoord, fencingToken int64, now time.Time) error
}

// Metrics carries FR-007 CloudWatch EMF metrics for a dispatch tick/slot.
type Metrics struct {
	SlotDispatchDuration time.Duration
	RecordsDispatched    int
	DispatchLagMinutes   int
	IsLeader             bool
}

// MetricEmitter emits dispatcher EMF metrics (FR-007).
type MetricEmitter interface {
	EmitDispatch(ctx context.Context, m Metrics) error
}
