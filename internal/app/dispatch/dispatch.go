package dispatch

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

const (
	// DefaultLeaseTTL is the leader lease duration (30 s, US3).
	DefaultLeaseTTL = 30 * time.Second
	// DefaultOrphanAge is the threshold after which a PENDING execution is re-dispatched (5 min, US4).
	DefaultOrphanAge = 5 * time.Minute
	// MaxSQSBatchSize is the SQS SendMessageBatch maximum of 10 entries.
	MaxSQSBatchSize = 10
)

// Config configures the Dispatcher UseCase.
type Config struct {
	OwnerID    string
	Shards     int
	LeaseTTL   time.Duration
	OrphanAge  time.Duration
	Location   *time.Location
	MaxRetries int
}

// SlotStats summarizes a single slot dispatch.
type SlotStats struct {
	Dispatched int
	Skipped    int
	Orphans    int
}

// TickStats summarizes a single dispatcher tick.
type TickStats struct {
	IsLeader        bool
	SlotsDispatched int
	RecordsSent     int
	OrphansResent   int
}

// UseCase implements minute-based slot dispatching, catch-up, leader fencing, orphan recovery,
// and assignslots.ImmediateDispatcher (001 FR-005b).
type UseCase struct {
	requests   RequestReader
	executions ExecutionCreator
	queue      JobQueue
	lease      LeaderLease
	watermarks WatermarkStore
	metrics    MetricEmitter
	clock      Clock
	logger     *slog.Logger
	cfg        Config
}

// New constructs a Dispatcher UseCase.
func New(
	requests RequestReader,
	executions ExecutionCreator,
	queue JobQueue,
	lease LeaderLease,
	watermarks WatermarkStore,
	metrics MetricEmitter,
	clk Clock,
	logger *slog.Logger,
	cfg Config,
) (*UseCase, error) {
	return &UseCase{
		requests:   requests,
		executions: executions,
		queue:      queue,
		lease:      lease,
		watermarks: watermarks,
		metrics:    metrics,
		clock:      clk,
		logger:     logger,
		cfg:        cfg,
	}, nil
}

// DispatchSlot dispatches all enabled requests for a single SlotCoord across all shards.
func (u *UseCase) DispatchSlot(_ context.Context, _ domain.SlotCoord) (SlotStats, error) {
	return SlotStats{}, errors.New("not implemented")
}

// DispatchNow implements assignslots.ImmediateDispatcher (001 FR-005b): idempotently creates
// PENDING, enqueues to SQS FIFO, and marks ENQUEUED for today's RunDate.
func (u *UseCase) DispatchNow(_ context.Context, _, _ string) error {
	return errors.New("not implemented")
}

// Tick acquires/renews leadership, catches up all pending slots from watermark to now,
// and advances the watermark per completed slot using the leader's fencing token.
func (u *UseCase) Tick(_ context.Context) (TickStats, error) {
	return TickStats{}, errors.New("not implemented")
}

// ReleaseLease releases the leader lease on graceful shutdown (SIGTERM).
func (u *UseCase) ReleaseLease(_ context.Context) error {
	return errors.New("not implemented")
}

// EMFEmitter writes CloudWatch EMF JSON lines for dispatcher metrics (FR-007).
type EMFEmitter struct {
	out       io.Writer
	namespace string
	clock     Clock
}

// NewEMFEmitter creates an EMF emitter for dispatcher metrics.
func NewEMFEmitter(out io.Writer, namespace string, clk Clock) *EMFEmitter {
	return &EMFEmitter{out: out, namespace: namespace, clock: clk}
}

// EmitDispatch writes one CloudWatch EMF JSON line containing FR-007 metrics.
func (e *EMFEmitter) EmitDispatch(_ context.Context, _ DispatchMetrics) error {
	return errors.New("not implemented")
}
