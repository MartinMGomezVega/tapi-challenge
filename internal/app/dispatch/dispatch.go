package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

const (
	// DefaultLeaseTTL is the leader lease duration (30 s, US3).
	DefaultLeaseTTL = 30 * time.Second
	// DefaultOrphanAge is the threshold after which a PENDING execution is re-dispatched (5 min, US4).
	DefaultOrphanAge = 5 * time.Minute
	// MaxSQSBatchSize is the SQS SendMessageBatch maximum of 10 entries.
	MaxSQSBatchSize = 10
	// DefaultMaxBatchRetries is the retry count for partial SendBatch failures.
	DefaultMaxBatchRetries = 3
	// DefaultMetricNamespace is the CloudWatch EMF namespace for dispatcher metrics.
	DefaultMetricNamespace = "Tapi/Scheduler"
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

// New constructs a Dispatcher UseCase with validated dependencies and defaults.
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
	if executions == nil || queue == nil || clk == nil {
		return nil, fmt.Errorf("%w: executions, queue, and clock are required", ErrInvalidConfig)
	}
	if cfg.Shards <= 0 {
		cfg.Shards = 1
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = DefaultLeaseTTL
	}
	if cfg.OrphanAge <= 0 {
		cfg.OrphanAge = DefaultOrphanAge
	}
	if cfg.Location == nil {
		cfg.Location = time.UTC
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = DefaultMaxBatchRetries
	}
	if logger == nil {
		logger = slog.Default()
	}

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

// DispatchSlot dispatches all enabled requests for a single SlotCoord across all shards in parallel.
func (u *UseCase) DispatchSlot(ctx context.Context, coord domain.SlotCoord) (SlotStats, error) {
	if u.requests == nil {
		return SlotStats{}, fmt.Errorf("%w: request reader is nil", ErrInvalidConfig)
	}
	if err := coord.Validate(); err != nil {
		return SlotStats{}, err
	}

	var (
		mu    sync.Mutex
		total SlotStats
	)

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(u.cfg.Shards)

	for shard := range u.cfg.Shards {
		g.Go(func() error {
			st, err := u.dispatchShard(gctx, coord, shard)
			mu.Lock()
			total.Dispatched += st.Dispatched
			total.Skipped += st.Skipped
			total.Orphans += st.Orphans
			mu.Unlock()
			return err
		})
	}

	if err := g.Wait(); err != nil {
		return total, err
	}
	return total, nil
}

func (u *UseCase) dispatchShard(ctx context.Context, coord domain.SlotCoord, shard int) (SlotStats, error) {
	var (
		stats SlotStats
		page  string
		batch = make([]JobMessage, 0, MaxSQSBatchSize)
	)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := u.sendAndMarkEnqueued(ctx, batch); err != nil {
			return err
		}
		stats.Dispatched += len(batch)
		batch = batch[:0]
		return nil
	}

	for {
		keys, nextPage, err := u.requests.ListBySlotShard(ctx, coord.Slot, shard, page)
		if err != nil {
			return stats, fmt.Errorf("list slot %d shard %d: %w", coord.Slot, shard, err)
		}

		for _, rec := range keys {
			if !rec.Enabled {
				stats.Skipped++
				continue
			}

			now := u.clock.Now().UTC()
			res, err := u.executions.CreatePending(ctx, rec.ID, coord.Date, rec.Provider, coord.Slot, now)
			if err != nil {
				return stats, fmt.Errorf("create pending %s#%s: %w", rec.ID, coord.Date, err)
			}

			shouldSend := res.Created
			if !res.Created && res.Status == StatusPending && now.Sub(res.CreatedAt) > u.cfg.OrphanAge {
				shouldSend = true
				stats.Orphans++
			}
			if !shouldSend {
				stats.Skipped++
				continue
			}

			batch = append(batch, JobMessage{
				RequestID:    rec.ID,
				RunDate:      coord.Date,
				Provider:     rec.Provider,
				Slot:         coord.Slot,
				DispatchedAt: now,
			})
			if len(batch) == MaxSQSBatchSize {
				if err := flush(); err != nil {
					return stats, err
				}
			}
		}

		if nextPage == "" {
			break
		}
		page = nextPage
	}

	if err := flush(); err != nil {
		return stats, err
	}
	return stats, nil
}

// DispatchNow implements assignslots.ImmediateDispatcher (001 FR-005b): idempotently creates
// PENDING, enqueues to SQS FIFO, and marks ENQUEUED for today's RunDate.
func (u *UseCase) DispatchNow(ctx context.Context, reqID, provider string) error {
	now := u.clock.Now().UTC()
	coord := domain.SlotOf(now, u.cfg.Location)

	res, err := u.executions.CreatePending(ctx, reqID, coord.Date, provider, coord.Slot, now)
	if err != nil {
		return fmt.Errorf("immediate create pending %s#%s: %w", reqID, coord.Date, err)
	}
	if !res.Created && (res.Status != StatusPending || now.Sub(res.CreatedAt) <= u.cfg.OrphanAge) {
		return nil
	}

	return u.sendAndMarkEnqueued(ctx, []JobMessage{
		{
			RequestID:    reqID,
			RunDate:      coord.Date,
			Provider:     provider,
			Slot:         coord.Slot,
			DispatchedAt: now,
		},
	})
}

func (u *UseCase) sendAndMarkEnqueued(ctx context.Context, msgs []JobMessage) error {
	pending := msgs
	var sent []ExecKey

	for attempt := range u.cfg.MaxRetries + 1 {
		failed, err := u.queue.SendBatch(ctx, pending)
		if err != nil {
			if attempt == u.cfg.MaxRetries {
				return fmt.Errorf("send batch after %d attempts: %w", attempt+1, err)
			}
			continue
		}

		failedSet := make(map[string]struct{}, len(failed))
		for _, f := range failed {
			failedSet[f.DedupID()] = struct{}{}
		}
		for _, m := range pending {
			if _, isFailed := failedSet[m.DedupID()]; !isFailed {
				sent = append(sent, ExecKey{RequestID: m.RequestID, RunDate: m.RunDate})
			}
		}

		if len(failed) == 0 {
			break
		}
		if attempt == u.cfg.MaxRetries {
			return fmt.Errorf("send batch: %d messages still failed after retries", len(failed))
		}
		pending = failed
	}

	if len(sent) > 0 {
		if err := u.executions.MarkEnqueued(ctx, sent, u.clock.Now().UTC()); err != nil {
			return fmt.Errorf("mark enqueued (%d keys): %w", len(sent), err)
		}
	}
	return nil
}

// Tick acquires/renews leadership, catches up all pending slots from watermark to now,
// and advances the watermark per completed slot using the leader's fencing token.
func (u *UseCase) Tick(ctx context.Context) (TickStats, error) {
	if u.lease == nil || u.watermarks == nil || strings.TrimSpace(u.cfg.OwnerID) == "" {
		return TickStats{}, fmt.Errorf("%w: lease, watermarks, and ownerID are required for Tick", ErrInvalidConfig)
	}

	now := u.clock.Now().UTC()
	token, acquired, err := u.lease.TryAcquire(ctx, u.cfg.OwnerID, u.cfg.LeaseTTL, now)
	if err != nil {
		return TickStats{}, fmt.Errorf("acquire leader lease: %w", err)
	}
	if !acquired {
		u.emitMetrics(ctx, Metrics{IsLeader: false})
		return TickStats{IsLeader: false}, nil
	}

	target := domain.SlotOf(now, u.cfg.Location)
	wm, found, err := u.watermarks.Load(ctx)
	if err != nil {
		return TickStats{IsLeader: true}, fmt.Errorf("load watermark: %w", err)
	}

	var slots []domain.SlotCoord
	if !found {
		slots = []domain.SlotCoord{target}
	} else {
		slots, err = domain.RangeSlots(wm, target)
		if err != nil {
			return TickStats{IsLeader: true}, fmt.Errorf("compute slot range: %w", err)
		}
	}

	stats := TickStats{IsLeader: true}
	if len(slots) == 0 {
		u.emitMetrics(ctx, Metrics{IsLeader: true, DispatchLagMinutes: 0})
		return stats, nil
	}

	for _, coord := range slots {
		slotStart := time.Now()
		slotStats, err := u.DispatchSlot(ctx, coord)
		if err != nil {
			return stats, fmt.Errorf("dispatch slot %s#%04d: %w", coord.Date, coord.Slot, err)
		}

		if err := u.watermarks.Advance(ctx, coord, token, u.clock.Now().UTC()); err != nil {
			return stats, fmt.Errorf("advance watermark to %s#%04d: %w", coord.Date, coord.Slot, err)
		}

		stats.SlotsDispatched++
		stats.RecordsSent += slotStats.Dispatched
		stats.OrphansResent += slotStats.Orphans

		lag, _ := domain.LagMinutes(coord, domain.SlotOf(u.clock.Now(), u.cfg.Location))
		u.emitMetrics(ctx, Metrics{
			SlotDispatchDuration: time.Since(slotStart),
			RecordsDispatched:    slotStats.Dispatched,
			DispatchLagMinutes:   lag,
			IsLeader:             true,
		})

		u.logger.InfoContext(ctx, "slot dispatched",
			slog.String("run_date", string(coord.Date)),
			slog.Int("slot", coord.Slot),
			slog.Int("dispatched", slotStats.Dispatched),
			slog.Int("orphans", slotStats.Orphans),
			slog.Int("lag_minutes", lag),
		)
	}

	return stats, nil
}

// ReleaseLease releases the leader lease on graceful shutdown (SIGTERM).
func (u *UseCase) ReleaseLease(ctx context.Context) error {
	if u.lease == nil || strings.TrimSpace(u.cfg.OwnerID) == "" {
		return nil
	}
	return u.lease.Release(ctx, u.cfg.OwnerID)
}

func (u *UseCase) emitMetrics(ctx context.Context, m Metrics) {
	if u.metrics == nil {
		return
	}
	if err := u.metrics.EmitDispatch(ctx, m); err != nil {
		u.logger.WarnContext(ctx, "failed to emit dispatch EMF metrics", slog.String("error", err.Error()))
	}
}

// EMFEmitter writes CloudWatch EMF JSON lines for dispatcher metrics (FR-007).
type EMFEmitter struct {
	mu        sync.Mutex
	out       io.Writer
	namespace string
	clock     Clock
}

// NewEMFEmitter creates an EMF emitter for dispatcher metrics.
func NewEMFEmitter(out io.Writer, namespace string, clk Clock) *EMFEmitter {
	if out == nil {
		out = os.Stdout
	}
	if namespace == "" {
		namespace = DefaultMetricNamespace
	}
	return &EMFEmitter{out: out, namespace: namespace, clock: clk}
}

type emfMetricDef struct {
	Name string `json:"Name"`
	Unit string `json:"Unit"`
}

type emfDirective struct {
	Namespace  string         `json:"Namespace"`
	Dimensions [][]string     `json:"Dimensions"`
	Metrics    []emfMetricDef `json:"Metrics"`
}

type emfMeta struct {
	Timestamp         int64          `json:"Timestamp"`
	CloudWatchMetrics []emfDirective `json:"CloudWatchMetrics"`
}

type emfDispatchEvent struct {
	AWS                  emfMeta `json:"_aws"`
	Service              string  `json:"service"`
	SlotDispatchDuration float64 `json:"SlotDispatchDuration"`
	RecordsDispatched    int     `json:"RecordsDispatched"`
	DispatchLagMinutes   int     `json:"DispatchLagMinutes"`
	IsLeader             int     `json:"IsLeader"`
}

// EmitDispatch writes one CloudWatch EMF JSON line containing FR-007 metrics.
func (e *EMFEmitter) EmitDispatch(_ context.Context, m Metrics) error {
	now := time.Now().UTC()
	if e.clock != nil {
		now = e.clock.Now().UTC()
	}
	isLeader := 0
	if m.IsLeader {
		isLeader = 1
	}

	ev := emfDispatchEvent{
		AWS: emfMeta{
			Timestamp: now.UnixMilli(),
			CloudWatchMetrics: []emfDirective{
				{
					Namespace:  e.namespace,
					Dimensions: [][]string{{"service"}},
					Metrics: []emfMetricDef{
						{Name: "SlotDispatchDuration", Unit: "Milliseconds"},
						{Name: "RecordsDispatched", Unit: "Count"},
						{Name: "DispatchLagMinutes", Unit: "Count"},
						{Name: "IsLeader", Unit: "Count"},
					},
				},
			},
		},
		Service:              "dispatcher",
		SlotDispatchDuration: float64(m.SlotDispatchDuration.Microseconds()) / 1000.0,
		RecordsDispatched:    m.RecordsDispatched,
		DispatchLagMinutes:   m.DispatchLagMinutes,
		IsLeader:             isLeader,
	}

	b, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal dispatch EMF: %w", err)
	}
	b = append(b, '\n')

	e.mu.Lock()
	defer e.mu.Unlock()
	if _, err := e.out.Write(b); err != nil {
		return fmt.Errorf("write dispatch EMF: %w", err)
	}
	return nil
}
