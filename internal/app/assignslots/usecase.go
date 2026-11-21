package assignslots

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

const (
	// DefaultWorkers is the default number of providers processed concurrently.
	DefaultWorkers = 4
	// DefaultRebalanceThreshold triggers a full rebalance when a provider has more than 1440 new records.
	DefaultRebalanceThreshold = domain.SlotsPerDay
)

// ErrInvalidConfig indicates missing required dependencies or invalid job arguments.
var ErrInvalidConfig = errors.New("invalid assignslots configuration")

// Config configures the AssignSlots use case.
type Config struct {
	Workers            int
	WCULimit           float64 // max SetSlot writes/sec; <= 0 disables rate limiting
	RebalanceThreshold int     // defaults to 1440
	Location           *time.Location
}

// Stats summarizes a backfill run.
type Stats struct {
	ProvidersProcessed  int
	ProvidersSkipped    int
	ProvidersRebalanced int
	ProvidersIncrement  int
	SlotsUpdated        int
	SlotsUnchanged      int
	DispatchedNow       int
	ConcurrentOverflow  int // records with rank >= k assigned via AssignIncremental
}

// UseCase orchestrates resumable, rate-limited streaming backfill per provider.
type UseCase struct {
	assigner    *domain.SlotAssigner
	reader      ProviderIndexReader
	writer      SlotWriter
	registry    ProviderRegistry
	checkpoints Checkpointer
	dispatcher  ImmediateDispatcher
	clock       Clock
	logger      *slog.Logger
	limiter     *rate.Limiter
	cfg         Config
}

// New constructs an AssignSlots UseCase with validated dependencies and sensible defaults.
func New(
	assigner *domain.SlotAssigner,
	reader ProviderIndexReader,
	writer SlotWriter,
	registry ProviderRegistry,
	checkpoints Checkpointer,
	dispatcher ImmediateDispatcher,
	clk Clock,
	logger *slog.Logger,
	cfg Config,
) (*UseCase, error) {
	if assigner == nil || reader == nil || writer == nil || registry == nil || checkpoints == nil || clk == nil {
		return nil, fmt.Errorf("%w: required dependency is nil", ErrInvalidConfig)
	}
	if dispatcher == nil {
		dispatcher = NoopImmediateDispatcher{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.Workers <= 0 {
		cfg.Workers = DefaultWorkers
	}
	if cfg.RebalanceThreshold <= 0 {
		cfg.RebalanceThreshold = DefaultRebalanceThreshold
	}
	if cfg.Location == nil {
		cfg.Location = time.UTC
	}

	var limiter *rate.Limiter
	if cfg.WCULimit > 0 {
		burst := max(1, int(cfg.WCULimit))
		limiter = rate.NewLimiter(rate.Limit(cfg.WCULimit), burst)
	}

	return &UseCase{
		assigner:    assigner,
		reader:      reader,
		writer:      writer,
		registry:    registry,
		checkpoints: checkpoints,
		dispatcher:  dispatcher,
		clock:       clk,
		logger:      logger,
		limiter:     limiter,
		cfg:         cfg,
	}, nil
}

// Run executes the backfill job across all registered providers in parallel with bounded concurrency.
func (u *UseCase) Run(ctx context.Context, jobID string) (Stats, error) {
	if strings.TrimSpace(jobID) == "" {
		return Stats{}, fmt.Errorf("%w: jobID must not be empty", ErrInvalidConfig)
	}

	providers, err := u.registry.ListProviders(ctx)
	if err != nil {
		return Stats{}, fmt.Errorf("list providers: %w", err)
	}

	var (
		mu    sync.Mutex
		total Stats
	)

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(u.cfg.Workers)

	for _, state := range providers {
		g.Go(func() error {
			pStats, err := u.processProvider(gctx, jobID, state)
			mu.Lock()
			mergeStats(&total, pStats)
			mu.Unlock()
			return err
		})
	}

	if err := g.Wait(); err != nil {
		return total, err
	}
	return total, nil
}

func (u *UseCase) processProvider(ctx context.Context, jobID string, state ProviderState) (Stats, error) {
	cp, found, err := u.checkpoints.Load(ctx, jobID, state.Provider)
	if err != nil {
		return Stats{}, fmt.Errorf("load checkpoint (%s): %w", state.Provider, err)
	}
	if found && cp.Status == CheckpointDone {
		return Stats{ProvidersSkipped: 1}, nil
	}

	incremental := state.Backfilled && state.NewSinceBackfill <= u.cfg.RebalanceThreshold
	if found && cp.Status == CheckpointPending {
		incremental = cp.Incremental
	}

	if incremental {
		return u.runIncremental(ctx, jobID, state, cp, found)
	}
	return u.runFullRebalance(ctx, jobID, state, cp, found)
}

func (u *UseCase) runFullRebalance(
	ctx context.Context,
	jobID string,
	state ProviderState,
	cp BackfillCheckpoint,
	resuming bool,
) (Stats, error) {
	var (
		stats  Stats
		cursor string
		rank   int
		k      int
		hist   [domain.SlotsPerDay]int
	)

	if resuming && cp.Status == CheckpointPending {
		cursor = cp.Cursor
		rank = cp.Rank
		k = cp.Count
		hist = cp.Histogram
	} else {
		var err error
		k, err = u.reader.CountByProvider(ctx, state.Provider)
		if err != nil {
			return stats, fmt.Errorf("count provider %s: %w", state.Provider, err)
		}
		if k == 0 {
			return u.finishProvider(ctx, jobID, state.Provider, 0, hist, false, stats)
		}
	}

	for {
		items, nextCursor, err := u.reader.ListByProvider(ctx, state.Provider, cursor)
		if err != nil {
			return stats, fmt.Errorf("list provider %s: %w", state.Provider, err)
		}

		for _, item := range items {
			var assigned domain.SlotAssignment
			if rank < k {
				assigned, err = u.assigner.Assign(state.Provider, item.ID, rank, k)
			} else {
				assigned, err = u.assigner.AssignIncremental(state.Provider, item.ID, hist[:])
				stats.ConcurrentOverflow++
			}
			if err != nil {
				return stats, fmt.Errorf("compute slot for %s/%s: %w", state.Provider, item.ID, err)
			}

			if item.Slot != assigned.Slot || item.Shard != assigned.Shard {
				if err := u.waitWCU(ctx); err != nil {
					return stats, err
				}
				if err := u.writer.SetSlot(ctx, item.ID, assigned.Slot, assigned.Shard, item.Version); err != nil {
					return stats, fmt.Errorf("set slot %s/%s: %w", state.Provider, item.ID, err)
				}
				stats.SlotsUpdated++

				dispatched, err := u.maybeDispatchSameDay(ctx, item, assigned.Slot)
				if err != nil {
					return stats, err
				}
				if dispatched {
					stats.DispatchedNow++
				}
			} else {
				stats.SlotsUnchanged++
			}

			hist[assigned.Slot]++
			rank++
		}

		if nextCursor == "" {
			break
		}

		cursor = nextCursor
		if err := u.checkpoints.Save(ctx, jobID, state.Provider, BackfillCheckpoint{
			Status:      CheckpointPending,
			Incremental: false,
			Cursor:      cursor,
			Rank:        rank,
			Count:       k,
			Histogram:   hist,
			UpdatedAt:   u.clock.Now().UTC(),
		}); err != nil {
			return stats, fmt.Errorf("save pending checkpoint (%s): %w", state.Provider, err)
		}
	}

	return u.finishProvider(ctx, jobID, state.Provider, rank, hist, false, stats)
}

func (u *UseCase) runIncremental(
	ctx context.Context,
	jobID string,
	state ProviderState,
	cp BackfillCheckpoint,
	resuming bool,
) (Stats, error) {
	var (
		stats      Stats
		cursor     string
		totalCount = state.Count
		hist       = state.Histogram
	)

	if resuming && cp.Status == CheckpointPending {
		cursor = cp.Cursor
		totalCount = cp.Count
		hist = cp.Histogram
	}

	for {
		items, nextCursor, err := u.reader.ListByProvider(ctx, state.Provider, cursor)
		if err != nil {
			return stats, fmt.Errorf("list provider %s: %w", state.Provider, err)
		}

		for _, item := range items {
			if item.Slot >= 0 && item.Shard >= 0 {
				stats.SlotsUnchanged++
				continue
			}

			assigned, err := u.assigner.AssignIncremental(state.Provider, item.ID, hist[:])
			if err != nil {
				return stats, fmt.Errorf("assign incremental %s/%s: %w", state.Provider, item.ID, err)
			}

			if err := u.waitWCU(ctx); err != nil {
				return stats, err
			}
			if err := u.writer.SetSlot(ctx, item.ID, assigned.Slot, assigned.Shard, item.Version); err != nil {
				return stats, fmt.Errorf("set incremental slot %s/%s: %w", state.Provider, item.ID, err)
			}

			hist[assigned.Slot]++
			totalCount++
			stats.SlotsUpdated++
		}

		if nextCursor == "" {
			break
		}

		cursor = nextCursor
		if err := u.checkpoints.Save(ctx, jobID, state.Provider, BackfillCheckpoint{
			Status:      CheckpointPending,
			Incremental: true,
			Cursor:      cursor,
			Count:       totalCount,
			Histogram:   hist,
			UpdatedAt:   u.clock.Now().UTC(),
		}); err != nil {
			return stats, fmt.Errorf("save incremental checkpoint (%s): %w", state.Provider, err)
		}
	}

	return u.finishProvider(ctx, jobID, state.Provider, totalCount, hist, true, stats)
}

func (u *UseCase) finishProvider(
	ctx context.Context,
	jobID, provider string,
	count int,
	hist [domain.SlotsPerDay]int,
	incremental bool,
	stats Stats,
) (Stats, error) {
	now := u.clock.Now().UTC()
	if err := u.registry.SaveProviderState(ctx, ProviderState{
		Provider:         provider,
		Count:            count,
		Histogram:        hist,
		NewSinceBackfill: 0,
		Backfilled:       true,
		UpdatedAt:        now,
	}); err != nil {
		return stats, fmt.Errorf("save provider state (%s): %w", provider, err)
	}

	if err := u.checkpoints.Save(ctx, jobID, provider, BackfillCheckpoint{
		Status:      CheckpointDone,
		Incremental: incremental,
		Rank:        count,
		Count:       count,
		Histogram:   hist,
		UpdatedAt:   now,
	}); err != nil {
		return stats, fmt.Errorf("save done checkpoint (%s): %w", provider, err)
	}

	stats.ProvidersProcessed = 1
	if incremental {
		stats.ProvidersIncrement = 1
	} else {
		stats.ProvidersRebalanced = 1
	}

	u.logger.InfoContext(ctx, "provider backfill completed",
		slog.String("job_id", jobID),
		slog.String("provider", provider),
		slog.Bool("incremental", incremental),
		slog.Int("count", count),
		slog.Int("slots_updated", stats.SlotsUpdated),
		slog.Int("slots_unchanged", stats.SlotsUnchanged),
		slog.Int("dispatched_now", stats.DispatchedNow),
		slog.Int("concurrent_overflow", stats.ConcurrentOverflow),
	)
	return stats, nil
}

func (u *UseCase) waitWCU(ctx context.Context) error {
	if u.limiter == nil {
		return nil
	}
	if err := u.limiter.Wait(ctx); err != nil {
		return fmt.Errorf("wcu rate limiter: %w", err)
	}
	return nil
}

// needsSameDayDispatch enforces FR-005b (R1): an enabled, previously slotted request moved
// from a current/future slot to an already-passed slot today (slot_new < slot_now <= slot_old)
// would otherwise skip today's execution.
func (u *UseCase) needsSameDayDispatch(item domain.ProviderRequestItem, newSlot int) bool {
	if !item.Enabled || item.Slot < 0 {
		return false
	}
	slotNow := slotOf(u.clock.Now().In(u.cfg.Location))
	return newSlot < slotNow && slotNow <= item.Slot
}

func (u *UseCase) maybeDispatchSameDay(ctx context.Context, item domain.ProviderRequestItem, newSlot int) (bool, error) {
	if !u.needsSameDayDispatch(item, newSlot) {
		return false, nil
	}
	if err := u.dispatcher.DispatchNow(ctx, item.ID, item.Provider); err != nil {
		return false, fmt.Errorf("immediate dispatch %s/%s: %w", item.Provider, item.ID, err)
	}
	return true, nil
}

func slotOf(t time.Time) int {
	return t.Hour()*60 + t.Minute()
}

func mergeStats(dst *Stats, src Stats) {
	dst.ProvidersProcessed += src.ProvidersProcessed
	dst.ProvidersSkipped += src.ProvidersSkipped
	dst.ProvidersRebalanced += src.ProvidersRebalanced
	dst.ProvidersIncrement += src.ProvidersIncrement
	dst.SlotsUpdated += src.SlotsUpdated
	dst.SlotsUnchanged += src.SlotsUnchanged
	dst.DispatchedNow += src.DispatchedNow
	dst.ConcurrentOverflow += src.ConcurrentOverflow
}
