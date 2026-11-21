// Package reconcile implements daily coverage reconciliation and idempotent re-dispatch (feature 005).
package reconcile

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/martingomezvega/tapi-challenge/internal/app/dispatch"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

const (
	defaultParallelism = 64
	defaultStaleAfter  = 2 * time.Hour
	batchGetChunkSize  = 100
	sqsBatchChunkSize  = 10
)

// CatalogReader lists requests for a (slot, shard) partition from gsi_slot_shard.
type CatalogReader interface {
	ListBySlotShard(ctx context.Context, slot, shard int, pageToken string) (items []domain.RequestKey, nextToken string, err error)
}

// ExecutionStore reads executions in batch and supports idempotent re-dispatch.
type ExecutionStore interface {
	BatchGet(ctx context.Context, reqIDs []string, date domain.RunDate) (map[string]domain.ExecutionSnapshot, error)
	CreatePending(ctx context.Context, reqID string, date domain.RunDate, provider string, slot int, now time.Time) (dispatch.CreatePendingResult, error)
	MarkEnqueued(ctx context.Context, keys []dispatch.ExecKey, now time.Time) error
	ResetStale(ctx context.Context, reqID string, date domain.RunDate, staleBefore, now time.Time) (bool, error)
}

// JobQueue enqueues re-dispatched job messages onto the main FIFO queue.
type JobQueue interface {
	SendBatch(ctx context.Context, msgs []dispatch.JobMessage) ([]dispatch.JobMessage, error)
}

// ReportStore persists JSON and CSV daily reconciliation reports (S3).
type ReportStore interface {
	PutWithContentType(ctx context.Context, key string, data []byte, contentType string) error
}

// MetricsEmitter publishes DailyCoverage and MissingExecutions metrics (EMF).
type MetricsEmitter interface {
	RecordReconcile(date domain.RunDate, coverage float64, missing, stuck int)
}

// Config configures a reconciliation run.
type Config struct {
	Shards      int
	Parallelism int
	StaleAfter  time.Duration
	Location    *time.Location
	Redispatch  bool
}

// ProviderSummary aggregates daily counts for one provider.
type ProviderSummary struct {
	Expected        int            `json:"expected"`
	Terminal        int            `json:"terminal"`
	Missing         int            `json:"missing"`
	Stuck           int            `json:"stuck"`
	InFlight        int            `json:"in_flight"`
	Redispatched    int            `json:"redispatched"`
	ByStatus        map[string]int `json:"by_status"`
	CoveragePercent float64        `json:"coverage_percent"`
}

// Summary is the daily reconciliation report written to S3 JSON/CSV (005 US1.3, FR-004).
type Summary struct {
	RunDate       domain.RunDate             `json:"run_date"`
	GeneratedAt   time.Time                  `json:"generated_at"`
	Expected      int                        `json:"expected"`
	Terminal      int                        `json:"terminal"`
	Missing       int                        `json:"missing"`
	Stuck         int                        `json:"stuck"`
	InFlight      int                        `json:"in_flight"`
	Redispatched  int                        `json:"redispatched"`
	Coverage      float64                    `json:"coverage"`
	ByStatus      map[string]int             `json:"by_status"`
	TopErrors     map[string]int             `json:"top_errors"`
	ByProvider    map[string]ProviderSummary `json:"by_provider"`
	JSONReportKey string                     `json:"json_report_key,omitempty"`
	CSVReportKey  string                     `json:"csv_report_key,omitempty"`
}

// UseCase runs daily reconciliation across all 1440 x Shards partitions without full table Scans.
type UseCase struct {
	catalog CatalogReader
	execs   ExecutionStore
	queue   JobQueue
	reports ReportStore
	metrics MetricsEmitter
	now     func() time.Time
	logger  *slog.Logger
	cfg     Config
}

// New constructs a reconcile UseCase.
func New(
	catalog CatalogReader,
	execs ExecutionStore,
	queue JobQueue,
	reports ReportStore,
	metrics MetricsEmitter,
	now func() time.Time,
	logger *slog.Logger,
	cfg Config,
) *UseCase {
	if cfg.Shards <= 0 {
		cfg.Shards = 1
	}
	if cfg.Parallelism <= 0 {
		cfg.Parallelism = defaultParallelism
	}
	if cfg.StaleAfter <= 0 {
		cfg.StaleAfter = defaultStaleAfter
	}
	if cfg.Location == nil {
		cfg.Location = time.UTC
	}
	if now == nil {
		now = time.Now
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &UseCase{
		catalog: catalog,
		execs:   execs,
		queue:   queue,
		reports: reports,
		metrics: metrics,
		now:     now,
		logger:  logger,
		cfg:     cfg,
	}
}

type partitionStats struct {
	expected     int
	terminal     int
	missing      int
	stuck        int
	inFlight     int
	redispatched int
	byStatus     map[string]int
	topErrors    map[string]int
	byProvider   map[string]*ProviderSummary
}

func newPartitionStats() *partitionStats {
	return &partitionStats{
		byStatus:   make(map[string]int),
		topErrors:  make(map[string]int),
		byProvider: make(map[string]*ProviderSummary),
	}
}

func (p *partitionStats) provider(name string) *ProviderSummary {
	ps, ok := p.byProvider[name]
	if !ok {
		ps = &ProviderSummary{ByStatus: make(map[string]int)}
		p.byProvider[name] = ps
	}
	return ps
}

// Run reconciles day D, optionally re-dispatches missing/stuck executions, and persists S3 JSON/CSV reports.
func (u *UseCase) Run(ctx context.Context, date domain.RunDate) (Summary, error) {
	if _, err := domain.ParseRunDate(string(date)); err != nil {
		return Summary{}, err
	}

	now := u.now().UTC()
	staleBefore := now.Add(-u.cfg.StaleAfter)

	var (
		mu    sync.Mutex
		total = newPartitionStats()
	)

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(u.cfg.Parallelism)

	for slot := range domain.SlotsPerDay {
		for shard := range u.cfg.Shards {
			g.Go(func() error {
				ps, err := u.reconcilePartition(gctx, date, slot, shard, staleBefore, now)
				if err != nil {
					return err
				}
				mu.Lock()
				mergePartitionStats(total, ps)
				mu.Unlock()
				return nil
			})
		}
	}

	if err := g.Wait(); err != nil {
		return Summary{}, err
	}

	summary := Summary{
		RunDate:      date,
		GeneratedAt:  now,
		Expected:     total.expected,
		Terminal:     total.terminal,
		Missing:      total.missing,
		Stuck:        total.stuck,
		InFlight:     total.inFlight,
		Redispatched: total.redispatched,
		Coverage:     domain.ComputeCoverage(total.terminal, total.expected),
		ByStatus:     total.byStatus,
		TopErrors:    total.topErrors,
		ByProvider:   make(map[string]ProviderSummary, len(total.byProvider)),
	}
	for prov, ps := range total.byProvider {
		ps.CoveragePercent = domain.ComputeCoverage(ps.Terminal, ps.Expected) * 100.0
		summary.ByProvider[prov] = *ps
	}

	if u.reports != nil {
		jsonKey := fmt.Sprintf("reports/run_date=%s/summary.json", date)
		csvKey := fmt.Sprintf("reports/run_date=%s/summary.csv", date)

		jsonBytes, err := json.Marshal(summary)
		if err != nil {
			return summary, fmt.Errorf("marshal summary json: %w", err)
		}
		if err := u.reports.PutWithContentType(ctx, jsonKey, jsonBytes, "application/json"); err != nil {
			return summary, fmt.Errorf("store json report: %w", err)
		}
		csvBytes, err := encodeSummaryCSV(summary)
		if err != nil {
			return summary, fmt.Errorf("encode summary csv: %w", err)
		}
		if err := u.reports.PutWithContentType(ctx, csvKey, csvBytes, "text/csv"); err != nil {
			return summary, fmt.Errorf("store csv report: %w", err)
		}
		summary.JSONReportKey = jsonKey
		summary.CSVReportKey = csvKey
	}

	if u.metrics != nil {
		u.metrics.RecordReconcile(date, summary.Coverage, summary.Missing, summary.Stuck)
	}

	u.logger.Info("daily reconciliation completed",
		"run_date", string(date),
		"expected", summary.Expected,
		"terminal", summary.Terminal,
		"missing", summary.Missing,
		"stuck", summary.Stuck,
		"redispatched", summary.Redispatched,
		"coverage", summary.Coverage,
	)
	return summary, nil
}

func (u *UseCase) reconcilePartition(
	ctx context.Context,
	date domain.RunDate,
	slot, shard int,
	staleBefore, now time.Time,
) (*partitionStats, error) {
	ps := newPartitionStats()
	pageToken := ""

	for {
		items, nextToken, err := u.catalog.ListBySlotShard(ctx, slot, shard, pageToken)
		if err != nil {
			return nil, fmt.Errorf("list slot=%d shard=%d: %w", slot, shard, err)
		}

		expected := make([]domain.RequestKey, 0, len(items))
		for _, item := range items {
			if !item.Enabled {
				continue
			}
			if !domain.ShouldExpectExecution(item.CreatedAt, date, slot, u.cfg.Location) {
				continue
			}
			expected = append(expected, item)
		}

		if len(expected) > 0 {
			if err := u.evaluateChunk(ctx, date, slot, expected, staleBefore, now, ps); err != nil {
				return nil, err
			}
		}

		if nextToken == "" {
			return ps, nil
		}
		pageToken = nextToken
	}
}

func (u *UseCase) evaluateChunk(
	ctx context.Context,
	date domain.RunDate,
	slot int,
	expected []domain.RequestKey,
	staleBefore, now time.Time,
	ps *partitionStats,
) error {
	for start := 0; start < len(expected); start += batchGetChunkSize {
		end := min(start+batchGetChunkSize, len(expected))
		chunk := expected[start:end]

		ids := make([]string, len(chunk))
		for i, k := range chunk {
			ids[i] = k.ID
		}
		snaps, err := u.execs.BatchGet(ctx, ids, date)
		if err != nil {
			return fmt.Errorf("batch get executions slot=%d: %w", slot, err)
		}

		var toSend []dispatch.JobMessage
		var markEnqueued []dispatch.ExecKey

		for _, req := range chunk {
			ps.expected++
			prov := ps.provider(req.Provider)
			prov.Expected++

			var snapPtr *domain.ExecutionSnapshot
			if snap, ok := snaps[req.ID]; ok {
				snapCopy := snap
				snapPtr = &snapCopy
			}

			cls := domain.ClassifyExecution(snapPtr, now, u.cfg.StaleAfter)
			switch cls {
			case domain.ReconcileTerminal:
				ps.terminal++
				prov.Terminal++
				st := string(snapPtr.Status)
				ps.byStatus[st]++
				prov.ByStatus[st]++
				if snapPtr.ErrorClass != "" {
					ps.topErrors[snapPtr.ErrorClass]++
				}

			case domain.ReconcileMissing:
				ps.missing++
				prov.Missing++
				if u.cfg.Redispatch && u.queue != nil {
					res, err := u.execs.CreatePending(ctx, req.ID, date, req.Provider, slot, now)
					if err != nil {
						return fmt.Errorf("reconcile create pending %s: %w", req.ID, err)
					}
					if res.Created {
						toSend = append(toSend, dispatch.JobMessage{
							RequestID:    req.ID,
							RunDate:      date,
							Provider:     req.Provider,
							Slot:         slot,
							DispatchedAt: now,
						})
						markEnqueued = append(markEnqueued, dispatch.ExecKey{RequestID: req.ID, RunDate: date})
						ps.redispatched++
						prov.Redispatched++
					}
				}

			case domain.ReconcileStuck:
				ps.stuck++
				prov.Stuck++
				st := string(snapPtr.Status)
				ps.byStatus[st]++
				prov.ByStatus[st]++
				if u.cfg.Redispatch && u.queue != nil {
					reset, err := u.execs.ResetStale(ctx, req.ID, date, staleBefore, now)
					if err != nil {
						return fmt.Errorf("reconcile reset stale %s: %w", req.ID, err)
					}
					if reset {
						toSend = append(toSend, dispatch.JobMessage{
							RequestID:    req.ID,
							RunDate:      date,
							Provider:     req.Provider,
							Slot:         slot,
							DispatchedAt: now,
						})
						ps.redispatched++
						prov.Redispatched++
					}
				}

			case domain.ReconcileInFlight:
				ps.inFlight++
				prov.InFlight++
				st := string(snapPtr.Status)
				ps.byStatus[st]++
				prov.ByStatus[st]++
			}
		}

		if len(toSend) > 0 {
			for s := 0; s < len(toSend); s += sqsBatchChunkSize {
				e := min(s+sqsBatchChunkSize, len(toSend))
				failed, err := u.queue.SendBatch(ctx, toSend[s:e])
				if err != nil {
					return fmt.Errorf("reconcile send batch: %w", err)
				}
				if len(failed) > 0 {
					return fmt.Errorf("reconcile send batch had %d failed entries", len(failed))
				}
			}
		}
		if len(markEnqueued) > 0 {
			if err := u.execs.MarkEnqueued(ctx, markEnqueued, now); err != nil {
				return fmt.Errorf("reconcile mark enqueued: %w", err)
			}
		}
	}
	return nil
}

func mergePartitionStats(dst, src *partitionStats) {
	dst.expected += src.expected
	dst.terminal += src.terminal
	dst.missing += src.missing
	dst.stuck += src.stuck
	dst.inFlight += src.inFlight
	dst.redispatched += src.redispatched
	for k, v := range src.byStatus {
		dst.byStatus[k] += v
	}
	for k, v := range src.topErrors {
		dst.topErrors[k] += v
	}
	for prov, sp := range src.byProvider {
		dp := dst.provider(prov)
		dp.Expected += sp.Expected
		dp.Terminal += sp.Terminal
		dp.Missing += sp.Missing
		dp.Stuck += sp.Stuck
		dp.InFlight += sp.InFlight
		dp.Redispatched += sp.Redispatched
		for k, v := range sp.ByStatus {
			dp.ByStatus[k] += v
		}
	}
}

func encodeSummaryCSV(s Summary) ([]byte, error) {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	if err := w.Write([]string{"run_date", "provider", "expected", "terminal", "missing", "stuck", "in_flight", "redispatched", "coverage_percent"}); err != nil {
		return nil, err
	}
	provs := make([]string, 0, len(s.ByProvider))
	for p := range s.ByProvider {
		provs = append(provs, p)
	}
	slices.Sort(provs)
	for _, p := range provs {
		ps := s.ByProvider[p]
		row := []string{
			string(s.RunDate),
			p,
			strconv.Itoa(ps.Expected),
			strconv.Itoa(ps.Terminal),
			strconv.Itoa(ps.Missing),
			strconv.Itoa(ps.Stuck),
			strconv.Itoa(ps.InFlight),
			strconv.Itoa(ps.Redispatched),
			fmt.Sprintf("%.4f", ps.CoveragePercent),
		}
		if err := w.Write(row); err != nil {
			return nil, err
		}
	}
	w.Flush()
	return buf.Bytes(), w.Error()
}
