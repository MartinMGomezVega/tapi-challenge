// Package reconcile implements daily coverage reconciliation and idempotent re-dispatch (feature 005).
package reconcile

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/app/dispatch"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
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
	RunDate         domain.RunDate             `json:"run_date"`
	GeneratedAt     time.Time                  `json:"generated_at"`
	Expected        int                        `json:"expected"`
	Terminal        int                        `json:"terminal"`
	Missing         int                        `json:"missing"`
	Stuck           int                        `json:"stuck"`
	InFlight        int                        `json:"in_flight"`
	Redispatched    int                        `json:"redispatched"`
	Coverage        float64                    `json:"coverage"`
	ByStatus        map[string]int             `json:"by_status"`
	TopErrors       map[string]int             `json:"top_errors"`
	ByProvider      map[string]ProviderSummary `json:"by_provider"`
	JSONReportKey   string                     `json:"json_report_key,omitempty"`
	CSVReportKey    string                     `json:"csv_report_key,omitempty"`
}

// UseCase runs daily reconciliation across all 1440 x Shards partitions without full table Scans.
type UseCase struct{}

// New constructs a reconcile UseCase.
func New(
	_ CatalogReader,
	_ ExecutionStore,
	_ JobQueue,
	_ ReportStore,
	_ MetricsEmitter,
	_ func() time.Time,
	_ *slog.Logger,
	_ Config,
) *UseCase {
	return &UseCase{}
}

// Run reconciles day D, optionally re-dispatches missing/stuck executions, and persists S3 JSON/CSV reports.
func (u *UseCase) Run(_ context.Context, _ domain.RunDate) (Summary, error) {
	return Summary{}, errors.New("not implemented")
}
