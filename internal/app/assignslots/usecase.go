package assignslots

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

const (
	// DefaultWorkers is the default number of providers processed concurrently.
	DefaultWorkers = 4
	// DefaultRebalanceThreshold triggers a full rebalance when a provider has more than 1440 new records.
	DefaultRebalanceThreshold = domain.SlotsPerDay
)

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
	cfg         Config
}

// New constructs an AssignSlots UseCase.
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
	return &UseCase{
		assigner:    assigner,
		reader:      reader,
		writer:      writer,
		registry:    registry,
		checkpoints: checkpoints,
		dispatcher:  dispatcher,
		clock:       clk,
		logger:      logger,
		cfg:         cfg,
	}, nil
}

// Run executes the backfill job across all registered providers.
func (u *UseCase) Run(_ context.Context, _ string) (Stats, error) {
	return Stats{}, errors.New("not implemented")
}
