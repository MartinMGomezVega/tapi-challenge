package execute

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/platform/clock"
)

// Config configures the ExecuteRequest use case.
type Config struct {
	OwnerID     string
	LeaseTTL    time.Duration
	CallTimeout time.Duration
	MaxInline   int
}

// UseCase executes a single provider request end-to-end with lease, idempotency, and blob fallback.
type UseCase struct{}

// New creates a new ExecuteRequest use case.
func New(
	_ ExecutionStore,
	_ RequestGetter,
	_ ProviderAPI,
	_ BlobStore,
	_ MessageAcker,
	_ RetryPolicy,
	_ clock.Provider,
	_ *slog.Logger,
	_ Config,
) *UseCase {
	return &UseCase{}
}

// WithLimiter attaches an optional per-provider rate limiter (003 FR-006).
func (u *UseCase) WithLimiter(_ ProviderLimiter) *UseCase { return u }

// WithBreaker attaches an optional per-provider circuit breaker (004 US3).
func (u *UseCase) WithBreaker(_ CircuitBreaker) *UseCase { return u }

// WithMetrics attaches an optional metrics recorder.
func (u *UseCase) WithMetrics(_ MetricsRecorder) *UseCase { return u }

// Execute processes a single SQS FIFO message.
func (u *UseCase) Execute(_ context.Context, _ Message) error {
	return errors.New("not implemented")
}

// ValidateConcurrencyBudget enforces k * t <= lambdaBudget / 2 (003 US4).
func ValidateConcurrencyBudget(_, _, _ int) error {
	return errors.New("not implemented")
}

// ProviderRateLimiter enforces a per-provider requests-per-second ceiling (003 FR-006).
type ProviderRateLimiter struct{}

// NewProviderRateLimiter creates a per-provider limiter (rps <= 0 disables limiting).
func NewProviderRateLimiter(_ float64, _ int) *ProviderRateLimiter { return &ProviderRateLimiter{} }

// Wait blocks until provider has a token or ctx is cancelled.
func (l *ProviderRateLimiter) Wait(_ context.Context, _ string) error {
	return errors.New("not implemented")
}
