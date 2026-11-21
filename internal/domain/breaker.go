package domain

import "time"

// BreakerState represents the circuit breaker state for a single provider (004 US4).
type BreakerState int

// BreakerState values.
const (
	BreakerClosed BreakerState = iota
	BreakerOpen
	BreakerHalfOpen
)

// BreakerConfig configures the per-provider sliding-window circuit breaker.
type BreakerConfig struct {
	WindowSize       int           // default 20
	FailureThreshold float64       // default 0.50
	Cooldown         time.Duration // default 60s
	Now              func() time.Time
}

// ProviderBreaker tracks per-provider health with a sliding window of outcomes (004 FR-005).
type ProviderBreaker struct{}

// NewProviderBreaker creates a per-provider circuit breaker.
func NewProviderBreaker(_ BreakerConfig) *ProviderBreaker { return &ProviderBreaker{} }

// Allow reports whether a call to provider is allowed; when false, returns remaining cooldown.
func (b *ProviderBreaker) Allow(_ string) (bool, time.Duration) { return true, 0 }

// Record records a provider call outcome (retryableFailure=true on transient/throttled errors).
func (b *ProviderBreaker) Record(_ string, _ bool) {}

// State returns the current BreakerState for provider.
func (b *ProviderBreaker) State(_ string) BreakerState { return BreakerClosed }
