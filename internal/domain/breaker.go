package domain

import (
	"sync"
	"time"
)

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

type providerWindow struct {
	mu           sync.Mutex
	state        BreakerState
	openedAt     time.Time
	probeInLight bool
	ring         []bool
	head         int
	count        int
	failures     int
}

// ProviderBreaker tracks per-provider health with a sliding window of outcomes (004 FR-005).
type ProviderBreaker struct {
	cfg       BreakerConfig
	providers sync.Map // map[string]*providerWindow
}

// NewProviderBreaker creates a per-provider circuit breaker with 004 US4 defaults.
func NewProviderBreaker(cfg BreakerConfig) *ProviderBreaker {
	if cfg.WindowSize <= 0 {
		cfg.WindowSize = 20
	}
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 0.50
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = 60 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &ProviderBreaker{cfg: cfg}
}

func (b *ProviderBreaker) get(provider string) *providerWindow {
	if v, ok := b.providers.Load(provider); ok {
		if pw, ok := v.(*providerWindow); ok {
			return pw
		}
	}
	pw := &providerWindow{ring: make([]bool, b.cfg.WindowSize)}
	actual, _ := b.providers.LoadOrStore(provider, pw)
	if existing, ok := actual.(*providerWindow); ok {
		return existing
	}
	return pw
}

// Allow reports whether a call to provider is allowed; when false, returns remaining cooldown.
func (b *ProviderBreaker) Allow(provider string) (bool, time.Duration) {
	pw := b.get(provider)
	pw.mu.Lock()
	defer pw.mu.Unlock()

	now := b.cfg.Now()
	switch pw.state {
	case BreakerClosed:
		return true, 0
	case BreakerOpen:
		elapsed := now.Sub(pw.openedAt)
		if elapsed < b.cfg.Cooldown {
			return false, b.cfg.Cooldown - elapsed
		}
		// Cooldown expired -> transition to half-open and allow a single probe.
		pw.state = BreakerHalfOpen
		pw.probeInLight = true
		return true, 0
	case BreakerHalfOpen:
		if pw.probeInLight {
			return false, b.cfg.Cooldown
		}
		pw.probeInLight = true
		return true, 0
	default:
		return true, 0
	}
}

// Record records a provider call outcome (retryableFailure=true on transient/throttled errors).
func (b *ProviderBreaker) Record(provider string, retryableFailure bool) {
	pw := b.get(provider)
	pw.mu.Lock()
	defer pw.mu.Unlock()

	now := b.cfg.Now()
	if pw.state == BreakerHalfOpen {
		pw.probeInLight = false
		if retryableFailure {
			pw.state = BreakerOpen
			pw.openedAt = now
			return
		}
		// Probe succeeded -> close and reset sliding window.
		pw.state = BreakerClosed
		pw.head = 0
		pw.count = 0
		pw.failures = 0
		clear(pw.ring)
		return
	}

	// Update circular sliding window.
	if pw.count == len(pw.ring) {
		if pw.ring[pw.head] {
			pw.failures--
		}
	} else {
		pw.count++
	}
	pw.ring[pw.head] = retryableFailure
	if retryableFailure {
		pw.failures++
	}
	pw.head = (pw.head + 1) % len(pw.ring)

	if pw.count == len(pw.ring) && float64(pw.failures)/float64(pw.count) >= b.cfg.FailureThreshold {
		pw.state = BreakerOpen
		pw.openedAt = now
	}
}

// State returns the current BreakerState for provider.
func (b *ProviderBreaker) State(provider string) BreakerState {
	pw := b.get(provider)
	pw.mu.Lock()
	defer pw.mu.Unlock()
	return pw.state
}
