package domain

import (
	"math/rand/v2"
	"time"
)

// DecisionKind represents the retry policy decision for a provider call outcome (004 FR-002).
type DecisionKind int

// DecisionKind values.
const (
	DecisionComplete DecisionKind = iota
	DecisionRetryInProcess
	DecisionRetryLater
	DecisionFailPermanent
	DecisionExhausted
)

// RetryDecision is the pure domain decision returned by RetryPolicy.Decide.
type RetryDecision struct {
	Kind  DecisionKind
	Delay time.Duration
	Class ErrorClass
}

// RetryPolicyConfig configures backoff bounds and retry limits (004 FR-002, FR-003).
type RetryPolicyConfig struct {
	MaxReceiveCount      int
	MaxInProcessAttempts int
	FastErrorThreshold   time.Duration
	InProcessMinDelay    time.Duration
	InProcessMaxDelay    time.Duration
	BaseDelay            time.Duration
	MaxDelay             time.Duration
	JitterFraction       float64 // 0.20 => +/-20%
	RandFloat64          func() float64
}

// RetryPolicy is a pure decision policy mapping (ErrorClass, attempts, receiveCount, duration, retryAfter) to RetryDecision.
type RetryPolicy struct {
	cfg RetryPolicyConfig
}

// NewRetryPolicy builds a RetryPolicy with defaults from 004/spec.md.
func NewRetryPolicy(cfg RetryPolicyConfig) *RetryPolicy {
	if cfg.MaxReceiveCount <= 0 {
		cfg.MaxReceiveCount = 6
	}
	if cfg.MaxInProcessAttempts <= 0 {
		cfg.MaxInProcessAttempts = 2
	}
	if cfg.FastErrorThreshold <= 0 {
		cfg.FastErrorThreshold = time.Second
	}
	if cfg.InProcessMinDelay <= 0 {
		cfg.InProcessMinDelay = 200 * time.Millisecond
	}
	if cfg.InProcessMaxDelay <= 0 {
		cfg.InProcessMaxDelay = time.Second
	}
	if cfg.BaseDelay <= 0 {
		cfg.BaseDelay = 30 * time.Second
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = 15 * time.Minute
	}
	if cfg.JitterFraction <= 0 {
		cfg.JitterFraction = 0.20
	}
	if cfg.RandFloat64 == nil {
		cfg.RandFloat64 = rand.Float64 //nolint:gosec // non-cryptographic backoff jitter
	}
	return &RetryPolicy{cfg: cfg}
}

// Decide computes the retry action and backoff delay (004 US2-US3).
func (p *RetryPolicy) Decide(
	class ErrorClass,
	inProcessAttempt int,
	receiveCount int,
	callDuration time.Duration,
	retryAfter time.Duration,
) RetryDecision {
	switch class {
	case ErrClassNone, ErrClassAlreadyProcessed:
		return RetryDecision{Kind: DecisionComplete, Class: class}

	case ErrClassInvalidRequest, ErrClassAuth, ErrClassNotFound:
		return RetryDecision{Kind: DecisionFailPermanent, Class: class}

	case ErrClassInvalidResponse:
		// 004 US1: 2xx invalid response retries at most once, then fails permanently.
		if receiveCount >= 2 {
			return RetryDecision{Kind: DecisionFailPermanent, Class: class}
		}
		return RetryDecision{
			Kind:  DecisionRetryLater,
			Delay: p.computeDeferredDelay(receiveCount, retryAfter),
			Class: class,
		}

	default:
		// Retryable classes: TRANSIENT_NETWORK, TRANSIENT_SERVER, THROTTLED, UNKNOWN.
		if receiveCount >= p.cfg.MaxReceiveCount {
			return RetryDecision{Kind: DecisionExhausted, Class: class}
		}

		// Fast in-process retry only for TRANSIENT_NETWORK / TRANSIENT_SERVER with attempt < 2 and duration < 1s.
		if (class == ErrClassTransientNetwork || class == ErrClassTransientServer) &&
			inProcessAttempt < p.cfg.MaxInProcessAttempts &&
			callDuration < p.cfg.FastErrorThreshold {
			return RetryDecision{
				Kind:  DecisionRetryInProcess,
				Delay: p.computeInProcessDelay(),
				Class: class,
			}
		}

		return RetryDecision{
			Kind:  DecisionRetryLater,
			Delay: p.computeDeferredDelay(receiveCount, retryAfter),
			Class: class,
		}
	}
}

func (p *RetryPolicy) computeInProcessDelay() time.Duration {
	u := clampUnit(p.cfg.RandFloat64())
	span := p.cfg.InProcessMaxDelay - p.cfg.InProcessMinDelay
	return p.cfg.InProcessMinDelay + time.Duration(float64(span)*u)
}

func (p *RetryPolicy) computeDeferredDelay(receiveCount int, retryAfter time.Duration) time.Duration {
	exp := max(receiveCount-1, 0)
	if exp > 10 {
		exp = 10
	}
	base := p.cfg.BaseDelay * (1 << exp)
	if retryAfter > base {
		base = retryAfter
	}
	if base > p.cfg.MaxDelay {
		base = p.cfg.MaxDelay
	}

	// Symmetric jitter in [1 - jitter, 1 + jitter], clamped to [BaseDelay, MaxDelay].
	u := clampUnit(p.cfg.RandFloat64())
	factor := (1.0 - p.cfg.JitterFraction) + (2.0 * p.cfg.JitterFraction * u)
	d := time.Duration(float64(base) * factor)
	if d < p.cfg.BaseDelay {
		d = p.cfg.BaseDelay
	}
	if d > p.cfg.MaxDelay {
		d = p.cfg.MaxDelay
	}
	return d
}

func clampUnit(u float64) float64 {
	if u < 0 {
		return 0
	}
	if u > 1 {
		return 1
	}
	return u
}
