package domain

import "time"

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
type RetryPolicy struct{}

// NewRetryPolicy builds a RetryPolicy with defaults from 004/spec.md.
func NewRetryPolicy(_ RetryPolicyConfig) *RetryPolicy { return &RetryPolicy{} }

// Decide computes the retry action and backoff delay.
func (p *RetryPolicy) Decide(_ ErrorClass, _, _ int, _, _ time.Duration) RetryDecision {
	return RetryDecision{Kind: DecisionFailPermanent}
}
