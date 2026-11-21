// Package execute implements the worker execution use case (feature 003) and retry/breaker orchestration (feature 004).
package execute

import (
	"context"
	"errors"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

// Default settings from 003/plan.md.
const (
	DefaultLeaseTTL    = 60 * time.Second
	DefaultCallTimeout = 25 * time.Second
	DefaultMinExtend   = 5 * time.Second
	ReasonNotFound     = "REQUEST_NOT_FOUND"
	ReasonLeaseLost    = "LEASE_LOST"
)

// ErrLeaseLost indicates the conditional DynamoDB update failed because another worker took over the lease.
var ErrLeaseLost = errors.New("execution lease lost")

// ErrInvalidBudget indicates the worker concurrency configuration violates the account Lambda budget (003 US4).
var ErrInvalidBudget = errors.New("concurrency budget violated")

// ExecKey uniquely identifies one daily execution (request_id, run_date).
type ExecKey struct {
	RequestID string
	RunDate   domain.RunDate
}

// AcquireOutcome classifies the result of attempting to acquire an execution lease.
type AcquireOutcome int

// AcquireOutcome values returned by ExecutionStore.Acquire.
const (
	AcquireAcquired AcquireOutcome = iota
	AcquireAlreadyTerminal
	AcquireLeasedByOther
)

// Execution is the state snapshot returned by ExecutionStore.Acquire.
type Execution struct {
	RequestID      string
	RunDate        domain.RunDate
	Provider       string
	Status         domain.ExecutionStatus
	Attempts       int
	LeaseOwner     string
	LeaseExpiresAt time.Time
}

// Result holds persisted success metadata for a completed execution (003 FR-004).
type Result struct {
	HTTPStatus    int
	ResponseBody  string
	ResponseS3Key string
	LatencyMs     int64
	CompletedAt   time.Time
}

// ErrInfo holds persisted failure metadata for retryable, permanent, or exhausted executions.
type ErrInfo struct {
	ErrorClass   string
	ErrorMessage string
	HTTPStatus   int
	LatencyMs    int64
	NextRetryAt  time.Time
	CompletedAt  time.Time
}

// Message is the worker-side view of an SQS FIFO job message.
type Message struct {
	RequestID      string
	RunDate        domain.RunDate
	Provider       string
	Slot           int
	TraceContext   string
	ReceiptHandle  string
	ReceiveCount   int
	MessageGroupID string
}

// Response is the raw provider HTTP response returned by ProviderAPI.Call.
type Response struct {
	StatusCode  int
	Body        []byte
	RetryAfter  string
	ErrorHeader string
}

// RetryAction is the action selected by RetryPolicy.Decide.
type RetryAction int

// RetryAction values returned by RetryPolicy.Decide.
const (
	ActionComplete RetryAction = iota
	ActionRetryInProcess
	ActionRetryLater
	ActionFailPermanent
	ActionFailExhausted
)

// RetryDecision describes how ExecuteRequest should handle an API response or error (003 FR-007 / 004).
type RetryDecision struct {
	Action       RetryAction
	ErrorClass   string
	ErrorMessage string
	Backoff      time.Duration
}

// ExecutionStore manages conditional execution state transitions in DynamoDB.
type ExecutionStore interface {
	Acquire(ctx context.Context, key ExecKey, owner string, ttl time.Duration, now time.Time) (Execution, AcquireOutcome, error)
	Complete(ctx context.Context, key ExecKey, owner string, res Result) error
	ScheduleRetry(ctx context.Context, key ExecKey, owner string, info ErrInfo) error
	FailPermanent(ctx context.Context, key ExecKey, owner string, info ErrInfo) error
	FailExhausted(ctx context.Context, key ExecKey, owner string, info ErrInfo) error
	MarkSkipped(ctx context.Context, key ExecKey, owner string, now time.Time) error
}

// RequestGetter loads the latest request specification from the catalog.
type RequestGetter interface {
	Get(ctx context.Context, id string) (domain.Request, error)
}

// ProviderAPI invokes the internal provider API gateway endpoint.
type ProviderAPI interface {
	Call(ctx context.Context, req domain.Request, idemKey, traceParent string) (Response, error)
}

// BlobStore persists oversized (> 350 KiB) API response payloads in S3.
type BlobStore interface {
	Put(ctx context.Context, key string, data []byte) error
}

// MessageAcker acknowledges, nacks, or extends visibility for an SQS message.
type MessageAcker interface {
	Ack(ctx context.Context, m Message) error
	Nack(ctx context.Context, m Message, delay time.Duration) error
	Extend(ctx context.Context, m Message, d time.Duration) error
}

// RetryPolicy classifies provider responses and errors into execution actions (003 FR-007, implemented in 004).
type RetryPolicy interface {
	Decide(err error, resp Response, attempts, receiveCount int) RetryDecision
}

// ProviderLimiter optionally rate-limits calls per provider (003 FR-006).
type ProviderLimiter interface {
	Wait(ctx context.Context, provider string) error
}

// CircuitBreaker optionally short-circuits calls to unhealthy providers (004 US3).
type CircuitBreaker interface {
	Allow(provider string) (allowed bool, retryAfter time.Duration)
	Record(provider string, retryableFailure bool)
}

// MetricsRecorder emits business metrics for worker execution (005).
type MetricsRecorder interface {
	RecordExecution(provider string, status domain.ExecutionStatus, errClass string, latency time.Duration)
	RecordBreakerOpen(provider string, open bool)
}
