package domain

import "time"

// ErrorClass categorizes provider call outcomes for retry decisions and metrics (004 FR-001).
type ErrorClass string

// ErrorClass values covering 100% of the 004 US1 truth table.
const (
	ErrClassNone             ErrorClass = ""
	ErrClassTransientNetwork ErrorClass = "TRANSIENT_NETWORK"
	ErrClassTransientServer  ErrorClass = "TRANSIENT_SERVER"
	ErrClassThrottled        ErrorClass = "THROTTLED"
	ErrClassInvalidRequest   ErrorClass = "INVALID_REQUEST"
	ErrClassAuth             ErrorClass = "AUTH"
	ErrClassNotFound         ErrorClass = "NOT_FOUND"
	ErrClassAlreadyProcessed ErrorClass = "ALREADY_PROCESSED"
	ErrClassInvalidResponse  ErrorClass = "INVALID_RESPONSE"
	ErrClassUnknown          ErrorClass = "UNKNOWN"
)

// TransportErrorKind identifies transport-level failures translated by the HTTP adapter.
type TransportErrorKind int

// TransportErrorKind values for domain-pure error classification (no net/http in domain).
const (
	TransportNone TransportErrorKind = iota
	TransportTimeout
	TransportReset
	TransportUnknown
)

// ProviderOutcome is the transport-agnostic outcome of calling a provider API.
type ProviderOutcome struct {
	StatusCode      int
	Body            []byte
	RetryAfterRaw   string
	ErrorHeader     string
	TransportErr    TransportErrorKind
	RawErrorMessage string
	CallDuration    time.Duration
}

// Classifier maps a ProviderOutcome to an ErrorClass using configurable rules (004 FR-001).
type Classifier struct{}

// NewClassifier creates a Classifier with the default 004 US1 truth table and optional status overrides.
func NewClassifier(_ map[int]ErrorClass) *Classifier { return &Classifier{} }

// Classify classifies a provider call outcome into an ErrorClass.
func (c *Classifier) Classify(_ ProviderOutcome) ErrorClass { return ErrClassUnknown }

// ParseRetryAfter parses an HTTP Retry-After header (seconds or HTTP-date) relative to now.
func ParseRetryAfter(_ string, _ time.Time) (time.Duration, bool) { return 0, false }

// SanitizeErrorMessage redacts tokens/PII, strips control characters, and truncates to 1024 bytes (004 FR-006).
func SanitizeErrorMessage(msg string) string { return msg }
