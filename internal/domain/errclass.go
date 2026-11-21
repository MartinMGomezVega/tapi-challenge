package domain

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

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
type Classifier struct {
	statusRules map[int]ErrorClass
}

// NewClassifier creates a Classifier with the default 004 US1 truth table and optional status overrides.
func NewClassifier(overrides map[int]ErrorClass) *Classifier {
	rules := map[int]ErrorClass{
		400: ErrClassInvalidRequest,
		401: ErrClassAuth,
		403: ErrClassAuth,
		404: ErrClassNotFound,
		408: ErrClassTransientNetwork,
		409: ErrClassAlreadyProcessed,
		422: ErrClassInvalidRequest,
		429: ErrClassThrottled,
		500: ErrClassTransientServer,
		502: ErrClassTransientServer,
		503: ErrClassTransientServer,
		504: ErrClassTransientServer,
	}
	for code, cls := range overrides {
		rules[code] = cls
	}
	return &Classifier{statusRules: rules}
}

// Classify classifies a provider call outcome into an ErrorClass.
func (c *Classifier) Classify(o ProviderOutcome) ErrorClass {
	if o.TransportErr != TransportNone {
		switch o.TransportErr {
		case TransportTimeout, TransportReset:
			return ErrClassTransientNetwork
		default:
			return ErrClassUnknown
		}
	}

	// Lambda / API Gateway throttling headers take precedence over generic 500s (004 US1).
	if strings.Contains(strings.ToLower(o.ErrorHeader), "toomanyrequests") ||
		strings.Contains(strings.ToLower(o.ErrorHeader), "throttl") {
		return ErrClassThrottled
	}

	if o.StatusCode >= 200 && o.StatusCode < 300 {
		trimmed := strings.TrimSpace(string(o.Body))
		if trimmed != "" && !json.Valid([]byte(trimmed)) {
			return ErrClassInvalidResponse
		}
		return ErrClassNone
	}

	if cls, ok := c.statusRules[o.StatusCode]; ok {
		return cls
	}
	return ErrClassUnknown
}

var httpDateLayouts = []string{
	time.RFC1123,
	time.RFC1123Z,
	time.RFC850,
	time.ANSIC,
}

// ParseRetryAfter parses an HTTP Retry-After header (integer seconds or HTTP-date) relative to now.
func ParseRetryAfter(raw string, now time.Time) (time.Duration, bool) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(s); err == nil {
		if secs < 0 {
			return 0, false
		}
		return time.Duration(secs) * time.Second, true
	}
	for _, layout := range httpDateLayouts {
		if ts, err := time.Parse(layout, s); err == nil {
			delta := ts.Sub(now)
			if delta < 0 {
				return 0, true
			}
			return delta, true
		}
	}
	return 0, false
}

const maxSanitizedErrorBytes = 1024

var (
	reBearerToken = regexp.MustCompile(`(?i)\b(Bearer|Basic|AWS4-HMAC-SHA256)\s+[A-Za-z0-9._~+/=-]+`)
	reEmail       = regexp.MustCompile(`[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}`)
	reLongDigits  = regexp.MustCompile(`\b\d{8,}\b`)
)

// SanitizeErrorMessage redacts tokens/emails/long numeric IDs, removes control chars,
// and truncates to 1024 bytes (004 FR-006).
func SanitizeErrorMessage(msg string) string {
	s := reBearerToken.ReplaceAllString(msg, "$1 [REDACTED]")
	s = reEmail.ReplaceAllString(s, "[REDACTED_EMAIL]")
	s = reLongDigits.ReplaceAllString(s, "[REDACTED_NUM]")

	var b strings.Builder
	b.Grow(min(len(s), maxSanitizedErrorBytes))
	for _, r := range s {
		if unicode.IsControl(r) {
			b.WriteByte(' ')
			continue
		}
		if b.Len()+len(string(r)) > maxSanitizedErrorBytes {
			break
		}
		b.WriteRune(r)
	}
	return strings.TrimSpace(b.String())
}
