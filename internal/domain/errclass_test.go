package domain_test

import (
	"strings"
	"testing"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

// T001: 100% of 004 US1 truth table covered table-driven, plus custom status overrides.
func TestClassifier_TruthTable(t *testing.T) {
	t.Parallel()

	c := domain.NewClassifier(nil)

	cases := []struct {
		name    string
		outcome domain.ProviderOutcome
		want    domain.ErrorClass
	}{
		{"200 valid json", domain.ProviderOutcome{StatusCode: 200, Body: []byte(`{"debt":0}`)}, domain.ErrClassNone},
		{"204 empty body", domain.ProviderOutcome{StatusCode: 204}, domain.ErrClassNone},
		{"200 invalid json body", domain.ProviderOutcome{StatusCode: 200, Body: []byte(`not-json{`)}, domain.ErrClassInvalidResponse},
		{"timeout transport", domain.ProviderOutcome{TransportErr: domain.TransportTimeout}, domain.ErrClassTransientNetwork},
		{"connection reset transport", domain.ProviderOutcome{TransportErr: domain.TransportReset}, domain.ErrClassTransientNetwork},
		{"http 408 timeout", domain.ProviderOutcome{StatusCode: 408}, domain.ErrClassTransientNetwork},
		{"http 500", domain.ProviderOutcome{StatusCode: 500}, domain.ErrClassTransientServer},
		{"http 502", domain.ProviderOutcome{StatusCode: 502}, domain.ErrClassTransientServer},
		{"http 503", domain.ProviderOutcome{StatusCode: 503}, domain.ErrClassTransientServer},
		{"http 504", domain.ProviderOutcome{StatusCode: 504}, domain.ErrClassTransientServer},
		{"http 429", domain.ProviderOutcome{StatusCode: 429}, domain.ErrClassThrottled},
		{"lambda TooManyRequestsException header", domain.ProviderOutcome{StatusCode: 500, ErrorHeader: "TooManyRequestsException"}, domain.ErrClassThrottled},
		{"http 400", domain.ProviderOutcome{StatusCode: 400}, domain.ErrClassInvalidRequest},
		{"http 422", domain.ProviderOutcome{StatusCode: 422}, domain.ErrClassInvalidRequest},
		{"http 401", domain.ProviderOutcome{StatusCode: 401}, domain.ErrClassAuth},
		{"http 403", domain.ProviderOutcome{StatusCode: 403}, domain.ErrClassAuth},
		{"http 404", domain.ProviderOutcome{StatusCode: 404}, domain.ErrClassNotFound},
		{"http 409 already processed", domain.ProviderOutcome{StatusCode: 409}, domain.ErrClassAlreadyProcessed},
		{"unknown transport error", domain.ProviderOutcome{TransportErr: domain.TransportUnknown}, domain.ErrClassUnknown},
		{"unlisted status 418", domain.ProviderOutcome{StatusCode: 418}, domain.ErrClassUnknown},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := c.Classify(tc.outcome); got != tc.want {
				t.Fatalf("Classify(%+v) = %q, want %q", tc.outcome, got, tc.want)
			}
		})
	}

	// Custom configurable override.
	custom := domain.NewClassifier(map[int]domain.ErrorClass{
		418: domain.ErrClassInvalidRequest,
	})
	if got := custom.Classify(domain.ProviderOutcome{StatusCode: 418}); got != domain.ErrClassInvalidRequest {
		t.Fatalf("custom override 418 = %q, want INVALID_REQUEST", got)
	}
}

// T002: Retry-After parsing (integer seconds and HTTP-date).
func TestParseRetryAfter(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name   string
		raw    string
		want   time.Duration
		wantOK bool
	}{
		{"empty", "", 0, false},
		{"seconds 45", "45", 45 * time.Second, true},
		{"seconds with whitespace", "  120 ", 120 * time.Second, true},
		{"seconds zero", "0", 0, true},
		{"negative seconds invalid", "-5", 0, false},
		{"RFC1123 future date", "Sat, 21 Nov 2026 12:03:00 GMT", 3 * time.Minute, true},
		{"RFC1123 past date clamps to 0", "Sat, 21 Nov 2026 11:55:00 GMT", 0, true},
		{"garbage", "not-a-date", 0, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := domain.ParseRetryAfter(tc.raw, now)
			if ok != tc.wantOK || got != tc.want {
				t.Fatalf("ParseRetryAfter(%q) = (%v, %v), want (%v, %v)", tc.raw, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

// T024: Error message sanitization strips tokens/emails/PII and truncates to 1 KB.
func TestSanitizeErrorMessage(t *testing.T) {
	t.Parallel()

	raw := "upstream failed: Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.secret.sig user=juan.perez@example.com\naccount=123456789012"
	got := domain.SanitizeErrorMessage(raw)
	if strings.Contains(got, "eyJhbGciOiJIUzI1NiJ9") {
		t.Fatalf("bearer token leaked in sanitized message: %q", got)
	}
	if strings.Contains(got, "juan.perez@example.com") {
		t.Fatalf("email leaked in sanitized message: %q", got)
	}
	if strings.Contains(got, "\n") {
		t.Fatalf("control char newline leaked: %q", got)
	}

	huge := strings.Repeat("a", 2500)
	trunc := domain.SanitizeErrorMessage(huge)
	if len(trunc) > 1024 {
		t.Fatalf("truncated len = %d, want <= 1024", len(trunc))
	}
}
