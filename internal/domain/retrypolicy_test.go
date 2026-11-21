package domain_test

import (
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

// T006: RetryInProcess only for fast (< 1s) transient errors with inProcessAttempt < 2.
func TestRetryPolicy_InProcessConditions(t *testing.T) {
	t.Parallel()

	p := domain.NewRetryPolicy(domain.RetryPolicyConfig{
		RandFloat64: func() float64 { return 0.5 },
	})

	// Fast TRANSIENT_SERVER at attempt 1 -> RetryInProcess in [200ms, 1s].
	d := p.Decide(domain.ErrClassTransientServer, 1, 1, 200*time.Millisecond, 0)
	if d.Kind != domain.DecisionRetryInProcess || d.Delay < 200*time.Millisecond || d.Delay > time.Second {
		t.Fatalf("fast 5xx attempt 1 = %+v, want RetryInProcess in [200ms, 1s]", d)
	}

	// Slow TRANSIENT_SERVER (>= 1s) -> RetryLater (free slot).
	dSlow := p.Decide(domain.ErrClassTransientServer, 1, 1, 1500*time.Millisecond, 0)
	if dSlow.Kind != domain.DecisionRetryLater {
		t.Fatalf("slow 5xx = %+v, want RetryLater", dSlow)
	}

	// Attempt 2 (>= max 2) -> RetryLater.
	dAtt2 := p.Decide(domain.ErrClassTransientNetwork, 2, 1, 100*time.Millisecond, 0)
	if dAtt2.Kind != domain.DecisionRetryLater {
		t.Fatalf("attempt 2 = %+v, want RetryLater", dAtt2)
	}

	// THROTTLED (429) must NEVER retry in-process (respect backpressure).
	d429 := p.Decide(domain.ErrClassThrottled, 1, 1, 50*time.Millisecond, 0)
	if d429.Kind != domain.DecisionRetryLater {
		t.Fatalf("429 = %+v, want RetryLater", d429)
	}
}

// T007: Property test: RetryLater delay in [30s, 15m], monotonic without jitter, +/-20% jitter, respects Retry-After.
func TestRetryPolicy_BackoffProperties(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(rt *rapid.T) {
		rc := rapid.IntRange(1, 5).Draw(rt, "receiveCount")
		retryAfterSec := rapid.IntRange(0, 1800).Draw(rt, "retryAfterSec")
		u := rapid.Float64Range(0, 1).Draw(rt, "u")

		p := domain.NewRetryPolicy(domain.RetryPolicyConfig{
			RandFloat64: func() float64 { return u },
		})
		retryAfter := time.Duration(retryAfterSec) * time.Second
		d := p.Decide(domain.ErrClassThrottled, 2, rc, 2*time.Second, retryAfter)
		if d.Kind != domain.DecisionRetryLater {
			rt.Fatalf("got kind=%v, want DecisionRetryLater", d.Kind)
		}
		if d.Delay < 30*time.Second || d.Delay > 15*time.Minute {
			rt.Fatalf("delay %v out of [30s, 15m] (rc=%d retryAfter=%v u=%v)", d.Delay, rc, retryAfter, u)
		}
	})

	// Monotonicity without jitter (u=0.5 => multiplier=1.0).
	pDet := domain.NewRetryPolicy(domain.RetryPolicyConfig{
		RandFloat64: func() float64 { return 0.5 },
	})
	var prev time.Duration
	for rc := 1; rc <= 5; rc++ {
		d := pDet.Decide(domain.ErrClassTransientServer, 2, rc, 2*time.Second, 0)
		if d.Delay < prev {
			t.Fatalf("non-monotonic delay at rc=%d: %v < %v", rc, d.Delay, prev)
		}
		prev = d.Delay
	}

	// Retry-After > 15m is capped at 15m; Retry-After 120s overrides 30s base.
	dCap := pDet.Decide(domain.ErrClassThrottled, 2, 1, 2*time.Second, 45*time.Minute)
	if dCap.Delay != 15*time.Minute {
		t.Fatalf("Retry-After 45m delay = %v, want 15m", dCap.Delay)
	}
	dRA := pDet.Decide(domain.ErrClassThrottled, 2, 1, 2*time.Second, 2*time.Minute)
	if dRA.Delay != 2*time.Minute {
		t.Fatalf("Retry-After 2m delay = %v, want 2m", dRA.Delay)
	}
}

// T008 & T009: receiveCount >= 6 -> Exhausted; non-retryables -> FailPermanent; ALREADY_PROCESSED -> Complete;
// INVALID_RESPONSE retries up to 1 time then FailPermanent.
func TestRetryPolicy_TerminalAndExhaustedDecisions(t *testing.T) {
	t.Parallel()

	p := domain.NewRetryPolicy(domain.RetryPolicyConfig{
		RandFloat64: func() float64 { return 0.5 },
	})

	if d := p.Decide(domain.ErrClassNone, 1, 1, 10*time.Millisecond, 0); d.Kind != domain.DecisionComplete {
		t.Fatalf("ErrClassNone -> %+v, want Complete", d)
	}
	if d := p.Decide(domain.ErrClassAlreadyProcessed, 1, 1, 10*time.Millisecond, 0); d.Kind != domain.DecisionComplete {
		t.Fatalf("ALREADY_PROCESSED -> %+v, want Complete", d)
	}

	for _, cls := range []domain.ErrorClass{
		domain.ErrClassInvalidRequest,
		domain.ErrClassAuth,
		domain.ErrClassNotFound,
	} {
		if d := p.Decide(cls, 1, 1, 10*time.Millisecond, 0); d.Kind != domain.DecisionFailPermanent {
			t.Fatalf("%s -> %+v, want FailPermanent", cls, d)
		}
	}

	// INVALID_RESPONSE retries once, then fails permanently.
	if d := p.Decide(domain.ErrClassInvalidResponse, 1, 1, 10*time.Millisecond, 0); d.Kind != domain.DecisionRetryLater {
		t.Fatalf("INVALID_RESPONSE first attempt -> %+v, want RetryLater", d)
	}
	if d := p.Decide(domain.ErrClassInvalidResponse, 1, 2, 10*time.Millisecond, 0); d.Kind != domain.DecisionFailPermanent {
		t.Fatalf("INVALID_RESPONSE second receive -> %+v, want FailPermanent", d)
	}

	// receiveCount >= 6 -> Exhausted for retryable classes.
	for _, cls := range []domain.ErrorClass{
		domain.ErrClassTransientNetwork,
		domain.ErrClassTransientServer,
		domain.ErrClassThrottled,
		domain.ErrClassUnknown,
	} {
		if d := p.Decide(cls, 1, 6, 10*time.Millisecond, 0); d.Kind != domain.DecisionExhausted {
			t.Fatalf("%s at rc=6 -> %+v, want Exhausted", cls, d)
		}
	}
}
