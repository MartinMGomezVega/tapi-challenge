package domain_test

import (
	"testing"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/domain"
	"github.com/martingomezvega/tapi-challenge/internal/platform/clock"
)

// T017: Closed -> Open (>= 50% of 20) -> Half-Open (after 60s) -> Closed (on 1 success) or Open (on 1 failure).
func TestProviderBreaker_LifecycleWithFakeClock(t *testing.T) {
	t.Parallel()

	t0 := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	clk := clock.NewFake(t0)
	brk := domain.NewProviderBreaker(domain.BreakerConfig{
		WindowSize:       20,
		FailureThreshold: 0.50,
		Cooldown:         60 * time.Second,
		Now:              clk.Now,
	})

	// 10 successes + 9 failures = 19 (< window 20) -> remains closed.
	for range 10 {
		brk.Record("edenor", false)
	}
	for range 9 {
		brk.Record("edenor", true)
	}
	if ok, _ := brk.Allow("edenor"); !ok || brk.State("edenor") != domain.BreakerClosed {
		t.Fatalf("expected closed before hitting 50%% of 20")
	}

	// 10th failure reaches 10/20 = 50% -> opens for 60s.
	brk.Record("edenor", true)
	ok, cooldown := brk.Allow("edenor")
	if ok || brk.State("edenor") != domain.BreakerOpen || cooldown != 60*time.Second {
		t.Fatalf("expected open with 60s cooldown, got ok=%v state=%v cd=%v", ok, brk.State("edenor"), cooldown)
	}

	// Another provider is completely unaffected.
	if okOther, _ := brk.Allow("edesur"); !okOther {
		t.Fatalf("edesur must not be affected by edenor breaker")
	}

	// Advance 30s -> still open with 30s remaining.
	clk.Advance(30 * time.Second)
	ok, cooldown = brk.Allow("edenor")
	if ok || cooldown != 30*time.Second {
		t.Fatalf("at +30s expected open with 30s remaining, got ok=%v cd=%v", ok, cooldown)
	}

	// Advance +31s (total 61s) -> transitions to half-open and allows 1 probe.
	clk.Advance(31 * time.Second)
	if ok, _ := brk.Allow("edenor"); !ok || brk.State("edenor") != domain.BreakerHalfOpen {
		t.Fatalf("expected half-open probe allowed, got ok=%v state=%v", ok, brk.State("edenor"))
	}
	// A second concurrent probe while half-open is rejected.
	if ok2, _ := brk.Allow("edenor"); ok2 {
		t.Fatalf("second concurrent probe in half-open should be rejected")
	}

	// Probe fails -> reopens for another 60s.
	brk.Record("edenor", true)
	if ok, cd := brk.Allow("edenor"); ok || brk.State("edenor") != domain.BreakerOpen || cd != 60*time.Second {
		t.Fatalf("failed probe should reopen: ok=%v state=%v cd=%v", ok, brk.State("edenor"), cd)
	}

	// Advance 61s -> half-open -> probe succeeds -> closed!
	clk.Advance(61 * time.Second)
	if ok, _ := brk.Allow("edenor"); !ok {
		t.Fatalf("expected second half-open probe allowed")
	}
	brk.Record("edenor", false)
	if ok, _ := brk.Allow("edenor"); !ok || brk.State("edenor") != domain.BreakerClosed {
		t.Fatalf("successful probe should close breaker: ok=%v state=%v", ok, brk.State("edenor"))
	}
}
