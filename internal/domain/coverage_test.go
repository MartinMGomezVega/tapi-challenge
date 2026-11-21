package domain_test

import (
	"testing"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

// T008: Pure Coverage calculation (terminal / expected; SKIPPED counts as terminal).
func TestComputeCoverage(t *testing.T) {
	t.Parallel()

	if got := domain.ComputeCoverage(0, 0); got != 1.0 {
		t.Fatalf("ComputeCoverage(0, 0) = %v, want 1.0", got)
	}
	if got := domain.ComputeCoverage(999, 1000); got != 0.999 {
		t.Fatalf("ComputeCoverage(999, 1000) = %v, want 0.999", got)
	}
	if got := domain.ComputeCoverage(1000, 1000); got != 1.0 {
		t.Fatalf("ComputeCoverage(1000, 1000) = %v, want 1.0", got)
	}
	if !domain.StatusSkipped.IsTerminal() {
		t.Fatalf("SKIPPED must be treated as terminal")
	}
}

// T009: Classification missing / stuck(>2h) / terminal / in-flight and created_at exclusion.
func TestClassifyExecutionAndCreatedAtExclusion(t *testing.T) {
	t.Parallel()

	date := domain.RunDate("2026-11-21")
	now := time.Date(2026, 11, 22, 0, 30, 0, 0, time.UTC)
	staleAfter := 2 * time.Hour

	// Missing execution
	if got := domain.ClassifyExecution(nil, now, staleAfter); got != domain.ReconcileMissing {
		t.Fatalf("nil exec -> %v, want ReconcileMissing", got)
	}

	// Terminal statuses (including SKIPPED)
	for _, st := range []domain.ExecutionStatus{
		domain.StatusSucceeded,
		domain.StatusFailedPermanent,
		domain.StatusFailedExhausted,
		domain.StatusSkipped,
	} {
		snap := &domain.ExecutionSnapshot{Status: st, UpdatedAt: now.Add(-5 * time.Hour)}
		if got := domain.ClassifyExecution(snap, now, staleAfter); got != domain.ReconcileTerminal {
			t.Fatalf("terminal %s -> %v, want ReconcileTerminal", st, got)
		}
	}

	// Non-terminal older than 2h -> stuck
	stuck := &domain.ExecutionSnapshot{Status: domain.StatusEnqueued, UpdatedAt: now.Add(-2*time.Hour - time.Minute)}
	if got := domain.ClassifyExecution(stuck, now, staleAfter); got != domain.ReconcileStuck {
		t.Fatalf("old ENQUEUED -> %v, want ReconcileStuck", got)
	}

	// Non-terminal younger than 2h -> still in-flight (not stuck yet)
	fresh := &domain.ExecutionSnapshot{Status: domain.StatusInProgress, UpdatedAt: now.Add(-30 * time.Minute)}
	if got := domain.ClassifyExecution(fresh, now, staleAfter); got != domain.ReconcileInFlight {
		t.Fatalf("fresh IN_PROGRESS -> %v, want ReconcileInFlight", got)
	}

	// created_at exclusion: slot 600 = 10:00 UTC.
	// Created at 09:59 UTC -> expected. Created at 10:05 UTC -> excluded for day D.
	before := time.Date(2026, 11, 21, 9, 59, 0, 0, time.UTC)
	after := time.Date(2026, 11, 21, 10, 5, 0, 0, time.UTC)
	if !domain.ShouldExpectExecution(before, date, 600, time.UTC) {
		t.Fatalf("created before slot should be expected")
	}
	if domain.ShouldExpectExecution(after, date, 600, time.UTC) {
		t.Fatalf("created after slot must be excluded from day D")
	}
}
