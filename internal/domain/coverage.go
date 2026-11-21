package domain

import "time"

// ReconcileClass classifies an expected daily request during reconciliation (005 US1).
type ReconcileClass int

// ReconcileClass values.
const (
	ReconcileTerminal ReconcileClass = iota
	ReconcileMissing
	ReconcileStuck
	ReconcileInFlight
)

// ExecutionSnapshot is the minimal execution state needed for pure reconciliation classification.
type ExecutionSnapshot struct {
	RequestID  string
	Status     ExecutionStatus
	ErrorClass string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// ComputeCoverage returns terminal / expected in [0, 1] (1.0 when expected == 0).
func ComputeCoverage(terminal, expected int) float64 {
	if expected <= 0 {
		return 1.0
	}
	if terminal <= 0 {
		return 0.0
	}
	if terminal >= expected {
		return 1.0
	}
	return float64(terminal) / float64(expected)
}

// SlotStartTime returns the wall-clock start time of slot in date using tz (UTC when tz is nil).
func SlotStartTime(date RunDate, slot int, tz *time.Location) time.Time {
	if tz == nil {
		tz = time.UTC
	}
	base, err := date.Time()
	if err != nil {
		return time.Time{}
	}
	dayStart := time.Date(base.Year(), base.Month(), base.Day(), 0, 0, 0, 0, tz)
	return dayStart.Add(time.Duration(slot) * time.Minute)
}

// ShouldExpectExecution reports whether a request created at createdAt is expected to run
// on date at slot (false if created strictly after the slot started, 005 Edge Cases).
func ShouldExpectExecution(createdAt time.Time, date RunDate, slot int, tz *time.Location) bool {
	if createdAt.IsZero() {
		return true
	}
	slotStart := SlotStartTime(date, slot, tz)
	if slotStart.IsZero() {
		return true
	}
	return !createdAt.After(slotStart)
}

// ClassifyExecution classifies a single expected request against its ExecutionSnapshot (nil if missing).
func ClassifyExecution(snap *ExecutionSnapshot, now time.Time, staleAfter time.Duration) ReconcileClass {
	if snap == nil {
		return ReconcileMissing
	}
	if snap.Status.IsTerminal() {
		return ReconcileTerminal
	}
	ref := snap.UpdatedAt
	if ref.IsZero() {
		ref = snap.CreatedAt
	}
	if staleAfter > 0 && now.Sub(ref) > staleAfter {
		return ReconcileStuck
	}
	return ReconcileInFlight
}
