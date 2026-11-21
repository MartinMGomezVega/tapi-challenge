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
func ComputeCoverage(_, _ int) float64 { return 0 }

// SlotStartTime returns the wall-clock start time of slot in date using tz (UTC when tz is nil).
func SlotStartTime(_ RunDate, _ int, _ *time.Location) time.Time { return time.Time{} }

// ShouldExpectExecution reports whether a request created at createdAt is expected to run
// on date at slot (false if created strictly after the slot started, 005 Edge Cases).
func ShouldExpectExecution(_ time.Time, _ RunDate, _ int, _ *time.Location) bool { return false }

// ClassifyExecution classifies a single expected request against its ExecutionSnapshot (nil if missing).
func ClassifyExecution(_ *ExecutionSnapshot, _ time.Time, _ time.Duration) ReconcileClass {
	return ReconcileMissing
}
