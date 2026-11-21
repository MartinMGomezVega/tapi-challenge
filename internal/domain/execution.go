package domain

import (
	"errors"
	"fmt"
)

// ExecutionStatus represents the lifecycle state of a daily execution (003 FR-002).
type ExecutionStatus string

// ExecutionStatus values for the daily execution lifecycle.
const (
	StatusPending         ExecutionStatus = "PENDING"
	StatusEnqueued        ExecutionStatus = "ENQUEUED"
	StatusInProgress      ExecutionStatus = "IN_PROGRESS"
	StatusRetrying        ExecutionStatus = "RETRYING"
	StatusSucceeded       ExecutionStatus = "SUCCEEDED"
	StatusFailedPermanent ExecutionStatus = "FAILED_PERMANENT"
	StatusFailedExhausted ExecutionStatus = "FAILED_EXHAUSTED"
	StatusSkipped         ExecutionStatus = "SKIPPED"
)

// MaxInlineResponseBytes is the DynamoDB inline response threshold (350 KiB; items cap at 400 KB).
const MaxInlineResponseBytes = 350 * 1024

// Domain sentinel errors for execution status transitions.
var (
	ErrInvalidStatus     = errors.New("invalid execution status")
	ErrInvalidTransition = errors.New("invalid execution status transition")
)

var validStatuses = map[ExecutionStatus]bool{
	StatusPending:         true,
	StatusEnqueued:        true,
	StatusInProgress:      true,
	StatusRetrying:        true,
	StatusSucceeded:       true,
	StatusFailedPermanent: true,
	StatusFailedExhausted: true,
	StatusSkipped:         true,
}

var allowedTransitions = map[ExecutionStatus]map[ExecutionStatus]bool{
	StatusPending: {
		StatusEnqueued:   true,
		StatusInProgress: true,
	},
	StatusEnqueued: {
		StatusInProgress: true,
	},
	StatusRetrying: {
		StatusInProgress: true,
	},
	StatusInProgress: {
		StatusInProgress:      true, // expired lease takeover
		StatusEnqueued:        true, // stale reset by reconciler (005)
		StatusRetrying:        true,
		StatusSucceeded:       true,
		StatusFailedPermanent: true,
		StatusFailedExhausted: true,
		StatusSkipped:         true,
	},
}

// Valid reports whether s is a known ExecutionStatus.
func (s ExecutionStatus) Valid() bool {
	return validStatuses[s]
}

// IsTerminal reports whether s is a terminal execution status.
func (s ExecutionStatus) IsTerminal() bool {
	switch s {
	case StatusSucceeded, StatusFailedPermanent, StatusFailedExhausted, StatusSkipped:
		return true
	default:
		return false
	}
}

// CanTransition reports whether transitioning from s to next is valid in normal execution flow.
func (s ExecutionStatus) CanTransition(next ExecutionStatus) bool {
	return allowedTransitions[s][next]
}

// Transition validates and returns the next ExecutionStatus.
func (s ExecutionStatus) Transition(next ExecutionStatus) (ExecutionStatus, error) {
	if !s.Valid() || !next.Valid() {
		return "", fmt.Errorf("%w: %q -> %q", ErrInvalidStatus, s, next)
	}
	if !s.CanTransition(next) {
		return "", fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, s, next)
	}
	return next, nil
}

// CanRedrive reports whether an execution in status s can be manually redriven from the DLQ.
func (s ExecutionStatus) CanRedrive() bool {
	return s == StatusFailedExhausted
}

// ResultBlobKey builds the deterministic S3 object key for oversized responses (003 US1.2).
func ResultBlobKey(runDate RunDate, provider, requestID string) string {
	return fmt.Sprintf("results/run_date=%s/provider=%s/%s.json", runDate, provider, requestID)
}
