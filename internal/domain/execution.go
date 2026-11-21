package domain

import "errors"

// ExecutionStatus represents the lifecycle state of a daily execution (003 FR-002).
type ExecutionStatus string

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

var (
	ErrInvalidStatus     = errors.New("invalid execution status")
	ErrInvalidTransition = errors.New("invalid execution status transition")
)

// Valid reports whether s is a known ExecutionStatus.
func (s ExecutionStatus) Valid() bool { return false }

// IsTerminal reports whether s is a terminal execution status.
func (s ExecutionStatus) IsTerminal() bool { return false }

// CanTransition reports whether transitioning from s to next is valid in normal execution flow.
func (s ExecutionStatus) CanTransition(next ExecutionStatus) bool { return false }

// Transition validates and returns the next ExecutionStatus.
func (s ExecutionStatus) Transition(next ExecutionStatus) (ExecutionStatus, error) {
	return "", ErrInvalidTransition
}

// CanRedrive reports whether an execution in status s can be manually redriven from the DLQ.
func (s ExecutionStatus) CanRedrive() bool { return false }

// ResultBlobKey builds the deterministic S3 object key for oversized responses (003 US1.2).
func ResultBlobKey(runDate RunDate, provider, requestID string) string { return "" }
