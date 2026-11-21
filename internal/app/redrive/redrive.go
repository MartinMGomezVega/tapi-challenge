// Package redrive implements the DLQ operator redrive use case (004 US5, FR-007).
package redrive

import (
	"context"
	"errors"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

// DLQMessage represents one message read from the FIFO DLQ.
type DLQMessage struct {
	RequestID      string
	RunDate        domain.RunDate
	Provider       string
	Slot           int
	TraceContext   string
	ReceiptHandle  string
	MessageGroupID string
}

// DLQSource reads and deletes messages from the FIFO DLQ.
type DLQSource interface {
	ReceiveBatch(ctx context.Context, maxMessages int) ([]DLQMessage, error)
	Delete(ctx context.Context, receiptHandle string) error
}

// MainQueueSender re-enqueues redriven messages onto the main FIFO queue preserving DedupId and GroupId.
type MainQueueSender interface {
	Send(ctx context.Context, msg DLQMessage) error
}

// ExecutionResetter transitions a FAILED_EXHAUSTED execution back to ENQUEUED preserving historical attempts.
type ExecutionResetter interface {
	ResetForRedrive(ctx context.Context, reqID string, date domain.RunDate, now time.Time) error
}

// Filter controls which DLQ messages are redriven.
type Filter struct {
	Provider string
	Since    domain.RunDate // inclusive; empty means all dates
	DryRun   bool
	MaxCount int // 0 means unlimited
}

// Report summarizes a redrive run.
type Report struct {
	Inspected int
	Redriven  int
	Skipped   int
}

// UseCase orchestrates filtered DLQ redrive (004 FR-007).
type UseCase struct{}

// New constructs a redrive UseCase.
func New(_ DLQSource, _ MainQueueSender, _ ExecutionResetter, _ func() time.Time) *UseCase {
	return &UseCase{}
}

// Run drains matching messages from the DLQ, resets executions to ENQUEUED, and re-enqueues them.
func (u *UseCase) Run(_ context.Context, _ Filter) (Report, error) {
	return Report{}, errors.New("not implemented")
}
