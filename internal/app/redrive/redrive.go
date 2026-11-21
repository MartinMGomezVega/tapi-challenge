// Package redrive implements the DLQ operator redrive use case (004 US5, FR-007).
package redrive

import (
	"context"
	"fmt"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

const defaultBatchSize = 10

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

// DedupID returns the canonical idempotency/deduplication key (request_id#run_date).
func (m DLQMessage) DedupID() string {
	return domain.IdempotencyKey(m.RequestID, m.RunDate)
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
type UseCase struct {
	dlq      DLQSource
	sender   MainQueueSender
	resetter ExecutionResetter
	now      func() time.Time
}

// New constructs a redrive UseCase.
func New(dlq DLQSource, sender MainQueueSender, resetter ExecutionResetter, now func() time.Time) *UseCase {
	if now == nil {
		now = time.Now
	}
	return &UseCase{
		dlq:      dlq,
		sender:   sender,
		resetter: resetter,
		now:      now,
	}
}

// Run drains matching messages from the DLQ, resets executions to ENQUEUED, and re-enqueues them.
func (u *UseCase) Run(ctx context.Context, filter Filter) (Report, error) {
	var rep Report
	for {
		if filter.MaxCount > 0 && rep.Redriven >= filter.MaxCount {
			return rep, nil
		}
		batch, err := u.dlq.ReceiveBatch(ctx, defaultBatchSize)
		if err != nil {
			return rep, fmt.Errorf("receive dlq batch: %w", err)
		}
		if len(batch) == 0 {
			return rep, nil
		}

		for _, msg := range batch {
			if filter.MaxCount > 0 && rep.Redriven >= filter.MaxCount {
				return rep, nil
			}
			rep.Inspected++
			if !matchesFilter(msg, filter) {
				rep.Skipped++
				continue
			}
			if filter.DryRun {
				rep.Redriven++
				continue
			}
			if err := u.resetter.ResetForRedrive(ctx, msg.RequestID, msg.RunDate, u.now().UTC()); err != nil {
				return rep, fmt.Errorf("reset execution %s: %w", msg.DedupID(), err)
			}
			if err := u.sender.Send(ctx, msg); err != nil {
				return rep, fmt.Errorf("re-enqueue %s: %w", msg.DedupID(), err)
			}
			if err := u.dlq.Delete(ctx, msg.ReceiptHandle); err != nil {
				return rep, fmt.Errorf("delete dlq %s: %w", msg.DedupID(), err)
			}
			rep.Redriven++
		}
	}
}

func matchesFilter(msg DLQMessage, f Filter) bool {
	if f.Provider != "" && msg.Provider != f.Provider {
		return false
	}
	if f.Since != "" && string(msg.RunDate) < string(f.Since) {
		return false
	}
	return true
}
