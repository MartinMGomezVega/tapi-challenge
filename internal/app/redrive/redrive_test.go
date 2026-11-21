package redrive_test

import (
	"context"
	"testing"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/app/redrive"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

type fakeDLQ struct {
	batches [][]redrive.DLQMessage
	deleted []string
}

func (f *fakeDLQ) ReceiveBatch(_ context.Context, _ int) ([]redrive.DLQMessage, error) {
	if len(f.batches) == 0 {
		return nil, nil
	}
	b := f.batches[0]
	f.batches = f.batches[1:]
	return b, nil
}

func (f *fakeDLQ) Delete(_ context.Context, rh string) error {
	f.deleted = append(f.deleted, rh)
	return nil
}

type fakeSender struct {
	sent []redrive.DLQMessage
}

func (f *fakeSender) Send(_ context.Context, m redrive.DLQMessage) error {
	f.sent = append(f.sent, m)
	return nil
}

type fakeResetter struct {
	reset []string
}

func (f *fakeResetter) ResetForRedrive(_ context.Context, reqID string, date domain.RunDate, _ time.Time) error {
	f.reset = append(f.reset, domain.IdempotencyKey(reqID, date))
	return nil
}

// T021: redrive filters by provider and since date, resets execution to ENQUEUED, and re-enqueues with same DedupId.
func TestRedrive_FiltersByProviderAndSince(t *testing.T) {
	t.Parallel()

	dlq := &fakeDLQ{
		batches: [][]redrive.DLQMessage{
			{
				{RequestID: "r1", RunDate: "2026-11-20", Provider: "edenor", ReceiptHandle: "rh-1"},
				{RequestID: "r2", RunDate: "2026-11-21", Provider: "edenor", ReceiptHandle: "rh-2"},
				{RequestID: "r3", RunDate: "2026-11-21", Provider: "edesur", ReceiptHandle: "rh-3"},
				{RequestID: "r4", RunDate: "2026-11-22", Provider: "edenor", ReceiptHandle: "rh-4"},
			},
		},
	}
	sender := &fakeSender{}
	resetter := &fakeResetter{}
	now := func() time.Time { return time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC) }

	uc := redrive.New(dlq, sender, resetter, now)
	rep, err := uc.Run(context.Background(), redrive.Filter{
		Provider: "edenor",
		Since:    domain.RunDate("2026-11-21"),
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if rep.Inspected != 4 || rep.Redriven != 2 || rep.Skipped != 2 {
		t.Fatalf("unexpected report: %+v", rep)
	}
	if len(resetter.reset) != 2 || resetter.reset[0] != "r2#2026-11-21" || resetter.reset[1] != "r4#2026-11-22" {
		t.Fatalf("reset = %v", resetter.reset)
	}
	if len(sender.sent) != 2 || sender.sent[0].RequestID != "r2" || sender.sent[1].RequestID != "r4" {
		t.Fatalf("sent = %+v", sender.sent)
	}
	if len(dlq.deleted) != 2 || dlq.deleted[0] != "rh-2" || dlq.deleted[1] != "rh-4" {
		t.Fatalf("deleted = %v", dlq.deleted)
	}
}
