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

type errDLQ struct {
	recvErr error
	delErr  error
	msgs    []redrive.DLQMessage
}

func (e *errDLQ) ReceiveBatch(_ context.Context, _ int) ([]redrive.DLQMessage, error) {
	if e.recvErr != nil {
		return nil, e.recvErr
	}
	out := e.msgs
	e.msgs = nil
	return out, nil
}
func (e *errDLQ) Delete(_ context.Context, _ string) error { return e.delErr }

type errSender struct{ err error }

func (e errSender) Send(_ context.Context, _ redrive.DLQMessage) error { return e.err }

type errResetter struct{ err error }

func (e errResetter) ResetForRedrive(_ context.Context, _ string, _ domain.RunDate, _ time.Time) error {
	return e.err
}

func TestRedrive_DryRunMaxCountAndErrors(t *testing.T) {
	t.Parallel()

	sample := []redrive.DLQMessage{
		{RequestID: "r1", RunDate: "2026-11-21", Provider: "edenor", ReceiptHandle: "rh-1"},
		{RequestID: "r2", RunDate: "2026-11-21", Provider: "edenor", ReceiptHandle: "rh-2"},
	}

	// DryRun + MaxCount=1
	dlq := &fakeDLQ{batches: [][]redrive.DLQMessage{sample}}
	sender := &fakeSender{}
	resetter := &fakeResetter{}
	uc := redrive.New(dlq, sender, resetter, nil)
	rep, err := uc.Run(context.Background(), redrive.Filter{DryRun: true, MaxCount: 1})
	if err != nil || rep.Redriven != 1 || len(sender.sent) != 0 || len(resetter.reset) != 0 || len(dlq.deleted) != 0 {
		t.Fatalf("dryrun/maxcount mismatch: rep=%+v err=%v", rep, err)
	}

	// Error paths
	if _, err := redrive.New(&errDLQ{recvErr: context.Canceled}, errSender{}, errResetter{}, nil).Run(context.Background(), redrive.Filter{}); err == nil {
		t.Fatalf("expected receive error")
	}
	if _, err := redrive.New(&errDLQ{msgs: sample}, errSender{}, errResetter{err: context.Canceled}, nil).Run(context.Background(), redrive.Filter{}); err == nil {
		t.Fatalf("expected reset error")
	}
	if _, err := redrive.New(&errDLQ{msgs: sample}, errSender{err: context.Canceled}, errResetter{}, nil).Run(context.Background(), redrive.Filter{}); err == nil {
		t.Fatalf("expected send error")
	}
	if _, err := redrive.New(&errDLQ{msgs: sample, delErr: context.Canceled}, errSender{}, errResetter{}, nil).Run(context.Background(), redrive.Filter{}); err == nil {
		t.Fatalf("expected delete error")
	}
}
