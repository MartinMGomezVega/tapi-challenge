//go:build integration

package sqsq_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/martingomezvega/tapi-challenge/internal/adapters/sqsq"
	"github.com/martingomezvega/tapi-challenge/internal/app/dispatch"
	"github.com/martingomezvega/tapi-challenge/test/testenv"
)

// T012: FIFO SendBatch sets MessageGroupId=provider, MessageDeduplicationId=id#date, and deduplicates.
func TestQueue_SendBatch_FIFO_Integration(t *testing.T) {
	env := testenv.StartLocalStack(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	qURL, _, err := env.EnsureFIFOQueue(ctx, "it-dispatch.fifo", "it-dispatch-dlq.fifo")
	if err != nil {
		t.Fatalf("EnsureFIFOQueue: %v", err)
	}

	q := sqsq.NewQueue(env.SQS, qURL)
	ts := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

	msgs := []dispatch.JobMessage{
		{RequestID: "req-1", RunDate: "2026-11-20", Provider: "edenor", Slot: 720, DispatchedAt: ts, TraceContext: "00-trace-1"},
		{RequestID: "req-2", RunDate: "2026-11-20", Provider: "metrogas", Slot: 720, DispatchedAt: ts},
	}

	failed, err := q.SendBatch(ctx, msgs)
	if err != nil || len(failed) != 0 {
		t.Fatalf("first SendBatch() = (failed=%d, err=%v), want (0, nil)", len(failed), err)
	}

	// Resending exact same messages within 5 min dedup window must be deduplicated by SQS FIFO.
	failed2, err := q.SendBatch(ctx, msgs)
	if err != nil || len(failed2) != 0 {
		t.Fatalf("duplicate SendBatch() = (failed=%d, err=%v), want (0, nil)", len(failed2), err)
	}

	out, err := env.SQS.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(qURL),
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     2,
		MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{
			sqstypes.MessageSystemAttributeNameMessageGroupId,
			sqstypes.MessageSystemAttributeNameMessageDeduplicationId,
		},
		MessageAttributeNames: []string{"All"},
	})
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	if len(out.Messages) != 2 {
		t.Fatalf("received %d messages, want 2 (duplicates must be deduplicated)", len(out.Messages))
	}

	gotByID := make(map[string]dispatch.JobMessage, 2)
	for _, raw := range out.Messages {
		var jm dispatch.JobMessage
		if err := json.Unmarshal([]byte(aws.ToString(raw.Body)), &jm); err != nil {
			t.Fatalf("unmarshal SQS body: %v", err)
		}
		if gotGroup := raw.Attributes["MessageGroupId"]; gotGroup != jm.Provider {
			t.Errorf("MessageGroupId = %q, want %q", gotGroup, jm.Provider)
		}
		if gotDedup := raw.Attributes["MessageDeduplicationId"]; gotDedup != jm.DedupID() {
			t.Errorf("MessageDeduplicationId = %q, want %q", gotDedup, jm.DedupID())
		}
		gotByID[jm.RequestID] = jm
	}
	if gotByID["req-1"].TraceContext != "00-trace-1" {
		t.Errorf("req-1 TraceContext = %q, want 00-trace-1", gotByID["req-1"].TraceContext)
	}
}
