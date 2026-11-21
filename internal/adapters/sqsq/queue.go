// Package sqsq implements SQS FIFO producer and consumer adapters (ADR-003).
package sqsq

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/martingomezvega/tapi-challenge/internal/app/dispatch"
)

// Queue sends batches of job messages to an SQS FIFO queue with MessageGroupId=provider
// and MessageDeduplicationId=request_id#run_date (002 FR-003).
type Queue struct {
	client   *sqs.Client
	queueURL string
}

// NewQueue creates an SQS FIFO producer targeting queueURL.
func NewQueue(client *sqs.Client, queueURL string) *Queue {
	return &Queue{client: client, queueURL: queueURL}
}

// SendBatch sends up to 10 messages via SQS SendMessageBatch and returns any failed entries.
func (q *Queue) SendBatch(ctx context.Context, msgs []dispatch.JobMessage) ([]dispatch.JobMessage, error) {
	if len(msgs) == 0 {
		return nil, nil
	}

	entries := make([]sqstypes.SendMessageBatchRequestEntry, 0, len(msgs))
	byID := make(map[string]dispatch.JobMessage, len(msgs))

	for i, m := range msgs {
		body, err := json.Marshal(m)
		if err != nil {
			return nil, fmt.Errorf("marshal job message %s: %w", m.DedupID(), err)
		}
		entryID := strconv.Itoa(i)
		byID[entryID] = m

		entry := sqstypes.SendMessageBatchRequestEntry{
			Id:                     aws.String(entryID),
			MessageBody:            aws.String(string(body)),
			MessageGroupId:         aws.String(m.Provider),
			MessageDeduplicationId: aws.String(m.DedupID()),
		}
		if m.TraceContext != "" {
			entry.MessageAttributes = map[string]sqstypes.MessageAttributeValue{
				"traceparent": {
					DataType:    aws.String("String"),
					StringValue: aws.String(m.TraceContext),
				},
			}
		}
		entries = append(entries, entry)
	}

	out, err := q.client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(q.queueURL),
		Entries:  entries,
	})
	if err != nil {
		return msgs, fmt.Errorf("sqs send message batch (%d entries): %w", len(entries), err)
	}

	if len(out.Failed) == 0 {
		return nil, nil
	}

	failed := make([]dispatch.JobMessage, 0, len(out.Failed))
	for _, f := range out.Failed {
		if m, ok := byID[aws.ToString(f.Id)]; ok {
			failed = append(failed, m)
		}
	}
	return failed, nil
}
