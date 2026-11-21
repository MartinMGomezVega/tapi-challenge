// Package sqsq implements SQS FIFO producer and consumer adapters (ADR-003).
package sqsq

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

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
func (q *Queue) SendBatch(_ context.Context, _ []dispatch.JobMessage) ([]dispatch.JobMessage, error) {
	return nil, errors.New("not implemented")
}
