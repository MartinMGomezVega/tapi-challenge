package sqsq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/martingomezvega/tapi-challenge/internal/app/dispatch"
	"github.com/martingomezvega/tapi-challenge/internal/app/redrive"
)

// DLQAdapter bridges SQS FIFO DLQ and main queue for operator redrive (004 FR-007).
type DLQAdapter struct {
	client       *sqs.Client
	dlqURL       string
	mainQueueURL string
}

// NewDLQAdapter builds an SQS FIFO adapter for the redrive use case.
func NewDLQAdapter(client *sqs.Client, dlqURL, mainQueueURL string) *DLQAdapter {
	return &DLQAdapter{
		client:       client,
		dlqURL:       dlqURL,
		mainQueueURL: mainQueueURL,
	}
}

// ReceiveBatch reads up to maxMessages from the FIFO DLQ.
func (a *DLQAdapter) ReceiveBatch(ctx context.Context, maxMessages int) ([]redrive.DLQMessage, error) {
	n := int32(min(max(maxMessages, 1), sqsMaxBatchReceive)) //nolint:gosec // bounded in [1, 10]
	out, err := a.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(a.dlqURL),
		MaxNumberOfMessages: n,
		WaitTimeSeconds:     1,
		MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{
			sqstypes.MessageSystemAttributeNameMessageGroupId,
		},
		MessageAttributeNames: []string{"traceparent"},
	})
	if err != nil {
		return nil, fmt.Errorf("dlq receive: %w", err)
	}
	res := make([]redrive.DLQMessage, 0, len(out.Messages))
	for _, raw := range out.Messages {
		parsed, err := parseMessage(raw)
		if err != nil {
			continue
		}
		res = append(res, redrive.DLQMessage{
			RequestID:      parsed.RequestID,
			RunDate:        parsed.RunDate,
			Provider:       parsed.Provider,
			Slot:           parsed.Slot,
			TraceContext:   parsed.TraceContext,
			ReceiptHandle:  parsed.ReceiptHandle,
			MessageGroupID: parsed.MessageGroupID,
		})
	}
	return res, nil
}

// Delete removes a redriven message from the DLQ.
func (a *DLQAdapter) Delete(ctx context.Context, receiptHandle string) error {
	_, err := a.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(a.dlqURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	if err != nil {
		return fmt.Errorf("dlq delete: %w", err)
	}
	return nil
}

// Send re-enqueues a redriven message onto the main FIFO queue preserving MessageGroupId and MessageDeduplicationId.
func (a *DLQAdapter) Send(ctx context.Context, msg redrive.DLQMessage) error {
	body, err := json.Marshal(dispatch.JobMessage{
		RequestID:    msg.RequestID,
		RunDate:      msg.RunDate,
		Provider:     msg.Provider,
		Slot:         msg.Slot,
		DispatchedAt: time.Now().UTC(),
		TraceContext: msg.TraceContext,
	})
	if err != nil {
		return fmt.Errorf("marshal redrive message %s: %w", msg.DedupID(), err)
	}
	groupID := msg.MessageGroupID
	if groupID == "" {
		groupID = msg.Provider
	}
	in := &sqs.SendMessageInput{
		QueueUrl:               aws.String(a.mainQueueURL),
		MessageBody:            aws.String(string(body)),
		MessageGroupId:         aws.String(groupID),
		MessageDeduplicationId: aws.String(msg.DedupID()),
	}
	if msg.TraceContext != "" {
		in.MessageAttributes = map[string]sqstypes.MessageAttributeValue{
			"traceparent": {DataType: aws.String("String"), StringValue: aws.String(msg.TraceContext)},
		}
	}
	if _, err := a.client.SendMessage(ctx, in); err != nil {
		return fmt.Errorf("redrive send %s: %w", msg.DedupID(), err)
	}
	return nil
}
