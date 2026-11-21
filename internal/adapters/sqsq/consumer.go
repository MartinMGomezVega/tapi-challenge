package sqsq

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/martingomezvega/tapi-challenge/internal/app/execute"
)

// SQSAPI is the minimal AWS SQS client surface needed by Consumer.
type SQSAPI interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}

// Handler processes one SQS FIFO message.
type Handler interface {
	Execute(ctx context.Context, msg execute.Message) error
}

// ConsumerConfig configures the SQS FIFO consumer pipeline (003 FR-001, FR-005, US4, US5).
type ConsumerConfig struct {
	QueueURL          string
	Receivers         int
	MaxInflight       int
	WaitTimeSeconds   int32
	VisibilityTimeout time.Duration
	ShutdownTimeout   time.Duration
}

// Consumer polls SQS FIFO, pre-acquires semaphore slots, groups batches by MessageGroupId
// for sequential per-provider execution, runs visibility heartbeats, and shuts down gracefully.
type Consumer struct{}

// NewConsumer constructs an SQS FIFO Consumer.
func NewConsumer(_ SQSAPI, _ ConsumerConfig, _ *slog.Logger) *Consumer { return &Consumer{} }

// Run starts receiver goroutines until ctx is cancelled, then drains inflight messages cleanly.
func (c *Consumer) Run(_ context.Context, _ Handler) error {
	return errors.New("not implemented")
}

// Ack deletes the message from SQS.
func (c *Consumer) Ack(_ context.Context, _ execute.Message) error {
	return errors.New("not implemented")
}

// Nack releases or delays visibility of the message in SQS.
func (c *Consumer) Nack(_ context.Context, _ execute.Message, _ time.Duration) error {
	return errors.New("not implemented")
}

// Extend extends visibility timeout for the message in SQS.
func (c *Consumer) Extend(_ context.Context, _ execute.Message, _ time.Duration) error {
	return errors.New("not implemented")
}
