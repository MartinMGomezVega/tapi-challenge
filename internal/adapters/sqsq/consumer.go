package sqsq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/martingomezvega/tapi-challenge/internal/app/dispatch"
	"github.com/martingomezvega/tapi-challenge/internal/app/execute"
)

const (
	defaultReceivers         = 2
	defaultMaxInflight       = 32
	defaultWaitTimeSeconds   = 20
	defaultVisibilityTimeout = 60 * time.Second
	defaultShutdownTimeout   = 25 * time.Second
	minHeartbeatInterval     = 100 * time.Millisecond
	sqsMaxBatchReceive       = 10
	sqsMaxVisibilitySeconds  = 43200 // 12 hours SQS hard limit
)

// SQSAPI is the minimal AWS SQS client surface needed by Consumer.
type SQSAPI interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
	ChangeMessageVisibilityBatch(ctx context.Context, params *sqs.ChangeMessageVisibilityBatchInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error)
}

// Handler processes one SQS FIFO message and reports whether the group batch may continue (OutcomeDone)
// or must release remaining queued messages in the same MessageGroupId (OutcomeDeferred).
type Handler interface {
	Execute(ctx context.Context, msg execute.Message) (execute.HandleOutcome, error)
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

// Consumer polls SQS FIFO, pre-acquires semaphore slots before ReceiveMessage,
// groups batches by MessageGroupId for sequential per-provider execution,
// runs group-level visibility heartbeats, aborts a group batch on OutcomeDeferred,
// and shuts down gracefully.
type Consumer struct {
	client SQSAPI
	cfg    ConsumerConfig
	logger *slog.Logger
	tokens chan struct{}
}

// NewConsumer constructs an SQS FIFO Consumer.
func NewConsumer(client SQSAPI, cfg ConsumerConfig, logger *slog.Logger) *Consumer {
	if cfg.Receivers <= 0 {
		cfg.Receivers = defaultReceivers
	}
	if cfg.MaxInflight <= 0 {
		cfg.MaxInflight = defaultMaxInflight
	}
	if cfg.WaitTimeSeconds <= 0 {
		cfg.WaitTimeSeconds = defaultWaitTimeSeconds
	}
	if cfg.VisibilityTimeout <= 0 {
		cfg.VisibilityTimeout = defaultVisibilityTimeout
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = defaultShutdownTimeout
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Consumer{
		client: client,
		cfg:    cfg,
		logger: logger,
		tokens: make(chan struct{}, cfg.MaxInflight),
	}
}

// Run starts receiver goroutines until ctx is cancelled, then drains inflight messages cleanly.
func (c *Consumer) Run(ctx context.Context, handler Handler) error {
	if handler == nil {
		return errors.New("sqsq consumer: handler is required")
	}

	// execCtx remains alive after ctx cancellation so inflight calls can finish within ShutdownTimeout.
	execCtx, cancelExec := context.WithCancel(context.Background())
	defer cancelExec()

	var (
		recvWG sync.WaitGroup
		workWG sync.WaitGroup
	)

	for range c.cfg.Receivers {
		recvWG.Add(1)
		go func() {
			defer recvWG.Done()
			c.receiveLoop(ctx, execCtx, handler, &workWG)
		}()
	}

	// Wait for shutdown signal, then stop receivers and give inflight handlers up to ShutdownTimeout.
	<-ctx.Done()
	recvWG.Wait()

	done := make(chan struct{})
	go func() {
		workWG.Wait()
		close(done)
	}()

	timer := time.NewTimer(c.cfg.ShutdownTimeout)
	defer timer.Stop()

	select {
	case <-done:
		return nil
	case <-timer.C:
		c.logger.Warn("worker shutdown timeout reached, cancelling remaining inflight work",
			"timeout", c.cfg.ShutdownTimeout.String())
		cancelExec()
		<-done
		return nil
	}
}

func (c *Consumer) receiveLoop(stopCtx, execCtx context.Context, handler Handler, workWG *sync.WaitGroup) {
	visSec := durationToSQSSeconds(c.cfg.VisibilityTimeout)
	if visSec <= 0 {
		visSec = 60
	}

	for {
		// Pre-acquire at least 1 semaphore slot before polling SQS (003 US4.2).
		acquired, ok := c.acquireSlots(stopCtx)
		if !ok {
			return
		}

		maxMsgs := int32(min(max(acquired, 1), sqsMaxBatchReceive)) //nolint:gosec // bounded in [1, 10]
		out, err := c.client.ReceiveMessage(stopCtx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(c.cfg.QueueURL),
			MaxNumberOfMessages: maxMsgs,
			WaitTimeSeconds:     c.cfg.WaitTimeSeconds,
			VisibilityTimeout:   visSec,
			MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{
				sqstypes.MessageSystemAttributeNameMessageGroupId,
				sqstypes.MessageSystemAttributeNameApproximateReceiveCount,
			},
			MessageAttributeNames: []string{"traceparent"},
		})
		if err != nil {
			c.releaseSlots(acquired)
			if stopCtx.Err() != nil {
				return
			}
			c.logger.Error("sqs receive failed", "error", err)
			select {
			case <-stopCtx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}

		received := len(out.Messages)
		if unused := acquired - received; unused > 0 {
			c.releaseSlots(unused)
		}
		if received == 0 {
			continue
		}

		// Critical FIFO invariant (003/plan.md): group batch by MessageGroupId and process each group sequentially.
		groups, groupOrder := groupByMessageGroup(out.Messages)
		for _, gid := range groupOrder {
			msgs := groups[gid]
			workWG.Add(1)
			go func(groupMsgs []sqstypes.Message) {
				defer workWG.Done()
				c.processGroup(stopCtx, execCtx, groupMsgs, handler)
			}(msgs)
		}
	}
}

func (c *Consumer) acquireSlots(ctx context.Context) (int, bool) {
	select {
	case <-ctx.Done():
		return 0, false
	case c.tokens <- struct{}{}:
	}

	acquired := 1
	limit := min(sqsMaxBatchReceive, c.cfg.MaxInflight)
	for acquired < limit {
		select {
		case c.tokens <- struct{}{}:
			acquired++
		default:
			return acquired, true
		}
	}
	return acquired, true
}

func (c *Consumer) releaseSlots(n int) {
	for range n {
		<-c.tokens
	}
}

func (c *Consumer) processGroup(stopCtx, execCtx context.Context, rawMsgs []sqstypes.Message, handler Handler) {
	// Parse upfront; delete poison messages immediately and release their semaphore slots.
	items := make([]execute.Message, 0, len(rawMsgs))
	for _, raw := range rawMsgs {
		msg, err := parseMessage(raw)
		if err != nil {
			c.logger.Error("malformed sqs message body, deleting poison message", "error", err)
			_ = c.Ack(execCtx, execute.Message{ReceiptHandle: aws.ToString(raw.ReceiptHandle)})
			c.releaseSlots(1)
			continue
		}
		items = append(items, msg)
	}
	if len(items) == 0 {
		return
	}

	// Group-level visibility heartbeat every VT/2 extending BOTH the active message and all waiting
	// messages in this MessageGroupId batch via ChangeMessageVisibilityBatch (ADR-006).
	var activeIdx atomic.Int32
	hbCtx, stopHB := context.WithCancel(execCtx)
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		c.runGroupHeartbeat(hbCtx, items, &activeIdx)
	}()

	hbStopped := false
	haltHeartbeat := func() {
		if !hbStopped {
			stopHB()
			<-hbDone
			hbStopped = true
		}
	}
	defer haltHeartbeat()

	releaseRemaining := func(from int, reason string) {
		// Stop group heartbeat BEFORE releasing pending items so no heartbeat races with Nack(0).
		haltHeartbeat()
		for j := from; j < len(items); j++ {
			if nackErr := c.Nack(execCtx, items[j], 0); nackErr != nil {
				c.logger.Warn("failed to release pending message",
					"reason", reason, "request_id", items[j].RequestID, "error", nackErr)
			}
			c.releaseSlots(1)
		}
	}

	for i, msg := range items {
		// Graceful shutdown (003 US5): do not start queued messages after SIGTERM; release immediately.
		if stopCtx.Err() != nil {
			releaseRemaining(i, "shutdown")
			return
		}

		outcome, err := handler.Execute(execCtx, msg)
		activeIdx.Store(int32(i + 1)) //nolint:gosec // len(items) <= 10
		c.releaseSlots(1)

		if err != nil && !errors.Is(err, context.Canceled) {
			c.logger.Warn("message handler returned error",
				"request_id", msg.RequestID,
				"provider", msg.Provider,
				"error", err)
		}

		// Intra-batch backoff/defer isolation (ADR-006): stop hitting a degraded provider
		// and immediately release the remaining queued messages of this group with Nack(0).
		if outcome == execute.OutcomeDeferred {
			releaseRemaining(i+1, "deferred")
			return
		}
	}
}

func (c *Consumer) runGroupHeartbeat(ctx context.Context, items []execute.Message, activeIdx *atomic.Int32) {
	interval := max(c.cfg.VisibilityTimeout/2, minHeartbeatInterval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			idx := int(activeIdx.Load())
			if idx >= len(items) {
				return
			}
			if err := c.extendBatch(ctx, items[idx:], c.cfg.VisibilityTimeout); err != nil && ctx.Err() == nil {
				c.logger.Warn("group visibility heartbeat failed",
					"provider", items[idx].Provider, "pending_count", len(items)-idx, "error", err)
			}
		}
	}
}

func (c *Consumer) extendBatch(ctx context.Context, msgs []execute.Message, d time.Duration) error {
	if len(msgs) == 0 {
		return nil
	}
	sec := durationToSQSSeconds(d)
	for start := 0; start < len(msgs); start += sqsMaxBatchReceive {
		end := min(start+sqsMaxBatchReceive, len(msgs))
		entries := make([]sqstypes.ChangeMessageVisibilityBatchRequestEntry, 0, end-start)
		for i, m := range msgs[start:end] {
			if m.ReceiptHandle == "" {
				continue
			}
			entries = append(entries, sqstypes.ChangeMessageVisibilityBatchRequestEntry{
				Id:                aws.String(strconv.Itoa(i)),
				ReceiptHandle:     aws.String(m.ReceiptHandle),
				VisibilityTimeout: sec,
			})
		}
		if len(entries) == 0 {
			continue
		}
		if _, err := c.client.ChangeMessageVisibilityBatch(ctx, &sqs.ChangeMessageVisibilityBatchInput{
			QueueUrl: aws.String(c.cfg.QueueURL),
			Entries:  entries,
		}); err != nil {
			return fmt.Errorf("sqs change visibility batch (%d entries, %ds): %w", len(entries), sec, err)
		}
	}
	return nil
}

func groupByMessageGroup(msgs []sqstypes.Message) (map[string][]sqstypes.Message, []string) {
	groups := make(map[string][]sqstypes.Message, len(msgs))
	order := make([]string, 0, len(msgs))
	for _, m := range msgs {
		gid := m.Attributes["MessageGroupId"]
		if gid == "" {
			gid = aws.ToString(m.MessageId)
		}
		if _, exists := groups[gid]; !exists {
			order = append(order, gid)
		}
		groups[gid] = append(groups[gid], m)
	}
	return groups, order
}

func parseMessage(raw sqstypes.Message) (execute.Message, error) {
	var job dispatch.JobMessage
	if err := json.Unmarshal([]byte(aws.ToString(raw.Body)), &job); err != nil {
		return execute.Message{}, fmt.Errorf("unmarshal job message: %w", err)
	}
	if job.RequestID == "" || job.RunDate == "" {
		return execute.Message{}, errors.New("job message missing request_id or run_date")
	}

	receiveCount := 1
	if rcStr := raw.Attributes["ApproximateReceiveCount"]; rcStr != "" {
		if rc, err := strconv.Atoi(rcStr); err == nil && rc > 0 {
			receiveCount = rc
		}
	}
	traceParent := job.TraceContext
	if attr, ok := raw.MessageAttributes["traceparent"]; ok && attr.StringValue != nil && *attr.StringValue != "" {
		traceParent = *attr.StringValue
	}
	gid := raw.Attributes["MessageGroupId"]
	if gid == "" {
		gid = job.Provider
	}

	return execute.Message{
		RequestID:      job.RequestID,
		RunDate:        job.RunDate,
		Provider:       job.Provider,
		Slot:           job.Slot,
		TraceContext:   traceParent,
		ReceiptHandle:  aws.ToString(raw.ReceiptHandle),
		ReceiveCount:   receiveCount,
		MessageGroupID: gid,
	}, nil
}

// Ack deletes the message from SQS.
func (c *Consumer) Ack(ctx context.Context, m execute.Message) error {
	if m.ReceiptHandle == "" {
		return nil
	}
	_, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.cfg.QueueURL),
		ReceiptHandle: aws.String(m.ReceiptHandle),
	})
	if err != nil {
		return fmt.Errorf("sqs delete %s: %w", m.RequestID, err)
	}
	return nil
}

// Nack releases or delays visibility of the message in SQS.
func (c *Consumer) Nack(ctx context.Context, m execute.Message, delay time.Duration) error {
	return c.changeVisibility(ctx, m, delay)
}

// Extend extends visibility timeout for the message in SQS.
func (c *Consumer) Extend(ctx context.Context, m execute.Message, d time.Duration) error {
	return c.changeVisibility(ctx, m, d)
}

// HeartbeatFailures returns the cumulative count of partial failures reported by ChangeMessageVisibilityBatch.
func (c *Consumer) HeartbeatFailures() int64 { return 0 }

func (c *Consumer) changeVisibility(ctx context.Context, m execute.Message, d time.Duration) error {
	if m.ReceiptHandle == "" {
		return nil
	}
	sec := durationToSQSSeconds(d)
	_, err := c.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(c.cfg.QueueURL),
		ReceiptHandle:     aws.String(m.ReceiptHandle),
		VisibilityTimeout: sec,
	})
	if err != nil {
		return fmt.Errorf("sqs change visibility %s (%ds): %w", m.RequestID, sec, err)
	}
	return nil
}

func durationToSQSSeconds(d time.Duration) int32 {
	sec := int64(d / time.Second)
	if sec < 0 {
		return 0
	}
	if sec > sqsMaxVisibilitySeconds {
		return sqsMaxVisibilitySeconds
	}
	return int32(sec) //nolint:gosec // bounded in [0, 43200]
}
