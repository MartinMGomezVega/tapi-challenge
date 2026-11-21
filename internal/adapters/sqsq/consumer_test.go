package sqsq_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/martingomezvega/tapi-challenge/internal/adapters/sqsq"
	"github.com/martingomezvega/tapi-challenge/internal/app/dispatch"
	"github.com/martingomezvega/tapi-challenge/internal/app/execute"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

type fakeSQSClient struct {
	mu           sync.Mutex
	batches      [][]sqstypes.Message
	maxRequested int32
	receiveCalls int
	deleted      []string
	visChanges   map[string][]int32
	onReceive    func(maxMsgs int32)
}

func (f *fakeSQSClient) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	f.mu.Lock()
	f.receiveCalls++
	if in.MaxNumberOfMessages > f.maxRequested {
		f.maxRequested = in.MaxNumberOfMessages
	}
	cb := f.onReceive
	var batch []sqstypes.Message
	if len(f.batches) > 0 {
		batch = f.batches[0]
		f.batches = f.batches[1:]
	}
	f.mu.Unlock()

	if cb != nil {
		cb(in.MaxNumberOfMessages)
	}
	if len(batch) > 0 {
		return &sqs.ReceiveMessageOutput{Messages: batch}, nil
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(5 * time.Millisecond):
		return &sqs.ReceiveMessageOutput{}, nil
	}
}

func (f *fakeSQSClient) DeleteMessage(_ context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, aws.ToString(in.ReceiptHandle))
	return &sqs.DeleteMessageOutput{}, nil
}

func (f *fakeSQSClient) ChangeMessageVisibility(_ context.Context, in *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.visChanges == nil {
		f.visChanges = make(map[string][]int32)
	}
	rh := aws.ToString(in.ReceiptHandle)
	f.visChanges[rh] = append(f.visChanges[rh], in.VisibilityTimeout)
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func makeSQSMsg(id, provider, rh string) sqstypes.Message {
	body, _ := json.Marshal(dispatch.JobMessage{
		RequestID: id,
		RunDate:   domain.RunDate("2026-11-21"),
		Provider:  provider,
		Slot:      1,
	})
	return sqstypes.Message{
		MessageId:     aws.String("mid-" + id),
		ReceiptHandle: aws.String(rh),
		Body:          aws.String(string(body)),
		Attributes: map[string]string{
			"MessageGroupId":          provider,
			"ApproximateReceiveCount": "1",
		},
	}
}

type handlerFunc func(ctx context.Context, msg execute.Message) error

func (f handlerFunc) Execute(ctx context.Context, msg execute.Message) error { return f(ctx, msg) }

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// T014: Batch containing multiple messages of the same MessageGroupId runs strictly sequentially.
func TestConsumer_SameGroupProcessedSequentially(t *testing.T) {
	t.Parallel()

	var batch []sqstypes.Message
	for i := range 6 {
		batch = append(batch, makeSQSMsg("req-"+string(rune('a'+i)), "edenor", "rh-"+string(rune('a'+i))))
	}
	fake := &fakeSQSClient{batches: [][]sqstypes.Message{batch}}

	consumer := sqsq.NewConsumer(fake, sqsq.ConsumerConfig{
		QueueURL:          "https://sqs.local/q.fifo",
		Receivers:         1,
		MaxInflight:       10,
		VisibilityTimeout: 60 * time.Second,
		ShutdownTimeout:   5 * time.Second,
	}, discardLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		inflight    atomic.Int32
		maxInflight atomic.Int32
		processed   atomic.Int32
		mu          sync.Mutex
		order       []string
	)

	h := handlerFunc(func(ctx context.Context, msg execute.Message) error {
		cur := inflight.Add(1)
		for {
			prev := maxInflight.Load()
			if cur <= prev || maxInflight.CompareAndSwap(prev, cur) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		mu.Lock()
		order = append(order, msg.RequestID)
		mu.Unlock()
		inflight.Add(-1)
		if processed.Add(1) == 6 {
			cancel()
		}
		return consumer.Ack(ctx, msg)
	})

	if err := consumer.Run(ctx, h); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if maxInflight.Load() != 1 {
		t.Fatalf("same-group maxInflight = %d, want 1", maxInflight.Load())
	}
	if len(order) != 6 {
		t.Fatalf("processed = %d, want 6", len(order))
	}
	for i := range 6 {
		want := "req-" + string(rune('a'+i))
		if order[i] != want {
			t.Fatalf("order[%d] = %s, want %s (FIFO order broken)", i, order[i], want)
		}
	}
}

// T015: Distinct MessageGroupIds execute in parallel.
func TestConsumer_DistinctGroupsProcessedInParallel(t *testing.T) {
	t.Parallel()

	batch := []sqstypes.Message{
		makeSQSMsg("r1", "prov-1", "rh-1"),
		makeSQSMsg("r2", "prov-2", "rh-2"),
		makeSQSMsg("r3", "prov-3", "rh-3"),
	}
	fake := &fakeSQSClient{batches: [][]sqstypes.Message{batch}}

	consumer := sqsq.NewConsumer(fake, sqsq.ConsumerConfig{
		QueueURL:          "https://sqs.local/q.fifo",
		Receivers:         1,
		MaxInflight:       6,
		VisibilityTimeout: 60 * time.Second,
		ShutdownTimeout:   5 * time.Second,
	}, discardLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Barrier: all 3 groups must enter Execute concurrently before any can return.
	var entered atomic.Int32
	allEntered := make(chan struct{})
	var done atomic.Int32

	h := handlerFunc(func(ctx context.Context, msg execute.Message) error {
		if entered.Add(1) == 3 {
			close(allEntered)
		}
		select {
		case <-allEntered:
		case <-ctx.Done():
			return ctx.Err()
		}
		if done.Add(1) == 3 {
			cancel()
		}
		return consumer.Ack(ctx, msg)
	})

	if err := consumer.Run(ctx, h); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if entered.Load() != 3 {
		t.Fatalf("expected all 3 distinct groups to run concurrently, entered=%d", entered.Load())
	}
}

// T022: Receiver never requests more messages than available semaphore slots.
func TestConsumer_PreAcquiresSemaphoreSlotsBeforeReceive(t *testing.T) {
	t.Parallel()

	batch := []sqstypes.Message{
		makeSQSMsg("r1", "p1", "rh-1"),
		makeSQSMsg("r2", "p2", "rh-2"),
	}
	fake := &fakeSQSClient{batches: [][]sqstypes.Message{batch}}

	// MaxInflight = 2 -> ReceiveMessage must NEVER request > 2, and while 2 are inflight must NOT poll.
	var secondPollWhileFull atomic.Bool
	var inflight atomic.Int32
	fake.onReceive = func(maxMsgs int32) {
		if maxMsgs > 2 {
			t.Errorf("requested %d messages with MaxInflight=2", maxMsgs)
		}
		if inflight.Load() >= 2 {
			secondPollWhileFull.Store(true)
		}
	}

	consumer := sqsq.NewConsumer(fake, sqsq.ConsumerConfig{
		QueueURL:          "https://sqs.local/q.fifo",
		Receivers:         1,
		MaxInflight:       2,
		VisibilityTimeout: 60 * time.Second,
		ShutdownTimeout:   5 * time.Second,
	}, discardLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var done atomic.Int32
	h := handlerFunc(func(ctx context.Context, msg execute.Message) error {
		inflight.Add(1)
		time.Sleep(25 * time.Millisecond)
		inflight.Add(-1)
		if done.Add(1) == 2 {
			cancel()
		}
		return consumer.Ack(ctx, msg)
	})

	if err := consumer.Run(ctx, h); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if secondPollWhileFull.Load() {
		t.Fatalf("receiver polled while semaphore was full")
	}
	if fake.maxRequested > 2 {
		t.Fatalf("maxRequested = %d, want <= 2", fake.maxRequested)
	}
}

// T025: Graceful shutdown finishes inflight work and Nacks(0) unstarted queued items in same group.
func TestConsumer_GracefulShutdownFinishesInflightAndNacksPending(t *testing.T) {
	t.Parallel()

	// 2 messages in the same group: msg1 starts, SIGTERM cancels ctx while msg1 runs,
	// msg1 finishes and Acks, msg2 was never started and must be Nacked with visibility=0.
	batch := []sqstypes.Message{
		makeSQSMsg("r1", "edenor", "rh-1"),
		makeSQSMsg("r2", "edenor", "rh-2"),
	}
	fake := &fakeSQSClient{batches: [][]sqstypes.Message{batch}}

	consumer := sqsq.NewConsumer(fake, sqsq.ConsumerConfig{
		QueueURL:          "https://sqs.local/q.fifo",
		Receivers:         1,
		MaxInflight:       4,
		VisibilityTimeout: 60 * time.Second,
		ShutdownTimeout:   5 * time.Second,
	}, discardLogger())

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	var completed []string
	var mu sync.Mutex

	h := handlerFunc(func(execCtx context.Context, msg execute.Message) error {
		if msg.RequestID == "r1" {
			close(started)
			time.Sleep(30 * time.Millisecond)
		}
		mu.Lock()
		completed = append(completed, msg.RequestID)
		mu.Unlock()
		return consumer.Ack(execCtx, msg)
	})

	go func() {
		<-started
		cancel()
	}()

	if err := consumer.Run(ctx, h); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(completed) != 1 || completed[0] != "r1" {
		t.Fatalf("completed = %v, want [r1] only", completed)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	vis := fake.visChanges["rh-2"]
	if len(vis) == 0 || vis[len(vis)-1] != 0 {
		t.Fatalf("expected rh-2 to be Nack(0), got visChanges=%v", fake.visChanges)
	}
}
