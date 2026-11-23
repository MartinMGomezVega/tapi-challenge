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
	mu                sync.Mutex
	batches           [][]sqstypes.Message
	maxRequested      int32
	receiveCalls      int
	deleted           []string
	visChanges        map[string][]int32
	batchVisChanges   map[string][]int32
	batchFailEntries  []sqstypes.BatchResultErrorEntry
	onReceive         func(maxMsgs int32)
	trackVisibility   bool
	visibilityTimeout time.Duration
	visibleAt         map[string]time.Time
	deletedSet        map[string]bool
	rawByRH           map[string]sqstypes.Message
	redelivered       []string
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
		if f.trackVisibility {
			if f.visibleAt == nil {
				f.visibleAt = make(map[string]time.Time)
				f.deletedSet = make(map[string]bool)
				f.rawByRH = make(map[string]sqstypes.Message)
			}
			vt := f.visibilityTimeout
			if vt <= 0 {
				vt = time.Duration(in.VisibilityTimeout) * time.Second
			}
			exp := time.Now().Add(vt)
			for _, m := range batch {
				rh := aws.ToString(m.ReceiptHandle)
				f.visibleAt[rh] = exp
				f.rawByRH[rh] = m
			}
		}
	} else if f.trackVisibility {
		// Simulate SQS re-delivering any non-deleted message whose visibility timeout expired.
		now := time.Now()
		for rh, exp := range f.visibleAt {
			if !f.deletedSet[rh] && !now.Before(exp) {
				f.redelivered = append(f.redelivered, rh)
				f.visibleAt[rh] = now.Add(f.visibilityTimeout)
				batch = append(batch, f.rawByRH[rh])
			}
		}
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
	case <-time.After(15 * time.Millisecond):
		return &sqs.ReceiveMessageOutput{}, nil
	}
}

func (f *fakeSQSClient) DeleteMessage(_ context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rh := aws.ToString(in.ReceiptHandle)
	f.deleted = append(f.deleted, rh)
	if f.deletedSet != nil {
		f.deletedSet[rh] = true
	}
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
	if f.visibleAt != nil && !f.deletedSet[rh] {
		f.visibleAt[rh] = time.Now().Add(time.Duration(in.VisibilityTimeout) * time.Second)
	}
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeSQSClient) ChangeMessageVisibilityBatch(_ context.Context, in *sqs.ChangeMessageVisibilityBatchInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.batchVisChanges == nil {
		f.batchVisChanges = make(map[string][]int32)
	}
	if f.visChanges == nil {
		f.visChanges = make(map[string][]int32)
	}
	now := time.Now()
	for _, entry := range in.Entries {
		rh := aws.ToString(entry.ReceiptHandle)
		f.batchVisChanges[rh] = append(f.batchVisChanges[rh], entry.VisibilityTimeout)
		f.visChanges[rh] = append(f.visChanges[rh], entry.VisibilityTimeout)
		if f.visibleAt != nil && !f.deletedSet[rh] {
			f.visibleAt[rh] = now.Add(time.Duration(entry.VisibilityTimeout) * time.Second)
		}
	}
	return &sqs.ChangeMessageVisibilityBatchOutput{
		Failed: f.batchFailEntries,
	}, nil
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

type handlerFunc func(ctx context.Context, msg execute.Message) (execute.HandleOutcome, error)

func (f handlerFunc) Execute(ctx context.Context, msg execute.Message) (execute.HandleOutcome, error) {
	return f(ctx, msg)
}

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

	h := handlerFunc(func(ctx context.Context, msg execute.Message) (execute.HandleOutcome, error) {
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
		return execute.OutcomeDone, consumer.Ack(ctx, msg)
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

	h := handlerFunc(func(ctx context.Context, msg execute.Message) (execute.HandleOutcome, error) {
		if entered.Add(1) == 3 {
			close(allEntered)
		}
		select {
		case <-allEntered:
		case <-ctx.Done():
			return execute.OutcomeDeferred, ctx.Err()
		}
		if done.Add(1) == 3 {
			cancel()
		}
		return execute.OutcomeDone, consumer.Ack(ctx, msg)
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
	h := handlerFunc(func(ctx context.Context, msg execute.Message) (execute.HandleOutcome, error) {
		inflight.Add(1)
		time.Sleep(25 * time.Millisecond)
		inflight.Add(-1)
		if done.Add(1) == 2 {
			cancel()
		}
		return execute.OutcomeDone, consumer.Ack(ctx, msg)
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

	h := handlerFunc(func(execCtx context.Context, msg execute.Message) (execute.HandleOutcome, error) {
		if msg.RequestID == "r1" {
			close(started)
			time.Sleep(30 * time.Millisecond)
		}
		mu.Lock()
		completed = append(completed, msg.RequestID)
		mu.Unlock()
		return execute.OutcomeDone, consumer.Ack(execCtx, msg)
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

// Review Finding 2: Batch of 5 messages in same MessageGroupId where #2 returns OutcomeDeferred ->
// #3, #4, #5 must NOT be executed and must receive Nack(0).
func TestConsumer_DeferredMessageAbortsRestOfGroupWithNack0(t *testing.T) {
	t.Parallel()

	var batch []sqstypes.Message
	for i := 1; i <= 5; i++ {
		id := "m" + string(rune('0'+i))
		batch = append(batch, makeSQSMsg(id, "edenor", "rh-"+id))
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
		mu       sync.Mutex
		executed []string
	)
	h := handlerFunc(func(execCtx context.Context, msg execute.Message) (execute.HandleOutcome, error) {
		mu.Lock()
		executed = append(executed, msg.RequestID)
		mu.Unlock()
		if msg.RequestID == "m2" {
			// Simulate RetryLater / BreakerOpen / FailExhausted backoff on m2.
			_ = consumer.Nack(execCtx, msg, 30*time.Second)
			// Allow processGroup to release m3..m5 before cancelling consumer loop.
			go func() {
				time.Sleep(25 * time.Millisecond)
				cancel()
			}()
			return execute.OutcomeDeferred, nil
		}
		return execute.OutcomeDone, consumer.Ack(execCtx, msg)
	})

	if err := consumer.Run(ctx, h); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(executed) != 2 || executed[0] != "m1" || executed[1] != "m2" {
		t.Fatalf("executed = %v, want [m1 m2] only (m3..m5 must not hit degraded provider)", executed)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	for _, rh := range []string{"rh-m3", "rh-m4", "rh-m5"} {
		vis := fake.visChanges[rh]
		if len(vis) == 0 || vis[len(vis)-1] != 0 {
			t.Fatalf("expected %s to receive Nack(0), got visChanges=%v", rh, fake.visChanges)
		}
	}
}

// Review Finding 3: Short VT (2s) with slow handler across a 4-message group batch (~2.8s total > VT) ->
// group-level ChangeMessageVisibilityBatch heartbeat keeps waiting messages from expiring or re-delivering.
func TestConsumer_GroupHeartbeatExtendsWaitingMessages(t *testing.T) {
	t.Parallel()

	var batch []sqstypes.Message
	for i := 1; i <= 4; i++ {
		id := "w" + string(rune('0'+i))
		batch = append(batch, makeSQSMsg(id, "edenor", "rh-"+id))
	}
	fake := &fakeSQSClient{
		batches:           [][]sqstypes.Message{batch},
		trackVisibility:   true,
		visibilityTimeout: 2 * time.Second,
	}

	consumer := sqsq.NewConsumer(fake, sqsq.ConsumerConfig{
		QueueURL:          "https://sqs.local/q.fifo",
		Receivers:         1,
		MaxInflight:       10,
		VisibilityTimeout: 2 * time.Second,
		ShutdownTimeout:   5 * time.Second,
	}, discardLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		mu       sync.Mutex
		executed []string
		doneCnt  atomic.Int32
	)
	h := handlerFunc(func(execCtx context.Context, msg execute.Message) (execute.HandleOutcome, error) {
		time.Sleep(700 * time.Millisecond) // 4 * 700ms = 2.8s > 2s VT
		mu.Lock()
		executed = append(executed, msg.RequestID)
		mu.Unlock()
		err := consumer.Ack(execCtx, msg)
		if doneCnt.Add(1) == 4 {
			cancel()
		}
		return execute.OutcomeDone, err
	})

	if err := consumer.Run(ctx, h); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.redelivered) != 0 {
		t.Fatalf("waiting messages expired VT and were re-delivered: %v", fake.redelivered)
	}
	// Waiting message w4 (last in batch) must have been extended via ChangeMessageVisibilityBatch while waiting!
	if len(fake.batchVisChanges["rh-w4"]) == 0 {
		t.Fatalf("expected waiting message rh-w4 to be extended via ChangeMessageVisibilityBatch, got batchVisChanges=%v", fake.batchVisChanges)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(executed) != 4 {
		t.Fatalf("executed = %v (len=%d), want exactly 4 without duplicates", executed, len(executed))
	}
}

// Review-2 Finding 2a: Handler calls Nack(30s) inside Execute and a heartbeat tick fires immediately
// afterwards before Execute returns -> final visibility MUST remain 30s, never overwritten to VT.
func TestConsumer_HeartbeatDoesNotOverwriteNackBackoff(t *testing.T) {
	t.Parallel()

	batch := []sqstypes.Message{
		makeSQSMsg("d1", "edenor", "rh-d1"),
		makeSQSMsg("d2", "edenor", "rh-d2"),
	}
	fake := &fakeSQSClient{batches: [][]sqstypes.Message{batch}}

	// VT = 1s -> heartbeat ticks every 500ms.
	consumer := sqsq.NewConsumer(fake, sqsq.ConsumerConfig{
		QueueURL:          "https://sqs.local/q.fifo",
		Receivers:         1,
		MaxInflight:       4,
		VisibilityTimeout: 1 * time.Second,
		ShutdownTimeout:   5 * time.Second,
	}, discardLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := handlerFunc(func(execCtx context.Context, msg execute.Message) (execute.HandleOutcome, error) {
		// Defer message with 30s backoff, then linger > 1 heartbeat interval before returning!
		if err := consumer.Nack(execCtx, msg, 30*time.Second); err != nil {
			t.Errorf("Nack(30s): %v", err)
		}
		time.Sleep(750 * time.Millisecond) // crosses the 500ms heartbeat tick while still inside Execute
		go func() {
			time.Sleep(30 * time.Millisecond)
			cancel()
		}()
		return execute.OutcomeDeferred, nil
	})

	if err := consumer.Run(ctx, h); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	vis := fake.visChanges["rh-d1"]
	if len(vis) == 0 || vis[len(vis)-1] != 30 {
		t.Fatalf("final visibility for deferred rh-d1 = %v, want last value 30 (heartbeat must not overwrite backoff)", vis)
	}
	// Waiting message d2 must be released with Nack(0).
	vis2 := fake.visChanges["rh-d2"]
	if len(vis2) == 0 || vis2[len(vis2)-1] != 0 {
		t.Fatalf("expected waiting rh-d2 to receive Nack(0), got %v", vis2)
	}
}

// Review-2 Finding 2b: extendBatch inspects Failed entries in ChangeMessageVisibilityBatchOutput and counts/logs them.
func TestConsumer_ExtendBatchCountsPartialFailures(t *testing.T) {
	t.Parallel()

	batch := []sqstypes.Message{
		makeSQSMsg("f1", "edenor", "rh-f1"),
	}
	fake := &fakeSQSClient{
		batches: [][]sqstypes.Message{batch},
		batchFailEntries: []sqstypes.BatchResultErrorEntry{
			{
				Id:          aws.String("0"),
				Code:        aws.String("ReceiptHandleIsInvalid"),
				Message:     aws.String("receipt handle expired"),
				SenderFault: true,
			},
		},
	}

	consumer := sqsq.NewConsumer(fake, sqsq.ConsumerConfig{
		QueueURL:          "https://sqs.local/q.fifo",
		Receivers:         1,
		MaxInflight:       4,
		VisibilityTimeout: 1 * time.Second,
		ShutdownTimeout:   5 * time.Second,
	}, discardLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := handlerFunc(func(execCtx context.Context, msg execute.Message) (execute.HandleOutcome, error) {
		time.Sleep(650 * time.Millisecond) // triggers at least 1 heartbeat tick (500ms)
		err := consumer.Ack(execCtx, msg)
		cancel()
		return execute.OutcomeDone, err
	})

	if err := consumer.Run(ctx, h); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := consumer.HeartbeatFailures(); got < 1 {
		t.Fatalf("HeartbeatFailures() = %d, want >= 1 for partial ChangeMessageVisibilityBatch failure", got)
	}
}
