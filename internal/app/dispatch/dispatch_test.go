package dispatch_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/app/assignslots"
	"github.com/martingomezvega/tapi-challenge/internal/app/dispatch"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
	"github.com/martingomezvega/tapi-challenge/internal/platform/clock"
)

// Compile-time guarantee that dispatch.UseCase satisfies 001's FR-005b ImmediateDispatcher port.
var _ assignslots.ImmediateDispatcher = (*dispatch.UseCase)(nil)

type fakeRequests struct {
	mu       sync.Mutex
	byKey    map[string][]domain.RequestKey // "slot#shard" -> keys
	pageSize int
	failSlot int // if >= 0, fail ListBySlotShard for this slot
	failErr  error
}

func newFakeRequests(pageSize int) *fakeRequests {
	return &fakeRequests{
		byKey:    make(map[string][]domain.RequestKey),
		pageSize: pageSize,
		failSlot: -1,
	}
}

func (f *fakeRequests) add(slot, shard int, keys ...domain.RequestKey) {
	k := domain.FormatSlotShard(slot, shard)
	f.byKey[k] = append(f.byKey[k], keys...)
}

func (f *fakeRequests) ListBySlotShard(_ context.Context, slot, shard int, page string) ([]domain.RequestKey, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failSlot >= 0 && slot == f.failSlot {
		return nil, "", f.failErr
	}
	all := f.byKey[domain.FormatSlotShard(slot, shard)]
	start := 0
	if page != "" {
		var err error
		start, err = strconv.Atoi(page)
		if err != nil {
			return nil, "", err
		}
	}
	if start >= len(all) {
		return nil, "", nil
	}
	end := min(start+f.pageSize, len(all))
	next := ""
	if end < len(all) {
		next = strconv.Itoa(end)
	}
	return slices.Clone(all[start:end]), next, nil
}

type execRecord struct {
	status    string
	provider  string
	slot      int
	createdAt time.Time
}

type fakeExecutions struct {
	mu      sync.Mutex
	records map[string]execRecord
}

func newFakeExecutions() *fakeExecutions {
	return &fakeExecutions{records: make(map[string]execRecord)}
}

func (f *fakeExecutions) CreatePending(_ context.Context, reqID string, date domain.RunDate, provider string, slot int, now time.Time) (dispatch.CreatePendingResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := domain.IdempotencyKey(reqID, date)
	if existing, ok := f.records[key]; ok {
		return dispatch.CreatePendingResult{
			Created:   false,
			Status:    existing.status,
			CreatedAt: existing.createdAt,
		}, nil
	}
	f.records[key] = execRecord{
		status:    dispatch.StatusPending,
		provider:  provider,
		slot:      slot,
		createdAt: now,
	}
	return dispatch.CreatePendingResult{
		Created:   true,
		Status:    dispatch.StatusPending,
		CreatedAt: now,
	}, nil
}

func (f *fakeExecutions) MarkEnqueued(_ context.Context, keys []dispatch.ExecKey, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, k := range keys {
		id := domain.IdempotencyKey(k.RequestID, k.RunDate)
		rec := f.records[id]
		rec.status = dispatch.StatusEnqueued
		f.records[id] = rec
	}
	return nil
}

func (f *fakeExecutions) statusOf(reqID string, date domain.RunDate) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.records[domain.IdempotencyKey(reqID, date)].status
}

type fakeQueue struct {
	mu             sync.Mutex
	sent           []dispatch.JobMessage
	batchSizes     []int
	failOnceIDs    map[string]bool // fail first attempt for these request IDs (partial batch failure)
	alwaysFail     bool
	alwaysFailErr  error
}

func newFakeQueue() *fakeQueue {
	return &fakeQueue{failOnceIDs: make(map[string]bool)}
}

func (q *fakeQueue) SendBatch(_ context.Context, msgs []dispatch.JobMessage) ([]dispatch.JobMessage, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.alwaysFail {
		return msgs, q.alwaysFailErr
	}
	q.batchSizes = append(q.batchSizes, len(msgs))
	var failed []dispatch.JobMessage
	for _, m := range msgs {
		if q.failOnceIDs[m.RequestID] {
			delete(q.failOnceIDs, m.RequestID)
			failed = append(failed, m)
			continue
		}
		q.sent = append(q.sent, m)
	}
	return failed, nil
}

func (q *fakeQueue) messages() []dispatch.JobMessage {
	q.mu.Lock()
	defer q.mu.Unlock()
	return slices.Clone(q.sent)
}

type fakeCoordination struct {
	mu         sync.Mutex
	owner      string
	expiresAt  time.Time
	token      int64
	hasWM      bool
	wm         domain.SlotCoord
	releasedBy []string
}

func (c *fakeCoordination) TryAcquire(_ context.Context, owner string, ttl time.Duration, now time.Time) (int64, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.owner == "" || !now.Before(c.expiresAt) || c.owner == owner {
		if c.owner != owner {
			c.token++
		}
		c.owner = owner
		c.expiresAt = now.Add(ttl)
		return c.token, true, nil
	}
	return 0, false, nil
}

func (c *fakeCoordination) Release(_ context.Context, owner string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.releasedBy = append(c.releasedBy, owner)
	if c.owner == owner {
		c.owner = ""
		c.expiresAt = time.Time{}
	}
	return nil
}

func (c *fakeCoordination) Load(_ context.Context) (domain.SlotCoord, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.wm, c.hasWM, nil
}

func (c *fakeCoordination) Advance(_ context.Context, coord domain.SlotCoord, fencingToken int64, _ time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if fencingToken != c.token {
		return dispatch.ErrFencingMismatch
	}
	c.wm = coord
	c.hasWM = true
	return nil
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// T005 + T006 + T007: DispatchSlot creates PENDING, sends enabled only, skips already-created, marks ENQUEUED.
func TestDispatchSlot_EnabledDisabledAndIdempotency(t *testing.T) {
	t.Parallel()

	reqs := newFakeRequests(10)
	reqs.add(742, 0,
		domain.RequestKey{ID: "r1", Provider: "edenor", Enabled: true},
		domain.RequestKey{ID: "r2", Provider: "edenor", Enabled: false}, // T006: must be omitted
		domain.RequestKey{ID: "r3", Provider: "metrogas", Enabled: true},
	)
	reqs.add(742, 1,
		domain.RequestKey{ID: "r4", Provider: "aysa", Enabled: true},
	)

	execs := newFakeExecutions()
	q := newFakeQueue()
	coord := &fakeCoordination{}
	fc := clock.NewFake(time.Date(2026, 11, 20, 12, 22, 0, 0, time.UTC))

	uc, err := dispatch.New(reqs, execs, q, coord, coord, nil, fc, discardLogger(), dispatch.Config{
		OwnerID: "disp-1",
		Shards:  2,
	})
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	target := domain.SlotCoord{Date: "2026-11-20", Slot: 742}
	st1, err := uc.DispatchSlot(context.Background(), target)
	if err != nil {
		t.Fatalf("DispatchSlot(1): %v", err)
	}
	if st1.Dispatched != 3 {
		t.Fatalf("Dispatched = %d, want 3 (r1, r3, r4)", st1.Dispatched)
	}

	msgs := q.messages()
	if len(msgs) != 3 {
		t.Fatalf("queue len = %d, want 3", len(msgs))
	}
	for _, id := range []string{"r1", "r3", "r4"} {
		if got := execs.statusOf(id, "2026-11-20"); got != dispatch.StatusEnqueued {
			t.Errorf("status(%s) = %q, want ENQUEUED", id, got)
		}
	}
	if got := execs.statusOf("r2", "2026-11-20"); got != "" {
		t.Errorf("disabled r2 unexpectedly created execution with status %q", got)
	}

	// T007: Re-dispatching same slot must send 0 new messages.
	st2, err := uc.DispatchSlot(context.Background(), target)
	if err != nil {
		t.Fatalf("DispatchSlot(2): %v", err)
	}
	if st2.Dispatched != 0 || len(q.messages()) != 3 {
		t.Errorf("re-dispatch sent duplicates: stats=%+v queueLen=%d", st2, len(q.messages()))
	}
}

// T008: Batching in chunks of 10 and retrying partial SendBatch failures.
func TestDispatchSlot_Batch10AndPartialRetry(t *testing.T) {
	t.Parallel()

	reqs := newFakeRequests(50)
	for i := range 23 {
		reqs.add(10, 0, domain.RequestKey{
			ID:       fmt.Sprintf("req-%02d", i),
			Provider: fmt.Sprintf("prov-%d", i%3),
			Enabled:  true,
		})
	}

	execs := newFakeExecutions()
	q := newFakeQueue()
	q.failOnceIDs["req-03"] = true
	q.failOnceIDs["req-17"] = true
	coord := &fakeCoordination{}
	fc := clock.NewFake(time.Date(2026, 11, 20, 0, 10, 0, 0, time.UTC))

	uc, err := dispatch.New(reqs, execs, q, coord, coord, nil, fc, discardLogger(), dispatch.Config{
		OwnerID: "disp-1",
		Shards:  1,
	})
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	st, err := uc.DispatchSlot(context.Background(), domain.SlotCoord{Date: "2026-11-20", Slot: 10})
	if err != nil {
		t.Fatalf("DispatchSlot(): %v", err)
	}
	if st.Dispatched != 23 {
		t.Fatalf("Dispatched = %d, want 23", st.Dispatched)
	}
	for _, bs := range q.batchSizes {
		if bs > dispatch.MaxSQSBatchSize {
			t.Errorf("batch size %d exceeded SQS max %d", bs, dispatch.MaxSQSBatchSize)
		}
	}
	if len(q.messages()) != 23 {
		t.Fatalf("sent messages = %d, want 23 after partial retries", len(q.messages()))
	}
	for i := range 23 {
		id := fmt.Sprintf("req-%02d", i)
		if got := execs.statusOf(id, "2026-11-20"); got != dispatch.StatusEnqueued {
			t.Errorf("status(%s) = %q, want ENQUEUED", id, got)
		}
	}
}

// 001 FR-005b wiring: DispatchNow creates PENDING, enqueues to FIFO, marks ENQUEUED, and is idempotent.
func TestUseCase_DispatchNow_ImmediateDispatcher(t *testing.T) {
	t.Parallel()

	execs := newFakeExecutions()
	q := newFakeQueue()
	coord := &fakeCoordination{}
	fc := clock.NewFake(time.Date(2026, 11, 20, 14, 5, 0, 0, time.UTC))

	uc, err := dispatch.New(newFakeRequests(10), execs, q, coord, coord, nil, fc, discardLogger(), dispatch.Config{
		OwnerID: "slotter-1",
		Shards:  4,
	})
	if err != nil {
		t.Fatalf("dispatch.New: %v", err)
	}

	if err := uc.DispatchNow(context.Background(), "req-rebalanced", "edenor"); err != nil {
		t.Fatalf("first DispatchNow(): %v", err)
	}
	if err := uc.DispatchNow(context.Background(), "req-rebalanced", "edenor"); err != nil {
		t.Fatalf("second DispatchNow(): %v", err)
	}

	msgs := q.messages()
	if len(msgs) != 1 {
		t.Fatalf("DispatchNow sent %d messages across 2 calls, want 1", len(msgs))
	}
	if msgs[0].DedupID() != "req-rebalanced#2026-11-20" || msgs[0].Provider != "edenor" {
		t.Errorf("unexpected message: %+v", msgs[0])
	}
	if got := execs.statusOf("req-rebalanced", "2026-11-20"); got != dispatch.StatusEnqueued {
		t.Errorf("status = %q, want ENQUEUED", got)
	}
}

// T014 + T015 + T016: Catch-up 15 min, midnight crossing, and no watermark advance on slot failure.
func TestTick_CatchUpMidnightAndFailureSafety(t *testing.T) {
	t.Parallel()

	t.Run("lagging 15 minutes dispatches 15 slots in order", func(t *testing.T) {
		t.Parallel()

		reqs := newFakeRequests(10)
		for s := 601; s <= 615; s++ {
			reqs.add(s, 0, domain.RequestKey{ID: fmt.Sprintf("r-%d", s), Provider: "p", Enabled: true})
		}
		execs := newFakeExecutions()
		q := newFakeQueue()
		coord := &fakeCoordination{hasWM: true, wm: domain.SlotCoord{Date: "2026-11-20", Slot: 600}}
		fc := clock.NewFake(time.Date(2026, 11, 20, 10, 15, 0, 0, time.UTC)) // slot 615

		uc, _ := dispatch.New(reqs, execs, q, coord, coord, nil, fc, discardLogger(), dispatch.Config{OwnerID: "d1", Shards: 1})
		st, err := uc.Tick(context.Background())
		if err != nil {
			t.Fatalf("Tick(): %v", err)
		}
		if st.SlotsDispatched != 15 || st.RecordsSent != 15 {
			t.Fatalf("stats = %+v, want 15 slots and 15 records", st)
		}
		wm, _, _ := coord.Load(context.Background())
		if wm != (domain.SlotCoord{Date: "2026-11-20", Slot: 615}) {
			t.Errorf("watermark = %+v, want (2026-11-20, 615)", wm)
		}
		msgs := q.messages()
		for i, m := range msgs {
			if m.Slot != 601+i {
				t.Fatalf("msg[%d].Slot = %d, want %d (chronological order)", i, m.Slot, 601+i)
			}
		}
	})

	t.Run("midnight crossing D 1435..1439 and D+1 0..3", func(t *testing.T) {
		t.Parallel()

		reqs := newFakeRequests(10)
		for _, s := range []int{1435, 1436, 1437, 1438, 1439, 0, 1, 2, 3} {
			reqs.add(s, 0, domain.RequestKey{ID: fmt.Sprintf("m-%d", s), Provider: "p", Enabled: true})
		}
		execs := newFakeExecutions()
		q := newFakeQueue()
		coord := &fakeCoordination{hasWM: true, wm: domain.SlotCoord{Date: "2026-11-20", Slot: 1434}}
		fc := clock.NewFake(time.Date(2026, 11, 21, 0, 3, 0, 0, time.UTC))

		uc, _ := dispatch.New(reqs, execs, q, coord, coord, nil, fc, discardLogger(), dispatch.Config{OwnerID: "d1", Shards: 1})
		st, err := uc.Tick(context.Background())
		if err != nil {
			t.Fatalf("Tick(): %v", err)
		}
		if st.SlotsDispatched != 9 {
			t.Fatalf("SlotsDispatched = %d, want 9", st.SlotsDispatched)
		}
		msgs := q.messages()
		if msgs[0].RunDate != "2026-11-20" || msgs[0].Slot != 1435 ||
			msgs[4].RunDate != "2026-11-20" || msgs[4].Slot != 1439 ||
			msgs[5].RunDate != "2026-11-21" || msgs[5].Slot != 0 ||
			msgs[8].RunDate != "2026-11-21" || msgs[8].Slot != 3 {
			t.Errorf("midnight crossing messages mismatch: %+v", msgs)
		}
	})

	t.Run("watermark does not advance when slot fails mid-way", func(t *testing.T) {
		t.Parallel()

		reqs := newFakeRequests(10)
		reqs.add(11, 0, domain.RequestKey{ID: "ok-11", Provider: "p", Enabled: true})
		reqs.add(12, 0, domain.RequestKey{ID: "fail-12", Provider: "p", Enabled: true})
		reqs.failSlot = 12
		reqs.failErr = errors.New("dynamodb throttled")

		execs := newFakeExecutions()
		q := newFakeQueue()
		coord := &fakeCoordination{hasWM: true, wm: domain.SlotCoord{Date: "2026-11-20", Slot: 10}}
		fc := clock.NewFake(time.Date(2026, 11, 20, 0, 13, 0, 0, time.UTC))

		uc, _ := dispatch.New(reqs, execs, q, coord, coord, nil, fc, discardLogger(), dispatch.Config{OwnerID: "d1", Shards: 1})
		if _, err := uc.Tick(context.Background()); err == nil {
			t.Fatal("expected Tick() to fail on slot 12")
		}
		wm, _, _ := coord.Load(context.Background())
		if wm != (domain.SlotCoord{Date: "2026-11-20", Slot: 11}) {
			t.Errorf("watermark = %+v, want (2026-11-20, 11) (stopped before failing slot 12)", wm)
		}
	})
}

// T018 + T019: Single leader lease, failover after TTL expiry, and fencing rejection for ex-leader.
func TestTick_LeaderElectionFailoverAndFencing(t *testing.T) {
	t.Parallel()

	reqs := newFakeRequests(10)
	reqs.add(1, 0, domain.RequestKey{ID: "r1", Provider: "p", Enabled: true})
	reqs.add(2, 0, domain.RequestKey{ID: "r2", Provider: "p", Enabled: true})

	execs := newFakeExecutions()
	q := newFakeQueue()
	coord := &fakeCoordination{hasWM: true, wm: domain.SlotCoord{Date: "2026-11-20", Slot: 0}}
	fc := clock.NewFake(time.Date(2026, 11, 20, 0, 1, 0, 0, time.UTC))

	uc1, _ := dispatch.New(reqs, execs, q, coord, coord, nil, fc, discardLogger(), dispatch.Config{OwnerID: "replica-1", Shards: 1, LeaseTTL: 30 * time.Second})
	uc2, _ := dispatch.New(reqs, execs, q, coord, coord, nil, fc, discardLogger(), dispatch.Config{OwnerID: "replica-2", Shards: 1, LeaseTTL: 30 * time.Second})

	// T018: replica-1 acquires lease; replica-2 is standby.
	st1, err := uc1.Tick(context.Background())
	if err != nil || !st1.IsLeader || st1.SlotsDispatched != 1 {
		t.Fatalf("replica-1 Tick() = (%+v, %v), want leader dispatching 1 slot", st1, err)
	}
	st2, err := uc2.Tick(context.Background())
	if err != nil || st2.IsLeader || st2.SlotsDispatched != 0 {
		t.Fatalf("replica-2 Tick() = (%+v, %v), want standby", st2, err)
	}

	// T019: Lease expires after 31s -> replica-2 takes over; ex-leader's old token fails fencing.
	oldToken := coord.token
	fc.Advance(60 * time.Second) // now 00:02:00 (slot 2), lease expired 30s ago
	st2After, err := uc2.Tick(context.Background())
	if err != nil || !st2After.IsLeader || st2After.SlotsDispatched != 1 {
		t.Fatalf("replica-2 failover Tick() = (%+v, %v), want leader dispatching slot 2", st2After, err)
	}
	if err := coord.Advance(context.Background(), domain.SlotCoord{Date: "2026-11-20", Slot: 3}, oldToken, fc.Now()); !errors.Is(err, dispatch.ErrFencingMismatch) {
		t.Errorf("stale leader Advance() error = %v, want %v", err, dispatch.ErrFencingMismatch)
	}
}

// T022: Orphan PENDING older than 5 min is re-dispatched; fresh PENDING and ENQUEUED are not.
func TestDispatchSlot_OrphanPendingRecovery(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	reqs := newFakeRequests(10)
	reqs.add(720, 0,
		domain.RequestKey{ID: "orphan-old", Provider: "edenor", Enabled: true},
		domain.RequestKey{ID: "pending-fresh", Provider: "edenor", Enabled: true},
		domain.RequestKey{ID: "already-enqueued", Provider: "edenor", Enabled: true},
	)

	execs := newFakeExecutions()
	execs.records["orphan-old#2026-11-20"] = execRecord{status: dispatch.StatusPending, createdAt: now.Add(-6 * time.Minute)}
	execs.records["pending-fresh#2026-11-20"] = execRecord{status: dispatch.StatusPending, createdAt: now.Add(-2 * time.Minute)}
	execs.records["already-enqueued#2026-11-20"] = execRecord{status: dispatch.StatusEnqueued, createdAt: now.Add(-10 * time.Minute)}

	q := newFakeQueue()
	coord := &fakeCoordination{}
	uc, _ := dispatch.New(reqs, execs, q, coord, coord, nil, clock.NewFake(now), discardLogger(), dispatch.Config{
		OwnerID:   "d1",
		Shards:    1,
		OrphanAge: 5 * time.Minute,
	})

	st, err := uc.DispatchSlot(context.Background(), domain.SlotCoord{Date: "2026-11-20", Slot: 720})
	if err != nil {
		t.Fatalf("DispatchSlot(): %v", err)
	}
	if st.Orphans != 1 || st.Dispatched != 1 {
		t.Fatalf("stats = %+v, want Orphans=1 Dispatched=1", st)
	}
	msgs := q.messages()
	if len(msgs) != 1 || msgs[0].RequestID != "orphan-old" {
		t.Fatalf("sent = %+v, want only orphan-old", msgs)
	}
	if got := execs.statusOf("orphan-old", "2026-11-20"); got != dispatch.StatusEnqueued {
		t.Errorf("orphan-old status = %q, want ENQUEUED", got)
	}
}

// T025: EMF metrics emitter produces valid CloudWatch EMF JSON with all 4 FR-007 metrics.
func TestEMFEmitter_DispatchMetrics(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	ts := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	emitter := dispatch.NewEMFEmitter(&buf, "Tapi/Scheduler", clock.NewFake(ts))

	if err := emitter.EmitDispatch(context.Background(), dispatch.DispatchMetrics{
		SlotDispatchDuration: 120 * time.Millisecond,
		RecordsDispatched:    694,
		DispatchLagMinutes:   0,
		IsLeader:             true,
	}); err != nil {
		t.Fatalf("EmitDispatch(): %v", err)
	}

	var ev map[string]any
	if err := json.Unmarshal(buf.Bytes(), &ev); err != nil {
		t.Fatalf("invalid EMF JSON: %v", err)
	}
	for _, field := range []string{"SlotDispatchDuration", "RecordsDispatched", "DispatchLagMinutes", "IsLeader"} {
		if _, ok := ev[field]; !ok {
			t.Errorf("missing EMF metric field %q in %+v", field, ev)
		}
	}
}
