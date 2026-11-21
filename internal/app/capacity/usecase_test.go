package capacity_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/app/capacity"
)

type fakeLister struct {
	vols []capacity.ProviderVolume
	err  error
}

func (f fakeLister) ListProviderVolumes(_ context.Context) ([]capacity.ProviderVolume, error) {
	return f.vols, f.err
}

type fakeLatency struct {
	byProvider map[string]time.Duration
	err        error
}

func (f fakeLatency) P95Latency(_ context.Context, provider string) (time.Duration, bool, error) {
	if f.err != nil {
		return 0, false, f.err
	}
	d, ok := f.byProvider[provider]
	return d, ok, nil
}

type fakeEmitter struct {
	emitted []capacity.ProviderReport
	err     error
}

func (f *fakeEmitter) EmitProviderOverCapacity(_ context.Context, r capacity.ProviderReport) error {
	if f.err != nil {
		return f.err
	}
	f.emitted = append(f.emitted, r)
	return nil
}

type fakeClock struct{ now time.Time }

func (f fakeClock) Now() time.Time { return f.now }

// T023: k * p95 > 86400 * 0.7 marks provider as over capacity and emits ProviderOverCapacity (FR-006, US3).
func TestUseCase_CheckCapacityBoundary(t *testing.T) {
	t.Parallel()

	// With p95 = 400ms and safetyFactor = 0.7: budget = 60,480s -> exact boundary = 151,200 records.
	lister := fakeLister{
		vols: []capacity.ProviderVolume{
			{Provider: "healthy", Count: 100_000},
			{Provider: "exact-boundary", Count: 151_200},
			{Provider: "over-by-one", Count: 151_201},
			{Provider: "slow-over", Count: 50_000}, // p95 = 1500ms -> 75,000s > 60,480s
		},
	}
	lat := fakeLatency{
		byProvider: map[string]time.Duration{
			"slow-over": 1500 * time.Millisecond,
		},
	}
	emitter := &fakeEmitter{}

	uc, err := capacity.New(lister, lat, emitter, capacity.Config{})
	if err != nil {
		t.Fatalf("capacity.New: %v", err)
	}

	reports, err := uc.Check(context.Background())
	if err != nil {
		t.Fatalf("Check(): %v", err)
	}
	if len(reports) != 4 {
		t.Fatalf("len(reports) = %d, want 4", len(reports))
	}

	wantOver := map[string]bool{
		"healthy":        false,
		"exact-boundary": false,
		"over-by-one":    true,
		"slow-over":      true,
	}

	for _, r := range reports {
		if r.OverCapacity != wantOver[r.Provider] {
			t.Errorf("provider %s: OverCapacity = %t, want %t (req=%.2fs budget=%.2fs)",
				r.Provider, r.OverCapacity, wantOver[r.Provider], r.RequiredSec, r.BudgetSec)
		}
	}

	if len(emitter.emitted) != 4 {
		t.Fatalf("emitter received %d reports, want 4", len(emitter.emitted))
	}
}

func TestEMFEmitter_Format(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	ts := time.Date(2026, 11, 21, 0, 0, 0, 0, time.UTC)
	emitter := capacity.NewEMFEmitter(&buf, "Tapi/Scheduler", fakeClock{now: ts})

	err := emitter.EmitProviderOverCapacity(context.Background(), capacity.ProviderReport{
		Provider:     "edenor",
		DailyRecords: 160_000,
		P95Latency:   400 * time.Millisecond,
		RequiredSec:  64_000,
		BudgetSec:    60_480,
		OverCapacity: true,
	})
	if err != nil {
		t.Fatalf("EmitProviderOverCapacity(): %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(buf.Bytes(), &payload); err != nil {
		t.Fatalf("invalid EMF JSON: %v\nraw: %s", err, buf.String())
	}
	if payload["provider"] != "edenor" {
		t.Errorf("provider = %v, want edenor", payload["provider"])
	}
	if payload[capacity.MetricProviderOverCapacity] != float64(1) {
		t.Errorf("ProviderOverCapacity = %v, want 1", payload[capacity.MetricProviderOverCapacity])
	}
	awsMeta, ok := payload["_aws"].(map[string]any)
	if !ok {
		t.Fatalf("missing _aws EMF envelope: %+v", payload)
	}
	if int64(awsMeta["Timestamp"].(float64)) != ts.UnixMilli() {
		t.Errorf("Timestamp = %v, want %d", awsMeta["Timestamp"], ts.UnixMilli())
	}
}

func TestUseCase_ValidationAndErrors(t *testing.T) {
	t.Parallel()

	if _, err := capacity.New(nil, nil, nil, capacity.Config{}); !errors.Is(err, capacity.ErrInvalidConfig) {
		t.Errorf("New(nil lister) error = %v, want %v", err, capacity.ErrInvalidConfig)
	}
	if _, err := capacity.New(fakeLister{}, nil, nil, capacity.Config{SafetyFactor: 1.5}); !errors.Is(err, capacity.ErrInvalidConfig) {
		t.Errorf("New(safetyFactor=1.5) error = %v, want %v", err, capacity.ErrInvalidConfig)
	}

	boom := errors.New("dynamo unavailable")
	uc, err := capacity.New(fakeLister{err: boom}, nil, nil, capacity.Config{})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	if _, err := uc.Check(context.Background()); !errors.Is(err, boom) {
		t.Errorf("Check() error = %v, want %v", err, boom)
	}

	if _, err := capacity.New(fakeLister{}, nil, nil, capacity.Config{DefaultLatency: -1}); !errors.Is(err, capacity.ErrInvalidConfig) {
		t.Errorf("New(negative latency) error = %v, want %v", err, capacity.ErrInvalidConfig)
	}

	latErr := errors.New("metrics unavailable")
	ucLat, _ := capacity.New(fakeLister{vols: []capacity.ProviderVolume{{Provider: "p", Count: 10}}}, fakeLatency{err: latErr}, nil, capacity.Config{})
	if _, err := ucLat.Check(context.Background()); !errors.Is(err, latErr) {
		t.Errorf("Check(latency err) = %v, want %v", err, latErr)
	}

	emitErr := errors.New("stdout closed")
	ucEmit, _ := capacity.New(fakeLister{vols: []capacity.ProviderVolume{{Provider: "p", Count: 10}}}, nil, &fakeEmitter{err: emitErr}, capacity.Config{})
	if _, err := ucEmit.Check(context.Background()); !errors.Is(err, emitErr) {
		t.Errorf("Check(emitter err) = %v, want %v", err, emitErr)
	}

	var buf bytes.Buffer
	emf := capacity.NewEMFEmitter(&buf, "", nil)
	if err := emf.EmitProviderOverCapacity(context.Background(), capacity.ProviderReport{
		Provider:     "ok-prov",
		OverCapacity: false,
	}); err != nil {
		t.Fatalf("EmitProviderOverCapacity(healthy): %v", err)
	}
	var ev map[string]any
	if err := json.Unmarshal(buf.Bytes(), &ev); err != nil {
		t.Fatalf("unmarshal healthy EMF: %v", err)
	}
	if ev[capacity.MetricProviderOverCapacity] != float64(0) {
		t.Errorf("healthy ProviderOverCapacity = %v, want 0", ev[capacity.MetricProviderOverCapacity])
	}
}
