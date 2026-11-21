package telemetry_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/domain"
	"github.com/martingomezvega/tapi-challenge/internal/platform/telemetry"
)

// T001 (golden): JSON logger includes request_id, provider, run_date, trace_id, service.
func TestLogger_GoldenJSON(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	l := telemetry.NewLogger(&buf, "worker")
	l = telemetry.WithExecution(l, "req-001", "edenor", "2026-11-21", "4bf92f3577b34da6a3ce929d0e0e4736")
	l.Info("execution succeeded", "status", "SUCCEEDED", "attempt", 1)

	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal log json: %v (raw=%s)", err, buf.String())
	}
	delete(got, "time") // normalize wall clock

	gotBytes, err := json.MarshalIndent(got, "", "  ")
	if err != nil {
		t.Fatalf("marshal normalized: %v", err)
	}
	wantBytes, err := os.ReadFile(filepath.Join("testdata", "log.golden.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	if strings.TrimSpace(string(gotBytes)) != strings.TrimSpace(string(wantBytes)) {
		t.Fatalf("log golden mismatch:\ngot:\n%s\nwant:\n%s", gotBytes, wantBytes)
	}
}

// T003 (golden): CloudWatch EMF metric format matches contract golden file.
func TestEMFEmitter_Golden(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	em := telemetry.NewEMFEmitter(&buf, telemetry.DefaultNamespace)
	ts := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

	if err := em.Emit(ts,
		map[string]string{"provider": "edenor", "status": string(domain.StatusSucceeded)},
		telemetry.EMFMetric{Name: "ExecutionsByStatus", Unit: "Count", Value: 1},
		telemetry.EMFMetric{Name: "ProviderLatency", Unit: "Milliseconds", Value: 42.5},
	); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal emf: %v (raw=%s)", err, buf.String())
	}
	gotBytes, _ := json.MarshalIndent(got, "", "  ")

	wantBytes, err := os.ReadFile(filepath.Join("testdata", "emf.golden.json"))
	if err != nil {
		t.Fatalf("read emf golden: %v", err)
	}
	if strings.TrimSpace(string(gotBytes)) != strings.TrimSpace(string(wantBytes)) {
		t.Fatalf("emf golden mismatch:\ngot:\n%s\nwant:\n%s", gotBytes, wantBytes)
	}
}

// T005: W3C traceparent generation, propagation, and extraction preserve trace_id end-to-end.
func TestTraceParent_RoundTrip(t *testing.T) {
	t.Parallel()

	ctx, tp := telemetry.NewTraceParent(context.Background())
	if !strings.HasPrefix(tp, "00-") || len(strings.Split(tp, "-")) != 4 {
		t.Fatalf("invalid traceparent: %q", tp)
	}
	traceID := telemetry.TraceIDFromParent(tp)
	if len(traceID) != 32 {
		t.Fatalf("traceID = %q, want 32 hex chars", traceID)
	}

	ctx2, extractedID := telemetry.ExtractTraceContext(ctx, tp)
	if ctx2 == nil || extractedID != traceID {
		t.Fatalf("extracted traceID = %q, want %q", extractedID, traceID)
	}
}
