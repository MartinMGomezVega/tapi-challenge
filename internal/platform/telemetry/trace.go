package telemetry

import (
	"context"
	"crypto/rand"
	"strings"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var tracePropagator = propagation.TraceContext{}

// NewTraceParent starts a new W3C traceparent string backed by OpenTelemetry trace context (005 US2.2).
func NewTraceParent(ctx context.Context) (context.Context, string) {
	if ctx == nil {
		ctx = context.Background()
	}
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		var tid trace.TraceID
		var sid trace.SpanID
		_, _ = rand.Read(tid[:])
		_, _ = rand.Read(sid[:])
		sc = trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    tid,
			SpanID:     sid,
			TraceFlags: trace.FlagsSampled,
		})
		ctx = trace.ContextWithSpanContext(ctx, sc)
	}
	carrier := propagation.MapCarrier{}
	tracePropagator.Inject(ctx, carrier)
	return ctx, carrier.Get("traceparent")
}

// ExtractTraceContext extracts a W3C traceparent string into ctx and returns the 32-hex trace_id.
func ExtractTraceContext(ctx context.Context, traceParent string) (context.Context, string) {
	if ctx == nil {
		ctx = context.Background()
	}
	if traceParent == "" {
		return ctx, ""
	}
	carrier := propagation.MapCarrier{"traceparent": traceParent}
	extracted := tracePropagator.Extract(ctx, carrier)
	sc := trace.SpanContextFromContext(extracted)
	if sc.IsValid() {
		return extracted, sc.TraceID().String()
	}
	return extracted, TraceIDFromParent(traceParent)
}

// TraceIDFromParent returns the 32-hex trace_id embedded in a W3C traceparent header.
func TraceIDFromParent(traceParent string) string {
	parts := strings.Split(strings.TrimSpace(traceParent), "-")
	if len(parts) != 4 || len(parts[1]) != 32 {
		return ""
	}
	return parts[1]
}
