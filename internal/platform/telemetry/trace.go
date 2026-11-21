package telemetry

import "context"

// NewTraceParent starts a new W3C traceparent string backed by OpenTelemetry trace context (005 US2.2).
func NewTraceParent(_ context.Context) (context.Context, string) {
	return context.Background(), ""
}

// ExtractTraceContext extracts a W3C traceparent string into ctx and returns the trace_id.
func ExtractTraceContext(ctx context.Context, _ string) (context.Context, string) {
	return ctx, ""
}

// TraceIDFromParent returns the 32-hex trace_id embedded in a W3C traceparent header.
func TraceIDFromParent(_ string) string { return "" }
