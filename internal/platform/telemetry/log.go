// Package telemetry provides structured JSON logging, CloudWatch EMF metrics, and W3C/OTel trace propagation (feature 005).
package telemetry

import (
	"io"
	"log/slog"
	"os"
)

// NewLogger returns a JSON slog.Logger stamped with the service name (005 FR-005).
func NewLogger(w io.Writer, service string) *slog.Logger {
	if w == nil {
		w = os.Stdout
	}
	l := slog.New(slog.NewJSONHandler(w, nil))
	if service != "" {
		l = l.With("service", service)
	}
	return l
}

// WithExecution binds the mandatory execution correlation fields onto a logger (005 US2.1).
func WithExecution(l *slog.Logger, requestID, provider, runDate, traceID string) *slog.Logger {
	if l == nil {
		l = slog.Default()
	}
	return l.With(
		"request_id", requestID,
		"provider", provider,
		"run_date", runDate,
		"trace_id", traceID,
	)
}
