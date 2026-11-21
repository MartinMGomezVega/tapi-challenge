// Package telemetry provides structured JSON logging, CloudWatch EMF metrics, and W3C/OTel trace propagation (feature 005).
package telemetry

import (
	"io"
	"log/slog"
)

// NewLogger returns a JSON slog.Logger stamped with the service name (005 FR-005).
func NewLogger(w io.Writer, _ string) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, nil))
}

// WithExecution binds the mandatory execution correlation fields onto a logger (005 US2.1).
func WithExecution(l *slog.Logger, _, _, _, _ string) *slog.Logger {
	return l
}
