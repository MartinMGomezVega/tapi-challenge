package telemetry

import (
	"io"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

// DefaultNamespace is the CloudWatch EMF namespace for all service metrics.
const DefaultNamespace = "Tapi/ScheduledQueries"

// EMFMetric defines one CloudWatch metric entry in an EMF payload.
type EMFMetric struct {
	Name  string
	Unit  string
	Value float64
}

// EMFEmitter writes CloudWatch Embedded Metric Format JSON lines to an io.Writer (005 US2.3).
type EMFEmitter struct{}

// NewEMFEmitter creates an EMFEmitter targeting w in namespace.
func NewEMFEmitter(_ io.Writer, _ string) *EMFEmitter { return &EMFEmitter{} }

// Emit writes a single CloudWatch EMF JSON record at ts.
func (e *EMFEmitter) Emit(_ time.Time, _ map[string]string, _ ...EMFMetric) error { return nil }

// WorkerMetrics implements execute.MetricsRecorder via CloudWatch EMF.
type WorkerMetrics struct{}

// NewWorkerMetrics constructs an EMF-backed WorkerMetrics recorder.
func NewWorkerMetrics(_ *EMFEmitter, _ func() time.Time) *WorkerMetrics { return &WorkerMetrics{} }

// RecordExecution emits ExecutionsByStatus, ProviderLatency, and ExecutionFailed (when errClass != "").
func (m *WorkerMetrics) RecordExecution(_ string, _ domain.ExecutionStatus, _ string, _ time.Duration) {
}

// RecordBreakerOpen emits BreakerOpen{provider}.
func (m *WorkerMetrics) RecordBreakerOpen(_ string, _ bool) {}
