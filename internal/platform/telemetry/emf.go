package telemetry

import (
	"encoding/json"
	"io"
	"os"
	"slices"
	"sync"
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
type EMFEmitter struct {
	mu        sync.Mutex
	w         io.Writer
	namespace string
}

// NewEMFEmitter creates an EMFEmitter targeting w in namespace.
func NewEMFEmitter(w io.Writer, namespace string) *EMFEmitter {
	if w == nil {
		w = os.Stdout
	}
	if namespace == "" {
		namespace = DefaultNamespace
	}
	return &EMFEmitter{w: w, namespace: namespace}
}

type emfMetricDef struct {
	Name string `json:"Name"`
	Unit string `json:"Unit"`
}

type emfDirective struct {
	Namespace  string         `json:"Namespace"`
	Dimensions [][]string     `json:"Dimensions"`
	Metrics    []emfMetricDef `json:"Metrics"`
}

type emfAWSBlock struct {
	Timestamp         int64          `json:"Timestamp"`
	CloudWatchMetrics []emfDirective `json:"CloudWatchMetrics"`
}

// Emit writes a single CloudWatch EMF JSON line at ts.
func (e *EMFEmitter) Emit(ts time.Time, dims map[string]string, metrics ...EMFMetric) error {
	if len(metrics) == 0 {
		return nil
	}

	dimKeys := make([]string, 0, len(dims))
	for k := range dims {
		dimKeys = append(dimKeys, k)
	}
	slices.Sort(dimKeys)

	metricDefs := make([]emfMetricDef, 0, len(metrics))
	payload := make(map[string]any, len(dims)+len(metrics)+1)
	for _, k := range dimKeys {
		payload[k] = dims[k]
	}
	for _, m := range metrics {
		unit := m.Unit
		if unit == "" {
			unit = "Count"
		}
		metricDefs = append(metricDefs, emfMetricDef{Name: m.Name, Unit: unit})
		payload[m.Name] = m.Value
	}

	dimSet := [][]string{}
	if len(dimKeys) > 0 {
		dimSet = [][]string{dimKeys}
	}
	payload["_aws"] = emfAWSBlock{
		Timestamp: ts.UTC().UnixMilli(),
		CloudWatchMetrics: []emfDirective{
			{
				Namespace:  e.namespace,
				Dimensions: dimSet,
				Metrics:    metricDefs,
			},
		},
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	b = append(b, '\n')

	e.mu.Lock()
	defer e.mu.Unlock()
	_, err = e.w.Write(b)
	return err
}

// WorkerMetrics implements execute.MetricsRecorder via CloudWatch EMF (005 US2.3, 004 T020).
type WorkerMetrics struct {
	em  *EMFEmitter
	now func() time.Time
}

// NewWorkerMetrics constructs an EMF-backed WorkerMetrics recorder.
func NewWorkerMetrics(em *EMFEmitter, now func() time.Time) *WorkerMetrics {
	if now == nil {
		now = time.Now
	}
	return &WorkerMetrics{em: em, now: now}
}

// RecordExecution emits ExecutionsByStatus, ProviderLatency, and ExecutionFailed (when errClass != "").
func (m *WorkerMetrics) RecordExecution(provider string, status domain.ExecutionStatus, errClass string, latency time.Duration) {
	if m == nil || m.em == nil {
		return
	}
	ts := m.now().UTC()
	_ = m.em.Emit(ts,
		map[string]string{"provider": provider, "status": string(status)},
		EMFMetric{Name: "ExecutionsByStatus", Unit: "Count", Value: 1},
		EMFMetric{Name: "ProviderLatency", Unit: "Milliseconds", Value: float64(latency.Microseconds()) / 1000.0},
	)
	if errClass != "" {
		_ = m.em.Emit(ts,
			map[string]string{"provider": provider, "class": errClass},
			EMFMetric{Name: "ExecutionFailed", Unit: "Count", Value: 1},
		)
	}
}

// RecordBreakerOpen emits BreakerOpen{provider} (004 T020).
func (m *WorkerMetrics) RecordBreakerOpen(provider string, open bool) {
	if m == nil || m.em == nil {
		return
	}
	val := 0.0
	if open {
		val = 1.0
	}
	_ = m.em.Emit(m.now().UTC(),
		map[string]string{"provider": provider},
		EMFMetric{Name: "BreakerOpen", Unit: "Count", Value: val},
	)
}

// ReconcilerMetrics emits DailyCoverage, MissingExecutions, and ProviderOverCapacity via EMF.
type ReconcilerMetrics struct {
	em  *EMFEmitter
	now func() time.Time
}

// NewReconcilerMetrics constructs an EMF-backed ReconcilerMetrics emitter.
func NewReconcilerMetrics(em *EMFEmitter, now func() time.Time) *ReconcilerMetrics {
	if now == nil {
		now = time.Now
	}
	return &ReconcilerMetrics{em: em, now: now}
}

// RecordReconcile emits DailyCoverage (percent 0..100) and MissingExecutions (005 US1.3).
func (m *ReconcilerMetrics) RecordReconcile(date domain.RunDate, coverage float64, missing, stuck int) {
	if m == nil || m.em == nil {
		return
	}
	_ = m.em.Emit(m.now().UTC(),
		map[string]string{"run_date": string(date)},
		EMFMetric{Name: "DailyCoverage", Unit: "Percent", Value: coverage * 100.0},
		EMFMetric{Name: "MissingExecutions", Unit: "Count", Value: float64(missing + stuck)},
	)
}

// RecordProviderOverCapacity emits ProviderOverCapacity{provider} for feature 001 US3 / T025.
func (m *ReconcilerMetrics) RecordProviderOverCapacity(provider string, over bool) {
	if m == nil || m.em == nil {
		return
	}
	val := 0.0
	if over {
		val = 1.0
	}
	_ = m.em.Emit(m.now().UTC(),
		map[string]string{"provider": provider},
		EMFMetric{Name: "ProviderOverCapacity", Unit: "Count", Value: val},
	)
}
