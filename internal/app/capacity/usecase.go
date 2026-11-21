// Package capacity validates daily per-provider serial execution capacity (001-US3, FR-006).
package capacity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"time"
)

const (
	// SecondsPerDay is the number of seconds in a 24-hour day.
	SecondsPerDay = 86400.0
	// DefaultSafetyFactor reserves 30% headroom for retries, cold starts, and bursts (architecture.md §4).
	DefaultSafetyFactor = 0.7
	// DefaultP95Latency is the default assumed p95 latency of the 2-lambda internal API (400 ms).
	DefaultP95Latency = 400 * time.Millisecond
	// DefaultMetricNamespace is the default CloudWatch EMF namespace.
	DefaultMetricNamespace = "Tapi/Scheduler"
	// MetricProviderOverCapacity is the CloudWatch EMF metric name emitted for US3 alarms.
	MetricProviderOverCapacity = "ProviderOverCapacity"
)

// ErrInvalidConfig indicates invalid capacity checker configuration.
var ErrInvalidConfig = errors.New("invalid capacity configuration")

// ProviderVolume holds a provider's daily record count read from coordination.
type ProviderVolume struct {
	Provider string
	Count    int
}

// ProviderLister returns all registered providers and their daily counts.
type ProviderLister interface {
	ListProviderVolumes(ctx context.Context) ([]ProviderVolume, error)
}

// LatencyProvider optionally returns a provider-specific p95 latency; falls back to DefaultP95Latency.
type LatencyProvider interface {
	P95Latency(ctx context.Context, provider string) (time.Duration, bool, error)
}

// MetricEmitter publishes the ProviderOverCapacity EMF metric.
type MetricEmitter interface {
	EmitProviderOverCapacity(ctx context.Context, report ProviderReport) error
}

// Clock provides timestamps for EMF metric emission.
type Clock interface {
	Now() time.Time
}

// ProviderReport summarizes capacity evaluation for a single provider.
type ProviderReport struct {
	Provider       string
	DailyRecords   int
	P95Latency     time.Duration
	RequiredSec    float64
	BudgetSec      float64
	OverCapacity   bool
	UtilizationPct float64
}

// Config configures the capacity Check use case.
type Config struct {
	SafetyFactor   float64
	DefaultLatency time.Duration
}

// UseCase evaluates daily per-provider capacity against the serial execution limit (FR-006).
type UseCase struct {
	lister  ProviderLister
	latency LatencyProvider
	emitter MetricEmitter
	cfg     Config
}

// New creates a capacity validation UseCase.
func New(lister ProviderLister, latency LatencyProvider, emitter MetricEmitter, cfg Config) (*UseCase, error) {
	if lister == nil {
		return nil, fmt.Errorf("%w: lister must not be nil", ErrInvalidConfig)
	}
	if cfg.SafetyFactor == 0 {
		cfg.SafetyFactor = DefaultSafetyFactor
	}
	if math.IsNaN(cfg.SafetyFactor) || math.IsInf(cfg.SafetyFactor, 0) || cfg.SafetyFactor <= 0 || cfg.SafetyFactor > 1 {
		return nil, fmt.Errorf("%w: safety factor must be in (0, 1], got %v", ErrInvalidConfig, cfg.SafetyFactor)
	}
	if cfg.DefaultLatency == 0 {
		cfg.DefaultLatency = DefaultP95Latency
	}
	if cfg.DefaultLatency < 0 {
		return nil, fmt.Errorf("%w: default latency must be > 0", ErrInvalidConfig)
	}
	return &UseCase{
		lister:  lister,
		latency: latency,
		emitter: emitter,
		cfg:     cfg,
	}, nil
}

// Check evaluates all registered providers against k * p95 > 86400 * safetyFactor and emits EMF metrics.
func (u *UseCase) Check(ctx context.Context) ([]ProviderReport, error) {
	volumes, err := u.lister.ListProviderVolumes(ctx)
	if err != nil {
		return nil, fmt.Errorf("list provider volumes: %w", err)
	}

	budgetNanos := int64(math.Round(SecondsPerDay * u.cfg.SafetyFactor * float64(time.Second)))
	budget := float64(budgetNanos) / float64(time.Second)
	reports := make([]ProviderReport, 0, len(volumes))

	for _, vol := range volumes {
		p95 := u.cfg.DefaultLatency
		if u.latency != nil {
			d, ok, err := u.latency.P95Latency(ctx, vol.Provider)
			if err != nil {
				return nil, fmt.Errorf("read p95 latency (%s): %w", vol.Provider, err)
			}
			if ok && d > 0 {
				p95 = d
			}
		}

		requiredNanos := int64(vol.Count) * p95.Nanoseconds()
		required := float64(requiredNanos) / float64(time.Second)
		report := ProviderReport{
			Provider:       vol.Provider,
			DailyRecords:   vol.Count,
			P95Latency:     p95,
			RequiredSec:    required,
			BudgetSec:      budget,
			OverCapacity:   requiredNanos > budgetNanos,
			UtilizationPct: (required / budget) * 100.0,
		}
		if u.emitter != nil {
			if err := u.emitter.EmitProviderOverCapacity(ctx, report); err != nil {
				return nil, fmt.Errorf("emit ProviderOverCapacity (%s): %w", vol.Provider, err)
			}
		}
		reports = append(reports, report)
	}

	return reports, nil
}

// EMFEmitter writes CloudWatch Embedded Metric Format JSON lines for ProviderOverCapacity.
type EMFEmitter struct {
	mu        sync.Mutex
	out       io.Writer
	namespace string
	clock     Clock
}

// NewEMFEmitter creates an EMF metric writer targeting out (defaults to os.Stdout).
func NewEMFEmitter(out io.Writer, namespace string, clk Clock) *EMFEmitter {
	if out == nil {
		out = os.Stdout
	}
	if namespace == "" {
		namespace = DefaultMetricNamespace
	}
	return &EMFEmitter{out: out, namespace: namespace, clock: clk}
}

type emfMetricDef struct {
	Name string `json:"Name"`
	Unit string `json:"Unit"`
}

type emfMetricDirective struct {
	Namespace  string         `json:"Namespace"`
	Dimensions [][]string     `json:"Dimensions"`
	Metrics    []emfMetricDef `json:"Metrics"`
}

type emfMetadata struct {
	Timestamp         int64                `json:"Timestamp"`
	CloudWatchMetrics []emfMetricDirective `json:"CloudWatchMetrics"`
}

type emfCapacityEvent struct {
	AWS                  emfMetadata `json:"_aws"`
	Provider             string      `json:"provider"`
	ProviderOverCapacity int         `json:"ProviderOverCapacity"`
	DailyRecords         int         `json:"daily_records"`
	P95LatencyMs         int64       `json:"p95_latency_ms"`
	RequiredSeconds      float64     `json:"required_seconds"`
	CapacityBudgetSec    float64     `json:"capacity_budget_seconds"`
}

// EmitProviderOverCapacity writes one CloudWatch EMF JSON log event.
func (e *EMFEmitter) EmitProviderOverCapacity(_ context.Context, report ProviderReport) error {
	now := time.Now().UTC()
	if e.clock != nil {
		now = e.clock.Now().UTC()
	}

	metricVal := 0
	if report.OverCapacity {
		metricVal = 1
	}

	event := emfCapacityEvent{
		AWS: emfMetadata{
			Timestamp: now.UnixMilli(),
			CloudWatchMetrics: []emfMetricDirective{
				{
					Namespace:  e.namespace,
					Dimensions: [][]string{{"provider"}},
					Metrics:    []emfMetricDef{{Name: MetricProviderOverCapacity, Unit: "Count"}},
				},
			},
		},
		Provider:             report.Provider,
		ProviderOverCapacity: metricVal,
		DailyRecords:         report.DailyRecords,
		P95LatencyMs:         report.P95Latency.Milliseconds(),
		RequiredSeconds:      report.RequiredSec,
		CapacityBudgetSec:    report.BudgetSec,
	}

	b, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal EMF event: %w", err)
	}
	b = append(b, '\n')

	e.mu.Lock()
	defer e.mu.Unlock()
	if _, err := e.out.Write(b); err != nil {
		return fmt.Errorf("write EMF event: %w", err)
	}
	return nil
}
