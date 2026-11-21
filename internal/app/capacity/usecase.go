// Package capacity validates daily per-provider serial execution capacity (001-US3, FR-006).
package capacity

import (
	"context"
	"errors"
	"io"
	"time"
)

const (
	// SecondsPerDay is the number of seconds in a 24-hour day.
	SecondsPerDay = 86400.0
	// DefaultSafetyFactor reserves 30% headroom for retries, cold starts, and bursts (architecture.md §4).
	DefaultSafetyFactor = 0.7
	// DefaultP95Latency is the default assumed p95 latency of the 2-lambda internal API (400 ms).
	DefaultP95Latency = 400 * time.Millisecond
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
	return &UseCase{
		lister:  lister,
		latency: latency,
		emitter: emitter,
		cfg:     cfg,
	}, nil
}

// Check evaluates all registered providers and emits ProviderOverCapacity metrics.
func (u *UseCase) Check(_ context.Context) ([]ProviderReport, error) {
	return nil, errors.New("not implemented")
}

// EMFEmitter writes CloudWatch Embedded Metric Format JSON lines for ProviderOverCapacity.
type EMFEmitter struct {
	out       io.Writer
	namespace string
	clock     Clock
}

// NewEMFEmitter creates an EMF metric writer targeting out (e.g. os.Stdout).
func NewEMFEmitter(out io.Writer, namespace string, clk Clock) *EMFEmitter {
	return &EMFEmitter{out: out, namespace: namespace, clock: clk}
}

// EmitProviderOverCapacity writes one CloudWatch EMF JSON log event.
func (e *EMFEmitter) EmitProviderOverCapacity(_ context.Context, _ ProviderReport) error {
	return errors.New("not implemented")
}
