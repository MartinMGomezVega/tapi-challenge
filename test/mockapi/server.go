// Package mockapi implements a configurable mock internal API for integration and load tests (003-T003).
package mockapi

import (
	"net/http"
	"time"
)

// FaultRule configures deterministic or probabilistic error injection for a provider.
type FaultRule struct {
	StatusCode  int
	RetryAfter  string
	Delay       time.Duration
	Body        string
	Rate        float64 // 0..1; 0 with StatusCode>0 means 100%
	FirstNOnly  int     // if > 0, apply fault only to the first N requests for this provider
}

// Config configures mockapi behavior.
type Config struct {
	DefaultLatency time.Duration
	LatencyJitter  time.Duration
	ResponseSize   int
	Faults         map[string]FaultRule
}

// ProviderStats summarizes call metrics observed for a single provider.
type ProviderStats struct {
	Calls       int64 `json:"calls"`
	Inflight    int64 `json:"inflight"`
	MaxInflight int64 `json:"max_inflight"`
	Errors      int64 `json:"errors"`
}

// StatsSnapshot is returned by GET /stats and Stats().
type StatsSnapshot struct {
	TotalCalls         int64                    `json:"total_calls"`
	DuplicateIdemKeys  int64                    `json:"duplicate_idem_keys"`
	GlobalMaxInflight  int64                    `json:"global_max_inflight"`
	MaxProviderInflight int64                   `json:"max_provider_inflight"`
	Providers          map[string]ProviderStats `json:"providers"`
}

// Server is a concurrent-safe mock HTTP handler simulating the internal provider API.
type Server struct{}

// New creates a new mock API server.
func New(cfg Config) *Server { return &Server{} }

// ServeHTTP handles provider query calls and /stats + /healthz endpoints.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}

// SetFault updates or removes a provider fault rule dynamically.
func (s *Server) SetFault(provider string, rule *FaultRule) {}

// Stats returns a snapshot of observed calls, concurrency, and idempotency keys.
func (s *Server) Stats() StatsSnapshot { return StatsSnapshot{} }

// Reset clears collected stats.
func (s *Server) Reset() {}
