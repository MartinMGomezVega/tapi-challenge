// Package mockapi implements a configurable mock internal API for integration and load tests (003-T003).
package mockapi

import (
	"bytes"
	"encoding/json"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// FaultRule configures deterministic or probabilistic error injection for a provider.
type FaultRule struct {
	StatusCode int
	RetryAfter string
	Delay      time.Duration
	Body       string
	Rate       float64 // 0..1; 0 with StatusCode>0 means 100%
	FirstNOnly int     // if > 0, apply fault only to the first N requests for this provider
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
	TotalCalls          int64                    `json:"total_calls"`
	DuplicateIdemKeys   int64                    `json:"duplicate_idem_keys"`
	GlobalMaxInflight   int64                    `json:"global_max_inflight"`
	MaxProviderInflight int64                    `json:"max_provider_inflight"`
	Providers           map[string]ProviderStats `json:"providers"`
}

type providerCounters struct {
	calls       atomic.Int64
	inflight    atomic.Int64
	maxInflight atomic.Int64
	errors      atomic.Int64
}

// Server is a concurrent-safe mock HTTP handler simulating the internal provider API.
type Server struct {
	cfg            Config
	payload        []byte
	totalCalls     atomic.Int64
	dupIdemKeys    atomic.Int64
	globalInflight atomic.Int64
	globalMax      atomic.Int64

	mu        sync.RWMutex
	faults    map[string]FaultRule
	providers map[string]*providerCounters
	idemMu    sync.Mutex
	seenIdem  map[string]int64
}

// New creates a new mock API server.
func New(cfg Config) *Server {
	faults := make(map[string]FaultRule, len(cfg.Faults))
	for k, v := range cfg.Faults {
		faults[k] = v
	}

	var payload []byte
	if cfg.ResponseSize > 0 {
		pad := strings.Repeat("x", cfg.ResponseSize)
		payload = []byte(`{"status":"ok","data":"` + pad + `"}`)
	} else {
		payload = []byte(`{"status":"ok","debt":0}`)
	}

	return &Server{
		cfg:       cfg,
		payload:   payload,
		faults:    faults,
		providers: make(map[string]*providerCounters),
		seenIdem:  make(map[string]int64),
	}
}

// SetFault updates or removes a provider fault rule dynamically.
func (s *Server) SetFault(provider string, rule *FaultRule) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rule == nil {
		delete(s.faults, provider)
		return
	}
	s.faults[provider] = *rule
}

// Reset clears collected stats and seen idempotency keys.
func (s *Server) Reset() {
	s.totalCalls.Store(0)
	s.dupIdemKeys.Store(0)
	s.globalInflight.Store(0)
	s.globalMax.Store(0)

	s.mu.Lock()
	s.providers = make(map[string]*providerCounters)
	s.mu.Unlock()

	s.idemMu.Lock()
	s.seenIdem = make(map[string]int64)
	s.idemMu.Unlock()
}

// Stats returns a snapshot of observed calls, concurrency, and idempotency keys.
func (s *Server) Stats() StatsSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := StatsSnapshot{
		TotalCalls:        s.totalCalls.Load(),
		DuplicateIdemKeys: s.dupIdemKeys.Load(),
		GlobalMaxInflight: s.globalMax.Load(),
		Providers:         make(map[string]ProviderStats, len(s.providers)),
	}
	for p, c := range s.providers {
		ps := ProviderStats{
			Calls:       c.calls.Load(),
			Inflight:    c.inflight.Load(),
			MaxInflight: c.maxInflight.Load(),
			Errors:      c.errors.Load(),
		}
		if ps.MaxInflight > out.MaxProviderInflight {
			out.MaxProviderInflight = ps.MaxInflight
		}
		out.Providers[p] = ps
	}
	return out
}

// ServeHTTP handles provider query calls and /stats, /reset, /healthz endpoints.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/healthz":
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
		return
	case "/stats":
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.Stats())
		return
	case "/reset":
		s.Reset()
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Drain request body cleanly for HTTP keep-alive reuse.
	if r.Body != nil {
		_, _ = io.Copy(io.Discard, r.Body)
	}

	provider := extractProvider(r)
	pc := s.getOrCreateProvider(provider)

	callNum := pc.calls.Add(1)
	s.totalCalls.Add(1)

	curGlobal := s.globalInflight.Add(1)
	updateAtomicMax(&s.globalMax, curGlobal)
	defer s.globalInflight.Add(-1)

	curProv := pc.inflight.Add(1)
	updateAtomicMax(&pc.maxInflight, curProv)
	defer pc.inflight.Add(-1)

	if idemKey := r.Header.Get("Idempotency-Key"); idemKey != "" {
		s.idemMu.Lock()
		s.seenIdem[idemKey]++
		if s.seenIdem[idemKey] > 1 {
			s.dupIdemKeys.Add(1)
		}
		s.idemMu.Unlock()
	}

	s.mu.RLock()
	rule, hasFault := s.faults[provider]
	s.mu.RUnlock()

	delay := s.cfg.DefaultLatency
	if s.cfg.LatencyJitter > 0 {
		delay += time.Duration(rand.Int64N(int64(s.cfg.LatencyJitter)))
	}
	if hasFault && rule.Delay > 0 {
		delay = rule.Delay
	}
	if delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-r.Context().Done():
			timer.Stop()
			pc.errors.Add(1)
			return
		case <-timer.C:
		}
	}

	if hasFault && shouldApplyFault(rule, callNum) {
		pc.errors.Add(1)
		if rule.RetryAfter != "" {
			w.Header().Set("Retry-After", rule.RetryAfter)
		}
		w.Header().Set("Content-Type", "application/json")
		status := rule.StatusCode
		if status == 0 {
			status = http.StatusInternalServerError
		}
		w.WriteHeader(status)
		body := rule.Body
		if body == "" {
			body = `{"error":"injected_fault"}`
		}
		_, _ = w.Write([]byte(body))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = bytes.NewReader(s.payload).WriteTo(w)
}

func (s *Server) getOrCreateProvider(provider string) *providerCounters {
	s.mu.RLock()
	pc, ok := s.providers[provider]
	s.mu.RUnlock()
	if ok {
		return pc
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if pc, ok = s.providers[provider]; ok {
		return pc
	}
	pc = &providerCounters{}
	s.providers[provider] = pc
	return pc
}

func shouldApplyFault(rule FaultRule, callNum int64) bool {
	if rule.StatusCode == 0 {
		return false
	}
	if rule.FirstNOnly > 0 && callNum > int64(rule.FirstNOnly) {
		return false
	}
	if rule.Rate > 0 && rule.Rate < 1 {
		return rand.Float64() < rule.Rate
	}
	return true
}

func extractProvider(r *http.Request) string {
	if p := r.Header.Get("X-Provider"); p != "" {
		return p
	}
	// Expected path pattern: /providers/<provider>/...
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) >= 2 && parts[0] == "providers" {
		return parts[1]
	}
	if len(parts) >= 1 && parts[0] != "" {
		return parts[0]
	}
	return "unknown"
}

func updateAtomicMax(target *atomic.Int64, candidate int64) {
	for {
		cur := target.Load()
		if candidate <= cur {
			return
		}
		if target.CompareAndSwap(cur, candidate) {
			return
		}
	}
}
