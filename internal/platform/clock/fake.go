package clock

import (
	"sync"
	"time"
)

// Provider is the minimal clock interface implemented by Clock and FakeClock.
type Provider interface {
	Now() time.Time
}

// FakeClock is a thread-safe controllable clock for deterministic unit and simulation tests (002-T004).
type FakeClock struct {
	mu  sync.RWMutex
	now time.Time
}

// NewFake creates a FakeClock initialized at start (converted to UTC).
func NewFake(start time.Time) *FakeClock {
	return &FakeClock{now: start.UTC()}
}

// Now returns the current virtual UTC time.
func (f *FakeClock) Now() time.Time {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.now
}

// Advance moves the virtual clock forward by d.
func (f *FakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// Set sets the virtual clock to t (UTC).
func (f *FakeClock) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = t.UTC()
}
