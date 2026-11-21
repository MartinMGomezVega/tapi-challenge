// Package clock provides real and accelerated wall-clock implementations for local simulation.
package clock

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

const envClockSpeedup = "CLOCK_SPEEDUP"

// ErrInvalidSpeedup is returned when CLOCK_SPEEDUP is not a positive finite number.
var ErrInvalidSpeedup = errors.New("clock speedup must be a positive finite number")

// Clock provides real or accelerated UTC wall-clock time for local simulation (CLOCK_SPEEDUP).
type Clock struct {
	speedup float64
	source  func() time.Time
	origin  time.Time
}

// New creates a Clock backed by time.Now with the given speedup factor.
func New(speedup float64) (*Clock, error) {
	return NewWithSource(speedup, time.Now)
}

// NewWithSource creates a Clock using a custom time source, enabling deterministic testing.
func NewWithSource(speedup float64, source func() time.Time) (*Clock, error) {
	if math.IsNaN(speedup) || math.IsInf(speedup, 0) || speedup <= 0 {
		return nil, fmt.Errorf("%w: %v", ErrInvalidSpeedup, speedup)
	}
	if source == nil {
		source = time.Now
	}
	return &Clock{
		speedup: speedup,
		source:  source,
		origin:  source().UTC(),
	}, nil
}

// NewFromEnv creates a Clock configured from the CLOCK_SPEEDUP environment variable.
// When CLOCK_SPEEDUP is unset or empty, speedup defaults to 1.
func NewFromEnv() (*Clock, error) {
	raw := strings.TrimSpace(os.Getenv(envClockSpeedup))
	if raw == "" {
		return New(1)
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: %q: %w", ErrInvalidSpeedup, raw, err)
	}
	return New(v)
}

// Speedup returns the configured acceleration factor.
func (c *Clock) Speedup() float64 {
	return c.speedup
}

// Now returns the current (possibly accelerated) UTC time.
func (c *Clock) Now() time.Time {
	now := c.source().UTC()
	if c.speedup == 1 {
		return now
	}
	elapsed := now.Sub(c.origin)
	scaled := time.Duration(float64(elapsed) * c.speedup)
	return c.origin.Add(scaled)
}
