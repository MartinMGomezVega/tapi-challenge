package clock_test

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/platform/clock"
)

func TestNewWithSource_SpeedupScaling(t *testing.T) {
	t.Parallel()

	origin := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		speedup     float64
		realElapsed time.Duration
		wantElapsed time.Duration
	}{
		{
			name:        "speedup 1x matches real time",
			speedup:     1,
			realElapsed: 5 * time.Minute,
			wantElapsed: 5 * time.Minute,
		},
		{
			name:        "speedup 720x simulates 1 full day in 2 minutes",
			speedup:     720,
			realElapsed: 2 * time.Minute,
			wantElapsed: 24 * time.Hour,
		},
		{
			name:        "speedup 60x maps 1 real second to 1 virtual minute",
			speedup:     60,
			realElapsed: 10 * time.Second,
			wantElapsed: 10 * time.Minute,
		},
		{
			name:        "zero elapsed time stays at origin",
			speedup:     720,
			realElapsed: 0,
			wantElapsed: 0,
		},
		{
			name:        "fractional speedup 0.5x slows time by half",
			speedup:     0.5,
			realElapsed: 10 * time.Second,
			wantElapsed: 5 * time.Second,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			now := origin
			c, err := clock.NewWithSource(tc.speedup, func() time.Time { return now })
			if err != nil {
				t.Fatalf("NewWithSource(%v) unexpected error: %v", tc.speedup, err)
			}

			if got := c.Now(); !got.Equal(origin) {
				t.Fatalf("initial Now() = %v, want %v", got, origin)
			}

			now = origin.Add(tc.realElapsed)
			got := c.Now()
			want := origin.Add(tc.wantElapsed)
			if !got.Equal(want) {
				t.Errorf("Now() after %v at %vx = %v, want %v", tc.realElapsed, tc.speedup, got, want)
			}
			if got.Location() != time.UTC {
				t.Errorf("Now() location = %v, want UTC", got.Location())
			}
		})
	}
}

func TestNew_InvalidSpeedup(t *testing.T) {
	t.Parallel()

	invalid := []struct {
		name    string
		speedup float64
	}{
		{name: "zero", speedup: 0},
		{name: "negative", speedup: -10},
		{name: "NaN", speedup: math.NaN()},
		{name: "+Inf", speedup: math.Inf(1)},
		{name: "-Inf", speedup: math.Inf(-1)},
	}

	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := clock.New(tc.speedup)
			if !errors.Is(err, clock.ErrInvalidSpeedup) {
				t.Errorf("New(%v) error = %v, want %v", tc.speedup, err, clock.ErrInvalidSpeedup)
			}
		})
	}
}

func TestNewWithSource_NilSourceDefaultsToWallClock(t *testing.T) {
	t.Parallel()

	c, err := clock.NewWithSource(1, nil)
	if err != nil {
		t.Fatalf("NewWithSource(1, nil) unexpected error: %v", err)
	}
	if c.Now().IsZero() {
		t.Error("Now() returned zero time")
	}
}

func TestNewFromEnv(t *testing.T) {
	tests := []struct {
		name        string
		envValue    string
		wantSpeedup float64
		wantErr     error
	}{
		{
			name:        "unset defaults to 1",
			envValue:    "",
			wantSpeedup: 1,
		},
		{
			name:        "valid integer 720",
			envValue:    "720",
			wantSpeedup: 720,
		},
		{
			name:        "valid float 1.5",
			envValue:    "1.5",
			wantSpeedup: 1.5,
		},
		{
			name:     "non-numeric returns ErrInvalidSpeedup",
			envValue: "fast",
			wantErr:  clock.ErrInvalidSpeedup,
		},
		{
			name:     "zero returns ErrInvalidSpeedup",
			envValue: "0",
			wantErr:  clock.ErrInvalidSpeedup,
		},
		{
			name:     "negative returns ErrInvalidSpeedup",
			envValue: "-5",
			wantErr:  clock.ErrInvalidSpeedup,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CLOCK_SPEEDUP", tc.envValue)

			c, err := clock.NewFromEnv()
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("NewFromEnv() error = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewFromEnv() unexpected error: %v", err)
			}
			if c.Speedup() != tc.wantSpeedup {
				t.Errorf("Speedup() = %v, want %v", c.Speedup(), tc.wantSpeedup)
			}
		})
	}
}
