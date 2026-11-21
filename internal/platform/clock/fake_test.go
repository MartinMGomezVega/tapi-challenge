package clock_test

import (
	"sync"
	"testing"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/platform/clock"
)

func TestFakeClock(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	var p clock.Provider = clock.NewFake(start)
	fc := p.(*clock.FakeClock)

	if got := fc.Now(); !got.Equal(start) {
		t.Fatalf("Now() = %v, want %v", got, start)
	}

	fc.Advance(15 * time.Minute)
	if got, want := fc.Now(), start.Add(15*time.Minute); !got.Equal(want) {
		t.Fatalf("after Advance(15m): Now() = %v, want %v", got, want)
	}

	target := start.Add(24 * time.Hour)
	fc.Set(target)
	if got := fc.Now(); !got.Equal(target) {
		t.Fatalf("after Set: Now() = %v, want %v", got, target)
	}

	// Verify concurrency safety under -race.
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for range 50 {
				fc.Advance(time.Second)
			}
		}()
		go func() {
			defer wg.Done()
			for range 50 {
				_ = fc.Now()
			}
		}()
	}
	wg.Wait()
}
