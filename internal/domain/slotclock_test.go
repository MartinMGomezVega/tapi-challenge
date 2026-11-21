package domain_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

// T001: SlotOf(now, tz) returns exact (run_date, slot) in UTC and America/Argentina/Buenos_Aires.
func TestSlotOf(t *testing.T) {
	t.Parallel()

	ba, err := time.LoadLocation("America/Argentina/Buenos_Aires")
	if err != nil {
		t.Fatalf("LoadLocation(BA): %v", err)
	}

	tests := []struct {
		name string
		now  time.Time
		loc  *time.Location
		want domain.SlotCoord
	}{
		{
			name: "UTC midnight",
			now:  time.Date(2026, 11, 20, 0, 0, 45, 0, time.UTC),
			loc:  time.UTC,
			want: domain.SlotCoord{Date: "2026-11-20", Slot: 0},
		},
		{
			name: "UTC 12:22 -> slot 742",
			now:  time.Date(2026, 11, 20, 12, 22, 0, 0, time.UTC),
			loc:  time.UTC,
			want: domain.SlotCoord{Date: "2026-11-20", Slot: 742},
		},
		{
			name: "UTC 23:59 -> slot 1439",
			now:  time.Date(2026, 11, 20, 23, 59, 59, 0, time.UTC),
			loc:  time.UTC,
			want: domain.SlotCoord{Date: "2026-11-20", Slot: 1439},
		},
		{
			name: "nil location defaults to UTC",
			now:  time.Date(2026, 11, 20, 1, 5, 0, 0, time.UTC),
			loc:  nil,
			want: domain.SlotCoord{Date: "2026-11-20", Slot: 65},
		},
		{
			name: "Buenos Aires (UTC-3): 01:30 UTC on Nov 21 is 22:30 Nov 20 in BA (slot 1350)",
			now:  time.Date(2026, 11, 21, 1, 30, 0, 0, time.UTC),
			loc:  ba,
			want: domain.SlotCoord{Date: "2026-11-20", Slot: 1350},
		},
		{
			name: "Buenos Aires (UTC-3): 03:00 UTC on Nov 21 is 00:00 Nov 21 in BA (slot 0)",
			now:  time.Date(2026, 11, 21, 3, 0, 0, 0, time.UTC),
			loc:  ba,
			want: domain.SlotCoord{Date: "2026-11-21", Slot: 0},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := domain.SlotOf(tc.now, tc.loc)
			if got != tc.want {
				t.Errorf("SlotOf(%v, %v) = %+v, want %+v", tc.now, tc.loc, got, tc.want)
			}
		})
	}
}

// T002: RangeSlots(from, to) iterates chronologically in (from, to] including midnight crossing.
func TestRangeSlots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		from domain.SlotCoord
		to   domain.SlotCoord
		want []domain.SlotCoord
	}{
		{
			name: "equal coords produce empty range",
			from: domain.SlotCoord{Date: "2026-11-20", Slot: 600},
			to:   domain.SlotCoord{Date: "2026-11-20", Slot: 600},
			want: nil,
		},
		{
			name: "reverse coords produce empty range",
			from: domain.SlotCoord{Date: "2026-11-20", Slot: 605},
			to:   domain.SlotCoord{Date: "2026-11-20", Slot: 600},
			want: nil,
		},
		{
			name: "same day 3 slots",
			from: domain.SlotCoord{Date: "2026-11-20", Slot: 600},
			to:   domain.SlotCoord{Date: "2026-11-20", Slot: 603},
			want: []domain.SlotCoord{
				{Date: "2026-11-20", Slot: 601},
				{Date: "2026-11-20", Slot: 602},
				{Date: "2026-11-20", Slot: 603},
			},
		},
		{
			name: "midnight crossing D 1438..1439 and D+1 0..2",
			from: domain.SlotCoord{Date: "2026-11-20", Slot: 1437},
			to:   domain.SlotCoord{Date: "2026-11-21", Slot: 2},
			want: []domain.SlotCoord{
				{Date: "2026-11-20", Slot: 1438},
				{Date: "2026-11-20", Slot: 1439},
				{Date: "2026-11-21", Slot: 0},
				{Date: "2026-11-21", Slot: 1},
				{Date: "2026-11-21", Slot: 2},
			},
		},
		{
			name: "month boundary Nov 30 -> Dec 01",
			from: domain.SlotCoord{Date: "2026-11-30", Slot: 1439},
			to:   domain.SlotCoord{Date: "2026-12-01", Slot: 1},
			want: []domain.SlotCoord{
				{Date: "2026-12-01", Slot: 0},
				{Date: "2026-12-01", Slot: 1},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := domain.RangeSlots(tc.from, tc.to)
			if err != nil {
				t.Fatalf("RangeSlots(%+v, %+v) unexpected error: %v", tc.from, tc.to, err)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("RangeSlots() =\n  %v\nwant\n  %v", got, tc.want)
			}
		})
	}
}

func TestRangeSlots_FullDayAndValidation(t *testing.T) {
	t.Parallel()

	// Full 1440-slot day from (D-1, 1439) to (D, 1439).
	fullDay, err := domain.RangeSlots(
		domain.SlotCoord{Date: "2026-11-19", Slot: 1439},
		domain.SlotCoord{Date: "2026-11-20", Slot: 1439},
	)
	if err != nil {
		t.Fatalf("RangeSlots(fullDay): %v", err)
	}
	if len(fullDay) != domain.SlotsPerDay {
		t.Fatalf("len(fullDay) = %d, want %d", len(fullDay), domain.SlotsPerDay)
	}
	if fullDay[0] != (domain.SlotCoord{Date: "2026-11-20", Slot: 0}) ||
		fullDay[1439] != (domain.SlotCoord{Date: "2026-11-20", Slot: 1439}) {
		t.Errorf("fullDay endpoints = (%+v, %+v)", fullDay[0], fullDay[1439])
	}

	lag, err := domain.LagMinutes(
		domain.SlotCoord{Date: "2026-11-20", Slot: 1435},
		domain.SlotCoord{Date: "2026-11-21", Slot: 3},
	)
	if err != nil {
		t.Fatalf("LagMinutes(): %v", err)
	}
	if lag != 8 {
		t.Errorf("LagMinutes() = %d, want 8", lag)
	}

	if _, err := domain.ParseRunDate("not-a-date"); !errors.Is(err, domain.ErrInvalidRunDate) {
		t.Errorf("ParseRunDate(bad) err = %v, want %v", err, domain.ErrInvalidRunDate)
	}
	if _, err := domain.RangeSlots(
		domain.SlotCoord{Date: "2026-11-20", Slot: 1440},
		domain.SlotCoord{Date: "2026-11-21", Slot: 0},
	); !errors.Is(err, domain.ErrInvalidSlot) {
		t.Errorf("RangeSlots(invalid slot) err = %v, want %v", err, domain.ErrInvalidSlot)
	}
	if _, err := domain.RangeSlots(
		domain.SlotCoord{Date: "2026-11-20", Slot: 0},
		domain.SlotCoord{Date: "bad-date", Slot: 0},
	); !errors.Is(err, domain.ErrInvalidRunDate) {
		t.Errorf("RangeSlots(invalid to date) err = %v, want %v", err, domain.ErrInvalidRunDate)
	}
	if d, err := domain.ParseRunDate("2026-11-20"); err != nil || d != "2026-11-20" {
		t.Errorf("ParseRunDate(valid) = (%q, %v)", d, err)
	}
	if got := domain.IdempotencyKey("req-1", "2026-11-20"); got != "req-1#2026-11-20" {
		t.Errorf("IdempotencyKey() = %q, want req-1#2026-11-20", got)
	}
	if lag, err := domain.LagMinutes(
		domain.SlotCoord{Date: "2026-11-21", Slot: 5},
		domain.SlotCoord{Date: "2026-11-21", Slot: 2},
	); err != nil || lag != 0 {
		t.Errorf("LagMinutes(reverse) = (%d, %v), want (0, nil)", lag, err)
	}
}
