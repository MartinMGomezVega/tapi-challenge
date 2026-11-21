package domain

import (
	"cmp"
	"errors"
	"fmt"
	"strings"
	"time"
)

const runDateLayout = "2006-01-02"

// ErrInvalidRunDate indicates a malformed YYYY-MM-DD run date string.
var ErrInvalidRunDate = errors.New("invalid run date")

// RunDate identifies a logical execution day formatted as YYYY-MM-DD in RUN_DAY_TZ.
type RunDate string

// ParseRunDate validates a YYYY-MM-DD string and returns a RunDate.
func ParseRunDate(s string) (RunDate, error) {
	trimmed := strings.TrimSpace(s)
	t, err := time.Parse(runDateLayout, trimmed)
	if err != nil || t.Format(runDateLayout) != trimmed {
		return "", fmt.Errorf("%w: %q", ErrInvalidRunDate, s)
	}
	return RunDate(trimmed), nil
}

// Time returns midnight UTC for the RunDate.
func (d RunDate) Time() (time.Time, error) {
	t, err := time.Parse(runDateLayout, string(d))
	if err != nil || t.Format(runDateLayout) != string(d) {
		return time.Time{}, fmt.Errorf("%w: %q", ErrInvalidRunDate, string(d))
	}
	return t.UTC(), nil
}

// Next returns the following calendar day.
func (d RunDate) Next() (RunDate, error) {
	t, err := d.Time()
	if err != nil {
		return "", err
	}
	return RunDate(t.AddDate(0, 0, 1).Format(runDateLayout)), nil
}

// IdempotencyKey builds the 4-layer idempotency key "request_id#run_date" (ADR-005).
func IdempotencyKey(requestID string, date RunDate) string {
	return requestID + "#" + string(date)
}

// SlotCoord uniquely identifies a 1-minute slot within a logical RunDate.
type SlotCoord struct {
	Date RunDate
	Slot int
}

// Validate checks Date format and Slot bounds [0, 1440).
func (c SlotCoord) Validate() error {
	if _, err := c.Date.Time(); err != nil {
		return err
	}
	if c.Slot < 0 || c.Slot >= SlotsPerDay {
		return fmt.Errorf("%w: %d", ErrInvalidSlot, c.Slot)
	}
	return nil
}

// Compare compares two SlotCoords chronologically (-1, 0, +1).
func (c SlotCoord) Compare(other SlotCoord) int {
	if d := cmp.Compare(string(c.Date), string(other.Date)); d != 0 {
		return d
	}
	return cmp.Compare(c.Slot, other.Slot)
}

// Next returns the next 1-minute SlotCoord, crossing midnight into Date+1 slot 0 when Slot==1439.
func (c SlotCoord) Next() (SlotCoord, error) {
	if err := c.Validate(); err != nil {
		return SlotCoord{}, err
	}
	if c.Slot+1 < SlotsPerDay {
		return SlotCoord{Date: c.Date, Slot: c.Slot + 1}, nil
	}
	nextDate, err := c.Date.Next()
	if err != nil {
		return SlotCoord{}, err
	}
	return SlotCoord{Date: nextDate, Slot: 0}, nil
}

// SlotOf converts a timestamp into its logical (RunDate, slot) in location loc (defaults to UTC).
func SlotOf(now time.Time, loc *time.Location) SlotCoord {
	if loc == nil {
		loc = time.UTC
	}
	local := now.In(loc)
	return SlotCoord{
		Date: RunDate(local.Format(runDateLayout)),
		Slot: local.Hour()*60 + local.Minute(),
	}
}

// RangeSlots returns all SlotCoords strictly after from and up to and including to: (from, to].
// Handles single-day ranges, midnight crossings, and multi-day catch-up in chronological order.
func RangeSlots(from, to SlotCoord) ([]SlotCoord, error) {
	if err := from.Validate(); err != nil {
		return nil, fmt.Errorf("from coord: %w", err)
	}
	if err := to.Validate(); err != nil {
		return nil, fmt.Errorf("to coord: %w", err)
	}
	if from.Compare(to) >= 0 {
		return nil, nil
	}

	lag, err := LagMinutes(from, to)
	if err != nil {
		return nil, err
	}
	out := make([]SlotCoord, 0, lag)
	cur := from
	for cur.Compare(to) < 0 {
		next, err := cur.Next()
		if err != nil {
			return nil, err
		}
		out = append(out, next)
		cur = next
	}
	return out, nil
}

// LagMinutes returns the number of 1-minute slots between from and to (0 if to <= from).
func LagMinutes(from, to SlotCoord) (int, error) {
	if err := from.Validate(); err != nil {
		return 0, fmt.Errorf("from coord: %w", err)
	}
	if err := to.Validate(); err != nil {
		return 0, fmt.Errorf("to coord: %w", err)
	}
	if from.Compare(to) >= 0 {
		return 0, nil
	}
	fromT, err := from.Date.Time()
	if err != nil {
		return 0, err
	}
	toT, err := to.Date.Time()
	if err != nil {
		return 0, err
	}
	days := int(toT.Sub(fromT) / (24 * time.Hour))
	return days*SlotsPerDay + (to.Slot - from.Slot), nil
}
