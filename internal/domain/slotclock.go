package domain

import (
	"errors"
	"time"
)

// ErrInvalidRunDate indicates a malformed YYYY-MM-DD run date string.
var ErrInvalidRunDate = errors.New("invalid run date")

// RunDate identifies a logical execution day formatted as YYYY-MM-DD in RUN_DAY_TZ.
type RunDate string

// ParseRunDate validates a YYYY-MM-DD string and returns a RunDate.
func ParseRunDate(_ string) (RunDate, error) {
	return "", errors.New("not implemented")
}

// Time returns midnight UTC for the RunDate.
func (d RunDate) Time() (time.Time, error) {
	return time.Time{}, errors.New("not implemented")
}

// Next returns the following calendar day.
func (d RunDate) Next() (RunDate, error) {
	return "", errors.New("not implemented")
}

// SlotCoord uniquely identifies a 1-minute slot within a logical RunDate.
type SlotCoord struct {
	Date RunDate
	Slot int
}

// Compare compares two SlotCoords chronologically (-1, 0, +1).
func (c SlotCoord) Compare(_ SlotCoord) int {
	return 0
}

// Next returns the next 1-minute SlotCoord, crossing midnight into Date+1 slot 0 when Slot==1439.
func (c SlotCoord) Next() (SlotCoord, error) {
	return SlotCoord{}, errors.New("not implemented")
}

// SlotOf converts a timestamp into its logical (RunDate, slot) in location loc (defaults to UTC).
func SlotOf(_ time.Time, _ *time.Location) SlotCoord {
	return SlotCoord{Slot: -1}
}

// RangeSlots returns all SlotCoords strictly after from and up to and including to: (from, to].
// Handles single-day ranges, midnight crossings, and multi-day catch-up in chronological order.
func RangeSlots(_, _ SlotCoord) ([]SlotCoord, error) {
	return nil, errors.New("not implemented")
}

// LagMinutes returns the number of 1-minute slots between from and to (0 if to <= from).
func LagMinutes(_, _ SlotCoord) (int, error) {
	return 0, errors.New("not implemented")
}
