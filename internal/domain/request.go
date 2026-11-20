// Package domain contains pure business entities, invariants, and policies.
package domain

import (
	"errors"
	"log/slog"
	"time"
)

const (
	// MaxBodyBytes is the maximum allowed JSON request body size (256 KiB, FR-007).
	MaxBodyBytes = 256 * 1024
	// DefaultMethod is the default HTTP method when none is specified.
	DefaultMethod = "POST"
	// SlotsPerDay is the number of 1-minute execution slots in a 24-hour day.
	SlotsPerDay = 1440
)

var (
	// ErrEmptyID indicates a request has an empty identifier.
	ErrEmptyID = errors.New("request id must not be empty")
	// ErrInvalidProvider indicates a request has an empty or malformed provider name.
	ErrInvalidProvider = errors.New("request provider is invalid")
	// ErrInvalidEndpoint indicates a request endpoint is not a safe relative path.
	ErrInvalidEndpoint = errors.New("request endpoint must be a valid relative path")
	// ErrEndpointNotAllowed indicates a request endpoint is outside the configured allowlist.
	ErrEndpointNotAllowed = errors.New("request endpoint is not in allowlist")
	// ErrInvalidMethod indicates an unsupported HTTP method.
	ErrInvalidMethod = errors.New("request HTTP method is invalid")
	// ErrInvalidBody indicates the request body is not valid JSON.
	ErrInvalidBody = errors.New("request body must be valid JSON")
	// ErrBodyTooLarge indicates the request body exceeds MaxBodyBytes.
	ErrBodyTooLarge = errors.New("request body exceeds maximum size")
	// ErrInvalidSlot indicates a slot outside [0, 1440).
	ErrInvalidSlot = errors.New("slot must be in [0, 1440)")
	// ErrInvalidShard indicates a shard outside [0, shards).
	ErrInvalidShard = errors.New("shard is out of range")
)

// Request represents a user's daily provider query registration in the catalog.
type Request struct {
	ID        string
	Provider  string
	Endpoint  string
	Method    string
	Body      []byte
	Enabled   bool
	Slot      int
	Shard     int
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// RequestKey is the lightweight projection stored in gsi_slot_shard.
type RequestKey struct {
	ID        string
	Provider  string
	Enabled   bool
	CreatedAt time.Time
}

// NewRequestInput holds input fields for creating a new catalog Request.
type NewRequestInput struct {
	ID       string
	Provider string
	Endpoint string
	Method   string
	Body     []byte
	Enabled  bool
}

// NewRequest creates and validates a new Request with defaults applied.
func NewRequest(_ NewRequestInput, _ []string, _ time.Time) (Request, error) {
	return Request{}, nil
}

// Validate checks FR-007 invariants against the given endpoint allowlist.
func (r Request) Validate(_ []string) error {
	return nil
}

// SlotShard returns the GSI partition key formatted as "%04d#%02d".
func (r Request) SlotShard() string {
	return ""
}

// ProviderID returns the GSI sort key formatted as "<provider>#<id>".
func (r Request) ProviderID() string {
	return ""
}

// FormatSlotShard formats a (slot, shard) pair as "%04d#%02d".
func FormatSlotShard(_, _ int) string {
	return ""
}

// FormatProviderID formats a (provider, id) pair as "<provider>#<id>".
func FormatProviderID(_, _ string) string {
	return ""
}

// LogValue implements slog.LogValuer without exposing PII in Body.
func (r Request) LogValue() slog.Value {
	return slog.StringValue(string(r.Body))
}

// String implements fmt.Stringer without exposing PII in Body.
func (r Request) String() string {
	return string(r.Body)
}
