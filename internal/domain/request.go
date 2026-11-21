// Package domain contains pure business entities, invariants, and policies.
package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"
	"unicode"
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
	// ErrRequestNotFound indicates no request exists with the given ID.
	ErrRequestNotFound = errors.New("request not found")
	// ErrVersionConflict indicates an optimistic locking conflict on Request.Version.
	ErrVersionConflict = errors.New("request version conflict")
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

// ProviderRequestItem is the lightweight projection stored in gsi_provider (FR-004b),
// returned in ascending SK=id order so streaming backfill computes rank without sorting
// or extra GetItem calls.
type ProviderRequestItem struct {
	ID       string
	Provider string
	Version  int64
	Slot     int
	Shard    int
	Enabled  bool
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
func NewRequest(in NewRequestInput, allowlist []string, now time.Time) (Request, error) {
	method := strings.ToUpper(strings.TrimSpace(in.Method))
	if method == "" {
		method = DefaultMethod
	}

	ts := now.UTC()
	req := Request{
		ID:        strings.TrimSpace(in.ID),
		Provider:  strings.TrimSpace(in.Provider),
		Endpoint:  strings.TrimSpace(in.Endpoint),
		Method:    method,
		Body:      bytes.Clone(in.Body),
		Enabled:   in.Enabled,
		Slot:      -1,
		Shard:     -1,
		Version:   1,
		CreatedAt: ts,
		UpdatedAt: ts,
	}

	if err := req.Validate(allowlist); err != nil {
		return Request{}, err
	}
	return req, nil
}

// Validate checks FR-007 invariants against the given endpoint allowlist.
func (r Request) Validate(allowlist []string) error {
	if strings.TrimSpace(r.ID) == "" {
		return ErrEmptyID
	}
	if err := validateProvider(r.Provider); err != nil {
		return err
	}
	if err := validateEndpoint(r.Endpoint, allowlist); err != nil {
		return err
	}
	if err := validateMethod(r.Method); err != nil {
		return err
	}
	return validateBody(r.Body)
}

func validateProvider(provider string) error {
	trimmed := strings.TrimSpace(provider)
	if trimmed == "" {
		return fmt.Errorf("%w: must not be empty", ErrInvalidProvider)
	}
	if strings.ContainsRune(trimmed, '#') {
		return fmt.Errorf("%w: must not contain '#'", ErrInvalidProvider)
	}
	return nil
}

func validateEndpoint(endpoint string, allowlist []string) error {
	if endpoint == "" ||
		!strings.HasPrefix(endpoint, "/") ||
		strings.HasPrefix(endpoint, "//") ||
		strings.Contains(endpoint, "://") ||
		strings.ContainsFunc(endpoint, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
		return fmt.Errorf("%w: %q", ErrInvalidEndpoint, endpoint)
	}

	// Reject path traversal segments ("..") before checking allowlist prefixes.
	pathOnly, _, _ := strings.Cut(endpoint, "?")
	if slices.Contains(strings.Split(pathOnly, "/"), "..") {
		return fmt.Errorf("%w: path traversal not permitted", ErrInvalidEndpoint)
	}

	for _, prefix := range allowlist {
		if prefix != "" && strings.HasPrefix(pathOnly, prefix) {
			return nil
		}
	}
	return fmt.Errorf("%w: %q", ErrEndpointNotAllowed, pathOnly)
}

func validateMethod(method string) error {
	switch method {
	case "POST", "GET", "PUT", "PATCH", "DELETE":
		return nil
	default:
		return fmt.Errorf("%w: %q", ErrInvalidMethod, method)
	}
}

func validateBody(body []byte) error {
	if len(body) > MaxBodyBytes {
		return fmt.Errorf("%w: %d bytes (max %d)", ErrBodyTooLarge, len(body), MaxBodyBytes)
	}
	if len(body) == 0 || !json.Valid(body) {
		return ErrInvalidBody
	}
	return nil
}

// SlotShard returns the GSI partition key formatted as "%04d#%02d".
func (r Request) SlotShard() string {
	return FormatSlotShard(r.Slot, r.Shard)
}

// ProviderID returns the GSI sort key formatted as "<provider>#<id>".
func (r Request) ProviderID() string {
	return FormatProviderID(r.Provider, r.ID)
}

// FormatSlotShard formats a (slot, shard) pair as "%04d#%02d".
func FormatSlotShard(slot, shard int) string {
	return fmt.Sprintf("%04d#%02d", slot, shard)
}

// FormatProviderID formats a (provider, id) pair as "<provider>#<id>".
func FormatProviderID(provider, id string) string {
	return provider + "#" + id
}

// LogValue implements slog.LogValuer without exposing PII in Body.
func (r Request) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("id", r.ID),
		slog.String("provider", r.Provider),
		slog.String("endpoint", r.Endpoint),
		slog.String("method", r.Method),
		slog.Bool("enabled", r.Enabled),
		slog.Int("slot", r.Slot),
		slog.Int("shard", r.Shard),
		slog.Int64("version", r.Version),
		slog.Int("body_bytes", len(r.Body)),
	)
}

// String implements fmt.Stringer without exposing PII in Body.
func (r Request) String() string {
	return fmt.Sprintf(
		"Request{id=%s provider=%s method=%s endpoint=%s enabled=%t slot=%d shard=%d body_bytes=%d}",
		r.ID, r.Provider, r.Method, r.Endpoint, r.Enabled, r.Slot, r.Shard, len(r.Body),
	)
}
