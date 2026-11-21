// Package dynamo implements DynamoDB persistence adapters for catalog requests.
package dynamo

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

const (
	// DefaultPageSize is the default Query limit per page on gsi_slot_shard.
	DefaultPageSize int32 = 100
	gsiSlotShard          = "gsi_slot_shard"
)

// ErrInvalidPageToken indicates a malformed pagination cursor passed to ListBySlotShard.
var ErrInvalidPageToken = errors.New("invalid pagination token")

var errNotImplemented = errors.New("not implemented")

// RequestsOption configures a RequestsRepo instance.
type RequestsOption func(*RequestsRepo)

// WithPageSize sets the maximum number of items evaluated per GSI Query page.
func WithPageSize(size int32) RequestsOption {
	return func(r *RequestsRepo) {
		if size > 0 {
			r.pageSize = size
		}
	}
}

// WithClock overrides the clock used for UpdatedAt timestamps.
func WithClock(now func() time.Time) RequestsOption {
	return func(r *RequestsRepo) {
		if now != nil {
			r.now = now
		}
	}
}

// RequestsRepo provides DynamoDB access to the requests catalog table and gsi_slot_shard.
type RequestsRepo struct {
	client    *dynamodb.Client
	tableName string
	pageSize  int32
	now       func() time.Time
}

// NewRequestsRepo creates a DynamoDB-backed repository for catalog requests.
func NewRequestsRepo(client *dynamodb.Client, tableName string, opts ...RequestsOption) *RequestsRepo {
	r := &RequestsRepo{
		client:    client,
		tableName: tableName,
		pageSize:  DefaultPageSize,
		now:       time.Now,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Put stores a Request item in DynamoDB. Requests with Slot < 0 omit slot_shard so
// gsi_slot_shard remains sparse until a slot is assigned.
func (r *RequestsRepo) Put(_ context.Context, _ domain.Request) error {
	return errNotImplemented
}

// Get retrieves a full Request by its primary key id.
func (r *RequestsRepo) Get(_ context.Context, _ string) (domain.Request, error) {
	return domain.Request{}, errNotImplemented
}

// ListBySlotShard queries gsi_slot_shard for enabled requests in (slot, shard), paginated.
func (r *RequestsRepo) ListBySlotShard(_ context.Context, _, _ int, _ string) ([]domain.RequestKey, string, error) {
	return nil, "", errNotImplemented
}

// SetSlot atomically updates a request's slot, shard, and slot_shard conditioned on expectedVersion.
func (r *RequestsRepo) SetSlot(_ context.Context, _ string, _, _ int, _ int64) error {
	return errNotImplemented
}
