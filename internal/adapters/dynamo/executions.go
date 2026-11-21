package dynamo

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/martingomezvega/tapi-challenge/internal/app/dispatch"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

// DefaultExecutionTTL is the retention period for execution records (90 days, 003 FR-004).
const DefaultExecutionTTL = 90 * 24 * time.Hour

// ExecutionsRepo provides DynamoDB persistence for the executions table (PK request_id, SK run_date).
type ExecutionsRepo struct {
	client    *dynamodb.Client
	tableName string
	ttl       time.Duration
}

// NewExecutionsRepo creates a DynamoDB-backed repository for daily executions.
func NewExecutionsRepo(client *dynamodb.Client, tableName string, ttl time.Duration) *ExecutionsRepo {
	if ttl <= 0 {
		ttl = DefaultExecutionTTL
	}
	return &ExecutionsRepo{
		client:    client,
		tableName: tableName,
		ttl:       ttl,
	}
}

// CreatePending conditionally inserts a PENDING execution (attribute_not_exists(request_id)).
// If already present, returns Created=false along with the existing Status and CreatedAt.
func (r *ExecutionsRepo) CreatePending(_ context.Context, _ string, _ domain.RunDate, _ string, _ int, _ time.Time) (dispatch.CreatePendingResult, error) {
	return dispatch.CreatePendingResult{}, errors.New("not implemented")
}

// MarkEnqueued transitions PENDING executions to ENQUEUED conditionally.
func (r *ExecutionsRepo) MarkEnqueued(_ context.Context, _ []dispatch.ExecKey, _ time.Time) error {
	return errors.New("not implemented")
}
