package dynamo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

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

type executionItem struct {
	RequestID         string `dynamodbav:"request_id"`
	RunDate           string `dynamodbav:"run_date"`
	Provider          string `dynamodbav:"provider"`
	Slot              int    `dynamodbav:"slot"`
	Status            string `dynamodbav:"status"`
	Attempts          int    `dynamodbav:"attempts"`
	RunDateStatus     string `dynamodbav:"run_date_status"`
	ProviderRequestID string `dynamodbav:"provider_request_id"`
	CreatedAt         string `dynamodbav:"created_at"`
	UpdatedAt         string `dynamodbav:"updated_at"`
	TTL               int64  `dynamodbav:"ttl"`
}

// CreatePending conditionally inserts a PENDING execution (attribute_not_exists(request_id)).
// If already present, returns Created=false along with the existing Status and CreatedAt.
func (r *ExecutionsRepo) CreatePending(
	ctx context.Context,
	reqID string,
	date domain.RunDate,
	provider string,
	slot int,
	now time.Time,
) (dispatch.CreatePendingResult, error) {
	ts := now.UTC()
	tsStr := ts.Format(time.RFC3339Nano)
	item := executionItem{
		RequestID:         reqID,
		RunDate:           string(date),
		Provider:          provider,
		Slot:              slot,
		Status:            dispatch.StatusPending,
		Attempts:          0,
		RunDateStatus:     string(date) + "#" + dispatch.StatusPending,
		ProviderRequestID: provider + "#" + reqID,
		CreatedAt:         tsStr,
		UpdatedAt:         tsStr,
		TTL:               ts.Add(r.ttl).Unix(),
	}

	av, err := attributevalue.MarshalMap(item)
	if err != nil {
		return dispatch.CreatePendingResult{}, fmt.Errorf("marshal execution %s#%s: %w", reqID, date, err)
	}

	_, err = r.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(r.tableName),
		Item:                av,
		ConditionExpression: aws.String("attribute_not_exists(#req_id)"),
		ExpressionAttributeNames: map[string]string{
			"#req_id": "request_id",
		},
	})
	if err == nil {
		return dispatch.CreatePendingResult{
			Created:   true,
			Status:    dispatch.StatusPending,
			CreatedAt: ts,
		}, nil
	}

	var condErr *types.ConditionalCheckFailedException
	if !errors.As(err, &condErr) {
		return dispatch.CreatePendingResult{}, fmt.Errorf("create pending %s#%s: %w", reqID, date, err)
	}

	// Execution already exists; read existing status & created_at for orphan recovery (US4).
	getOut, getErr := r.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(r.tableName),
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			"request_id": &types.AttributeValueMemberS{Value: reqID},
			"run_date":   &types.AttributeValueMemberS{Value: string(date)},
		},
	})
	if getErr != nil {
		return dispatch.CreatePendingResult{}, fmt.Errorf("read existing execution %s#%s: %w", reqID, date, getErr)
	}
	if len(getOut.Item) == 0 {
		return dispatch.CreatePendingResult{Created: false}, nil
	}

	var existing executionItem
	if err := attributevalue.UnmarshalMap(getOut.Item, &existing); err != nil {
		return dispatch.CreatePendingResult{}, fmt.Errorf("unmarshal existing execution %s#%s: %w", reqID, date, err)
	}
	createdAt, _ := time.Parse(time.RFC3339Nano, existing.CreatedAt)
	return dispatch.CreatePendingResult{
		Created:   false,
		Status:    existing.Status,
		CreatedAt: createdAt.UTC(),
	}, nil
}

// MarkEnqueued transitions PENDING executions to ENQUEUED conditionally.
// Items already transitioned beyond PENDING are ignored idempotently.
func (r *ExecutionsRepo) MarkEnqueued(ctx context.Context, keys []dispatch.ExecKey, now time.Time) error {
	tsStr := now.UTC().Format(time.RFC3339Nano)
	for _, k := range keys {
		_, err := r.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(r.tableName),
			Key: map[string]types.AttributeValue{
				"request_id": &types.AttributeValueMemberS{Value: k.RequestID},
				"run_date":   &types.AttributeValueMemberS{Value: string(k.RunDate)},
			},
			UpdateExpression:    aws.String("SET #status = :enqueued, #rds = :rds, #updated_at = :now"),
			ConditionExpression: aws.String("#status = :pending"),
			ExpressionAttributeNames: map[string]string{
				"#status":     "status",
				"#rds":        "run_date_status",
				"#updated_at": "updated_at",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pending":  &types.AttributeValueMemberS{Value: dispatch.StatusPending},
				":enqueued": &types.AttributeValueMemberS{Value: dispatch.StatusEnqueued},
				":rds":      &types.AttributeValueMemberS{Value: string(k.RunDate) + "#" + dispatch.StatusEnqueued},
				":now":      &types.AttributeValueMemberS{Value: tsStr},
			},
		})
		if err != nil {
			var condErr *types.ConditionalCheckFailedException
			if errors.As(err, &condErr) {
				continue
			}
			return fmt.Errorf("mark enqueued %s#%s: %w", k.RequestID, k.RunDate, err)
		}
	}
	return nil
}
