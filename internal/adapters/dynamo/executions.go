package dynamo

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/martingomezvega/tapi-challenge/internal/app/dispatch"
	"github.com/martingomezvega/tapi-challenge/internal/app/execute"
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
	LeaseOwner        string `dynamodbav:"lease_owner,omitempty"`
	LeaseExpiresAt    int64  `dynamodbav:"lease_expires_at,omitempty"`
	HTTPStatus        int    `dynamodbav:"http_status,omitempty"`
	ResponseBody      string `dynamodbav:"response_body,omitempty"`
	ResponseS3Key     string `dynamodbav:"response_s3_key,omitempty"`
	LatencyMs         int64  `dynamodbav:"latency_ms,omitempty"`
	ErrorClass        string `dynamodbav:"error_class,omitempty"`
	ErrorMessage      string `dynamodbav:"error_message,omitempty"`
	NextRetryAt       string `dynamodbav:"next_retry_at,omitempty"`
	RunDateStatus     string `dynamodbav:"run_date_status"`
	ProviderRequestID string `dynamodbav:"provider_request_id"`
	CreatedAt         string `dynamodbav:"created_at"`
	UpdatedAt         string `dynamodbav:"updated_at"`
	CompletedAt       string `dynamodbav:"completed_at,omitempty"`
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

	existing, found, getErr := r.getItem(ctx, reqID, date)
	if getErr != nil {
		return dispatch.CreatePendingResult{}, getErr
	}
	if !found {
		return dispatch.CreatePendingResult{Created: false}, nil
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
			TableName:           aws.String(r.tableName),
			Key:                 execKeyMap(k.RequestID, k.RunDate),
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

// Acquire conditionally transitions PENDING|ENQUEUED|RETRYING (or expired IN_PROGRESS) to IN_PROGRESS
// owned by owner until now+ttl (003 FR-002).
func (r *ExecutionsRepo) Acquire(
	ctx context.Context,
	key execute.ExecKey,
	owner string,
	ttl time.Duration,
	now time.Time,
) (execute.Execution, execute.AcquireOutcome, error) {
	ts := now.UTC()
	nowUnix := ts.Unix()
	expiresUnix := ts.Add(ttl).Unix()
	nowStr := ts.Format(time.RFC3339Nano)

	out, err := r.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(r.tableName),
		Key:       execKeyMap(key.RequestID, key.RunDate),
		UpdateExpression: aws.String(
			"SET #status = :in_progress, #rds = :rds, #owner = :owner, #expires = :expires, " +
				"#updated_at = :now_str, #attempts = if_not_exists(#attempts, :zero) + :one",
		),
		ConditionExpression: aws.String(
			"(#status IN (:pending, :enqueued, :retrying)) OR (#status = :in_progress AND #expires <= :now_unix)",
		),
		ExpressionAttributeNames: map[string]string{
			"#status":     "status",
			"#rds":        "run_date_status",
			"#owner":      "lease_owner",
			"#expires":    "lease_expires_at",
			"#updated_at": "updated_at",
			"#attempts":   "attempts",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pending":     &types.AttributeValueMemberS{Value: string(domain.StatusPending)},
			":enqueued":    &types.AttributeValueMemberS{Value: string(domain.StatusEnqueued)},
			":retrying":    &types.AttributeValueMemberS{Value: string(domain.StatusRetrying)},
			":in_progress": &types.AttributeValueMemberS{Value: string(domain.StatusInProgress)},
			":rds":         &types.AttributeValueMemberS{Value: string(key.RunDate) + "#" + string(domain.StatusInProgress)},
			":owner":       &types.AttributeValueMemberS{Value: owner},
			":expires":     &types.AttributeValueMemberN{Value: strconv.FormatInt(expiresUnix, 10)},
			":now_unix":    &types.AttributeValueMemberN{Value: strconv.FormatInt(nowUnix, 10)},
			":now_str":     &types.AttributeValueMemberS{Value: nowStr},
			":zero":        &types.AttributeValueMemberN{Value: "0"},
			":one":         &types.AttributeValueMemberN{Value: "1"},
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	if err == nil {
		var item executionItem
		if uErr := attributevalue.UnmarshalMap(out.Attributes, &item); uErr != nil {
			return execute.Execution{}, 0, fmt.Errorf("unmarshal acquired %s#%s: %w", key.RequestID, key.RunDate, uErr)
		}
		return toExecuteRecord(item), execute.AcquireAcquired, nil
	}

	var condErr *types.ConditionalCheckFailedException
	if !errors.As(err, &condErr) {
		return execute.Execution{}, 0, fmt.Errorf("acquire execution %s#%s: %w", key.RequestID, key.RunDate, err)
	}

	existing, found, getErr := r.getItem(ctx, key.RequestID, key.RunDate)
	if getErr != nil {
		return execute.Execution{}, 0, getErr
	}
	if !found {
		return execute.Execution{}, 0, fmt.Errorf("execution %s#%s not found on acquire", key.RequestID, key.RunDate)
	}
	rec := toExecuteRecord(existing)
	if rec.Status.IsTerminal() {
		return rec, execute.AcquireAlreadyTerminal, nil
	}
	return rec, execute.AcquireLeasedByOther, nil
}

// Complete conditionally transitions IN_PROGRESS (held by owner) to SUCCEEDED with inline or S3 result.
func (r *ExecutionsRepo) Complete(ctx context.Context, key execute.ExecKey, owner string, res execute.Result) error {
	completedStr := res.CompletedAt.UTC().Format(time.RFC3339Nano)
	names := map[string]string{
		"#status":       "status",
		"#rds":          "run_date_status",
		"#owner":        "lease_owner",
		"#http_status":  "http_status",
		"#latency_ms":   "latency_ms",
		"#completed_at": "completed_at",
		"#updated_at":   "updated_at",
	}
	values := map[string]types.AttributeValue{
		":in_progress":  &types.AttributeValueMemberS{Value: string(domain.StatusInProgress)},
		":owner":        &types.AttributeValueMemberS{Value: owner},
		":succeeded":    &types.AttributeValueMemberS{Value: string(domain.StatusSucceeded)},
		":rds":          &types.AttributeValueMemberS{Value: string(key.RunDate) + "#" + string(domain.StatusSucceeded)},
		":http_status":  &types.AttributeValueMemberN{Value: strconv.Itoa(res.HTTPStatus)},
		":latency_ms":   &types.AttributeValueMemberN{Value: strconv.FormatInt(res.LatencyMs, 10)},
		":completed_at": &types.AttributeValueMemberS{Value: completedStr},
	}

	updateExpr := "SET #status = :succeeded, #rds = :rds, #http_status = :http_status, " +
		"#latency_ms = :latency_ms, #completed_at = :completed_at, #updated_at = :completed_at"
	if res.ResponseS3Key != "" {
		names["#resp_s3_key"] = "response_s3_key"
		values[":resp_s3_key"] = &types.AttributeValueMemberS{Value: res.ResponseS3Key}
		updateExpr += ", #resp_s3_key = :resp_s3_key"
	} else {
		names["#resp_body"] = "response_body"
		values[":resp_body"] = &types.AttributeValueMemberS{Value: res.ResponseBody}
		updateExpr += ", #resp_body = :resp_body"
	}

	return r.updateOwned(ctx, key, updateExpr, names, values)
}

// ScheduleRetry conditionally transitions IN_PROGRESS (held by owner) to RETRYING.
func (r *ExecutionsRepo) ScheduleRetry(ctx context.Context, key execute.ExecKey, owner string, info execute.ErrInfo) error {
	nowStr := time.Now().UTC().Format(time.RFC3339Nano)
	nextStr := info.NextRetryAt.UTC().Format(time.RFC3339Nano)
	names := map[string]string{
		"#status":        "status",
		"#rds":           "run_date_status",
		"#owner":         "lease_owner",
		"#err_class":     "error_class",
		"#err_msg":       "error_message",
		"#http_status":   "http_status",
		"#latency_ms":    "latency_ms",
		"#next_retry_at": "next_retry_at",
		"#updated_at":    "updated_at",
	}
	values := map[string]types.AttributeValue{
		":in_progress":   &types.AttributeValueMemberS{Value: string(domain.StatusInProgress)},
		":owner":         &types.AttributeValueMemberS{Value: owner},
		":retrying":      &types.AttributeValueMemberS{Value: string(domain.StatusRetrying)},
		":rds":           &types.AttributeValueMemberS{Value: string(key.RunDate) + "#" + string(domain.StatusRetrying)},
		":err_class":     &types.AttributeValueMemberS{Value: info.ErrorClass},
		":err_msg":       &types.AttributeValueMemberS{Value: info.ErrorMessage},
		":http_status":   &types.AttributeValueMemberN{Value: strconv.Itoa(info.HTTPStatus)},
		":latency_ms":    &types.AttributeValueMemberN{Value: strconv.FormatInt(info.LatencyMs, 10)},
		":next_retry_at": &types.AttributeValueMemberS{Value: nextStr},
		":now":           &types.AttributeValueMemberS{Value: nowStr},
	}
	updateExpr := "SET #status = :retrying, #rds = :rds, #err_class = :err_class, #err_msg = :err_msg, " +
		"#http_status = :http_status, #latency_ms = :latency_ms, #next_retry_at = :next_retry_at, #updated_at = :now"
	return r.updateOwned(ctx, key, updateExpr, names, values)
}

// FailPermanent conditionally transitions IN_PROGRESS (held by owner) to FAILED_PERMANENT.
func (r *ExecutionsRepo) FailPermanent(ctx context.Context, key execute.ExecKey, owner string, info execute.ErrInfo) error {
	return r.failTerminal(ctx, key, owner, domain.StatusFailedPermanent, info)
}

// FailExhausted conditionally transitions IN_PROGRESS (held by owner) to FAILED_EXHAUSTED.
func (r *ExecutionsRepo) FailExhausted(ctx context.Context, key execute.ExecKey, owner string, info execute.ErrInfo) error {
	return r.failTerminal(ctx, key, owner, domain.StatusFailedExhausted, info)
}

func (r *ExecutionsRepo) failTerminal(ctx context.Context, key execute.ExecKey, owner string, status domain.ExecutionStatus, info execute.ErrInfo) error {
	completed := info.CompletedAt
	if completed.IsZero() {
		completed = time.Now().UTC()
	}
	completedStr := completed.UTC().Format(time.RFC3339Nano)
	names := map[string]string{
		"#status":       "status",
		"#rds":          "run_date_status",
		"#owner":        "lease_owner",
		"#err_class":    "error_class",
		"#err_msg":      "error_message",
		"#http_status":  "http_status",
		"#latency_ms":   "latency_ms",
		"#completed_at": "completed_at",
		"#updated_at":   "updated_at",
	}
	values := map[string]types.AttributeValue{
		":in_progress":  &types.AttributeValueMemberS{Value: string(domain.StatusInProgress)},
		":owner":        &types.AttributeValueMemberS{Value: owner},
		":new_status":   &types.AttributeValueMemberS{Value: string(status)},
		":rds":          &types.AttributeValueMemberS{Value: string(key.RunDate) + "#" + string(status)},
		":err_class":    &types.AttributeValueMemberS{Value: info.ErrorClass},
		":err_msg":      &types.AttributeValueMemberS{Value: info.ErrorMessage},
		":http_status":  &types.AttributeValueMemberN{Value: strconv.Itoa(info.HTTPStatus)},
		":latency_ms":   &types.AttributeValueMemberN{Value: strconv.FormatInt(info.LatencyMs, 10)},
		":completed_at": &types.AttributeValueMemberS{Value: completedStr},
	}
	updateExpr := "SET #status = :new_status, #rds = :rds, #err_class = :err_class, #err_msg = :err_msg, " +
		"#http_status = :http_status, #latency_ms = :latency_ms, #completed_at = :completed_at, #updated_at = :completed_at"
	return r.updateOwned(ctx, key, updateExpr, names, values)
}

// MarkSkipped conditionally transitions IN_PROGRESS (held by owner) to SKIPPED.
func (r *ExecutionsRepo) MarkSkipped(ctx context.Context, key execute.ExecKey, owner string, now time.Time) error {
	tsStr := now.UTC().Format(time.RFC3339Nano)
	names := map[string]string{
		"#status":       "status",
		"#rds":          "run_date_status",
		"#owner":        "lease_owner",
		"#completed_at": "completed_at",
		"#updated_at":   "updated_at",
	}
	values := map[string]types.AttributeValue{
		":in_progress":  &types.AttributeValueMemberS{Value: string(domain.StatusInProgress)},
		":owner":        &types.AttributeValueMemberS{Value: owner},
		":skipped":      &types.AttributeValueMemberS{Value: string(domain.StatusSkipped)},
		":rds":          &types.AttributeValueMemberS{Value: string(key.RunDate) + "#" + string(domain.StatusSkipped)},
		":completed_at": &types.AttributeValueMemberS{Value: tsStr},
	}
	updateExpr := "SET #status = :skipped, #rds = :rds, #completed_at = :completed_at, #updated_at = :completed_at"
	return r.updateOwned(ctx, key, updateExpr, names, values)
}

// Get returns the current Execution record (strongly consistent) if it exists.
func (r *ExecutionsRepo) Get(ctx context.Context, reqID string, date domain.RunDate) (execute.Execution, bool, error) {
	item, found, err := r.getItem(ctx, reqID, date)
	if err != nil || !found {
		return execute.Execution{}, found, err
	}
	return toExecuteRecord(item), true, nil
}

// ResetForRedrive transitions a FAILED_EXHAUSTED execution back to ENQUEUED while preserving historical attempts (004 US5).
func (r *ExecutionsRepo) ResetForRedrive(ctx context.Context, reqID string, date domain.RunDate, now time.Time) error {
	tsStr := now.UTC().Format(time.RFC3339Nano)
	_, err := r.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           aws.String(r.tableName),
		Key:                 execKeyMap(reqID, date),
		UpdateExpression:    aws.String("SET #status = :enqueued, #rds = :rds, #updated_at = :now REMOVE #owner, #expires, #completed_at"),
		ConditionExpression: aws.String("#status = :exhausted"),
		ExpressionAttributeNames: map[string]string{
			"#status":       "status",
			"#rds":          "run_date_status",
			"#updated_at":   "updated_at",
			"#owner":        "lease_owner",
			"#expires":      "lease_expires_at",
			"#completed_at": "completed_at",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":exhausted": &types.AttributeValueMemberS{Value: string(domain.StatusFailedExhausted)},
			":enqueued":  &types.AttributeValueMemberS{Value: string(domain.StatusEnqueued)},
			":rds":       &types.AttributeValueMemberS{Value: string(date) + "#" + string(domain.StatusEnqueued)},
			":now":       &types.AttributeValueMemberS{Value: tsStr},
		},
	})
	if err != nil {
		var condErr *types.ConditionalCheckFailedException
		if errors.As(err, &condErr) {
			return nil
		}
		return fmt.Errorf("reset for redrive %s#%s: %w", reqID, date, err)
	}
	return nil
}

func (r *ExecutionsRepo) updateOwned(
	ctx context.Context,
	key execute.ExecKey,
	updateExpr string,
	names map[string]string,
	values map[string]types.AttributeValue,
) error {
	_, err := r.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(r.tableName),
		Key:                       execKeyMap(key.RequestID, key.RunDate),
		UpdateExpression:          aws.String(updateExpr),
		ConditionExpression:       aws.String("#status = :in_progress AND #owner = :owner"),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
	})
	if err != nil {
		var condErr *types.ConditionalCheckFailedException
		if errors.As(err, &condErr) {
			return fmt.Errorf("%w: %s#%s", execute.ErrLeaseLost, key.RequestID, key.RunDate)
		}
		return fmt.Errorf("update execution %s#%s: %w", key.RequestID, key.RunDate, err)
	}
	return nil
}

func (r *ExecutionsRepo) getItem(ctx context.Context, reqID string, date domain.RunDate) (executionItem, bool, error) {
	getOut, err := r.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(r.tableName),
		ConsistentRead: aws.Bool(true),
		Key:            execKeyMap(reqID, date),
	})
	if err != nil {
		return executionItem{}, false, fmt.Errorf("get execution %s#%s: %w", reqID, date, err)
	}
	if len(getOut.Item) == 0 {
		return executionItem{}, false, nil
	}
	var item executionItem
	if err := attributevalue.UnmarshalMap(getOut.Item, &item); err != nil {
		return executionItem{}, false, fmt.Errorf("unmarshal execution %s#%s: %w", reqID, date, err)
	}
	return item, true, nil
}

func execKeyMap(reqID string, date domain.RunDate) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"request_id": &types.AttributeValueMemberS{Value: reqID},
		"run_date":   &types.AttributeValueMemberS{Value: string(date)},
	}
}

func toExecuteRecord(item executionItem) execute.Execution {
	var exp time.Time
	if item.LeaseExpiresAt > 0 {
		exp = time.Unix(item.LeaseExpiresAt, 0).UTC()
	}
	return execute.Execution{
		RequestID:      item.RequestID,
		RunDate:        domain.RunDate(item.RunDate),
		Provider:       item.Provider,
		Status:         domain.ExecutionStatus(item.Status),
		Attempts:       item.Attempts,
		LeaseOwner:     item.LeaseOwner,
		LeaseExpiresAt: exp,
	}
}
