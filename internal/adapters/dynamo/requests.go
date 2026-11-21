// Package dynamo implements DynamoDB persistence adapters for catalog requests.
package dynamo

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

const (
	// DefaultPageSize is the default Query limit per page on GSIs.
	DefaultPageSize int32 = 100
	gsiSlotShard          = "gsi_slot_shard"
	gsiProvider           = "gsi_provider"
)

// ErrInvalidPageToken indicates a malformed pagination cursor passed to ListBySlotShard.
var ErrInvalidPageToken = errors.New("invalid pagination token")

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

type requestItem struct {
	ID         string `dynamodbav:"id"`
	Provider   string `dynamodbav:"provider"`
	ProviderID string `dynamodbav:"provider_id"`
	Endpoint   string `dynamodbav:"endpoint"`
	Method     string `dynamodbav:"method"`
	Body       []byte `dynamodbav:"body"`
	Enabled    bool   `dynamodbav:"enabled"`
	Slot       int    `dynamodbav:"slot"`
	Shard      int    `dynamodbav:"shard"`
	SlotShard  string `dynamodbav:"slot_shard,omitempty"`
	Version    int64  `dynamodbav:"version"`
	CreatedAt  string `dynamodbav:"created_at"`
	UpdatedAt  string `dynamodbav:"updated_at"`
}

type requestKeyItem struct {
	ID        string `dynamodbav:"id"`
	Provider  string `dynamodbav:"provider"`
	Enabled   bool   `dynamodbav:"enabled"`
	CreatedAt string `dynamodbav:"created_at"`
}

type providerRequestItem struct {
	ID       string `dynamodbav:"id"`
	Provider string `dynamodbav:"provider"`
	Version  int64  `dynamodbav:"version"`
	Slot     int    `dynamodbav:"slot"`
	Shard    int    `dynamodbav:"shard"`
	Enabled  bool   `dynamodbav:"enabled"`
}

// Put stores a Request item in DynamoDB. Requests with Slot < 0 omit slot_shard so
// gsi_slot_shard remains sparse until a slot is assigned.
func (r *RequestsRepo) Put(ctx context.Context, req domain.Request) error {
	item := requestItem{
		ID:         req.ID,
		Provider:   req.Provider,
		ProviderID: req.ProviderID(),
		Endpoint:   req.Endpoint,
		Method:     req.Method,
		Body:       req.Body,
		Enabled:    req.Enabled,
		Slot:       req.Slot,
		Shard:      req.Shard,
		Version:    req.Version,
		CreatedAt:  req.CreatedAt.UTC().Format(time.RFC3339Nano),
		UpdatedAt:  req.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
	if req.Slot >= 0 && req.Shard >= 0 {
		item.SlotShard = req.SlotShard()
	}

	av, err := attributevalue.MarshalMap(item)
	if err != nil {
		return fmt.Errorf("marshal request %s: %w", req.ID, err)
	}

	_, err = r.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(r.tableName),
		Item:      av,
	})
	if err != nil {
		return fmt.Errorf("put request %s: %w", req.ID, err)
	}
	return nil
}

// Get retrieves a full Request by its primary key id using a strongly consistent read.
func (r *RequestsRepo) Get(ctx context.Context, id string) (domain.Request, error) {
	if strings.TrimSpace(id) == "" {
		return domain.Request{}, domain.ErrEmptyID
	}

	out, err := r.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(r.tableName),
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: id},
		},
	})
	if err != nil {
		return domain.Request{}, fmt.Errorf("get request %s: %w", id, err)
	}
	if len(out.Item) == 0 {
		return domain.Request{}, fmt.Errorf("%w: %s", domain.ErrRequestNotFound, id)
	}

	var item requestItem
	if err := attributevalue.UnmarshalMap(out.Item, &item); err != nil {
		return domain.Request{}, fmt.Errorf("unmarshal request %s: %w", id, err)
	}

	createdAt, err := time.Parse(time.RFC3339Nano, item.CreatedAt)
	if err != nil {
		return domain.Request{}, fmt.Errorf("parse created_at for request %s: %w", id, err)
	}
	updatedAt, err := time.Parse(time.RFC3339Nano, item.UpdatedAt)
	if err != nil {
		return domain.Request{}, fmt.Errorf("parse updated_at for request %s: %w", id, err)
	}

	return domain.Request{
		ID:        item.ID,
		Provider:  item.Provider,
		Endpoint:  item.Endpoint,
		Method:    item.Method,
		Body:      item.Body,
		Enabled:   item.Enabled,
		Slot:      item.Slot,
		Shard:     item.Shard,
		Version:   item.Version,
		CreatedAt: createdAt.UTC(),
		UpdatedAt: updatedAt.UTC(),
	}, nil
}

// ListBySlotShard queries gsi_slot_shard for enabled requests in (slot, shard), paginated (FR-004).
func (r *RequestsRepo) ListBySlotShard(ctx context.Context, slot, shard int, page string) ([]domain.RequestKey, string, error) {
	if slot < 0 || slot >= domain.SlotsPerDay {
		return nil, "", fmt.Errorf("%w: %d", domain.ErrInvalidSlot, slot)
	}
	if shard < 0 {
		return nil, "", fmt.Errorf("%w: %d", domain.ErrInvalidShard, shard)
	}

	startKey, err := decodePageToken(page, "id", "slot_shard", "provider_id")
	if err != nil {
		return nil, "", err
	}

	slotShardKey := domain.FormatSlotShard(slot, shard)

	for {
		out, err := r.client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(r.tableName),
			IndexName:              aws.String(gsiSlotShard),
			KeyConditionExpression: aws.String("#slot_shard = :ss"),
			FilterExpression:       aws.String("#enabled = :enabled"),
			ExpressionAttributeNames: map[string]string{
				"#slot_shard": "slot_shard",
				"#enabled":    "enabled",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":ss":      &types.AttributeValueMemberS{Value: slotShardKey},
				":enabled": &types.AttributeValueMemberBOOL{Value: true},
			},
			Limit:             aws.Int32(r.pageSize),
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return nil, "", fmt.Errorf("query gsi_slot_shard (%s): %w", slotShardKey, err)
		}

		// When FilterExpression drops every item in a page, advance automatically
		// so callers never receive an empty page with a non-empty cursor.
		if len(out.Items) == 0 && len(out.LastEvaluatedKey) > 0 {
			startKey = out.LastEvaluatedKey
			continue
		}

		keys := make([]domain.RequestKey, 0, len(out.Items))
		for _, raw := range out.Items {
			var projected requestKeyItem
			if err := attributevalue.UnmarshalMap(raw, &projected); err != nil {
				return nil, "", fmt.Errorf("unmarshal request key: %w", err)
			}
			createdAt, err := time.Parse(time.RFC3339Nano, projected.CreatedAt)
			if err != nil {
				return nil, "", fmt.Errorf("parse created_at for %s: %w", projected.ID, err)
			}
			keys = append(keys, domain.RequestKey{
				ID:        projected.ID,
				Provider:  projected.Provider,
				Enabled:   projected.Enabled,
				CreatedAt: createdAt.UTC(),
			})
		}

		nextPage, err := encodePageToken(out.LastEvaluatedKey)
		if err != nil {
			return nil, "", err
		}
		return keys, nextPage, nil
	}
}

// SetSlot atomically updates a request's slot, shard, and slot_shard conditioned on expectedVersion.
func (r *RequestsRepo) SetSlot(ctx context.Context, id string, slot, shard int, expectedVersion int64) error {
	if strings.TrimSpace(id) == "" {
		return domain.ErrEmptyID
	}
	if slot < 0 || slot >= domain.SlotsPerDay {
		return fmt.Errorf("%w: %d", domain.ErrInvalidSlot, slot)
	}
	if shard < 0 {
		return fmt.Errorf("%w: %d", domain.ErrInvalidShard, shard)
	}

	_, err := r.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(r.tableName),
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: id},
		},
		UpdateExpression:    aws.String("SET #slot = :slot, #shard = :shard, #slot_shard = :slot_shard, #version = :next_version, #updated_at = :updated_at"),
		ConditionExpression: aws.String("attribute_exists(#id) AND #version = :expected_version"),
		ExpressionAttributeNames: map[string]string{
			"#id":         "id",
			"#slot":       "slot",
			"#shard":      "shard",
			"#slot_shard": "slot_shard",
			"#version":    "version",
			"#updated_at": "updated_at",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":slot":             &types.AttributeValueMemberN{Value: strconv.Itoa(slot)},
			":shard":            &types.AttributeValueMemberN{Value: strconv.Itoa(shard)},
			":slot_shard":       &types.AttributeValueMemberS{Value: domain.FormatSlotShard(slot, shard)},
			":expected_version": &types.AttributeValueMemberN{Value: strconv.FormatInt(expectedVersion, 10)},
			":next_version":     &types.AttributeValueMemberN{Value: strconv.FormatInt(expectedVersion+1, 10)},
			":updated_at":       &types.AttributeValueMemberS{Value: r.now().UTC().Format(time.RFC3339Nano)},
		},
	})
	if err != nil {
		var condErr *types.ConditionalCheckFailedException
		if errors.As(err, &condErr) {
			return fmt.Errorf("%w: id=%s expected_version=%d", domain.ErrVersionConflict, id, expectedVersion)
		}
		return fmt.Errorf("set slot for request %s: %w", id, err)
	}
	return nil
}

// CountByProvider returns the total number of requests registered for provider using
// gsi_provider with Select=COUNT (no Scan, no items transferred).
func (r *RequestsRepo) CountByProvider(ctx context.Context, provider string) (int, error) {
	if strings.TrimSpace(provider) == "" {
		return 0, domain.ErrInvalidProvider
	}

	var (
		total    int
		startKey map[string]types.AttributeValue
	)
	for {
		out, err := r.client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(r.tableName),
			IndexName:              aws.String(gsiProvider),
			KeyConditionExpression: aws.String("#provider = :provider"),
			ExpressionAttributeNames: map[string]string{
				"#provider": "provider",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":provider": &types.AttributeValueMemberS{Value: provider},
			},
			Select:            types.SelectCount,
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return 0, fmt.Errorf("count gsi_provider (%s): %w", provider, err)
		}
		total += int(out.Count)
		if len(out.LastEvaluatedKey) == 0 {
			return total, nil
		}
		startKey = out.LastEvaluatedKey
	}
}

// ListByProvider queries gsi_provider in ascending SK=id order, returning projected
// (id, provider, version, slot, shard, enabled) items paginated by cursor (FR-004b).
func (r *RequestsRepo) ListByProvider(ctx context.Context, provider, page string) ([]domain.ProviderRequestItem, string, error) {
	if strings.TrimSpace(provider) == "" {
		return nil, "", domain.ErrInvalidProvider
	}

	startKey, err := decodePageToken(page, "id", "provider")
	if err != nil {
		return nil, "", err
	}

	out, err := r.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(r.tableName),
		IndexName:              aws.String(gsiProvider),
		KeyConditionExpression: aws.String("#provider = :provider"),
		ExpressionAttributeNames: map[string]string{
			"#provider": "provider",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":provider": &types.AttributeValueMemberS{Value: provider},
		},
		ScanIndexForward:  aws.Bool(true),
		Limit:             aws.Int32(r.pageSize),
		ExclusiveStartKey: startKey,
	})
	if err != nil {
		return nil, "", fmt.Errorf("query gsi_provider (%s): %w", provider, err)
	}

	items := make([]domain.ProviderRequestItem, 0, len(out.Items))
	for _, raw := range out.Items {
		var projected providerRequestItem
		if err := attributevalue.UnmarshalMap(raw, &projected); err != nil {
			return nil, "", fmt.Errorf("unmarshal provider request item: %w", err)
		}
		items = append(items, domain.ProviderRequestItem{
			ID:       projected.ID,
			Provider: projected.Provider,
			Version:  projected.Version,
			Slot:     projected.Slot,
			Shard:    projected.Shard,
			Enabled:  projected.Enabled,
		})
	}

	nextPage, err := encodePageToken(out.LastEvaluatedKey)
	if err != nil {
		return nil, "", err
	}
	return items, nextPage, nil
}

func encodePageToken(key map[string]types.AttributeValue) (string, error) {
	if len(key) == 0 {
		return "", nil
	}
	raw := make(map[string]string, len(key))
	for k, v := range key {
		sv, ok := v.(*types.AttributeValueMemberS)
		if !ok {
			return "", fmt.Errorf("encode page token: unexpected non-string key %q", k)
		}
		raw[k] = sv.Value
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return "", fmt.Errorf("marshal page token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func decodePageToken(token string, requiredKeys ...string) (map[string]types.AttributeValue, error) {
	if token == "" {
		return nil, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidPageToken, err)
	}
	var raw map[string]string
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidPageToken, err)
	}
	for _, required := range requiredKeys {
		if raw[required] == "" {
			return nil, fmt.Errorf("%w: missing key %q", ErrInvalidPageToken, required)
		}
	}
	out := make(map[string]types.AttributeValue, len(raw))
	for k, v := range raw {
		out[k] = &types.AttributeValueMemberS{Value: v}
	}
	return out, nil
}
