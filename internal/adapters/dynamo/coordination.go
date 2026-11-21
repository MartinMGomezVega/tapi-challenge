package dynamo

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/martingomezvega/tapi-challenge/internal/app/assignslots"
	"github.com/martingomezvega/tapi-challenge/internal/app/capacity"
	"github.com/martingomezvega/tapi-challenge/internal/app/dispatch"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

const (
	providerKeyPrefix    = "provider#"
	backfillKeyPrefix    = "backfill#"
	dispatcherLeaderKey  = "dispatcher-leader"
	dispatcherWatermarkK = "dispatcher-watermark"
)

// CoordinationRepo persists provider registry entries (k + 1440-slot histogram)
// and resumable backfill checkpoints in the DynamoDB coordination table.
type CoordinationRepo struct {
	client    *dynamodb.Client
	tableName string
}

// NewCoordinationRepo creates a DynamoDB-backed store for the coordination table.
func NewCoordinationRepo(client *dynamodb.Client, tableName string) *CoordinationRepo {
	return &CoordinationRepo{
		client:    client,
		tableName: tableName,
	}
}

type providerStateItem struct {
	Name             string `dynamodbav:"name"`
	Provider         string `dynamodbav:"provider"`
	Count            int    `dynamodbav:"count"`
	Histogram        []int  `dynamodbav:"histogram"`
	NewSinceBackfill int    `dynamodbav:"new_since_backfill"`
	Backfilled       bool   `dynamodbav:"backfilled"`
	UpdatedAt        string `dynamodbav:"updated_at"`
}

type checkpointItem struct {
	Name        string `dynamodbav:"name"`
	Status      string `dynamodbav:"status"`
	Incremental bool   `dynamodbav:"incremental"`
	Cursor      string `dynamodbav:"cursor,omitempty"`
	Rank        int    `dynamodbav:"rank"`
	Count       int    `dynamodbav:"count"`
	Histogram   []int  `dynamodbav:"histogram"`
	UpdatedAt   string `dynamodbav:"updated_at"`
}

// ListProviders returns all registered providers sorted by provider name.
// The coordination table holds O(P) metadata items (independent of catalog size N).
func (r *CoordinationRepo) ListProviders(ctx context.Context) ([]assignslots.ProviderState, error) {
	var (
		states   []assignslots.ProviderState
		startKey map[string]types.AttributeValue
	)

	for {
		out, err := r.client.Scan(ctx, &dynamodb.ScanInput{
			TableName:        aws.String(r.tableName),
			FilterExpression: aws.String("begins_with(#name, :prefix)"),
			ExpressionAttributeNames: map[string]string{
				"#name": "name",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":prefix": &types.AttributeValueMemberS{Value: providerKeyPrefix},
			},
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return nil, fmt.Errorf("list provider registry: %w", err)
		}

		for _, raw := range out.Items {
			var item providerStateItem
			if err := attributevalue.UnmarshalMap(raw, &item); err != nil {
				return nil, fmt.Errorf("unmarshal provider state: %w", err)
			}
			state, err := item.toDomain()
			if err != nil {
				return nil, err
			}
			states = append(states, state)
		}

		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		startKey = out.LastEvaluatedKey
	}

	slices.SortFunc(states, func(a, b assignslots.ProviderState) int {
		return strings.Compare(a.Provider, b.Provider)
	})
	return states, nil
}

// ListProviderVolumes adapts ListProviders to satisfy capacity.ProviderLister (001-US3).
func (r *CoordinationRepo) ListProviderVolumes(ctx context.Context) ([]capacity.ProviderVolume, error) {
	states, err := r.ListProviders(ctx)
	if err != nil {
		return nil, err
	}
	vols := make([]capacity.ProviderVolume, len(states))
	for i, st := range states {
		vols[i] = capacity.ProviderVolume{
			Provider: st.Provider,
			Count:    st.Count,
		}
	}
	return vols, nil
}

// SaveProviderState upserts a provider's registry entry (count, histogram, backfill status).
func (r *CoordinationRepo) SaveProviderState(ctx context.Context, state assignslots.ProviderState) error {
	if strings.TrimSpace(state.Provider) == "" {
		return domain.ErrInvalidProvider
	}

	hist := make([]int, domain.SlotsPerDay)
	copy(hist, state.Histogram[:])

	item := providerStateItem{
		Name:             providerKeyPrefix + state.Provider,
		Provider:         state.Provider,
		Count:            state.Count,
		Histogram:        hist,
		NewSinceBackfill: state.NewSinceBackfill,
		Backfilled:       state.Backfilled,
		UpdatedAt:        state.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}

	av, err := attributevalue.MarshalMap(item)
	if err != nil {
		return fmt.Errorf("marshal provider state %s: %w", state.Provider, err)
	}
	_, err = r.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(r.tableName),
		Item:      av,
	})
	if err != nil {
		return fmt.Errorf("put provider state %s: %w", state.Provider, err)
	}
	return nil
}

// Load retrieves a backfill checkpoint for (job, provider). Returns found=false if absent.
func (r *CoordinationRepo) Load(ctx context.Context, job, provider string) (assignslots.BackfillCheckpoint, bool, error) {
	key := backfillKey(job, provider)
	out, err := r.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(r.tableName),
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			"name": &types.AttributeValueMemberS{Value: key},
		},
	})
	if err != nil {
		return assignslots.BackfillCheckpoint{}, false, fmt.Errorf("load checkpoint %s: %w", key, err)
	}
	if len(out.Item) == 0 {
		return assignslots.BackfillCheckpoint{}, false, nil
	}

	var item checkpointItem
	if err := attributevalue.UnmarshalMap(out.Item, &item); err != nil {
		return assignslots.BackfillCheckpoint{}, false, fmt.Errorf("unmarshal checkpoint %s: %w", key, err)
	}
	cp, err := item.toDomain()
	if err != nil {
		return assignslots.BackfillCheckpoint{}, false, err
	}
	return cp, true, nil
}

// Save persists a backfill checkpoint for (job, provider).
func (r *CoordinationRepo) Save(ctx context.Context, job, provider string, cp assignslots.BackfillCheckpoint) error {
	key := backfillKey(job, provider)
	hist := make([]int, domain.SlotsPerDay)
	copy(hist, cp.Histogram[:])

	item := checkpointItem{
		Name:        key,
		Status:      string(cp.Status),
		Incremental: cp.Incremental,
		Cursor:      cp.Cursor,
		Rank:        cp.Rank,
		Count:       cp.Count,
		Histogram:   hist,
		UpdatedAt:   cp.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}

	av, err := attributevalue.MarshalMap(item)
	if err != nil {
		return fmt.Errorf("marshal checkpoint %s: %w", key, err)
	}
	_, err = r.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(r.tableName),
		Item:      av,
	})
	if err != nil {
		return fmt.Errorf("put checkpoint %s: %w", key, err)
	}
	return nil
}

func backfillKey(job, provider string) string {
	return backfillKeyPrefix + job + "#" + provider
}

func (p providerStateItem) toDomain() (assignslots.ProviderState, error) {
	ts, err := time.Parse(time.RFC3339Nano, p.UpdatedAt)
	if err != nil {
		return assignslots.ProviderState{}, fmt.Errorf("parse provider updated_at (%s): %w", p.Provider, err)
	}
	var hist [domain.SlotsPerDay]int
	if len(p.Histogram) == domain.SlotsPerDay {
		copy(hist[:], p.Histogram)
	}
	return assignslots.ProviderState{
		Provider:         p.Provider,
		Count:            p.Count,
		Histogram:        hist,
		NewSinceBackfill: p.NewSinceBackfill,
		Backfilled:       p.Backfilled,
		UpdatedAt:        ts.UTC(),
	}, nil
}

func (c checkpointItem) toDomain() (assignslots.BackfillCheckpoint, error) {
	ts, err := time.Parse(time.RFC3339Nano, c.UpdatedAt)
	if err != nil {
		return assignslots.BackfillCheckpoint{}, fmt.Errorf("parse checkpoint updated_at (%s): %w", c.Name, err)
	}
	var hist [domain.SlotsPerDay]int
	if len(c.Histogram) == domain.SlotsPerDay {
		copy(hist[:], c.Histogram)
	}
	return assignslots.BackfillCheckpoint{
		Status:      assignslots.CheckpointStatus(c.Status),
		Incremental: c.Incremental,
		Cursor:      c.Cursor,
		Rank:        c.Rank,
		Count:       c.Count,
		Histogram:   hist,
		UpdatedAt:   ts.UTC(),
	}, nil
}

// DispatcherCoordinator implements dispatch.LeaderLease and dispatch.WatermarkStore
// over the DynamoDB coordination table using conditional writes and fencing tokens (002-T021).
type DispatcherCoordinator struct {
	client    *dynamodb.Client
	tableName string
}

var (
	_ dispatch.LeaderLease    = (*DispatcherCoordinator)(nil)
	_ dispatch.WatermarkStore = (*DispatcherCoordinator)(nil)
)

// NewDispatcherCoordinator creates a leader-lease + fenced-watermark store.
func NewDispatcherCoordinator(client *dynamodb.Client, tableName string) *DispatcherCoordinator {
	return &DispatcherCoordinator{client: client, tableName: tableName}
}

type leaseItem struct {
	Name      string `dynamodbav:"name"`
	Owner     string `dynamodbav:"owner"`
	ExpiresAt int64  `dynamodbav:"expires_at"`
	Version   int64  `dynamodbav:"version"`
}

type watermarkItem struct {
	Name         string `dynamodbav:"name"`
	RunDate      string `dynamodbav:"run_date"`
	LastSlot     int    `dynamodbav:"last_slot"`
	LeaseVersion int64  `dynamodbav:"lease_version"`
	UpdatedAt    string `dynamodbav:"updated_at"`
}

// TryAcquire conditionally acquires or renews the dispatcher-leader lease, returning its fencing token.
// Renewing an already-held valid lease preserves the fencing token; acquiring a fresh or expired lease
// atomically increments version so previous leaders are fenced out.
func (d *DispatcherCoordinator) TryAcquire(ctx context.Context, owner string, ttl time.Duration, now time.Time) (int64, bool, error) {
	nowMs := now.UTC().UnixMilli()
	expMs := now.UTC().Add(ttl).UnixMilli()

	// Fast path: renew if we already hold a non-expired lease (keeps same fencing token).
	renewOut, err := d.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(d.tableName),
		Key: map[string]types.AttributeValue{
			"name": &types.AttributeValueMemberS{Value: dispatcherLeaderKey},
		},
		UpdateExpression:    aws.String("SET #expires_at = :exp"),
		ConditionExpression: aws.String("#owner = :owner AND #expires_at > :now"),
		ExpressionAttributeNames: map[string]string{
			"#owner":      "owner",
			"#expires_at": "expires_at",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":owner": &types.AttributeValueMemberS{Value: owner},
			":now":   &types.AttributeValueMemberN{Value: strconv.FormatInt(nowMs, 10)},
			":exp":   &types.AttributeValueMemberN{Value: strconv.FormatInt(expMs, 10)},
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	if err == nil {
		var li leaseItem
		if uErr := attributevalue.UnmarshalMap(renewOut.Attributes, &li); uErr != nil {
			return 0, false, fmt.Errorf("unmarshal renewed lease: %w", uErr)
		}
		return li.Version, true, nil
	}
	var condErr *types.ConditionalCheckFailedException
	if !errors.As(err, &condErr) {
		return 0, false, fmt.Errorf("renew leader lease: %w", err)
	}

	// Acquire fresh or expired lease, atomically bumping fencing version.
	acqOut, err := d.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(d.tableName),
		Key: map[string]types.AttributeValue{
			"name": &types.AttributeValueMemberS{Value: dispatcherLeaderKey},
		},
		UpdateExpression:    aws.String("SET #owner = :owner, #expires_at = :exp, #version = if_not_exists(#version, :zero) + :one"),
		ConditionExpression: aws.String("attribute_not_exists(#owner) OR #owner = :empty OR #expires_at <= :now"),
		ExpressionAttributeNames: map[string]string{
			"#owner":      "owner",
			"#expires_at": "expires_at",
			"#version":    "version",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":owner": &types.AttributeValueMemberS{Value: owner},
			":empty": &types.AttributeValueMemberS{Value: ""},
			":now":   &types.AttributeValueMemberN{Value: strconv.FormatInt(nowMs, 10)},
			":exp":   &types.AttributeValueMemberN{Value: strconv.FormatInt(expMs, 10)},
			":zero":  &types.AttributeValueMemberN{Value: "0"},
			":one":   &types.AttributeValueMemberN{Value: "1"},
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	if err != nil {
		if errors.As(err, &condErr) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("acquire leader lease: %w", err)
	}

	var li leaseItem
	if err := attributevalue.UnmarshalMap(acqOut.Attributes, &li); err != nil {
		return 0, false, fmt.Errorf("unmarshal acquired lease: %w", err)
	}
	return li.Version, true, nil
}

// Release relinquishes leadership if currently held by owner.
func (d *DispatcherCoordinator) Release(ctx context.Context, owner string) error {
	_, err := d.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(d.tableName),
		Key: map[string]types.AttributeValue{
			"name": &types.AttributeValueMemberS{Value: dispatcherLeaderKey},
		},
		UpdateExpression:    aws.String("SET #owner = :empty, #expires_at = :zero"),
		ConditionExpression: aws.String("#owner = :owner"),
		ExpressionAttributeNames: map[string]string{
			"#owner":      "owner",
			"#expires_at": "expires_at",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":owner": &types.AttributeValueMemberS{Value: owner},
			":empty": &types.AttributeValueMemberS{Value: ""},
			":zero":  &types.AttributeValueMemberN{Value: "0"},
		},
	})
	if err != nil {
		var condErr *types.ConditionalCheckFailedException
		if errors.As(err, &condErr) {
			return nil
		}
		return fmt.Errorf("release leader lease: %w", err)
	}
	return nil
}

// Load returns the last persisted watermark SlotCoord, or found=false if not yet initialized.
func (d *DispatcherCoordinator) Load(ctx context.Context) (domain.SlotCoord, bool, error) {
	out, err := d.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(d.tableName),
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			"name": &types.AttributeValueMemberS{Value: dispatcherWatermarkK},
		},
	})
	if err != nil {
		return domain.SlotCoord{}, false, fmt.Errorf("load watermark: %w", err)
	}
	if len(out.Item) == 0 {
		return domain.SlotCoord{}, false, nil
	}

	var wm watermarkItem
	if err := attributevalue.UnmarshalMap(out.Item, &wm); err != nil {
		return domain.SlotCoord{}, false, fmt.Errorf("unmarshal watermark: %w", err)
	}
	return domain.SlotCoord{
		Date: domain.RunDate(wm.RunDate),
		Slot: wm.LastSlot,
	}, true, nil
}

// Advance atomically verifies fencingToken against dispatcher-leader.version and updates the watermark.
func (d *DispatcherCoordinator) Advance(ctx context.Context, coord domain.SlotCoord, fencingToken int64, now time.Time) error {
	wmAV, err := attributevalue.MarshalMap(watermarkItem{
		Name:         dispatcherWatermarkK,
		RunDate:      string(coord.Date),
		LastSlot:     coord.Slot,
		LeaseVersion: fencingToken,
		UpdatedAt:    now.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return fmt.Errorf("marshal watermark: %w", err)
	}

	_, err = d.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				ConditionCheck: &types.ConditionCheck{
					TableName: aws.String(d.tableName),
					Key: map[string]types.AttributeValue{
						"name": &types.AttributeValueMemberS{Value: dispatcherLeaderKey},
					},
					ConditionExpression: aws.String("#version = :token"),
					ExpressionAttributeNames: map[string]string{
						"#version": "version",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":token": &types.AttributeValueMemberN{Value: strconv.FormatInt(fencingToken, 10)},
					},
				},
			},
			{
				Put: &types.Put{
					TableName: aws.String(d.tableName),
					Item:      wmAV,
				},
			},
		},
	})
	if err != nil {
		var txErr *types.TransactionCanceledException
		if errors.As(err, &txErr) {
			return fmt.Errorf("%w: token=%d", dispatch.ErrFencingMismatch, fencingToken)
		}
		return fmt.Errorf("advance watermark: %w", err)
	}
	return nil
}
