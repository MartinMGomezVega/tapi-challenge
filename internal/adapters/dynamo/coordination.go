package dynamo

import (
	"context"
	"fmt"
	"slices"
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
	providerKeyPrefix = "provider#"
	backfillKeyPrefix = "backfill#"
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

// TryAcquire conditionally acquires or renews the dispatcher-leader lease, returning its fencing token.
func (d *DispatcherCoordinator) TryAcquire(_ context.Context, _ string, _ time.Duration, _ time.Time) (int64, bool, error) {
	return 0, false, fmt.Errorf("not implemented")
}

// Release relinquishes leadership if currently held by owner.
func (d *DispatcherCoordinator) Release(_ context.Context, _ string) error {
	return fmt.Errorf("not implemented")
}

// Load returns the last persisted watermark SlotCoord, or found=false if not yet initialized.
func (d *DispatcherCoordinator) Load(_ context.Context) (domain.SlotCoord, bool, error) {
	return domain.SlotCoord{}, false, fmt.Errorf("not implemented")
}

// Advance advances the watermark conditioned on fencingToken matching the current lease version.
func (d *DispatcherCoordinator) Advance(_ context.Context, _ domain.SlotCoord, _ int64, _ time.Time) error {
	return fmt.Errorf("not implemented")
}
