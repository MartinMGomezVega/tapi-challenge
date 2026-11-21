package dynamo

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/martingomezvega/tapi-challenge/internal/app/assignslots"
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

// ListProviders returns all registered providers sorted by provider name.
func (r *CoordinationRepo) ListProviders(_ context.Context) ([]assignslots.ProviderState, error) {
	return nil, errors.New("not implemented")
}

// SaveProviderState upserts a provider's registry entry (count, histogram, backfill status).
func (r *CoordinationRepo) SaveProviderState(_ context.Context, _ assignslots.ProviderState) error {
	return errors.New("not implemented")
}

// Load retrieves a backfill checkpoint for (job, provider). Returns found=false if absent.
func (r *CoordinationRepo) Load(_ context.Context, _, _ string) (assignslots.BackfillCheckpoint, bool, error) {
	return assignslots.BackfillCheckpoint{}, false, errors.New("not implemented")
}

// Save persists a backfill checkpoint for (job, provider).
func (r *CoordinationRepo) Save(_ context.Context, _, _ string, _ assignslots.BackfillCheckpoint) error {
	return errors.New("not implemented")
}
