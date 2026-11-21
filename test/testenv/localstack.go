// Package testenv provides testcontainers-go helpers for LocalStack integration tests.
package testenv

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
)

const (
	localstackImage = "localstack/localstack:4.0.3"
	defaultRegion   = "us-east-1"
	edgePort        = "4566/tcp"
)

// Env holds a running LocalStack instance and preconfigured AWS SDK v2 clients.
type Env struct {
	Container *localstack.LocalStackContainer
	Endpoint  string
	AWSConfig aws.Config
	DynamoDB  *dynamodb.Client
}

// StartLocalStack starts a LocalStack container with DynamoDB, SQS, and S3 enabled
// and registers cleanup on t.
func StartLocalStack(t testing.TB) *Env {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := localstack.Run(
		ctx,
		localstackImage,
		testcontainers.WithEnv(map[string]string{
			"SERVICES":           "dynamodb,sqs,s3",
			"DEFAULT_REGION":     defaultRegion,
			"AWS_DEFAULT_REGION": defaultRegion,
		}),
	)
	if err != nil {
		t.Fatalf("start localstack container: %v", err)
	}

	t.Cleanup(func() {
		termCtx, termCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer termCancel()
		if err := container.Terminate(termCtx); err != nil {
			t.Logf("terminate localstack container: %v", err)
		}
	})

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("localstack host: %v", err)
	}
	mappedPort, err := container.MappedPort(ctx, edgePort)
	if err != nil {
		t.Fatalf("localstack mapped port: %v", err)
	}

	endpoint := "http://" + net.JoinHostPort(host, mappedPort.Port())

	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion(defaultRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}

	ddb := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	return &Env{
		Container: container,
		Endpoint:  endpoint,
		AWSConfig: cfg,
		DynamoDB:  ddb,
	}
}

// EnsureRequestsTable idempotently creates the requests table with gsi_slot_shard
// and gsi_provider matching scripts/localstack-init.sh and waits until it is active.
func (e *Env) EnsureRequestsTable(ctx context.Context, tableName string) error {
	_, err := e.DynamoDB.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(tableName),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("provider"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("slot_shard"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("provider_id"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("gsi_slot_shard"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("slot_shard"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("provider_id"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{
					ProjectionType:   types.ProjectionTypeInclude,
					NonKeyAttributes: []string{"provider", "enabled", "created_at"},
				},
			},
			{
				IndexName: aws.String("gsi_provider"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("provider"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("id"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{
					ProjectionType:   types.ProjectionTypeInclude,
					NonKeyAttributes: []string{"version", "slot", "shard", "enabled"},
				},
			},
		},
	})
	if err != nil {
		var inUse *types.ResourceInUseException
		if !errors.As(err, &inUse) {
			return fmt.Errorf("create requests table %q: %w", tableName, err)
		}
	}

	waiter := dynamodb.NewTableExistsWaiter(e.DynamoDB)
	if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}, 30*time.Second); err != nil {
		return fmt.Errorf("wait for requests table %q: %w", tableName, err)
	}
	return nil
}

// EnsureCoordinationTable idempotently creates the coordination table matching
// scripts/localstack-init.sh and waits until it is active.
func (e *Env) EnsureCoordinationTable(ctx context.Context, tableName string) error {
	_, err := e.DynamoDB.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(tableName),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("name"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("name"), KeyType: types.KeyTypeHash},
		},
	})
	if err != nil {
		var inUse *types.ResourceInUseException
		if !errors.As(err, &inUse) {
			return fmt.Errorf("create coordination table %q: %w", tableName, err)
		}
	}

	waiter := dynamodb.NewTableExistsWaiter(e.DynamoDB)
	if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}, 30*time.Second); err != nil {
		return fmt.Errorf("wait for coordination table %q: %w", tableName, err)
	}
	return nil
}
