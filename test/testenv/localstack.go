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
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
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
	SQS       *sqs.Client
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
	sqsClient := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	return &Env{
		Container: container,
		Endpoint:  endpoint,
		AWSConfig: cfg,
		DynamoDB:  ddb,
		SQS:       sqsClient,
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

// EnsureExecutionsTable idempotently creates the executions table (PK request_id, SK run_date,
// GSI gsi_run_date_status) and waits until it is active.
func (e *Env) EnsureExecutionsTable(ctx context.Context, tableName string) error {
	_, err := e.DynamoDB.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(tableName),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("request_id"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("run_date"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("run_date_status"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("provider_request_id"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("request_id"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("run_date"), KeyType: types.KeyTypeRange},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("gsi_run_date_status"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("run_date_status"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("provider_request_id"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeKeysOnly,
				},
			},
		},
	})
	if err != nil {
		var inUse *types.ResourceInUseException
		if !errors.As(err, &inUse) {
			return fmt.Errorf("create executions table %q: %w", tableName, err)
		}
	}

	waiter := dynamodb.NewTableExistsWaiter(e.DynamoDB)
	if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}, 30*time.Second); err != nil {
		return fmt.Errorf("wait for executions table %q: %w", tableName, err)
	}
	return nil
}

// EnsureFIFOQueue creates a high-throughput SQS FIFO queue paired with a FIFO DLQ (maxReceiveCount=6).
func (e *Env) EnsureFIFOQueue(ctx context.Context, queueName, dlqName string) (queueURL, dlqURL string, err error) {
	dlqOut, err := e.SQS.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(dlqName),
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "false",
			"DeduplicationScope":        "messageGroup",
			"FifoThroughputLimit":       "perMessageGroupId",
		},
	})
	if err != nil {
		return "", "", fmt.Errorf("create fifo dlq %q: %w", dlqName, err)
	}
	dlqURL = aws.ToString(dlqOut.QueueUrl)

	attrOut, err := e.SQS.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(dlqURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return "", "", fmt.Errorf("get dlq arn %q: %w", dlqName, err)
	}
	dlqARN := attrOut.Attributes["QueueArn"]

	redrive := fmt.Sprintf(`{"deadLetterTargetArn":"%s","maxReceiveCount":"6"}`, dlqARN)
	qOut, err := e.SQS.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "false",
			"DeduplicationScope":        "messageGroup",
			"FifoThroughputLimit":       "perMessageGroupId",
			"VisibilityTimeout":         "60",
			"RedrivePolicy":             redrive,
		},
	})
	if err != nil {
		return "", "", fmt.Errorf("create fifo queue %q: %w", queueName, err)
	}
	return aws.ToString(qOut.QueueUrl), dlqURL, nil
}
