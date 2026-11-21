// Command redrive re-enqueues filtered messages from the SQS FIFO DLQ back to the main queue
// and transitions their DynamoDB execution status from FAILED_EXHAUSTED to ENQUEUED (004 FR-007).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/martingomezvega/tapi-challenge/internal/adapters/dynamo"
	"github.com/martingomezvega/tapi-challenge/internal/adapters/sqsq"
	"github.com/martingomezvega/tapi-challenge/internal/app/redrive"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("redrive failed", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	var (
		provider        = flag.String("provider", "", "Filter DLQ messages by provider (empty = all)")
		since           = flag.String("since", "", "Filter DLQ messages with run_date >= YYYY-MM-DD")
		dryRun          = flag.Bool("dry-run", false, "Inspect matching DLQ messages without mutating state")
		maxCount        = flag.Int("max", 0, "Maximum number of messages to redrive (0 = unlimited)")
		dlqURL          = flag.String("dlq-url", os.Getenv("DLQ_URL"), "SQS FIFO DLQ URL")
		queueURL        = flag.String("queue-url", os.Getenv("JOBS_QUEUE_URL"), "Main SQS FIFO queue URL")
		executionsTable = flag.String("executions-table", envOr("EXECUTIONS_TABLE", "executions"), "DynamoDB executions table")
		region          = flag.String("region", envOr("AWS_REGION", "us-east-1"), "AWS region")
		endpoint        = flag.String("endpoint", os.Getenv("AWS_ENDPOINT_URL"), "Custom AWS endpoint (LocalStack)")
	)
	flag.Parse()

	if *dlqURL == "" || *queueURL == "" {
		return errors.New("--dlq-url and --queue-url (or DLQ_URL / JOBS_QUEUE_URL) are required")
	}

	var sinceDate domain.RunDate
	if *since != "" {
		parsed, err := domain.ParseRunDate(*since)
		if err != nil {
			return fmt.Errorf("invalid --since: %w", err)
		}
		sinceDate = parsed
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(*region)}
	if *endpoint != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(envOr("AWS_ACCESS_KEY_ID", "test"), envOr("AWS_SECRET_ACCESS_KEY", "test"), ""),
		))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return fmt.Errorf("load aws config: %w", err)
	}

	ddbClient := dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		if *endpoint != "" {
			o.BaseEndpoint = aws.String(*endpoint)
		}
	})
	sqsClient := sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if *endpoint != "" {
			o.BaseEndpoint = aws.String(*endpoint)
		}
	})

	execRepo := dynamo.NewExecutionsRepo(ddbClient, *executionsTable, dynamo.DefaultExecutionTTL)
	dlqAdapter := sqsq.NewDLQAdapter(sqsClient, *dlqURL, *queueURL)

	uc := redrive.New(dlqAdapter, dlqAdapter, execRepo, time.Now)
	rep, err := uc.Run(ctx, redrive.Filter{
		Provider: *provider,
		Since:    sinceDate,
		DryRun:   *dryRun,
		MaxCount: *maxCount,
	})
	if err != nil {
		return err
	}

	logger.Info("dlq redrive completed",
		"provider", *provider,
		"since", string(sinceDate),
		"dry_run", *dryRun,
		"inspected", rep.Inspected,
		"redriven", rep.Redriven,
		"skipped", rep.Skipped,
	)
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
