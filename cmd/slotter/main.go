// Command slotter runs the resumable, rate-limited streaming backfill job for catalog slots (001-US2).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/martingomezvega/tapi-challenge/internal/adapters/dynamo"
	"github.com/martingomezvega/tapi-challenge/internal/app/assignslots"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
	"github.com/martingomezvega/tapi-challenge/internal/platform/clock"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("slotter failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	var (
		workers       int
		wcuLimit      float64
		shards        int
		pageSize      int
		jobID         string
		reqTable      string
		coordTable    string
		endpoint      string
		region        string
		tzName        string
		rebalanceOver int
	)

	flag.IntVar(&workers, "workers", assignslots.DefaultWorkers, "number of providers processed concurrently")
	flag.Float64Var(&wcuLimit, "wcu-limit", 200, "maximum SetSlot writes per second (<= 0 disables limit)")
	flag.IntVar(&shards, "shards", 4, "number of GSI shards S")
	flag.IntVar(&pageSize, "page-size", int(dynamo.DefaultPageSize), "DynamoDB gsi_provider Query page size")
	flag.StringVar(&jobID, "job-id", time.Now().UTC().Format("backfill-20060102"), "resumable backfill job ID")
	flag.StringVar(&reqTable, "requests-table", envOrDefault("REQUESTS_TABLE", "requests"), "DynamoDB requests table")
	flag.StringVar(&coordTable, "coordination-table", envOrDefault("COORDINATION_TABLE", "coordination"), "DynamoDB coordination table")
	flag.StringVar(&endpoint, "endpoint", os.Getenv("AWS_ENDPOINT_URL"), "optional AWS endpoint override (e.g. LocalStack)")
	flag.StringVar(&region, "region", envOrDefault("AWS_DEFAULT_REGION", "us-east-1"), "AWS region")
	flag.StringVar(&tzName, "tz", envOrDefault("RUN_DAY_TZ", "UTC"), "timezone for same-day slot calculation (FR-005b)")
	flag.IntVar(&rebalanceOver, "rebalance-threshold", assignslots.DefaultRebalanceThreshold, "new records threshold triggering full rebalance")
	flag.Parse()

	loc, err := time.LoadLocation(tzName)
	if err != nil {
		return fmt.Errorf("load timezone %q: %w", tzName, err)
	}

	assigner, err := domain.NewSlotAssigner(shards)
	if err != nil {
		return fmt.Errorf("init slot assigner: %w", err)
	}

	clk, err := clock.NewFromEnv()
	if err != nil {
		return fmt.Errorf("init clock: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ddb, err := newDynamoClient(ctx, region, endpoint)
	if err != nil {
		return err
	}

	var repoOpts []dynamo.RequestsOption
	if pageSize > 0 && pageSize <= 1000 {
		repoOpts = append(repoOpts, dynamo.WithPageSize(int32(pageSize))) // #nosec G115 -- bounds checked above
	}
	repoOpts = append(repoOpts, dynamo.WithClock(clk.Now))

	reqRepo := dynamo.NewRequestsRepo(ddb, reqTable, repoOpts...)
	coordRepo := dynamo.NewCoordinationRepo(ddb, coordTable)

	uc, err := assignslots.New(
		assigner,
		reqRepo,
		reqRepo,
		coordRepo,
		coordRepo,
		assignslots.NoopImmediateDispatcher{},
		clk,
		logger,
		assignslots.Config{
			Workers:            workers,
			WCULimit:           wcuLimit,
			RebalanceThreshold: rebalanceOver,
			Location:           loc,
		},
	)
	if err != nil {
		return fmt.Errorf("init assignslots use case: %w", err)
	}

	logger.InfoContext(ctx, "starting slotter backfill",
		slog.String("job_id", jobID),
		slog.Int("workers", workers),
		slog.Float64("wcu_limit", wcuLimit),
		slog.Int("shards", shards),
	)

	stats, err := uc.Run(ctx, jobID)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			logger.Warn("slotter interrupted gracefully; progress checkpointed",
				slog.String("job_id", jobID),
				slog.Int("slots_updated", stats.SlotsUpdated),
			)
			return nil
		}
		return fmt.Errorf("run backfill %q: %w", jobID, err)
	}

	logger.InfoContext(ctx, "slotter backfill finished",
		slog.String("job_id", jobID),
		slog.Int("providers_processed", stats.ProvidersProcessed),
		slog.Int("providers_skipped", stats.ProvidersSkipped),
		slog.Int("providers_rebalanced", stats.ProvidersRebalanced),
		slog.Int("providers_incremental", stats.ProvidersIncrement),
		slog.Int("slots_updated", stats.SlotsUpdated),
		slog.Int("slots_unchanged", stats.SlotsUnchanged),
		slog.Int("dispatched_now", stats.DispatchedNow),
		slog.Int("concurrent_overflow", stats.ConcurrentOverflow),
	)
	return nil
}

func newDynamoClient(ctx context.Context, region, endpoint string) (*dynamodb.Client, error) {
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(region),
	}
	if endpoint != "" && os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	var ddbOpts []func(*dynamodb.Options)
	if endpoint != "" {
		ddbOpts = append(ddbOpts, func(o *dynamodb.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
	}
	return dynamodb.NewFromConfig(cfg, ddbOpts...), nil
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
