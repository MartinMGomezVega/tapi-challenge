// Command reconciler runs the daily coverage reconciliation and provider capacity check job (005-T015, 001-T025).
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/martingomezvega/tapi-challenge/internal/adapters/dynamo"
	"github.com/martingomezvega/tapi-challenge/internal/adapters/s3blob"
	"github.com/martingomezvega/tapi-challenge/internal/adapters/sqsq"
	"github.com/martingomezvega/tapi-challenge/internal/app/capacity"
	"github.com/martingomezvega/tapi-challenge/internal/app/reconcile"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
	"github.com/martingomezvega/tapi-challenge/internal/platform/telemetry"
)

func main() {
	logger := telemetry.NewLogger(os.Stdout, "reconciler")
	if err := run(logger); err != nil {
		logger.Error("reconciler failed", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	var (
		dateStr       = flag.String("date", "", "Run date YYYY-MM-DD (defaults to yesterday in RUN_DAY_TZ)")
		redispatch    = flag.Bool("redispatch", envBoolOr("RECONCILE_REDISPATCH", true), "Re-dispatch missing and stuck executions")
		parallelism   = flag.Int("parallelism", envIntOr("RECONCILE_PARALLELISM", 64), "Max concurrent (slot, shard) workers")
		shards        = flag.Int("shards", envIntOr("SHARDS", 4), "Number of shards per slot")
		staleHours    = flag.Int("stale-hours", 2, "Hours after which non-terminal executions are considered stuck")
		checkCapacity = flag.Bool("check-capacity", true, "Run daily provider capacity check (001-T025)")
		p95Ms         = flag.Int("p95-ms", 400, "Assumed provider p95 latency in ms for capacity check")
		budgetRatio   = flag.Float64("budget-ratio", 0.70, "Max fraction of 24h day budget per provider")
		region        = flag.String("region", envOr("AWS_REGION", "us-east-1"), "AWS region")
		endpoint      = flag.String("endpoint", os.Getenv("AWS_ENDPOINT_URL"), "Custom AWS endpoint (LocalStack)")
		reqTable      = flag.String("requests-table", envOr("REQUESTS_TABLE", "requests"), "DynamoDB requests table")
		execTable     = flag.String("executions-table", envOr("EXECUTIONS_TABLE", "executions"), "DynamoDB executions table")
		coordTable    = flag.String("coordination-table", envOr("COORDINATION_TABLE", "coordination"), "DynamoDB coordination table")
		bucket        = flag.String("bucket", envOr("RESULTS_BUCKET", "tapi-results"), "S3 bucket for reports")
		queueURL      = flag.String("queue-url", os.Getenv("JOBS_QUEUE_URL"), "SQS FIFO queue URL")
		queueName     = flag.String("queue-name", envOr("JOBS_QUEUE_NAME", "tapi-jobs.fifo"), "SQS FIFO queue name")
		tzName        = flag.String("tz", envOr("RUN_DAY_TZ", "UTC"), "Run day timezone")
	)
	flag.Parse()

	loc, err := time.LoadLocation(*tzName)
	if err != nil {
		return fmt.Errorf("load tz %q: %w", *tzName, err)
	}

	var runDate domain.RunDate
	if *dateStr == "" {
		yesterday := time.Now().In(loc).AddDate(0, 0, -1)
		runDate = domain.RunDate(yesterday.Format("2006-01-02"))
	} else {
		parsed, err := domain.ParseRunDate(*dateStr)
		if err != nil {
			return err
		}
		runDate = parsed
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
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
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if *endpoint != "" {
			o.BaseEndpoint = aws.String(*endpoint)
			o.UsePathStyle = true
		}
	})

	resolvedQueueURL := *queueURL
	if *redispatch && resolvedQueueURL == "" {
		out, err := sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(*queueName)})
		if err != nil {
			return fmt.Errorf("resolve queue url %q: %w", *queueName, err)
		}
		resolvedQueueURL = aws.ToString(out.QueueUrl)
	}

	reqRepo := dynamo.NewRequestsRepo(ddbClient, *reqTable)
	execRepo := dynamo.NewExecutionsRepo(ddbClient, *execTable, dynamo.DefaultExecutionTTL)
	coordRepo := dynamo.NewCoordinationRepo(ddbClient, *coordTable)
	reportStore := s3blob.New(s3Client, *bucket)
	emf := telemetry.NewEMFEmitter(os.Stdout, telemetry.DefaultNamespace)
	recMetrics := telemetry.NewReconcilerMetrics(emf, time.Now)

	var queue reconcile.JobQueue
	if *redispatch && resolvedQueueURL != "" {
		queue = sqsq.NewQueue(sqsClient, resolvedQueueURL)
	}

	uc := reconcile.New(reqRepo, execRepo, queue, reportStore, recMetrics, time.Now, logger, reconcile.Config{
		Shards:      *shards,
		Parallelism: *parallelism,
		StaleAfter:  time.Duration(*staleHours) * time.Hour,
		Location:    loc,
		Redispatch:  *redispatch,
	})

	summary, err := uc.Run(ctx, runDate)
	if err != nil {
		return err
	}

	// Feature 001-T025: daily provider capacity check against 70% of 24h budget.
	if *checkCapacity {
		if err := runCapacityCheck(ctx, coordRepo, recMetrics, logger, time.Duration(*p95Ms)*time.Millisecond, *budgetRatio); err != nil {
			logger.Error("provider capacity check failed", "error", err)
		}
	}

	logger.Info("reconciler finished",
		"run_date", string(summary.RunDate),
		"expected", summary.Expected,
		"terminal", summary.Terminal,
		"coverage_pct", fmt.Sprintf("%.4f", summary.Coverage*100),
		"redispatched", summary.Redispatched,
		"json_report", summary.JSONReportKey,
		"csv_report", summary.CSVReportKey,
	)
	return nil
}

type coordVolumeLister struct{ coord *dynamo.CoordinationRepo }

func (c coordVolumeLister) ListProviderVolumes(ctx context.Context) ([]capacity.ProviderVolume, error) {
	states, err := c.coord.ListProviders(ctx)
	if err != nil {
		return nil, err
	}
	vols := make([]capacity.ProviderVolume, 0, len(states))
	for _, st := range states {
		vols = append(vols, capacity.ProviderVolume{
			Provider: st.Provider,
			Count:    st.Count,
		})
	}
	return vols, nil
}

type capacityMetricAdapter struct{ metrics *telemetry.ReconcilerMetrics }

func (a capacityMetricAdapter) EmitProviderOverCapacity(_ context.Context, rep capacity.ProviderReport) error {
	a.metrics.RecordProviderOverCapacity(rep.Provider, rep.OverCapacity)
	return nil
}

func runCapacityCheck(
	ctx context.Context,
	coord *dynamo.CoordinationRepo,
	metrics *telemetry.ReconcilerMetrics,
	logger *slog.Logger,
	defaultP95 time.Duration,
	budgetRatio float64,
) error {
	capUC, err := capacity.New(
		coordVolumeLister{coord: coord},
		nil,
		capacityMetricAdapter{metrics: metrics},
		capacity.Config{
			SafetyFactor:   budgetRatio,
			DefaultLatency: defaultP95,
		},
	)
	if err != nil {
		return err
	}
	reports, err := capUC.Check(ctx)
	if err != nil {
		return err
	}
	for _, rep := range reports {
		if rep.OverCapacity {
			logger.Warn("provider exceeds daily sequential capacity budget",
				"provider", rep.Provider,
				"count", rep.DailyRecords,
				"required_sec", fmt.Sprintf("%.1f", rep.RequiredSec),
				"budget_sec", fmt.Sprintf("%.1f", rep.BudgetSec),
				"utilization_pct", fmt.Sprintf("%.1f", rep.UtilizationPct),
			)
		}
	}
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envIntOr(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func envBoolOr(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return fallback
}
