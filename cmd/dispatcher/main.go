// Command dispatcher runs the minute-aligned slot dispatcher ECS service with leader election,
// catch-up, orphan recovery, /healthz endpoint, and graceful SIGTERM lease release (002-T024).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/martingomezvega/tapi-challenge/internal/adapters/dynamo"
	"github.com/martingomezvega/tapi-challenge/internal/adapters/sqsq"
	"github.com/martingomezvega/tapi-challenge/internal/app/dispatch"
	"github.com/martingomezvega/tapi-challenge/internal/platform/clock"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("dispatcher exited with error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	region := envOrDefault("AWS_DEFAULT_REGION", "us-east-1")
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	reqTable := envOrDefault("REQUESTS_TABLE", "requests")
	execTable := envOrDefault("EXECUTIONS_TABLE", "executions")
	coordTable := envOrDefault("COORDINATION_TABLE", "coordination")
	queueURL := os.Getenv("JOBS_QUEUE_URL")
	queueName := envOrDefault("JOBS_QUEUE_NAME", "tapi-jobs.fifo")
	shards := envInt("SHARDS", 4)
	healthAddr := envOrDefault("HEALTH_ADDR", ":8080")
	tzName := envOrDefault("RUN_DAY_TZ", "UTC")
	ownerID := os.Getenv("DISPATCHER_OWNER_ID")
	if ownerID == "" {
		host, _ := os.Hostname()
		ownerID = fmt.Sprintf("%s-%d", host, os.Getpid())
	}

	loc, err := time.LoadLocation(tzName)
	if err != nil {
		return fmt.Errorf("load RUN_DAY_TZ %q: %w", tzName, err)
	}

	clk, err := clock.NewFromEnv()
	if err != nil {
		return fmt.Errorf("init clock: %w", err)
	}

	ddb, sqsClient, err := newAWSClients(ctx, region, endpoint)
	if err != nil {
		return err
	}
	if queueURL == "" {
		out, err := sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(queueName)})
		if err != nil {
			return fmt.Errorf("resolve queue URL for %q: %w", queueName, err)
		}
		queueURL = aws.ToString(out.QueueUrl)
	}

	reqRepo := dynamo.NewRequestsRepo(ddb, reqTable)
	execRepo := dynamo.NewExecutionsRepo(ddb, execTable, dynamo.DefaultExecutionTTL)
	coord := dynamo.NewDispatcherCoordinator(ddb, coordTable)
	queue := sqsq.NewQueue(sqsClient, queueURL)
	emf := dispatch.NewEMFEmitter(os.Stdout, dispatch.DefaultMetricNamespace, clk)

	uc, err := dispatch.New(reqRepo, execRepo, queue, coord, coord, emf, clk, logger, dispatch.Config{
		OwnerID:   ownerID,
		Shards:    shards,
		LeaseTTL:  dispatch.DefaultLeaseTTL,
		OrphanAge: dispatch.DefaultOrphanAge,
		Location:  loc,
	})
	if err != nil {
		return fmt.Errorf("init dispatch use case: %w", err)
	}

	healthSrv := startHealthServer(healthAddr, logger)
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = healthSrv.Shutdown(shutCtx)
	}()

	// Scale real tick interval by CLOCK_SPEEDUP (e.g. 720x -> 1 virtual minute every ~83ms).
	tickInterval := max(10*time.Millisecond, time.Duration(float64(time.Minute)/clk.Speedup()))
	logger.InfoContext(ctx, "dispatcher started",
		slog.String("owner_id", ownerID),
		slog.Int("shards", shards),
		slog.Float64("clock_speedup", clk.Speedup()),
		slog.Duration("tick_interval", tickInterval),
	)

	// Immediate first tick on startup for fast catch-up.
	if _, err := uc.Tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.ErrorContext(ctx, "initial tick failed", slog.String("error", err.Error()))
	}

	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("dispatcher shutting down; releasing leader lease", slog.String("owner_id", ownerID))
			relCtx, relCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer relCancel()
			if err := uc.ReleaseLease(relCtx); err != nil {
				logger.Warn("failed to release leader lease", slog.String("error", err.Error()))
			}
			return nil
		case <-ticker.C:
			if _, err := uc.Tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
				logger.ErrorContext(ctx, "tick failed", slog.String("error", err.Error()))
			}
		}
	}
}

func startHealthServer(addr string, logger *slog.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("health server error", slog.String("error", err.Error()))
		}
	}()
	return srv
}

func newAWSClients(ctx context.Context, region, endpoint string) (*dynamodb.Client, *sqs.Client, error) {
	opts := []func(*config.LoadOptions) error{config.WithRegion(region)}
	if endpoint != "" && os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("load aws config: %w", err)
	}
	ddb := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})
	sqsClient := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})
	return ddb, sqsClient, nil
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}
