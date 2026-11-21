// Command worker runs the ECS Fargate worker service that consumes SQS FIFO jobs,
// calls the internal provider API with SigV4, and persists daily execution results (feature 003).
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
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/martingomezvega/tapi-challenge/internal/adapters/dynamo"
	"github.com/martingomezvega/tapi-challenge/internal/adapters/httpapi"
	"github.com/martingomezvega/tapi-challenge/internal/adapters/s3blob"
	"github.com/martingomezvega/tapi-challenge/internal/adapters/sqsq"
	"github.com/martingomezvega/tapi-challenge/internal/app/execute"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
	"github.com/martingomezvega/tapi-challenge/internal/platform/clock"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("worker exited with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	region := envOr("AWS_REGION", "us-east-1")
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	requestsTable := envOr("REQUESTS_TABLE", "requests")
	executionsTable := envOr("EXECUTIONS_TABLE", "executions")
	resultsBucket := envOr("RESULTS_BUCKET", "tapi-results")
	queueURL := os.Getenv("JOBS_QUEUE_URL")
	apiBaseURL := os.Getenv("PROVIDER_API_BASE_URL")
	healthAddr := envOr("HEALTH_ADDR", ":8081")

	inflightPerTask := envIntOr("INFLIGHT_PER_TASK", 32)
	maxTasks := envIntOr("MAX_TASKS", 10)
	lambdaBudget := envIntOr("LAMBDA_BUDGET", 1000)
	receivers := envIntOr("RECEIVERS", 2)
	visibilitySec := envIntOr("VISIBILITY_TIMEOUT_SEC", 60)
	stopTimeoutSec := envIntOr("STOP_TIMEOUT_SEC", 30)
	callTimeoutSec := envIntOr("CALL_TIMEOUT_SEC", 25)
	providerRPS := envFloatOr("PROVIDER_RPS", 0)
	ownerID := envOr("WORKER_ID", defaultOwnerID())

	// Fail-fast global concurrency budget check (003 US4).
	if err := execute.ValidateConcurrencyBudget(inflightPerTask, maxTasks, lambdaBudget); err != nil {
		return err
	}
	if queueURL == "" {
		return errors.New("JOBS_QUEUE_URL is required")
	}
	if apiBaseURL == "" {
		return errors.New("PROVIDER_API_BASE_URL is required")
	}

	clk, err := clock.NewFromEnv()
	if err != nil {
		return fmt.Errorf("clock init: %w", err)
	}

	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
	if endpoint != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(envOr("AWS_ACCESS_KEY_ID", "test"), envOr("AWS_SECRET_ACCESS_KEY", "test"), ""),
		))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return fmt.Errorf("load aws config: %w", err)
	}

	ddbClient := dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})
	sqsClient := sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	})

	reqRepo := dynamo.NewRequestsRepo(ddbClient, requestsTable)
	execRepo := dynamo.NewExecutionsRepo(ddbClient, executionsTable, dynamo.DefaultExecutionTTL)
	blobStore := s3blob.New(s3Client, resultsBucket)
	apiClient := httpapi.New(httpapi.Config{
		BaseURL:             apiBaseURL,
		Region:              region,
		Timeout:             time.Duration(callTimeoutSec) * time.Second,
		MaxIdleConnsPerHost: inflightPerTask,
		Credentials:         awsCfg.Credentials,
	})

	shutdownTimeout := time.Duration(max(stopTimeoutSec-5, 5)) * time.Second
	consumer := sqsq.NewConsumer(sqsClient, sqsq.ConsumerConfig{
		QueueURL:          queueURL,
		Receivers:         receivers,
		MaxInflight:       inflightPerTask,
		WaitTimeSeconds:   20,
		VisibilityTimeout: time.Duration(visibilitySec) * time.Second,
		ShutdownTimeout:   shutdownTimeout,
	}, logger)

	retryPol := execute.NewDomainRetryPolicy(
		domain.NewClassifier(nil),
		domain.NewRetryPolicy(domain.RetryPolicyConfig{}),
		httpapi.TranslateOutcome,
		clk,
	)
	breaker := domain.NewProviderBreaker(domain.BreakerConfig{Now: clk.Now})

	uc := execute.New(execRepo, reqRepo, apiClient, blobStore, consumer, retryPol, clk, logger, execute.Config{
		OwnerID:     ownerID,
		LeaseTTL:    time.Duration(visibilitySec) * time.Second,
		CallTimeout: time.Duration(callTimeoutSec) * time.Second,
	}).WithBreaker(breaker)
	if providerRPS > 0 {
		uc.WithLimiter(execute.NewProviderRateLimiter(providerRPS, 1))
	}

	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	healthSrv := &http.Server{
		Addr:              healthAddr,
		Handler:           healthMux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := healthSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("health server error", "error", err)
		}
	}()
	defer func() {
		sCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = healthSrv.Shutdown(sCtx)
	}()

	logger.Info("worker started",
		"owner_id", ownerID,
		"inflight_per_task", inflightPerTask,
		"receivers", receivers,
		"max_tasks", maxTasks,
		"lambda_budget", lambdaBudget,
	)

	return consumer.Run(ctx, uc)
}

func defaultOwnerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return fmt.Sprintf("worker-%d", os.Getpid())
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
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

func envFloatOr(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}
