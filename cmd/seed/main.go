// Command seed generates a synthetic catalog dataset with a Zipf provider distribution
// and registers all providers in DynamoDB coordination (001-T022).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"golang.org/x/sync/errgroup"

	"github.com/martingomezvega/tapi-challenge/internal/adapters/dynamo"
	"github.com/martingomezvega/tapi-challenge/internal/app/assignslots"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("seed failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	var (
		totalRecords int
		numProviders int
		zipfExponent float64
		workers      int
		reqTable     string
		coordTable   string
		endpoint     string
		region       string
	)

	flag.IntVar(&totalRecords, "records", 1_000_000, "total synthetic requests to seed")
	flag.IntVar(&numProviders, "providers", 50, "number of providers in Zipf distribution")
	flag.Float64Var(&zipfExponent, "zipf-s", 1.05, "Zipf exponent s (> 0)")
	flag.IntVar(&workers, "workers", 8, "concurrent provider seed workers")
	flag.StringVar(&reqTable, "requests-table", envOrDefault("REQUESTS_TABLE", "requests"), "DynamoDB requests table")
	flag.StringVar(&coordTable, "coordination-table", envOrDefault("COORDINATION_TABLE", "coordination"), "DynamoDB coordination table")
	flag.StringVar(&endpoint, "endpoint", os.Getenv("AWS_ENDPOINT_URL"), "optional AWS endpoint override (e.g. LocalStack)")
	flag.StringVar(&region, "region", envOrDefault("AWS_DEFAULT_REGION", "us-east-1"), "AWS region")
	flag.Parse()

	counts, err := zipfCounts(totalRecords, numProviders, zipfExponent)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ddb, err := newDynamoClient(ctx, region, endpoint)
	if err != nil {
		return err
	}

	reqRepo := dynamo.NewRequestsRepo(ddb, reqTable)
	coordRepo := dynamo.NewCoordinationRepo(ddb, coordTable)
	now := time.Now().UTC()

	logger.InfoContext(ctx, "starting synthetic catalog seed",
		slog.Int("records", totalRecords),
		slog.Int("providers", numProviders),
		slog.Float64("zipf_s", zipfExponent),
		slog.Int("workers", workers),
	)

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(max(1, workers))

	for pIdx, k := range counts {
		provider := fmt.Sprintf("provider-%03d", pIdx)
		g.Go(func() error {
			return seedProvider(gctx, reqRepo, coordRepo, provider, k, now)
		})
	}

	if err := g.Wait(); err != nil {
		if errors.Is(err, context.Canceled) {
			logger.Warn("seed interrupted gracefully")
			return nil
		}
		return err
	}

	logger.InfoContext(ctx, "synthetic catalog seed completed",
		slog.Int("records", totalRecords),
		slog.Int("providers", numProviders),
	)
	return nil
}

func seedProvider(
	ctx context.Context,
	reqRepo *dynamo.RequestsRepo,
	coordRepo *dynamo.CoordinationRepo,
	provider string,
	count int,
	ts time.Time,
) error {
	const chunkSize = 250
	endpoint := fmt.Sprintf("/v1/providers/%s/debt", provider)
	batch := make([]domain.Request, 0, chunkSize)

	for i := range count {
		batch = append(batch, domain.Request{
			ID:        fmt.Sprintf("%s-%07d", provider, i),
			Provider:  provider,
			Endpoint:  endpoint,
			Method:    domain.DefaultMethod,
			Body:      fmt.Appendf(nil, `{"service_ref":"%s-%07d"}`, provider, i),
			Enabled:   true,
			Slot:      -1,
			Shard:     -1,
			Version:   1,
			CreatedAt: ts,
			UpdatedAt: ts,
		})
		if len(batch) == chunkSize {
			if err := reqRepo.PutBatch(ctx, batch); err != nil {
				return fmt.Errorf("seed %s: %w", provider, err)
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if err := reqRepo.PutBatch(ctx, batch); err != nil {
			return fmt.Errorf("seed %s final batch: %w", provider, err)
		}
	}

	if err := coordRepo.SaveProviderState(ctx, assignslots.ProviderState{
		Provider:         provider,
		Count:            0,
		NewSinceBackfill: count,
		Backfilled:       false,
		UpdatedAt:        ts,
	}); err != nil {
		return fmt.Errorf("register provider %s: %w", provider, err)
	}
	return nil
}

func zipfCounts(total, providers int, exponent float64) ([]int, error) {
	if total < 1 || providers < 1 || providers > total || exponent <= 0 || math.IsNaN(exponent) || math.IsInf(exponent, 0) {
		return nil, fmt.Errorf("invalid zipf parameters: total=%d providers=%d s=%v", total, providers, exponent)
	}

	weights := make([]float64, providers)
	var sum float64
	for i := range providers {
		w := 1.0 / math.Pow(float64(i+1), exponent)
		weights[i] = w
		sum += w
	}

	counts := make([]int, providers)
	assigned := 0
	for i, w := range weights {
		c := max(1, int(math.Round(float64(total)*w/sum)))
		counts[i] = c
		assigned += c
	}
	counts[0] += total - assigned
	return counts, nil
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
