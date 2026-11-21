// Command mockapi runs the configurable mock internal API server for local docker-compose demos.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/martingomezvega/tapi-challenge/test/mockapi"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	addr := envOr("ADDR", ":8090")
	latencyMs := envIntOr("DEFAULT_LATENCY_MS", 5)
	jitterMs := envIntOr("LATENCY_JITTER_MS", 0)
	respSize := envIntOr("RESPONSE_SIZE_BYTES", 0)

	srv := mockapi.New(mockapi.Config{
		DefaultLatency: time.Duration(latencyMs) * time.Millisecond,
		LatencyJitter:  time.Duration(jitterMs) * time.Millisecond,
		ResponseSize:   respSize,
	})

	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           srv,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	go func() {
		logger.Info("mockapi listening", "addr", addr, "latency_ms", latencyMs)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("mockapi failed", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
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
