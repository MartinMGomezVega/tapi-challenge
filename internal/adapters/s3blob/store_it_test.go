//go:build integration

package s3blob_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/adapters/s3blob"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
	"github.com/martingomezvega/tapi-challenge/test/testenv"
)

func TestStore_PutGet_LocalStack(t *testing.T) {
	env := testenv.StartLocalStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const bucket = "tapi-results-test"
	if err := env.EnsureBucket(ctx, bucket); err != nil {
		t.Fatalf("EnsureBucket: %v", err)
	}

	store := s3blob.New(env.S3, bucket)
	key := domain.ResultBlobKey(domain.RunDate("2026-11-21"), "edenor", "req-big")
	payload := bytes.Repeat([]byte("z"), domain.MaxInlineResponseBytes+1024)

	if err := store.Put(ctx, key, payload); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: got %d bytes, want %d", len(got), len(payload))
	}
}
