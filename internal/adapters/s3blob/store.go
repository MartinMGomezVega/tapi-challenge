// Package s3blob implements S3 storage for oversized provider responses and reconciliation reports (003 FR-004, 005).
package s3blob

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Store persists blobs in an S3 bucket with deterministic keys.
type Store struct {
	client *s3.Client
	bucket string
}

// New creates an S3-backed BlobStore targeting bucket.
func New(client *s3.Client, bucket string) *Store {
	return &Store{client: client, bucket: bucket}
}

// Put writes data to key with application/json content type.
func (s *Store) Put(ctx context.Context, key string, data []byte) error {
	return s.PutWithContentType(ctx, key, data, "application/json")
}

// PutWithContentType writes data to key with a custom content type (used for CSV/JSON reports in 005).
func (s *Store) PutWithContentType(ctx context.Context, key string, data []byte, contentType string) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return fmt.Errorf("s3 put %s/%s: %w", s.bucket, key, err)
	}
	return nil
}

// Get reads the full object payload for key.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 get %s/%s: %w", s.bucket, key, err)
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}
