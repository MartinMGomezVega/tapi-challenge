// Package httpapi implements the SigV4-signed HTTP client for calling the internal provider API (003 FR-003).
package httpapi

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/martingomezvega/tapi-challenge/internal/app/execute"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

// Config configures the internal provider HTTP client.
type Config struct {
	BaseURL             string
	Region              string
	Service             string // defaults to "execute-api"
	Timeout             time.Duration
	MaxIdleConnsPerHost int
	Credentials         aws.CredentialsProvider
	Transport           http.RoundTripper
}

// Client invokes the internal provider API Gateway with SigV4, Idempotency-Key, and traceparent.
type Client struct{}

// New creates a tuned HTTP client for the internal API Gateway.
func New(_ Config) *Client { return &Client{} }

// Call executes the provider request and returns the raw HTTP response.
func (c *Client) Call(_ context.Context, _ domain.Request, _, _ string) (execute.Response, error) {
	return execute.Response{}, errors.New("not implemented")
}
