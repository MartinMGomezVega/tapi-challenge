// Package httpapi implements the SigV4-signed HTTP client for calling the internal provider API (003 FR-003).
package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"github.com/martingomezvega/tapi-challenge/internal/app/execute"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

const (
	defaultService          = "execute-api"
	defaultCallTimeout      = 25 * time.Second
	defaultIdleConnsPerHost = 32
	maxResponseReadBytes    = 16 * 1024 * 1024 // 16 MiB defensive cap
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
type Client struct {
	baseURL string
	region  string
	service string
	timeout time.Duration
	creds   aws.CredentialsProvider
	signer  *v4.Signer
	http    *http.Client
}

// New creates a tuned HTTP client for the internal API Gateway (003/plan.md).
func New(cfg Config) *Client {
	service := cfg.Service
	if service == "" {
		service = defaultService
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultCallTimeout
	}
	maxIdle := cfg.MaxIdleConnsPerHost
	if maxIdle <= 0 {
		maxIdle = defaultIdleConnsPerHost
	}

	tr := cfg.Transport
	if tr == nil {
		tr = &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          maxIdle * 2,
			MaxIdleConnsPerHost:   maxIdle,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   5 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			ForceAttemptHTTP2:     false, // HTTP/1.1 keep-alive per 003/plan.md
		}
	}

	return &Client{
		baseURL: strings.TrimRight(cfg.BaseURL, "/"),
		region:  cfg.Region,
		service: service,
		timeout: timeout,
		creds:   cfg.Credentials,
		signer:  v4.NewSigner(),
		http: &http.Client{
			Transport: tr,
		},
	}
}

// Call executes the provider request with SigV4 signing, Idempotency-Key, traceparent, and timeout.
func (c *Client) Call(ctx context.Context, req domain.Request, idemKey, traceParent string) (execute.Response, error) {
	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	method := req.Method
	if method == "" {
		method = domain.DefaultMethod
	}
	targetURL := c.baseURL + req.Endpoint
	bodyBytes := req.Body
	if bodyBytes == nil {
		bodyBytes = []byte{}
	}

	httpReq, err := http.NewRequestWithContext(callCtx, method, targetURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return execute.Response{}, fmt.Errorf("build http request %s: %w", req.ID, err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("X-Provider", req.Provider)
	if idemKey != "" {
		httpReq.Header.Set("Idempotency-Key", idemKey)
	}
	if traceParent != "" {
		httpReq.Header.Set("traceparent", traceParent)
	}

	if c.creds != nil {
		creds, err := c.creds.Retrieve(callCtx)
		if err != nil {
			return execute.Response{}, fmt.Errorf("retrieve aws credentials: %w", err)
		}
		sum := sha256.Sum256(bodyBytes)
		payloadHash := hex.EncodeToString(sum[:])
		if err := c.signer.SignHTTP(callCtx, creds, httpReq, payloadHash, c.service, c.region, time.Now().UTC()); err != nil {
			return execute.Response{}, fmt.Errorf("sign http request %s: %w", req.ID, err)
		}
	}

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return execute.Response{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseReadBytes))
	if err != nil {
		return execute.Response{}, fmt.Errorf("read response body %s: %w", req.ID, err)
	}

	return execute.Response{
		StatusCode: resp.StatusCode,
		Body:       respBody,
		RetryAfter: resp.Header.Get("Retry-After"),
	}, nil
}
