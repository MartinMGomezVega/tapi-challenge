package httpapi_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/martingomezvega/tapi-challenge/internal/adapters/httpapi"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// T011: HTTP client sends SigV4, Idempotency-Key, traceparent, and respects timeout.
func TestClient_Call_SigV4HeadersAndTimeout(t *testing.T) {
	t.Parallel()

	var (
		gotMethod      string
		gotPath        string
		gotAuth        string
		gotAmzDate     string
		gotIdemKey     string
		gotTraceParent string
		gotBody        string
	)

	tr := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotAmzDate = r.Header.Get("X-Amz-Date")
		gotIdemKey = r.Header.Get("Idempotency-Key")
		gotTraceParent = r.Header.Get("traceparent")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)

		rec := httptest.NewRecorder()
		rec.Header().Set("Retry-After", "12")
		rec.WriteHeader(http.StatusOK)
		_, _ = rec.WriteString(`{"debt":42}`)
		return rec.Result(), nil
	})

	client := httpapi.New(httpapi.Config{
		BaseURL:     "https://internal.api.example.com",
		Region:      "us-east-1",
		Service:     "execute-api",
		Timeout:     5 * time.Second,
		Credentials: credentials.NewStaticCredentialsProvider("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", ""),
		Transport:   tr,
	})

	req := domain.Request{
		ID:       "req-99",
		Provider: "edenor",
		Endpoint: "/providers/edenor/query",
		Method:   "POST",
		Body:     []byte(`{"account":"999"}`),
		Enabled:  true,
	}
	resp, err := client.Call(context.Background(), req, "req-99#2026-11-21", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	if err != nil {
		t.Fatalf("Call() error = %v", err)
	}
	if resp.StatusCode != http.StatusOK || string(resp.Body) != `{"debt":42}` || resp.RetryAfter != "12" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if gotMethod != http.MethodPost || gotPath != "/providers/edenor/query" || gotBody != `{"account":"999"}` {
		t.Fatalf("unexpected request: method=%s path=%s body=%s", gotMethod, gotPath, gotBody)
	}
	if !strings.HasPrefix(gotAuth, "AWS4-HMAC-SHA256 ") || gotAmzDate == "" {
		t.Fatalf("missing SigV4 headers: auth=%q date=%q", gotAuth, gotAmzDate)
	}
	if gotIdemKey != "req-99#2026-11-21" {
		t.Fatalf("Idempotency-Key = %q", gotIdemKey)
	}
	if gotTraceParent != "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" {
		t.Fatalf("traceparent = %q", gotTraceParent)
	}

	// Verify timeout enforcement.
	slowClient := httpapi.New(httpapi.Config{
		BaseURL:     "https://internal.api.example.com",
		Region:      "us-east-1",
		Timeout:     25 * time.Millisecond,
		Credentials: credentials.NewStaticCredentialsProvider("AKIAIOSFODNN7EXAMPLE", "secret", ""),
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			<-r.Context().Done()
			return nil, r.Context().Err()
		}),
	})
	_, err = slowClient.Call(context.Background(), req, "req-99#2026-11-21", "")
	if err == nil {
		t.Fatalf("expected timeout error on slow endpoint")
	}
}
