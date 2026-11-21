package mockapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/martingomezvega/tapi-challenge/test/mockapi"
)

type handlerTransport struct{ h http.Handler }

func (t handlerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	t.h.ServeHTTP(rec, req)
	return rec.Result(), nil
}

func newTestClient(h http.Handler) *http.Client {
	return &http.Client{Transport: handlerTransport{h: h}}
}

func TestMockAPI_ConcurrencyDetectionAndStats(t *testing.T) {
	t.Parallel()

	srv := mockapi.New(mockapi.Config{
		DefaultLatency: 30 * time.Millisecond,
	})
	client := newTestClient(srv)

	// Fire 2 concurrent calls to the same provider -> max_inflight must detect 2.
	var wg sync.WaitGroup
	for i := range 2 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req, err := http.NewRequest(http.MethodPost, "http://mockapi.local/providers/edenor/query", strings.NewReader(`{"acct":"1"}`))
			if err != nil {
				t.Errorf("new request: %v", err)
				return
			}
			req.Header.Set("Idempotency-Key", "req-same-"+string(rune('0'+idx))+"#2026-11-21")
			resp, err := client.Do(req)
			if err != nil {
				t.Errorf("do: %v", err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("status = %d, want 200", resp.StatusCode)
			}
		}(i)
	}
	wg.Wait()

	// Send duplicate Idempotency-Key -> DuplicateIdemKeys must increment.
	req, _ := http.NewRequest(http.MethodPost, "http://mockapi.local/providers/edenor/query", strings.NewReader(`{}`))
	req.Header.Set("Idempotency-Key", "req-same-0#2026-11-21")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("dup request: %v", err)
	}
	resp.Body.Close()

	// Verify /stats endpoint matches Stats().
	statsResp, err := client.Get("http://mockapi.local/stats")
	if err != nil {
		t.Fatalf("get /stats: %v", err)
	}
	defer statsResp.Body.Close()
	if statsResp.StatusCode != http.StatusOK {
		t.Fatalf("/stats status = %d, want 200", statsResp.StatusCode)
	}
	var snap mockapi.StatsSnapshot
	if err := json.NewDecoder(statsResp.Body).Decode(&snap); err != nil {
		t.Fatalf("decode stats: %v", err)
	}

	if snap.TotalCalls != 3 {
		t.Fatalf("TotalCalls = %d, want 3", snap.TotalCalls)
	}
	if snap.DuplicateIdemKeys != 1 {
		t.Fatalf("DuplicateIdemKeys = %d, want 1", snap.DuplicateIdemKeys)
	}
	ps := snap.Providers["edenor"]
	if ps.MaxInflight < 2 {
		t.Fatalf("edenor MaxInflight = %d, want >= 2", ps.MaxInflight)
	}
	if ps.Inflight != 0 {
		t.Fatalf("edenor Inflight after completion = %d, want 0", ps.Inflight)
	}
}

func TestMockAPI_FaultInjection(t *testing.T) {
	t.Parallel()

	srv := mockapi.New(mockapi.Config{
		Faults: map[string]mockapi.FaultRule{
			"bad429": {StatusCode: http.StatusTooManyRequests, RetryAfter: "15", Body: `{"error":"rate_limited"}`},
			"bad400": {StatusCode: http.StatusBadRequest, Body: `{"error":"invalid_account"}`},
			"flaky":  {StatusCode: http.StatusServiceUnavailable, FirstNOnly: 1},
		},
	})
	client := newTestClient(srv)

	// 429 with Retry-After
	resp, err := client.Post("http://mockapi.local/providers/bad429/query", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("bad429: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests || resp.Header.Get("Retry-After") != "15" {
		t.Fatalf("bad429 got status=%d Retry-After=%q", resp.StatusCode, resp.Header.Get("Retry-After"))
	}

	// 400
	resp, err = client.Post("http://mockapi.local/providers/bad400/query", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("bad400: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad400 status = %d, want 400", resp.StatusCode)
	}

	// flaky: first fails 503, second succeeds 200
	resp, err = client.Post("http://mockapi.local/providers/flaky/query", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("flaky #1: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("flaky #1 status = %d, want 503", resp.StatusCode)
	}
	resp, err = client.Post("http://mockapi.local/providers/flaky/query", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("flaky #2: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("flaky #2 status = %d, want 200", resp.StatusCode)
	}
}
