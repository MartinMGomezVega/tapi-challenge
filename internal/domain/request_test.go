package domain_test

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

func TestNewRequest_Validation(t *testing.T) {
	t.Parallel()

	allowlist := []string{"/v1/providers/", "/v1/billing/"}
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

	exact256KB := makeJSONOfSize(t, domain.MaxBodyBytes)
	over256KB := makeJSONOfSize(t, domain.MaxBodyBytes+1)

	tests := []struct {
		name       string
		input      domain.NewRequestInput
		allowlist  []string
		wantErr    error
		wantMethod string
	}{
		{
			name: "valid request with default POST method",
			input: domain.NewRequestInput{
				ID:       "req-001",
				Provider: "edenor",
				Endpoint: "/v1/providers/edenor/debt",
				Body:     []byte(`{"account":"12345"}`),
				Enabled:  true,
			},
			allowlist:  allowlist,
			wantMethod: domain.DefaultMethod,
		},
		{
			name: "valid request with explicit lowercase method normalized",
			input: domain.NewRequestInput{
				ID:       "req-002",
				Provider: "metrogas",
				Endpoint: "/v1/billing/check",
				Method:   "get",
				Body:     []byte(`{}`),
				Enabled:  true,
			},
			allowlist:  allowlist,
			wantMethod: "GET",
		},
		{
			name: "valid request with body exactly 256 KiB",
			input: domain.NewRequestInput{
				ID:       "req-256k",
				Provider: "aysa",
				Endpoint: "/v1/providers/aysa/bill",
				Method:   "POST",
				Body:     exact256KB,
				Enabled:  true,
			},
			allowlist:  allowlist,
			wantMethod: "POST",
		},
		{
			name: "empty id rejected",
			input: domain.NewRequestInput{
				ID:       "   ",
				Provider: "edenor",
				Endpoint: "/v1/providers/edenor/debt",
				Body:     []byte(`{}`),
			},
			allowlist: allowlist,
			wantErr:   domain.ErrEmptyID,
		},
		{
			name: "empty provider rejected",
			input: domain.NewRequestInput{
				ID:       "req-003",
				Provider: "  ",
				Endpoint: "/v1/providers/edenor/debt",
				Body:     []byte(`{}`),
			},
			allowlist: allowlist,
			wantErr:   domain.ErrInvalidProvider,
		},
		{
			name: "provider containing hash delimiter rejected",
			input: domain.NewRequestInput{
				ID:       "req-004",
				Provider: "edenor#bad",
				Endpoint: "/v1/providers/edenor/debt",
				Body:     []byte(`{}`),
			},
			allowlist: allowlist,
			wantErr:   domain.ErrInvalidProvider,
		},
		{
			name: "absolute URL endpoint rejected",
			input: domain.NewRequestInput{
				ID:       "req-005",
				Provider: "edenor",
				Endpoint: "https://evil.example/v1/providers/edenor",
				Body:     []byte(`{}`),
			},
			allowlist: allowlist,
			wantErr:   domain.ErrInvalidEndpoint,
		},
		{
			name: "protocol-relative endpoint rejected",
			input: domain.NewRequestInput{
				ID:       "req-006",
				Provider: "edenor",
				Endpoint: "//evil.example/v1/providers/edenor",
				Body:     []byte(`{}`),
			},
			allowlist: allowlist,
			wantErr:   domain.ErrInvalidEndpoint,
		},
		{
			name: "path traversal endpoint rejected",
			input: domain.NewRequestInput{
				ID:       "req-007",
				Provider: "edenor",
				Endpoint: "/v1/providers/../admin/secret",
				Body:     []byte(`{}`),
			},
			allowlist: allowlist,
			wantErr:   domain.ErrInvalidEndpoint,
		},
		{
			name: "endpoint outside allowlist rejected",
			input: domain.NewRequestInput{
				ID:       "req-008",
				Provider: "edenor",
				Endpoint: "/v2/internal/admin",
				Body:     []byte(`{}`),
			},
			allowlist: allowlist,
			wantErr:   domain.ErrEndpointNotAllowed,
		},
		{
			name: "empty allowlist fails closed",
			input: domain.NewRequestInput{
				ID:       "req-009",
				Provider: "edenor",
				Endpoint: "/v1/providers/edenor/debt",
				Body:     []byte(`{}`),
			},
			allowlist: nil,
			wantErr:   domain.ErrEndpointNotAllowed,
		},
		{
			name: "invalid HTTP method rejected",
			input: domain.NewRequestInput{
				ID:       "req-010",
				Provider: "edenor",
				Endpoint: "/v1/providers/edenor/debt",
				Method:   "TRACE",
				Body:     []byte(`{}`),
			},
			allowlist: allowlist,
			wantErr:   domain.ErrInvalidMethod,
		},
		{
			name: "malformed JSON body rejected",
			input: domain.NewRequestInput{
				ID:       "req-011",
				Provider: "edenor",
				Endpoint: "/v1/providers/edenor/debt",
				Body:     []byte(`{"account":`),
			},
			allowlist: allowlist,
			wantErr:   domain.ErrInvalidBody,
		},
		{
			name: "empty body is not valid JSON",
			input: domain.NewRequestInput{
				ID:       "req-012",
				Provider: "edenor",
				Endpoint: "/v1/providers/edenor/debt",
				Body:     nil,
			},
			allowlist: allowlist,
			wantErr:   domain.ErrInvalidBody,
		},
		{
			name: "body larger than 256 KiB rejected",
			input: domain.NewRequestInput{
				ID:       "req-013",
				Provider: "edenor",
				Endpoint: "/v1/providers/edenor/debt",
				Body:     over256KB,
			},
			allowlist: allowlist,
			wantErr:   domain.ErrBodyTooLarge,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req, err := domain.NewRequest(tc.input, tc.allowlist, now)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("NewRequest() error = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewRequest() unexpected error: %v", err)
			}
			if req.Method != tc.wantMethod {
				t.Errorf("Method = %q, want %q", req.Method, tc.wantMethod)
			}
			if req.ID != tc.input.ID {
				t.Errorf("ID = %q, want %q", req.ID, tc.input.ID)
			}
			if req.Version != 1 {
				t.Errorf("Version = %d, want 1", req.Version)
			}
			if !req.CreatedAt.Equal(now) || !req.UpdatedAt.Equal(now) {
				t.Errorf("timestamps = (%v, %v), want %v", req.CreatedAt, req.UpdatedAt, now)
			}
		})
	}
}

func TestRequest_KeysFormatting(t *testing.T) {
	t.Parallel()

	req := domain.Request{
		ID:       "req-42",
		Provider: "edenor",
		Slot:     742,
		Shard:    3,
	}

	if got, want := req.SlotShard(), "0742#03"; got != want {
		t.Errorf("SlotShard() = %q, want %q", got, want)
	}
	if got, want := req.ProviderID(), "edenor#req-42"; got != want {
		t.Errorf("ProviderID() = %q, want %q", got, want)
	}
}

func TestRequest_RedactsPIIInLogsAndString(t *testing.T) {
	t.Parallel()

	secretPII := "dni-30999888-secret-name"
	req := domain.Request{
		ID:       "req-pii",
		Provider: "edenor",
		Endpoint: "/v1/providers/edenor/debt",
		Method:   "POST",
		Body:     []byte(`{"customer":"` + secretPII + `"}`),
		Enabled:  true,
		Slot:     10,
		Shard:    1,
	}

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	logger.Info("processing request", "request", req)

	if strings.Contains(buf.String(), secretPII) {
		t.Errorf("slog output leaked PII from Body: %s", buf.String())
	}
	if strings.Contains(req.String(), secretPII) {
		t.Errorf("String() leaked PII from Body: %s", req.String())
	}
}

func makeJSONOfSize(t *testing.T, totalBytes int) []byte {
	t.Helper()
	const prefix = `{"p":"`
	const suffix = `"}`
	padLen := totalBytes - len(prefix) - len(suffix)
	if padLen < 0 {
		t.Fatalf("totalBytes %d too small", totalBytes)
	}
	return []byte(prefix + strings.Repeat("a", padLen) + suffix)
}
