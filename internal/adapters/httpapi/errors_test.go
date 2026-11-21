package httpapi_test

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/adapters/httpapi"
	"github.com/martingomezvega/tapi-challenge/internal/app/execute"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

type fakeNetTimeoutErr struct{}

func (fakeNetTimeoutErr) Error() string   { return "i/o timeout" }
func (fakeNetTimeoutErr) Timeout() bool   { return true }
func (fakeNetTimeoutErr) Temporary() bool { return true }

// T004: Adapter translates net.Error, context.DeadlineExceeded, ECONNRESET, and HTTP responses to domain.ProviderOutcome.
func TestTranslateOutcome(t *testing.T) {
	t.Parallel()

	o1 := httpapi.TranslateOutcome(execute.Response{}, context.DeadlineExceeded, 25*time.Second)
	if o1.TransportErr != domain.TransportTimeout {
		t.Fatalf("DeadlineExceeded -> %v, want TransportTimeout", o1.TransportErr)
	}

	o2 := httpapi.TranslateOutcome(execute.Response{}, fakeNetTimeoutErr{}, 2*time.Second)
	if o2.TransportErr != domain.TransportTimeout {
		t.Fatalf("net timeout -> %v, want TransportTimeout", o2.TransportErr)
	}

	o3 := httpapi.TranslateOutcome(execute.Response{}, syscall.ECONNRESET, 100*time.Millisecond)
	if o3.TransportErr != domain.TransportReset {
		t.Fatalf("ECONNRESET -> %v, want TransportReset", o3.TransportErr)
	}

	o4 := httpapi.TranslateOutcome(execute.Response{}, errors.New("weird socket err"), 50*time.Millisecond)
	if o4.TransportErr != domain.TransportUnknown {
		t.Fatalf("generic err -> %v, want TransportUnknown", o4.TransportErr)
	}

	o5 := httpapi.TranslateOutcome(execute.Response{
		StatusCode: 429,
		Body:       []byte(`{"message":"TooManyRequestsException"}`),
		RetryAfter: "30",
	}, nil, 40*time.Millisecond)
	if o5.StatusCode != 429 || o5.RetryAfterRaw != "30" || o5.TransportErr != domain.TransportNone || o5.CallDuration != 40*time.Millisecond {
		t.Fatalf("HTTP 429 outcome mismatch: %+v", o5)
	}
}
