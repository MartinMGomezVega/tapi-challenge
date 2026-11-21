package domain_test

import (
	"errors"
	"testing"

	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

func TestExecutionStatus_ValidAndTerminal(t *testing.T) {
	t.Parallel()

	cases := []struct {
		status   domain.ExecutionStatus
		valid    bool
		terminal bool
		redrive  bool
	}{
		{domain.StatusPending, true, false, false},
		{domain.StatusEnqueued, true, false, false},
		{domain.StatusInProgress, true, false, false},
		{domain.StatusRetrying, true, false, false},
		{domain.StatusSucceeded, true, true, false},
		{domain.StatusFailedPermanent, true, true, false},
		{domain.StatusFailedExhausted, true, true, true},
		{domain.StatusSkipped, true, true, false},
		{domain.ExecutionStatus(""), false, false, false},
		{domain.ExecutionStatus("UNKNOWN"), false, false, false},
	}

	for _, tc := range cases {
		t.Run(string(tc.status), func(t *testing.T) {
			t.Parallel()
			if got := tc.status.Valid(); got != tc.valid {
				t.Fatalf("Valid(%q) = %v, want %v", tc.status, got, tc.valid)
			}
			if got := tc.status.IsTerminal(); got != tc.terminal {
				t.Fatalf("IsTerminal(%q) = %v, want %v", tc.status, got, tc.terminal)
			}
			if got := tc.status.CanRedrive(); got != tc.redrive {
				t.Fatalf("CanRedrive(%q) = %v, want %v", tc.status, got, tc.redrive)
			}
		})
	}
}

func TestExecutionStatus_Transitions(t *testing.T) {
	t.Parallel()

	allStatuses := []domain.ExecutionStatus{
		domain.StatusPending,
		domain.StatusEnqueued,
		domain.StatusInProgress,
		domain.StatusRetrying,
		domain.StatusSucceeded,
		domain.StatusFailedPermanent,
		domain.StatusFailedExhausted,
		domain.StatusSkipped,
	}

	allowed := map[domain.ExecutionStatus]map[domain.ExecutionStatus]bool{
		domain.StatusPending: {
			domain.StatusEnqueued:   true,
			domain.StatusInProgress: true,
		},
		domain.StatusEnqueued: {
			domain.StatusInProgress: true,
		},
		domain.StatusRetrying: {
			domain.StatusInProgress: true,
		},
		domain.StatusInProgress: {
			domain.StatusInProgress:      true, // expired lease takeover
			domain.StatusEnqueued:        true, // reconciler stale reset (005)
			domain.StatusRetrying:        true,
			domain.StatusSucceeded:       true,
			domain.StatusFailedPermanent: true,
			domain.StatusFailedExhausted: true,
			domain.StatusSkipped:         true,
		},
	}

	for _, from := range allStatuses {
		for _, to := range allStatuses {
			want := allowed[from][to]
			name := string(from) + "->" + string(to)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				if got := from.CanTransition(to); got != want {
					t.Fatalf("CanTransition(%s -> %s) = %v, want %v", from, to, got, want)
				}
				next, err := from.Transition(to)
				if want {
					if err != nil {
						t.Fatalf("Transition(%s -> %s) unexpected error: %v", from, to, err)
					}
					if next != to {
						t.Fatalf("Transition(%s -> %s) = %s, want %s", from, to, next, to)
					}
				} else if !errors.Is(err, domain.ErrInvalidTransition) {
					t.Fatalf("Transition(%s -> %s) error = %v, want ErrInvalidTransition", from, to, err)
				}
			})
		}
	}

	// Unknown status fails with ErrInvalidStatus.
	if _, err := domain.ExecutionStatus("BOGUS").Transition(domain.StatusEnqueued); !errors.Is(err, domain.ErrInvalidStatus) {
		t.Fatalf("unknown from status error = %v, want ErrInvalidStatus", err)
	}
	if _, err := domain.StatusEnqueued.Transition(domain.ExecutionStatus("BOGUS")); !errors.Is(err, domain.ErrInvalidStatus) {
		t.Fatalf("unknown to status error = %v, want ErrInvalidStatus", err)
	}
}

func TestResultBlobKey(t *testing.T) {
	t.Parallel()

	got := domain.ResultBlobKey(domain.RunDate("2026-11-21"), "edenor", "req-001")
	want := "results/run_date=2026-11-21/provider=edenor/req-001.json"
	if got != want {
		t.Fatalf("ResultBlobKey() = %q, want %q", got, want)
	}
}
