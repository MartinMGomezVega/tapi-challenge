package execute

import (
	"fmt"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/domain"
	"github.com/martingomezvega/tapi-challenge/internal/platform/clock"
)

// OutcomeTranslator converts a raw provider call result into a pure domain.ProviderOutcome.
type OutcomeTranslator func(resp Response, err error, callDuration time.Duration) domain.ProviderOutcome

// DurationAwareRetryPolicy is an optional extension of RetryPolicy that receives callDuration
// to decide fast in-process retries vs deferred SQS visibility backoff (004 US2).
type DurationAwareRetryPolicy interface {
	RetryPolicy
	DecideWithDuration(err error, resp Response, attempts, receiveCount int, callDuration time.Duration) RetryDecision
}

// DomainRetryPolicy connects domain.Classifier and domain.RetryPolicy to the app/execute port (004-T012).
type DomainRetryPolicy struct {
	classifier *domain.Classifier
	policy     *domain.RetryPolicy
	translate  OutcomeTranslator
	clk        clock.Provider
}

// NewDomainRetryPolicy constructs a RetryPolicy backed by the pure domain classifier and retry policy.
func NewDomainRetryPolicy(
	classifier *domain.Classifier,
	policy *domain.RetryPolicy,
	translate OutcomeTranslator,
	clk clock.Provider,
) *DomainRetryPolicy {
	if classifier == nil {
		classifier = domain.NewClassifier(nil)
	}
	if policy == nil {
		policy = domain.NewRetryPolicy(domain.RetryPolicyConfig{})
	}
	if translate == nil {
		translate = defaultOutcomeTranslator
	}
	return &DomainRetryPolicy{
		classifier: classifier,
		policy:     policy,
		translate:  translate,
		clk:        clk,
	}
}

// Decide satisfies RetryPolicy using a zero callDuration fallback.
func (d *DomainRetryPolicy) Decide(err error, resp Response, attempts, receiveCount int) RetryDecision {
	return d.DecideWithDuration(err, resp, attempts, receiveCount, 0)
}

// DecideWithDuration classifies the call outcome and maps domain.RetryDecision into execute.RetryDecision.
func (d *DomainRetryPolicy) DecideWithDuration(
	err error,
	resp Response,
	attempts, receiveCount int,
	callDuration time.Duration,
) RetryDecision {
	outcome := d.translate(resp, err, callDuration)
	class := d.classifier.Classify(outcome)

	var retryAfter time.Duration
	if outcome.RetryAfterRaw != "" {
		now := time.Now().UTC()
		if d.clk != nil {
			now = d.clk.Now().UTC()
		}
		if parsed, ok := domain.ParseRetryAfter(outcome.RetryAfterRaw, now); ok {
			retryAfter = parsed
		}
	}

	dec := d.policy.Decide(class, attempts, receiveCount, callDuration, retryAfter)
	errMsg := domain.SanitizeErrorMessage(outcome.RawErrorMessage)
	if errMsg == "" && class != domain.ErrClassNone && class != domain.ErrClassAlreadyProcessed {
		errMsg = fmt.Sprintf("provider call failed (%s, HTTP %d)", class, resp.StatusCode)
	}

	return RetryDecision{
		Action:       mapDecisionKind(dec.Kind),
		ErrorClass:   string(dec.Class),
		ErrorMessage: errMsg,
		Backoff:      dec.Delay,
	}
}

func mapDecisionKind(k domain.DecisionKind) RetryAction {
	switch k {
	case domain.DecisionComplete:
		return ActionComplete
	case domain.DecisionRetryInProcess:
		return ActionRetryInProcess
	case domain.DecisionRetryLater:
		return ActionRetryLater
	case domain.DecisionExhausted:
		return ActionFailExhausted
	default:
		return ActionFailPermanent
	}
}

func defaultOutcomeTranslator(resp Response, err error, callDuration time.Duration) domain.ProviderOutcome {
	if err != nil {
		return domain.ProviderOutcome{
			TransportErr:    domain.TransportUnknown,
			RawErrorMessage: err.Error(),
			CallDuration:    callDuration,
		}
	}
	return domain.ProviderOutcome{
		StatusCode:    resp.StatusCode,
		Body:          resp.Body,
		RetryAfterRaw: resp.RetryAfter,
		ErrorHeader:   resp.ErrorHeader,
		CallDuration:  callDuration,
	}
}
