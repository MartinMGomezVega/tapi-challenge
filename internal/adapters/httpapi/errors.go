package httpapi

import (
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/app/execute"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

// TranslateOutcome converts an HTTP response or transport error into a domain.ProviderOutcome (004-T005).
func TranslateOutcome(_ execute.Response, _ error, _ time.Duration) domain.ProviderOutcome {
	return domain.ProviderOutcome{}
}
