package execute

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/martingomezvega/tapi-challenge/internal/domain"
	"github.com/martingomezvega/tapi-challenge/internal/platform/clock"
)

const (
	defaultPersistenceRetryDelay = 15 * time.Second
	defaultSimpleRetryDelay      = 30 * time.Second
	defaultMaxReceiveCount       = 6
	maxInProcessAttempts         = 2
	maxErrorMsgBytes             = 1024
)

// Config configures the ExecuteRequest use case.
type Config struct {
	OwnerID     string
	LeaseTTL    time.Duration
	CallTimeout time.Duration
	MaxInline   int
}

// UseCase executes a single provider request end-to-end with lease, idempotency, and blob fallback.
type UseCase struct {
	store   ExecutionStore
	reqs    RequestGetter
	api     ProviderAPI
	blobs   BlobStore
	acker   MessageAcker
	policy  RetryPolicy
	clock   clock.Provider
	logger  *slog.Logger
	cfg     Config
	limiter ProviderLimiter
	breaker CircuitBreaker
	metrics MetricsRecorder
}

// New creates a new ExecuteRequest use case.
func New(
	store ExecutionStore,
	reqs RequestGetter,
	api ProviderAPI,
	blobs BlobStore,
	acker MessageAcker,
	policy RetryPolicy,
	clk clock.Provider,
	logger *slog.Logger,
	cfg Config,
) *UseCase {
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = DefaultLeaseTTL
	}
	if cfg.CallTimeout <= 0 {
		cfg.CallTimeout = DefaultCallTimeout
	}
	if cfg.MaxInline <= 0 {
		cfg.MaxInline = domain.MaxInlineResponseBytes
	}
	if policy == nil {
		policy = defaultRetryPolicy{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &UseCase{
		store:  store,
		reqs:   reqs,
		api:    api,
		blobs:  blobs,
		acker:  acker,
		policy: policy,
		clock:  clk,
		logger: logger,
		cfg:    cfg,
	}
}

// WithLimiter attaches an optional per-provider rate limiter (003 FR-006).
func (u *UseCase) WithLimiter(l ProviderLimiter) *UseCase {
	u.limiter = l
	return u
}

// WithBreaker attaches an optional per-provider circuit breaker (004 US3).
func (u *UseCase) WithBreaker(b CircuitBreaker) *UseCase {
	u.breaker = b
	return u
}

// WithMetrics attaches an optional metrics recorder (005).
func (u *UseCase) WithMetrics(m MetricsRecorder) *UseCase {
	u.metrics = m
	return u
}

// Execute processes a single SQS FIFO message end-to-end (003 US1-US3, 004).
func (u *UseCase) Execute(ctx context.Context, msg Message) error {
	now := u.clock.Now().UTC()
	key := ExecKey{RequestID: msg.RequestID, RunDate: msg.RunDate}
	log := u.logger.With(
		"request_id", msg.RequestID,
		"provider", msg.Provider,
		"run_date", string(msg.RunDate),
		"trace_id", msg.TraceContext,
	)

	// Circuit breaker short-circuit (004 US3): release back to SQS without calling API or counting an attempt.
	if u.breaker != nil {
		if allowed, cooldown := u.breaker.Allow(msg.Provider); !allowed {
			if u.metrics != nil {
				u.metrics.RecordBreakerOpen(msg.Provider, true)
			}
			log.Warn("provider circuit breaker open, deferring message", "cooldown_ms", cooldown.Milliseconds())
			return u.acker.Nack(ctx, msg, cooldown)
		}
	}

	execRec, outcome, err := u.store.Acquire(ctx, key, u.cfg.OwnerID, u.cfg.LeaseTTL, now)
	if err != nil {
		log.Error("failed to acquire execution lease", "error", err)
		_ = u.acker.Nack(ctx, msg, defaultPersistenceRetryDelay)
		return fmt.Errorf("acquire %s#%s: %w", msg.RequestID, msg.RunDate, err)
	}

	switch outcome {
	case AcquireAlreadyTerminal:
		log.Info("execution already terminal, acking duplicate", "status", string(execRec.Status))
		return u.acker.Ack(ctx, msg)
	case AcquireLeasedByOther:
		rem := max(execRec.LeaseExpiresAt.Sub(now), DefaultMinExtend)
		log.Info("execution leased by another worker, extending visibility", "extend_ms", rem.Milliseconds())
		return u.acker.Extend(ctx, msg, rem)
	case AcquireAcquired:
		// proceed
	}

	req, err := u.reqs.Get(ctx, msg.RequestID)
	if err != nil {
		if errors.Is(err, domain.ErrRequestNotFound) {
			info := ErrInfo{
				ErrorClass:   ReasonNotFound,
				ErrorMessage: "request deleted from catalog before execution",
				CompletedAt:  u.clock.Now().UTC(),
			}
			if failErr := u.store.FailPermanent(ctx, key, u.cfg.OwnerID, info); failErr != nil && !errors.Is(failErr, ErrLeaseLost) {
				_ = u.acker.Nack(ctx, msg, defaultPersistenceRetryDelay)
				return fmt.Errorf("fail permanent not found %s: %w", msg.RequestID, failErr)
			}
			u.recordMetric(msg.Provider, domain.StatusFailedPermanent, ReasonNotFound, 0)
			log.Warn("request not found in catalog, marked FAILED_PERMANENT")
			return u.acker.Ack(ctx, msg)
		}
		log.Error("failed to load request from catalog", "error", err)
		_ = u.acker.Nack(ctx, msg, defaultPersistenceRetryDelay)
		return fmt.Errorf("get request %s: %w", msg.RequestID, err)
	}

	if !req.Enabled {
		if skipErr := u.store.MarkSkipped(ctx, key, u.cfg.OwnerID, u.clock.Now().UTC()); skipErr != nil && !errors.Is(skipErr, ErrLeaseLost) {
			_ = u.acker.Nack(ctx, msg, defaultPersistenceRetryDelay)
			return fmt.Errorf("mark skipped %s: %w", msg.RequestID, skipErr)
		}
		u.recordMetric(msg.Provider, domain.StatusSkipped, "", 0)
		log.Info("request disabled after dispatch, marked SKIPPED")
		return u.acker.Ack(ctx, msg)
	}

	if u.limiter != nil {
		if err := u.limiter.Wait(ctx, msg.Provider); err != nil {
			_ = u.acker.Nack(ctx, msg, 0)
			return err
		}
	}

	idemKey := domain.IdempotencyKey(msg.RequestID, msg.RunDate)
	attempts := max(execRec.Attempts, 1)
	receiveCount := max(msg.ReceiveCount, 1)

	var (
		resp     Response
		callErr  error
		latency  time.Duration
		decision RetryDecision
	)

	for inProc := 0; inProc < maxInProcessAttempts; inProc++ {
		callStart := time.Now()
		callCtx, cancel := context.WithTimeout(ctx, u.cfg.CallTimeout)
		resp, callErr = u.api.Call(callCtx, req, idemKey, msg.TraceContext)
		cancel()
		latency = time.Since(callStart)

		decision = u.policy.Decide(callErr, resp, attempts+inProc, receiveCount)
		if decision.Action != ActionRetryInProcess || inProc+1 >= maxInProcessAttempts {
			if decision.Action == ActionRetryInProcess {
				decision.Action = ActionRetryLater
				if decision.Backoff <= 0 {
					decision.Backoff = defaultSimpleRetryDelay
				}
			}
			break
		}
		if decision.Backoff > 0 {
			timer := time.NewTimer(decision.Backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				_ = u.acker.Nack(ctx, msg, 0)
				return ctx.Err()
			case <-timer.C:
			}
		}
	}

	if u.breaker != nil {
		switch decision.Action {
		case ActionComplete:
			u.breaker.Record(msg.Provider, false)
			if u.metrics != nil {
				u.metrics.RecordBreakerOpen(msg.Provider, false)
			}
		case ActionRetryLater, ActionFailExhausted:
			u.breaker.Record(msg.Provider, true)
		}
	}

	completedAt := u.clock.Now().UTC()
	switch decision.Action {
	case ActionComplete:
		res := Result{
			HTTPStatus:  resp.StatusCode,
			LatencyMs:   latency.Milliseconds(),
			CompletedAt: completedAt,
		}
		if len(resp.Body) > u.cfg.MaxInline {
			if u.blobs == nil {
				_ = u.acker.Nack(ctx, msg, defaultPersistenceRetryDelay)
				return fmt.Errorf("response exceeds %d bytes and no BlobStore configured", u.cfg.MaxInline)
			}
			blobKey := domain.ResultBlobKey(msg.RunDate, msg.Provider, msg.RequestID)
			if err := u.blobs.Put(ctx, blobKey, resp.Body); err != nil {
				log.Error("failed to store oversized response in S3", "s3_key", blobKey, "error", err)
				_ = u.acker.Nack(ctx, msg, defaultPersistenceRetryDelay)
				return fmt.Errorf("put blob %s: %w", blobKey, err)
			}
			res.ResponseS3Key = blobKey
		} else {
			res.ResponseBody = string(resp.Body)
		}
		if err := u.store.Complete(ctx, key, u.cfg.OwnerID, res); err != nil {
			if errors.Is(err, ErrLeaseLost) {
				log.Warn("lease lost before Complete, skipping ack")
				return nil
			}
			log.Error("failed to persist SUCCEEDED execution", "error", err)
			_ = u.acker.Nack(ctx, msg, defaultPersistenceRetryDelay)
			return fmt.Errorf("complete %s: %w", msg.RequestID, err)
		}
		u.recordMetric(msg.Provider, domain.StatusSucceeded, "", latency)
		log.Info("execution succeeded", "http_status", resp.StatusCode, "latency_ms", res.LatencyMs, "s3_key", res.ResponseS3Key)
		return u.acker.Ack(ctx, msg)

	case ActionFailPermanent:
		info := ErrInfo{
			ErrorClass:   decision.ErrorClass,
			ErrorMessage: truncateMessage(decision.ErrorMessage),
			HTTPStatus:   resp.StatusCode,
			LatencyMs:    latency.Milliseconds(),
			CompletedAt:  completedAt,
		}
		if err := u.store.FailPermanent(ctx, key, u.cfg.OwnerID, info); err != nil {
			if errors.Is(err, ErrLeaseLost) {
				return nil
			}
			_ = u.acker.Nack(ctx, msg, defaultPersistenceRetryDelay)
			return fmt.Errorf("fail permanent %s: %w", msg.RequestID, err)
		}
		u.recordMetric(msg.Provider, domain.StatusFailedPermanent, info.ErrorClass, latency)
		log.Warn("execution failed permanently", "error_class", info.ErrorClass, "http_status", resp.StatusCode, "latency_ms", info.LatencyMs)
		return u.acker.Ack(ctx, msg)

	case ActionFailExhausted:
		info := ErrInfo{
			ErrorClass:   decision.ErrorClass,
			ErrorMessage: truncateMessage(decision.ErrorMessage),
			HTTPStatus:   resp.StatusCode,
			LatencyMs:    latency.Milliseconds(),
			CompletedAt:  completedAt,
		}
		if err := u.store.FailExhausted(ctx, key, u.cfg.OwnerID, info); err != nil {
			if errors.Is(err, ErrLeaseLost) {
				return nil
			}
			_ = u.acker.Nack(ctx, msg, defaultPersistenceRetryDelay)
			return fmt.Errorf("fail exhausted %s: %w", msg.RequestID, err)
		}
		u.recordMetric(msg.Provider, domain.StatusFailedExhausted, info.ErrorClass, latency)
		log.Error("execution retries exhausted, routing to DLQ", "error_class", info.ErrorClass, "http_status", resp.StatusCode, "receive_count", receiveCount)
		// Do not delete so SQS RedrivePolicy moves the message to the DLQ.
		return u.acker.Nack(ctx, msg, 0)

	default: // ActionRetryLater
		backoff := decision.Backoff
		if backoff <= 0 {
			backoff = defaultSimpleRetryDelay
		}
		info := ErrInfo{
			ErrorClass:   decision.ErrorClass,
			ErrorMessage: truncateMessage(decision.ErrorMessage),
			HTTPStatus:   resp.StatusCode,
			LatencyMs:    latency.Milliseconds(),
			NextRetryAt:  completedAt.Add(backoff),
		}
		if err := u.store.ScheduleRetry(ctx, key, u.cfg.OwnerID, info); err != nil {
			if errors.Is(err, ErrLeaseLost) {
				return nil
			}
			_ = u.acker.Nack(ctx, msg, defaultPersistenceRetryDelay)
			return fmt.Errorf("schedule retry %s: %w", msg.RequestID, err)
		}
		u.recordMetric(msg.Provider, domain.StatusRetrying, info.ErrorClass, latency)
		log.Warn("execution scheduled for retry", "error_class", info.ErrorClass, "http_status", resp.StatusCode, "backoff_ms", backoff.Milliseconds())
		return u.acker.Nack(ctx, msg, backoff)
	}
}

func (u *UseCase) recordMetric(provider string, status domain.ExecutionStatus, errClass string, latency time.Duration) {
	if u.metrics != nil {
		u.metrics.RecordExecution(provider, status, errClass, latency)
	}
}

// ValidateConcurrencyBudget enforces k * t <= lambdaBudget / 2 (003 US4).
// Each API call triggers 2 downstream Lambdas, so the worker fleet must stay within half the regional Lambda quota.
func ValidateConcurrencyBudget(inflightPerTask, maxTasks, lambdaBudget int) error {
	if inflightPerTask <= 0 || maxTasks <= 0 || lambdaBudget <= 0 {
		return fmt.Errorf("%w: inflight_per_task=%d max_tasks=%d lambda_budget=%d must be > 0",
			ErrInvalidBudget, inflightPerTask, maxTasks, lambdaBudget)
	}
	if inflightPerTask*maxTasks > lambdaBudget/2 {
		return fmt.Errorf("%w: inflight_per_task(%d) * max_tasks(%d) = %d exceeds LAMBDA_BUDGET/2 (%d)",
			ErrInvalidBudget, inflightPerTask, maxTasks, inflightPerTask*maxTasks, lambdaBudget/2)
	}
	return nil
}

// ProviderRateLimiter enforces an optional per-provider requests-per-second ceiling (003 FR-006).
type ProviderRateLimiter struct {
	rps   float64
	burst int

	mu       sync.Mutex
	limiters map[string]*rate.Limiter
}

// NewProviderRateLimiter creates a per-provider limiter (rps <= 0 disables limiting).
func NewProviderRateLimiter(rps float64, burst int) *ProviderRateLimiter {
	if burst <= 0 {
		burst = 1
	}
	return &ProviderRateLimiter{
		rps:      rps,
		burst:    burst,
		limiters: make(map[string]*rate.Limiter),
	}
}

// Wait blocks until provider has a token or ctx is cancelled.
func (l *ProviderRateLimiter) Wait(ctx context.Context, provider string) error {
	if l == nil || l.rps <= 0 {
		return nil
	}
	l.mu.Lock()
	lim, ok := l.limiters[provider]
	if !ok {
		lim = rate.NewLimiter(rate.Limit(l.rps), l.burst)
		l.limiters[provider] = lim
	}
	l.mu.Unlock()
	return lim.Wait(ctx)
}

// defaultRetryPolicy is the feature-003 stub replaced by domain.RetryPolicy in feature 004.
type defaultRetryPolicy struct{}

func (defaultRetryPolicy) Decide(err error, resp Response, _, receiveCount int) RetryDecision {
	if err != nil {
		if receiveCount >= defaultMaxReceiveCount {
			return RetryDecision{Action: ActionFailExhausted, ErrorClass: "TRANSPORT_ERROR", ErrorMessage: err.Error()}
		}
		return RetryDecision{Action: ActionRetryLater, ErrorClass: "TRANSPORT_ERROR", ErrorMessage: err.Error(), Backoff: defaultSimpleRetryDelay}
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return RetryDecision{Action: ActionComplete}
	}
	if resp.StatusCode >= 400 && resp.StatusCode < 500 &&
		resp.StatusCode != 408 && resp.StatusCode != 429 {
		return RetryDecision{
			Action:       ActionFailPermanent,
			ErrorClass:   fmt.Sprintf("HTTP_%d", resp.StatusCode),
			ErrorMessage: fmt.Sprintf("permanent client error HTTP %d", resp.StatusCode),
		}
	}
	if receiveCount >= defaultMaxReceiveCount {
		return RetryDecision{
			Action:       ActionFailExhausted,
			ErrorClass:   fmt.Sprintf("HTTP_%d", resp.StatusCode),
			ErrorMessage: fmt.Sprintf("exhausted after HTTP %d", resp.StatusCode),
		}
	}
	return RetryDecision{
		Action:       ActionRetryLater,
		ErrorClass:   fmt.Sprintf("HTTP_%d", resp.StatusCode),
		ErrorMessage: fmt.Sprintf("transient error HTTP %d", resp.StatusCode),
		Backoff:      defaultSimpleRetryDelay,
	}
}

func truncateMessage(s string) string {
	if len(s) <= maxErrorMsgBytes {
		return s
	}
	return s[:maxErrorMsgBytes]
}
