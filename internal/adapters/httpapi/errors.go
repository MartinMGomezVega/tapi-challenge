package httpapi

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"syscall"
	"time"

	"github.com/martingomezvega/tapi-challenge/internal/app/execute"
	"github.com/martingomezvega/tapi-challenge/internal/domain"
)

// TranslateOutcome converts an HTTP response or transport error into a domain.ProviderOutcome (004-T005).
func TranslateOutcome(resp execute.Response, err error, callDuration time.Duration) domain.ProviderOutcome {
	if err != nil {
		kind := classifyTransportErr(err)
		return domain.ProviderOutcome{
			TransportErr:    kind,
			RawErrorMessage: err.Error(),
			CallDuration:    callDuration,
		}
	}

	errHeader := resp.ErrorHeader
	if errHeader == "" && strings.Contains(string(resp.Body), "TooManyRequestsException") {
		errHeader = "TooManyRequestsException"
	}

	return domain.ProviderOutcome{
		StatusCode:      resp.StatusCode,
		Body:            resp.Body,
		RetryAfterRaw:   resp.RetryAfter,
		ErrorHeader:     errHeader,
		TransportErr:    domain.TransportNone,
		RawErrorMessage: httpStatusMessage(resp.StatusCode, errHeader),
		CallDuration:    callDuration,
	}
}

func classifyTransportErr(err error) domain.TransportErrorKind {
	if errors.Is(err, context.DeadlineExceeded) {
		return domain.TransportTimeout
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return domain.TransportTimeout
	}
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return domain.TransportReset
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "timeout") || strings.Contains(msg, "deadline exceeded") {
		return domain.TransportTimeout
	}
	if strings.Contains(msg, "connection reset") || strings.Contains(msg, "broken pipe") || strings.Contains(msg, "eof") {
		return domain.TransportReset
	}
	return domain.TransportUnknown
}

func httpStatusMessage(status int, errHeader string) string {
	if errHeader != "" {
		return "HTTP " + itoa(status) + " (" + errHeader + ")"
	}
	return "HTTP " + itoa(status)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [12]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
