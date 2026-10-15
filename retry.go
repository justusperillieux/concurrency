package concurrency

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"time"
)

const (
	DefaultRetryMaxAttempts = 3
	DefaultRetryBaseDelay   = 100 * time.Millisecond
	DefaultRetryMaxDelay    = 10 * time.Second
	DefaultRetryMultiplier  = 2.0
)

// RetryPolicy configures Retry and RetryValue. Zero fields use defaults:
// MaxAttempts DefaultRetryMaxAttempts, BaseDelay DefaultRetryBaseDelay,
// MaxDelay DefaultRetryMaxDelay (or BaseDelay if that is larger), Multiplier
// DefaultRetryMultiplier, Jitter none, and Retryable nil retries every error
// that is not marked Permanent.
//
// The delay before attempt k+1 is min(BaseDelay*Multiplier^(k-1), MaxDelay).
// Jitter in [0, 1] then subtracts a uniformly random fraction of up to Jitter
// of that delay; Jitter 1 is full jitter in [0, delay].
type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Multiplier  float64
	Jitter      float64
	Retryable   func(error) bool
}

type permanentError struct {
	err error
}

func (e *permanentError) Error() string { return e.err.Error() }

func (e *permanentError) Unwrap() error { return e.err }

// Permanent marks err as non-retryable. Retry stops at the first error that
// is or wraps a Permanent error. If fn returns the Permanent error itself,
// Retry returns the error passed to Permanent; if fn wraps it further, Retry
// returns that wrapping error unchanged. Either way the result unwraps to err,
// so errors.Is and errors.As keep working. Permanent(nil) returns nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// Retry calls fn until it succeeds, returns a non-retryable error, the
// attempts are exhausted, or ctx is done. Attempts are numbered from 1.
//
// When attempts are exhausted or an error is not retryable, Retry returns the
// last error; see Permanent for how permanent errors are unwrapped. When ctx is
// done before the first attempt, Retry returns context.Cause(ctx). When ctx is
// done later, while waiting between attempts, it returns context.Cause(ctx)
// joined with the last attempt error. An invalid policy returns ErrInvalidLimit
// and a nil fn returns ErrNilFunction, both without calling fn.
func Retry(ctx context.Context, p RetryPolicy, fn func(ctx context.Context, attempt int) error) error {
	if fn == nil {
		return ErrNilFunction
	}
	_, err := RetryValue(ctx, p, func(ctx context.Context, attempt int) (struct{}, error) {
		return struct{}{}, fn(ctx, attempt)
	})
	return err
}

// RetryValue is Retry for functions that produce a value. It returns the value
// of the first successful attempt, or the zero value with an error.
func RetryValue[T any](ctx context.Context, p RetryPolicy, fn func(ctx context.Context, attempt int) (T, error)) (T, error) {
	var zero T
	if fn == nil {
		return zero, ErrNilFunction
	}
	p, err := p.normalized()
	if err != nil {
		return zero, err
	}

	var lastErr error
	for attempt := 1; ; attempt++ {
		if cause := context.Cause(ctx); cause != nil {
			if lastErr == nil {
				return zero, cause
			}
			return zero, errors.Join(cause, lastErr)
		}
		value, err := fn(ctx, attempt)
		if err == nil {
			return value, nil
		}
		lastErr = err

		if permanent, ok := err.(*permanentError); ok {
			return zero, permanent.err
		}
		var permanent *permanentError
		if errors.As(err, &permanent) {
			return zero, err
		}
		if p.Retryable != nil && !p.Retryable(err) {
			return zero, err
		}
		if attempt >= p.MaxAttempts {
			return zero, err
		}
		if err := sleepContext(ctx, p.delay(attempt)); err != nil {
			return zero, errors.Join(err, lastErr)
		}
	}
}

func (p RetryPolicy) normalized() (RetryPolicy, error) {
	if p.MaxAttempts < 0 || p.BaseDelay < 0 || p.MaxDelay < 0 ||
		math.IsNaN(p.Multiplier) || math.IsInf(p.Multiplier, 0) || (p.Multiplier != 0 && p.Multiplier < 1) ||
		math.IsNaN(p.Jitter) || p.Jitter < 0 || p.Jitter > 1 {
		return p, ErrInvalidLimit
	}
	if p.MaxAttempts == 0 {
		p.MaxAttempts = DefaultRetryMaxAttempts
	}
	if p.BaseDelay == 0 {
		p.BaseDelay = DefaultRetryBaseDelay
	}
	if p.MaxDelay == 0 {
		p.MaxDelay = max(DefaultRetryMaxDelay, p.BaseDelay)
	}
	if p.Multiplier == 0 {
		p.Multiplier = DefaultRetryMultiplier
	}
	return p, nil
}

func (p RetryPolicy) delay(attempt int) time.Duration {
	delay := p.MaxDelay
	if backoff := float64(p.BaseDelay) * math.Pow(p.Multiplier, float64(attempt-1)); backoff < float64(p.MaxDelay) {
		delay = time.Duration(backoff)
	}
	if p.Jitter > 0 {
		delay -= time.Duration(float64(delay) * p.Jitter * rand.Float64())
	}
	return max(delay, 0)
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return context.Cause(ctx)
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
