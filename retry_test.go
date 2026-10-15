package concurrency

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"
	"testing/synctest"
	"time"
)

func TestRetryBacksOffExponentiallyUntilSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		transient := errors.New("transient")
		var offsets []time.Duration
		var attempts []int
		value, err := RetryValue(context.Background(), RetryPolicy{
			MaxAttempts: 4,
			BaseDelay:   time.Second,
			Multiplier:  3,
		}, func(_ context.Context, attempt int) (string, error) {
			offsets = append(offsets, time.Since(start))
			attempts = append(attempts, attempt)
			if attempt < 4 {
				return "", transient
			}
			return "ok", nil
		})
		if err != nil || value != "ok" {
			t.Fatalf("RetryValue=(%q,%v), want (ok,nil)", value, err)
		}
		if want := []int{1, 2, 3, 4}; !slices.Equal(attempts, want) {
			t.Fatalf("attempts=%v, want %v", attempts, want)
		}
		if want := []time.Duration{0, time.Second, 4 * time.Second, 13 * time.Second}; !slices.Equal(offsets, want) {
			t.Fatalf("attempt offsets=%v, want %v", offsets, want)
		}
	})
}

func TestRetryCapsDelayAtMaxDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		var offsets []time.Duration
		err := Retry(context.Background(), RetryPolicy{
			MaxAttempts: 5,
			BaseDelay:   time.Second,
			MaxDelay:    3 * time.Second,
		}, func(context.Context, int) error {
			offsets = append(offsets, time.Since(start))
			return errors.New("transient")
		})
		if err == nil {
			t.Fatal("Retry succeeded, want the last attempt error")
		}
		want := []time.Duration{0, time.Second, 3 * time.Second, 6 * time.Second, 9 * time.Second}
		if !slices.Equal(offsets, want) {
			t.Fatalf("attempt offsets=%v, want %v", offsets, want)
		}
	})
}

func TestRetryDefaultsAndExhaustionReturnsLastError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		transient := errors.New("transient")
		var offsets []time.Duration
		err := Retry(context.Background(), RetryPolicy{}, func(_ context.Context, attempt int) error {
			offsets = append(offsets, time.Since(start))
			return fmt.Errorf("attempt %d: %w", attempt, transient)
		})
		if !errors.Is(err, transient) || err.Error() != "attempt 3: transient" {
			t.Fatalf("Retry=%v, want the third attempt error wrapping %v", err, transient)
		}
		want := []time.Duration{0, DefaultRetryBaseDelay, 3 * DefaultRetryBaseDelay}
		if !slices.Equal(offsets, want) {
			t.Fatalf("attempt offsets=%v, want %v", offsets, want)
		}
	})
}

func TestRetryLargeBackoffDoesNotOverflow(t *testing.T) {
	policy, err := RetryPolicy{BaseDelay: time.Hour, MaxDelay: math.MaxInt64, Multiplier: 1e6}.normalized()
	if err != nil {
		t.Fatal(err)
	}
	if got := policy.delay(100); got != math.MaxInt64 {
		t.Fatalf("delay(100)=%v, want %v", got, time.Duration(math.MaxInt64))
	}
	policy.Jitter = 1
	if got := policy.delay(100); got < 0 {
		t.Fatalf("jittered delay(100)=%v, want non-negative", got)
	}
}

func TestRetryJitterStaysWithinBounds(t *testing.T) {
	for _, jitter := range []float64{0.25, 1} {
		t.Run(fmt.Sprint(jitter), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				previous := time.Now()
				var delays []time.Duration
				_ = Retry(context.Background(), RetryPolicy{
					MaxAttempts: 50,
					BaseDelay:   time.Second,
					Multiplier:  1,
					Jitter:      jitter,
				}, func(_ context.Context, attempt int) error {
					now := time.Now()
					if attempt > 1 {
						delays = append(delays, now.Sub(previous))
					}
					previous = now
					return errors.New("transient")
				})
				low := time.Duration(float64(time.Second) * (1 - jitter))
				for _, delay := range delays {
					if delay < low || delay > time.Second {
						t.Fatalf("jittered delay=%v, want within [%v,%v]", delay, low, time.Second)
					}
				}
				if slices.Min(delays) == slices.Max(delays) {
					t.Fatalf("jittered delays are all %v, want variation", delays[0])
				}
			})
		})
	}
}

func TestRetryStopsOnPermanentError(t *testing.T) {
	inner := errors.New("not found")
	for _, test := range []struct {
		name    string
		wrap    func(error) error
		wantMsg string
	}{
		{name: "direct", wrap: Permanent, wantMsg: "not found"},
		{name: "wrapped", wrap: func(err error) error { return fmt.Errorf("lookup: %w", Permanent(err)) }, wantMsg: "lookup: not found"},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			err := Retry(context.Background(), RetryPolicy{MaxAttempts: 5}, func(_ context.Context, attempt int) error {
				calls++
				if attempt == 1 {
					return errors.New("transient")
				}
				return test.wrap(inner)
			})
			if calls != 2 {
				t.Fatalf("calls=%d, want 2", calls)
			}
			if !errors.Is(err, inner) || err.Error() != test.wantMsg {
				t.Fatalf("Retry=%v, want %q wrapping %v", err, test.wantMsg, inner)
			}
		})
	}

	if err := Permanent(nil); err != nil {
		t.Fatalf("Permanent(nil)=%v, want nil", err)
	}
	permanent := Permanent(inner)
	if !errors.Is(permanent, inner) || permanent.Error() != inner.Error() {
		t.Fatalf("Permanent(%v)=%v, want an error wrapping it", inner, permanent)
	}
}

func TestRetryStopsOnNonRetryableError(t *testing.T) {
	fatal := errors.New("fatal")
	calls := 0
	err := Retry(context.Background(), RetryPolicy{
		MaxAttempts: 5,
		Retryable:   func(err error) bool { return !errors.Is(err, fatal) },
	}, func(_ context.Context, attempt int) error {
		calls++
		if attempt == 1 {
			return errors.New("transient")
		}
		return fatal
	})
	if calls != 2 || !errors.Is(err, fatal) {
		t.Fatalf("Retry=(%v) after %d calls, want %v after 2 calls", err, calls, fatal)
	}
}

func TestRetryContextCanceledDuringBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		cause := errors.New("shutdown")
		transient := errors.New("transient")
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		time.AfterFunc(500*time.Millisecond, func() { cancel(cause) })

		calls := 0
		err := Retry(ctx, RetryPolicy{MaxAttempts: 5, BaseDelay: time.Second}, func(context.Context, int) error {
			calls++
			return transient
		})
		if !errors.Is(err, cause) || !errors.Is(err, transient) {
			t.Fatalf("Retry=%v, want errors.Is for %v and %v", err, cause, transient)
		}
		if calls != 1 {
			t.Fatalf("calls=%d, want 1", calls)
		}
		if elapsed := time.Since(start); elapsed != 500*time.Millisecond {
			t.Fatalf("Retry returned after %v, want 500ms", elapsed)
		}
	})
}

func TestRetryContextDeadlineBetweenAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
		defer cancel()
		calls := 0
		err := Retry(ctx, RetryPolicy{MaxAttempts: 10, BaseDelay: time.Second, Multiplier: 1}, func(context.Context, int) error {
			calls++
			return errors.New("transient")
		})
		if !errors.Is(err, context.DeadlineExceeded) || calls != 3 {
			t.Fatalf("Retry=%v after %d calls, want %v after 3 calls", err, calls, context.DeadlineExceeded)
		}
	})
}

func TestRetryCanceledContextSkipsAttempts(t *testing.T) {
	cause := errors.New("already canceled")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cause)
	value, err := RetryValue(ctx, RetryPolicy{}, func(context.Context, int) (int, error) {
		t.Fatal("fn called with a canceled context")
		return 0, nil
	})
	if value != 0 || err != cause {
		t.Fatalf("RetryValue=(%d,%v), want (0,%v)", value, err, cause)
	}
}

func TestRetryRejectsInvalidPolicyAndNilFunction(t *testing.T) {
	for _, test := range []struct {
		name   string
		policy RetryPolicy
	}{
		{name: "negative attempts", policy: RetryPolicy{MaxAttempts: -1}},
		{name: "negative base delay", policy: RetryPolicy{BaseDelay: -1}},
		{name: "negative max delay", policy: RetryPolicy{MaxDelay: -1}},
		{name: "multiplier below one", policy: RetryPolicy{Multiplier: 0.5}},
		{name: "NaN multiplier", policy: RetryPolicy{Multiplier: math.NaN()}},
		{name: "infinite multiplier", policy: RetryPolicy{Multiplier: math.Inf(1)}},
		{name: "negative jitter", policy: RetryPolicy{Jitter: -0.1}},
		{name: "jitter above one", policy: RetryPolicy{Jitter: 1.1}},
		{name: "NaN jitter", policy: RetryPolicy{Jitter: math.NaN()}},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := Retry(context.Background(), test.policy, func(context.Context, int) error {
				t.Fatal("fn called with an invalid policy")
				return nil
			})
			if !errors.Is(err, ErrInvalidLimit) {
				t.Fatalf("Retry=%v, want %v", err, ErrInvalidLimit)
			}
		})
	}

	if err := Retry(context.Background(), RetryPolicy{}, nil); !errors.Is(err, ErrNilFunction) {
		t.Fatalf("Retry(nil)=%v, want %v", err, ErrNilFunction)
	}
	if value, err := RetryValue[int](context.Background(), RetryPolicy{}, nil); value != 0 || !errors.Is(err, ErrNilFunction) {
		t.Fatalf("RetryValue(nil)=(%d,%v), want (0,%v)", value, err, ErrNilFunction)
	}
}
