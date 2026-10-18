package concurrency

import (
	"context"
	"errors"
	"math"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// FR-CONC-21
func TestLimiterRejectsInvalidConfiguration(t *testing.T) {
	for _, test := range []struct {
		name  string
		rate  float64
		burst int
	}{
		{name: "negative rate", rate: -1, burst: 1},
		{name: "NaN rate", rate: math.NaN(), burst: 1},
		{name: "zero burst", rate: 1, burst: 0},
		{name: "negative burst", rate: 1, burst: -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			limiter, err := NewLimiter(test.rate, test.burst)
			if limiter != nil || !errors.Is(err, ErrInvalidLimit) {
				t.Fatalf("NewLimiter(%v,%d)=(%v,%v), want (nil,%v)", test.rate, test.burst, limiter, err, ErrInvalidLimit)
			}
		})
	}

	limiter := mustLimiter(t, 1, 2)
	if err := limiter.SetRate(-1); !errors.Is(err, ErrInvalidLimit) {
		t.Fatalf("SetRate(-1)=%v, want %v", err, ErrInvalidLimit)
	}
	if err := limiter.SetRate(math.NaN()); !errors.Is(err, ErrInvalidLimit) {
		t.Fatalf("SetRate(NaN)=%v, want %v", err, ErrInvalidLimit)
	}
	if limiter.Rate() != 1 || limiter.Burst() != 2 {
		t.Fatalf("(Rate,Burst)=(%v,%d), want (1,2)", limiter.Rate(), limiter.Burst())
	}
	for _, n := range []int{-1, 3} {
		if err := limiter.Wait(context.Background(), n); !errors.Is(err, ErrInvalidLimit) {
			t.Fatalf("Wait(%d)=%v, want %v", n, err, ErrInvalidLimit)
		}
	}
}

// FR-CONC-22
func TestLimiterTryAcquireRefillsUpToBurst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		limiter := mustLimiter(t, 10, 5)
		assertTryAcquire(t, limiter, 5, true)
		assertTryAcquire(t, limiter, 1, false)
		assertTryAcquire(t, limiter, 0, true)
		assertTryAcquire(t, limiter, -1, false)

		time.Sleep(100 * time.Millisecond)
		assertTryAcquire(t, limiter, 1, true)
		assertTryAcquire(t, limiter, 1, false)

		time.Sleep(time.Hour)
		assertTryAcquire(t, limiter, 6, false)
		assertTryAcquire(t, limiter, 5, true)
		assertTryAcquire(t, limiter, 1, false)
	})
}

// FR-CONC-23
func TestLimiterWaitBlocksUntilRefilled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		limiter := mustLimiter(t, 2, 2)
		start := time.Now()
		for _, step := range []struct {
			n    int
			want time.Duration
		}{
			{n: 2, want: 0},
			{n: 1, want: 500 * time.Millisecond},
			{n: 2, want: 1500 * time.Millisecond},
			{n: 0, want: 1500 * time.Millisecond},
		} {
			if err := limiter.Wait(context.Background(), step.n); err != nil {
				t.Fatalf("Wait(%d)=%v, want nil", step.n, err)
			}
			if elapsed := time.Since(start); elapsed != step.want {
				t.Fatalf("Wait(%d) returned at %v, want %v", step.n, elapsed, step.want)
			}
		}
	})
}

// FR-CONC-24
func TestLimiterServesWaitersInOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		limiter := mustLimiter(t, 1, 4)
		assertTryAcquire(t, limiter, 4, true)
		start := time.Now()

		var mu sync.Mutex
		var order []int
		var offsets []time.Duration
		var wg sync.WaitGroup
		for _, n := range []int{4, 1} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := limiter.Wait(context.Background(), n); err != nil {
					t.Errorf("Wait(%d)=%v, want nil", n, err)
				}
				mu.Lock()
				order = append(order, n)
				offsets = append(offsets, time.Since(start))
				mu.Unlock()
			}()
			synctest.Wait()
		}

		time.Sleep(2 * time.Second)
		assertTryAcquire(t, limiter, 1, false)
		wg.Wait()
		if want := []int{4, 1}; !slices.Equal(order, want) {
			t.Fatalf("grant order=%v, want %v", order, want)
		}
		if want := []time.Duration{4 * time.Second, 5 * time.Second}; !slices.Equal(offsets, want) {
			t.Fatalf("grant offsets=%v, want %v", offsets, want)
		}
	})
}

// FR-CONC-25
func TestLimiterWaitCancellationReleasesQueuePosition(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		limiter := mustLimiter(t, 1, 4)
		assertTryAcquire(t, limiter, 4, true)
		start := time.Now()

		cause := errors.New("give up")
		headCtx, cancelHead := context.WithCancelCause(context.Background())
		headResult := make(chan error, 1)
		go func() { headResult <- limiter.Wait(headCtx, 4) }()
		synctest.Wait()

		nextResult := make(chan time.Duration, 1)
		go func() {
			if err := limiter.Wait(context.Background(), 1); err != nil {
				t.Errorf("queued Wait=%v, want nil", err)
			}
			nextResult <- time.Since(start)
		}()
		synctest.Wait()

		time.Sleep(time.Second)
		cancelHead(cause)
		if err := <-headResult; !errors.Is(err, cause) {
			t.Fatalf("canceled Wait=%v, want %v", err, cause)
		}
		if got := <-nextResult; got != time.Second {
			t.Fatalf("queued Wait returned at %v, want 1s", got)
		}

		deadlineCtx, cancelDeadline := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancelDeadline()
		if err := limiter.Wait(deadlineCtx, 1); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Wait past deadline=%v, want %v", err, context.DeadlineExceeded)
		}
		time.Sleep(900 * time.Millisecond)
		assertTryAcquire(t, limiter, 1, true)

		canceledCtx, cancel := context.WithCancelCause(context.Background())
		cancel(cause)
		if err := limiter.Wait(canceledCtx, 0); !errors.Is(err, cause) {
			t.Fatalf("Wait with canceled context=%v, want %v", err, cause)
		}
	})
}

// FR-CONC-26
func TestLimiterZeroRateWaitsForSetRate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		limiter := mustLimiter(t, 0, 1)
		assertTryAcquire(t, limiter, 1, true)

		result := make(chan time.Duration, 1)
		start := time.Now()
		go func() {
			if err := limiter.Wait(context.Background(), 1); err != nil {
				t.Errorf("Wait=%v, want nil", err)
			}
			result <- time.Since(start)
		}()
		time.Sleep(time.Hour)
		synctest.Wait()
		select {
		case got := <-result:
			t.Fatalf("Wait returned at %v with a zero rate", got)
		default:
		}

		if err := limiter.SetRate(2); err != nil {
			t.Fatal(err)
		}
		if got := <-result; got != time.Hour+500*time.Millisecond {
			t.Fatalf("Wait returned at %v, want 1h0m0.5s", got)
		}
	})
}

// FR-CONC-26
func TestLimiterSetRateReschedulesWaiters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		limiter := mustLimiter(t, 1, 1)
		assertTryAcquire(t, limiter, 1, true)
		start := time.Now()

		result := make(chan time.Duration, 1)
		go func() {
			if err := limiter.Wait(context.Background(), 1); err != nil {
				t.Errorf("Wait=%v, want nil", err)
			}
			result <- time.Since(start)
		}()
		time.Sleep(500 * time.Millisecond)
		if err := limiter.SetRate(0.5); err != nil {
			t.Fatal(err)
		}
		if got := <-result; got != 1500*time.Millisecond {
			t.Fatalf("Wait returned at %v, want 1.5s", got)
		}
	})
}

// FR-CONC-26
func TestLimiterInfiniteRateIsUnlimited(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		limiter := mustLimiter(t, 1, 2)
		assertTryAcquire(t, limiter, 2, true)

		results := make(chan error, 2)
		for range 2 {
			go func() { results <- limiter.Wait(context.Background(), 2) }()
		}
		synctest.Wait()
		if err := limiter.SetRate(math.Inf(1)); err != nil {
			t.Fatal(err)
		}
		for range 2 {
			if err := <-results; err != nil {
				t.Fatalf("Wait=%v, want nil", err)
			}
		}
		assertTryAcquire(t, limiter, 1000, true)
		if err := limiter.Wait(context.Background(), 1000); err != nil {
			t.Fatalf("unlimited Wait(1000)=%v, want nil", err)
		}

		if err := limiter.SetRate(1); err != nil {
			t.Fatal(err)
		}
		assertTryAcquire(t, limiter, 2, true)
		assertTryAcquire(t, limiter, 1, false)
	})
}

// FR-CONC-23
func TestLimiterConcurrentWaitersShareRate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		limiter := mustLimiter(t, 100, 10)
		start := time.Now()
		var wg sync.WaitGroup
		for range 100 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := limiter.Wait(context.Background(), 1); err != nil {
					t.Errorf("Wait=%v, want nil", err)
				}
			}()
		}
		wg.Wait()
		if elapsed := time.Since(start); elapsed < 900*time.Millisecond || elapsed > 901*time.Millisecond {
			t.Fatalf("100 waits at 100/s with burst 10 took %v, want about 900ms", elapsed)
		}
	})
}

func mustLimiter(t *testing.T, rate float64, burst int) *Limiter {
	t.Helper()
	limiter, err := NewLimiter(rate, burst)
	if err != nil {
		t.Fatal(err)
	}
	return limiter
}

func assertTryAcquire(t *testing.T, limiter *Limiter, n int, want bool) {
	t.Helper()
	if got := limiter.TryAcquire(n); got != want {
		t.Fatalf("TryAcquire(%d)=%v, want %v", n, got, want)
	}
}
