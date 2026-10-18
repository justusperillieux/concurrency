package concurrency

import (
	"context"
	"errors"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

// FR-CONC-27
func TestAdaptiveRejectsInvalidOptions(t *testing.T) {
	for _, test := range []struct {
		name    string
		options AdaptiveOptions
	}{
		{name: "missing max", options: AdaptiveOptions{}},
		{name: "negative min", options: AdaptiveOptions{Min: -1, Max: 2}},
		{name: "max below min", options: AdaptiveOptions{Min: 3, Max: 2}},
		{name: "initial below min", options: AdaptiveOptions{Min: 2, Max: 4, Initial: 1}},
		{name: "initial above max", options: AdaptiveOptions{Max: 4, Initial: 5}},
		{name: "negative increase", options: AdaptiveOptions{Max: 4, IncreaseAfter: -1}},
		{name: "negative factor", options: AdaptiveOptions{Max: 4, DecreaseFactor: -0.5}},
		{name: "factor of one", options: AdaptiveOptions{Max: 4, DecreaseFactor: 1}},
		{name: "NaN factor", options: AdaptiveOptions{Max: 4, DecreaseFactor: math.NaN()}},
	} {
		t.Run(test.name, func(t *testing.T) {
			adaptive, err := NewAdaptive(test.options)
			if adaptive != nil || !errors.Is(err, ErrInvalidLimit) {
				t.Fatalf("NewAdaptive(%+v)=(%v,%v), want (nil,%v)", test.options, adaptive, err, ErrInvalidLimit)
			}
		})
	}
}

// FR-CONC-27
func TestAdaptiveDefaults(t *testing.T) {
	adaptive := mustAdaptive(t, AdaptiveOptions{Max: 8})
	if adaptive.Limit() != 1 || adaptive.InFlight() != 0 {
		t.Fatalf("(Limit,InFlight)=(%d,%d), want (1,0)", adaptive.Limit(), adaptive.InFlight())
	}
	if adaptive.decreaseFactor != DefaultAdaptiveDecreaseFactor {
		t.Fatalf("decrease factor=%v, want %v", adaptive.decreaseFactor, DefaultAdaptiveDecreaseFactor)
	}

	adaptive = mustAdaptive(t, AdaptiveOptions{Min: 2, Max: 8})
	if adaptive.Limit() != 2 {
		t.Fatalf("Limit=%d, want Min 2", adaptive.Limit())
	}
}

// FR-CONC-28
func TestAdaptiveAdditiveIncrease(t *testing.T) {
	adaptive := mustAdaptive(t, AdaptiveOptions{Max: 3, Initial: 2, IncreaseAfter: 2})
	var limits []int
	for range 6 {
		acquire(t, adaptive).Release(Success)
		limits = append(limits, adaptive.Limit())
	}
	if want := []int{2, 3, 3, 3, 3, 3}; !slices.Equal(limits, want) {
		t.Fatalf("limits=%v, want %v", limits, want)
	}

	window := mustAdaptive(t, AdaptiveOptions{Max: 4, Initial: 2})
	limits = limits[:0]
	for range 6 {
		acquire(t, window).Release(Success)
		limits = append(limits, window.Limit())
	}
	if want := []int{2, 3, 3, 3, 4, 4}; !slices.Equal(limits, want) {
		t.Fatalf("window limits=%v, want %v", limits, want)
	}
}

// FR-CONC-29
func TestAdaptiveMultiplicativeDecreaseAndFailure(t *testing.T) {
	adaptive := mustAdaptive(t, AdaptiveOptions{Min: 2, Max: 20, Initial: 10, IncreaseAfter: 2, DecreaseFactor: 0.5})

	acquire(t, adaptive).Release(Success)
	acquire(t, adaptive).Release(Failure)
	acquire(t, adaptive).Release(Outcome(99))
	if got := adaptive.Limit(); got != 10 {
		t.Fatalf("Limit after failures=%d, want 10", got)
	}

	var limits []int
	for range 4 {
		acquire(t, adaptive).Release(Overload)
		limits = append(limits, adaptive.Limit())
	}
	if want := []int{5, 2, 2, 2}; !slices.Equal(limits, want) {
		t.Fatalf("limits=%v, want %v", limits, want)
	}

	acquire(t, adaptive).Release(Success)
	if got := adaptive.Limit(); got != 2 {
		t.Fatalf("Limit after one success=%d, want 2 because overload resets progress", got)
	}
}

// FR-CONC-30
func TestAdaptiveConcurrentOverloadsDecreaseOnce(t *testing.T) {
	adaptive := mustAdaptive(t, AdaptiveOptions{Max: 8, Initial: 8, DecreaseFactor: 0.5})
	permits := make([]Permit, 8)
	for index := range permits {
		permits[index] = acquire(t, adaptive)
	}
	for _, permit := range permits {
		permit.Release(Overload)
	}
	if got := adaptive.Limit(); got != 4 {
		t.Fatalf("Limit after one overload burst=%d, want 4", got)
	}
	acquire(t, adaptive).Release(Overload)
	if got := adaptive.Limit(); got != 2 {
		t.Fatalf("Limit after a later overload=%d, want 2", got)
	}
}

// FR-CONC-31
func TestAdaptiveBlocksAtLimitAndAdmitsInOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		adaptive := mustAdaptive(t, AdaptiveOptions{Max: 4, Initial: 2, IncreaseAfter: 1})
		first := acquire(t, adaptive)
		second := acquire(t, adaptive)

		var mu sync.Mutex
		var order []int
		permits := make(chan Permit, 3)
		for index := range 3 {
			go func() {
				permit, err := adaptive.Acquire(context.Background())
				if err != nil {
					t.Errorf("Acquire=%v, want nil", err)
					return
				}
				mu.Lock()
				order = append(order, index)
				mu.Unlock()
				permits <- permit
			}()
			synctest.Wait()
		}
		if got := len(permits); got != 0 {
			t.Fatalf("%d waiters admitted above the limit", got)
		}

		first.Release(Failure)
		synctest.Wait()
		if got := len(permits); got != 1 || !slices.Equal(order, []int{0}) {
			t.Fatalf("admitted waiters after release=%v, want [0]", order)
		}

		second.Release(Success)
		synctest.Wait()
		if got := len(permits); got != 3 || adaptive.Limit() != 3 || adaptive.InFlight() != 3 {
			t.Fatalf("(admitted,Limit,InFlight)=(%d,%d,%d), want (3,3,3)", got, adaptive.Limit(), adaptive.InFlight())
		}
		if slices.Sort(order[1:]); !slices.Equal(order, []int{0, 1, 2}) {
			t.Fatalf("admitted waiters=%v, want [0 1 2]", order)
		}
		for range 3 {
			(<-permits).Release(Failure)
		}
		if got := adaptive.InFlight(); got != 0 {
			t.Fatalf("InFlight=%d, want 0", got)
		}
	})
}

// FR-CONC-31
func TestAdaptiveInFlightCanExceedLoweredLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		adaptive := mustAdaptive(t, AdaptiveOptions{Max: 4, Initial: 4, DecreaseFactor: 0.5})
		permits := []Permit{acquire(t, adaptive), acquire(t, adaptive), acquire(t, adaptive), acquire(t, adaptive)}
		permits[0].Release(Overload)
		if adaptive.Limit() != 2 || adaptive.InFlight() != 3 {
			t.Fatalf("(Limit,InFlight)=(%d,%d), want (2,3)", adaptive.Limit(), adaptive.InFlight())
		}

		admitted := make(chan Permit, 1)
		go func() {
			permit, err := adaptive.Acquire(context.Background())
			if err != nil {
				t.Errorf("Acquire=%v, want nil", err)
			}
			admitted <- permit
		}()
		synctest.Wait()
		permits[1].Release(Failure)
		synctest.Wait()
		if len(admitted) != 0 {
			t.Fatal("waiter admitted while InFlight was at the lowered limit")
		}
		permits[2].Release(Failure)
		permit := <-admitted
		permit.Release(Failure)
		permits[3].Release(Failure)
		if got := adaptive.InFlight(); got != 0 {
			t.Fatalf("InFlight=%d, want 0", got)
		}
	})
}

// FR-CONC-32
func TestAdaptiveAcquireCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		adaptive := mustAdaptive(t, AdaptiveOptions{Max: 1})
		held := acquire(t, adaptive)

		cause := errors.New("stop waiting")
		ctx, cancel := context.WithCancelCause(context.Background())
		canceled := make(chan error, 1)
		go func() {
			_, err := adaptive.Acquire(ctx)
			canceled <- err
		}()
		synctest.Wait()

		admitted := make(chan Permit, 1)
		go func() {
			permit, err := adaptive.Acquire(context.Background())
			if err != nil {
				t.Errorf("Acquire=%v, want nil", err)
			}
			admitted <- permit
		}()
		synctest.Wait()

		cancel(cause)
		if err := <-canceled; !errors.Is(err, cause) {
			t.Fatalf("canceled Acquire=%v, want %v", err, cause)
		}
		held.Release(Failure)
		permit := <-admitted
		if got := adaptive.InFlight(); got != 1 {
			t.Fatalf("InFlight=%d, want 1", got)
		}
		permit.Release(Failure)

		if _, err := adaptive.Acquire(ctx); !errors.Is(err, cause) {
			t.Fatalf("Acquire with canceled context=%v, want %v", err, cause)
		}
		if got := adaptive.InFlight(); got != 0 {
			t.Fatalf("InFlight=%d, want 0", got)
		}
	})
}

// FR-CONC-33
func TestAdaptivePermitReleaseIsIdempotent(t *testing.T) {
	adaptive := mustAdaptive(t, AdaptiveOptions{Max: 2, IncreaseAfter: 1})
	permit := acquire(t, adaptive)
	permit.Release(Success)
	permit.Release(Success)
	copied := permit
	copied.Release(Overload)
	if adaptive.InFlight() != 0 || adaptive.Limit() != 2 {
		t.Fatalf("(InFlight,Limit)=(%d,%d), want (0,2)", adaptive.InFlight(), adaptive.Limit())
	}
	Permit{}.Release(Success)
}

// NFR-CONC-02
func TestAdaptiveConcurrentUseRespectsBounds(t *testing.T) {
	adaptive := mustAdaptive(t, AdaptiveOptions{Min: 2, Max: 6, IncreaseAfter: 1, DecreaseFactor: 0.5})
	var running atomic.Int32
	var peak atomic.Int32
	var wg sync.WaitGroup
	for worker := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for iteration := range 50 {
				permit, err := adaptive.Acquire(context.Background())
				if err != nil {
					t.Errorf("Acquire=%v, want nil", err)
					return
				}
				current := running.Add(1)
				for old := peak.Load(); current > old && !peak.CompareAndSwap(old, current); old = peak.Load() {
				}
				running.Add(-1)
				permit.Release(Outcome((worker + iteration) % 3))
			}
		}()
	}
	wg.Wait()
	if got := peak.Load(); got > 6 {
		t.Fatalf("peak concurrency=%d, want at most Max 6", got)
	}
	if limit := adaptive.Limit(); limit < 2 || limit > 6 || adaptive.InFlight() != 0 {
		t.Fatalf("(Limit,InFlight)=(%d,%d), want limit in [2,6] and nothing in flight", limit, adaptive.InFlight())
	}
}

func mustAdaptive(t *testing.T, options AdaptiveOptions) *Adaptive {
	t.Helper()
	adaptive, err := NewAdaptive(options)
	if err != nil {
		t.Fatal(err)
	}
	return adaptive
}

func acquire(t *testing.T, adaptive *Adaptive) Permit {
	t.Helper()
	permit, err := adaptive.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return permit
}
