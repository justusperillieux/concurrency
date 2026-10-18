package concurrency

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

type parallelOutcome struct {
	values []int
	err    error
}

// FR-CONC-12
func TestParallelMapPreservesInputOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		releases := map[int]chan struct{}{
			1: make(chan struct{}),
			2: make(chan struct{}),
			3: make(chan struct{}),
		}
		started := make(chan int, 3)
		completed := make(chan int, 3)
		result := make(chan parallelOutcome, 1)
		go func() {
			values, err := ParallelMap(context.Background(), []int{3, 1, 2}, 3, func(_ context.Context, value int) (int, error) {
				started <- value
				<-releases[value]
				completed <- value
				return value * 10, nil
			})
			result <- parallelOutcome{values: values, err: err}
		}()

		seen := make(map[int]bool, 3)
		for range 3 {
			seen[<-started] = true
		}
		for _, value := range []int{1, 2, 3} {
			if !seen[value] {
				t.Fatalf("callback for value %d did not start", value)
			}
			close(releases[value])
			if got := <-completed; got != value {
				t.Fatalf("completion=%d, want %d", got, value)
			}
		}

		got := <-result
		if got.err != nil || !slices.Equal(got.values, []int{30, 10, 20}) {
			t.Fatalf("ParallelMap()=(%v,%v), want ([30 10 20],nil)", got.values, got.err)
		}
	})
}

// FR-CONC-13
func TestParallelMapRejectsInvalidInputs(t *testing.T) {
	t.Run("nil function", func(t *testing.T) {
		values, err := ParallelMap[int, int](context.Background(), []int{1}, 0, nil)
		if values != nil || !errors.Is(err, ErrNilFunction) {
			t.Fatalf("ParallelMap()=(%v,%v), want (nil,%v)", values, err, ErrNilFunction)
		}
	})

	for _, limit := range []int{0, -1} {
		t.Run("invalid limit", func(t *testing.T) {
			values, err := ParallelMap(context.Background(), []int{1}, limit, func(context.Context, int) (int, error) {
				return 1, nil
			})
			if values != nil || !errors.Is(err, ErrInvalidLimit) {
				t.Fatalf("ParallelMap(limit=%d)=(%v,%v), want (nil,%v)", limit, values, err, ErrInvalidLimit)
			}
		})
	}
}

// FR-CONC-14
func TestParallelMapReturnsCallbackErrorWithoutPartialResults(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		want := errors.New("mapping failed")
		values, err := ParallelMap(context.Background(), []int{1}, 1, func(context.Context, int) (int, error) {
			return 0, want
		})
		if values != nil || !errors.Is(err, want) {
			t.Fatalf("ParallelMap()=(%v,%v), want (nil,%v)", values, err, want)
		}
	})
}

// FR-CONC-14
func TestParallelMapPreservesCallbackErrorsWhenAdmissionStops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		wants := []error{
			errors.New("input 0 failed"),
			errors.New("input 1 failed"),
		}
		releases := []chan struct{}{make(chan struct{}), make(chan struct{})}
		started := make(chan int, 2)
		completed := make(chan int, 2)
		var calls atomic.Int32
		result := make(chan parallelOutcome, 1)
		go func() {
			values, err := ParallelMap(context.Background(), []int{0, 1, 2}, 2, func(_ context.Context, value int) (int, error) {
				calls.Add(1)
				if value == 2 {
					return 2, nil
				}
				started <- value
				<-releases[value]
				completed <- value
				return 0, wants[value]
			})
			result <- parallelOutcome{values: values, err: err}
		}()

		seen := map[int]bool{<-started: true, <-started: true}
		if !seen[0] || !seen[1] {
			t.Fatalf("started callbacks=%v, want inputs 0 and 1", seen)
		}
		synctest.Wait()
		if got := calls.Load(); got != 2 {
			t.Fatalf("callback calls before failure=%d, want 2", got)
		}

		close(releases[1])
		if got := <-completed; got != 1 {
			t.Fatalf("first completed input=%d, want 1", got)
		}
		synctest.Wait()
		select {
		case got := <-result:
			t.Fatalf("ParallelMap returned before input 0 completed: (%v,%v)", got.values, got.err)
		default:
		}

		close(releases[0])
		if got := <-completed; got != 0 {
			t.Fatalf("second completed input=%d, want 0", got)
		}
		got := <-result
		if got.values != nil {
			t.Fatalf("ParallelMap values=%v, want nil", got.values)
		}
		if gotCalls := calls.Load(); gotCalls != 2 {
			t.Fatalf("callback calls=%d, want 2", gotCalls)
		}
		for _, want := range wants {
			if !errors.Is(got.err, want) {
				t.Fatalf("ParallelMap error=%v, want errors.Is(_, %v)", got.err, want)
			}
		}
		if got, want := got.err.Error(), "input 0 failed\ninput 1 failed"; got != want {
			t.Fatalf("ParallelMap error=%q, want %q", got, want)
		}
	})
}

// FR-CONC-15
func TestParallelMapStopsAdmissionOnParentCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, cancel := context.WithCancelCause(context.Background())
		started := make(chan struct{})
		var calls atomic.Int32
		result := make(chan parallelOutcome, 1)
		go func() {
			values, err := ParallelMap(parent, []int{1, 2}, 1, func(ctx context.Context, value int) (int, error) {
				calls.Add(1)
				if value == 1 {
					close(started)
					<-ctx.Done()
					return 0, context.Cause(ctx)
				}
				return value, nil
			})
			result <- parallelOutcome{values: values, err: err}
		}()

		<-started
		want := errors.New("stop mapping")
		cancel(want)
		got := <-result
		if got.values != nil || !errors.Is(got.err, want) {
			t.Fatalf("ParallelMap()=(%v,%v), want (nil,%v)", got.values, got.err, want)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("callback calls=%d, want 1", got)
		}
	})
}
