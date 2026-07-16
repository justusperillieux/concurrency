package concurrency

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const futureTestTimeout = time.Second

func TestFutureValueErrorAndIndependentWaitCancellation(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	workCtx, stopWork := context.WithCancel(context.Background())
	defer stopWork()
	future := Go(workCtx, func(context.Context) (int, error) {
		close(started)
		<-release
		return 42, nil
	})
	waitForFutureSignal(t, started)

	waitCtx, stopWait := context.WithCancel(context.Background())
	stopWait()
	if value, err := future.Await(waitCtx); value != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("Await=(%d,%v), want (0, context.Canceled)", value, err)
	}

	close(release)
	if value, err := future.Await(context.Background()); err != nil || value != 42 {
		t.Fatalf("Await=(%d,%v), want (42, nil)", value, err)
	}
	if value, err, ok := future.Try(); !ok || err != nil || value != 42 {
		t.Fatalf("Try=(%d,%v,%v), want (42, nil, true)", value, err, ok)
	}
}

func TestFutureConstructorsAndNilFunction(t *testing.T) {
	completed := Completed("ready")
	if value, err := completed.Await(context.Background()); err != nil || value != "ready" {
		t.Fatalf("Completed Await=(%q,%v), want (ready, nil)", value, err)
	}
	assertFutureDone(t, completed.Done())

	want := errors.New("failed")
	failed := Failed[int](want)
	if value, err := failed.Await(context.Background()); value != 0 || !errors.Is(err, want) {
		t.Fatalf("Failed Await=(%d,%v), want (0, %v)", value, err, want)
	}
	assertFutureDone(t, failed.Done())

	nilFunction := Go[int](context.Background(), nil)
	if value, err := nilFunction.Await(context.Background()); value != 0 || !errors.Is(err, ErrNilFunction) {
		t.Fatalf("nil function Await=(%d,%v), want (0, %v)", value, err, ErrNilFunction)
	}
	if value, err, ok := nilFunction.Try(); value != 0 || !errors.Is(err, ErrNilFunction) || !ok {
		t.Fatalf("nil function Try=(%d,%v,%v), want (0, %v, true)", value, err, ok, ErrNilFunction)
	}
}

func TestFutureTryAndDoneReportPublication(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	future := Go(context.Background(), func(context.Context) (string, error) {
		close(started)
		<-release
		return "published", nil
	})
	waitForFutureSignal(t, started)

	if value, err, ok := future.Try(); value != "" || err != nil || ok {
		t.Fatalf("pending Try=(%q,%v,%v), want (\"\", nil, false)", value, err, ok)
	}
	assertFuturePending(t, future.Done())

	close(release)
	assertFutureDone(t, future.Done())
	if value, err, ok := future.Try(); value != "published" || err != nil || !ok {
		t.Fatalf("resolved Try=(%q,%v,%v), want (published, nil, true)", value, err, ok)
	}
}

func TestFutureReadyResultWinsCanceledWaitContext(t *testing.T) {
	waitCause := errors.New("wait canceled")
	waitCtx, cancelWait := context.WithCancelCause(context.Background())
	cancelWait(waitCause)

	t.Run("value", func(t *testing.T) {
		value, err := Completed(17).Await(waitCtx)
		if value != 17 || err != nil {
			t.Fatalf("Await=(%d,%v), want (17, nil)", value, err)
		}
	})

	t.Run("error", func(t *testing.T) {
		resultErr := errors.New("result failed")
		value, err := Failed[int](resultErr).Await(waitCtx)
		if value != 0 || !errors.Is(err, resultErr) {
			t.Fatalf("Await=(%d,%v), want (0, %v)", value, err, resultErr)
		}
		if errors.Is(err, waitCause) {
			t.Fatalf("Await error=%v, must prefer published result over %v", err, waitCause)
		}
	})
}

func TestFutureAwaitReturnsCancellationCause(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	future := Go(context.Background(), func(context.Context) (int, error) {
		close(started)
		<-release
		return 1, nil
	})
	waitForFutureSignal(t, started)

	waitCause := errors.New("stop this wait")
	waitCtx, cancelWait := context.WithCancelCause(context.Background())
	cancelWait(waitCause)
	if value, err := future.Await(waitCtx); value != 0 || !errors.Is(err, waitCause) {
		t.Fatalf("Await=(%d,%v), want (0, %v)", value, err, waitCause)
	}

	close(release)
	assertFutureDone(t, future.Done())
}

func TestFutureGoRunsAsynchronouslyOnceWithWorkContext(t *testing.T) {
	type contextKey struct{}
	workCtx := context.WithValue(context.Background(), contextKey{}, "work context")
	release := make(chan struct{})
	started := make(chan context.Context, 1)
	var releaseOnce sync.Once
	releaseWork := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseWork()

	var calls atomic.Int32
	returned := make(chan *Future[string], 1)
	go func() {
		returned <- Go(workCtx, func(ctx context.Context) (string, error) {
			calls.Add(1)
			started <- ctx
			<-release
			return "done", nil
		})
	}()

	var receivedCtx context.Context
	select {
	case receivedCtx = <-started:
	case <-time.After(futureTestTimeout):
		t.Fatal("Go function did not start")
	}
	if receivedCtx != workCtx {
		t.Fatalf("function context=%v, want original work context", receivedCtx)
	}

	var future *Future[string]
	select {
	case future = <-returned:
	case <-time.After(futureTestTimeout):
		t.Fatal("Go did not return while its function was blocked")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("function calls=%d, want 1", got)
	}

	releaseWork()
	if value, err := future.Await(context.Background()); value != "done" || err != nil {
		t.Fatalf("Await=(%q,%v), want (done, nil)", value, err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("function calls after completion=%d, want 1", got)
	}
}

func TestFutureResolvePublishesOnlyFirstResult(t *testing.T) {
	future := newFuture[int]()
	if future.Done() == nil {
		t.Fatal("newFuture Done() returned nil")
	}
	assertFuturePending(t, future.Done())

	future.resolve(11, nil)

	var attempts sync.WaitGroup
	for i := 0; i < 32; i++ {
		attempts.Add(1)
		go func(value int) {
			defer attempts.Done()
			future.resolve(value, errors.New("late result"))
		}(i)
	}
	attempts.Wait()

	if value, err := future.Await(context.Background()); value != 11 || err != nil {
		t.Fatalf("Await=(%d,%v), want first result (11, nil)", value, err)
	}
}

func TestFutureSentinelErrorsHaveStableText(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "closed", err: ErrClosed, want: "concurrency: closed"},
		{name: "invalid limit", err: ErrInvalidLimit, want: "concurrency: invalid limit"},
		{name: "nil function", err: ErrNilFunction, want: "concurrency: nil function"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.err == nil {
				t.Fatal("sentinel is nil")
			}
			if got := test.err.Error(); got != test.want {
				t.Fatalf("Error()=%q, want %q", got, test.want)
			}
		})
	}
	if ErrClosed == ErrInvalidLimit || ErrClosed == ErrNilFunction || ErrInvalidLimit == ErrNilFunction {
		t.Fatal("sentinel errors must be distinct")
	}
}

func waitForFutureSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(futureTestTimeout):
		t.Fatal("timed out waiting for future signal")
	}
}

func assertFuturePending(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
		t.Fatal("future is done while work is pending")
	default:
	}
}

func assertFutureDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(futureTestTimeout):
		t.Fatal("timed out waiting for future completion")
	}
}
