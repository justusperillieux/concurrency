package concurrency

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const poolTestTimeout = time.Second

type poolSubmitOutcome[T any] struct {
	future *Future[T]
	err    error
}

func TestPoolRejectsInvalidConfiguration(t *testing.T) {
	worker := func(context.Context, int) (int, error) { return 0, nil }
	for _, test := range []struct {
		name    string
		workers int
		queue   int
	}{
		{name: "zero workers", workers: 0, queue: 0},
		{name: "negative workers", workers: -1, queue: 0},
		{name: "negative queue", workers: 1, queue: -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			pool, err := NewPool(context.Background(), test.workers, test.queue, worker)
			if pool != nil || !errors.Is(err, ErrInvalidLimit) {
				t.Fatalf("NewPool(%d,%d)=(%v,%v), want (nil,%v)", test.workers, test.queue, pool, err, ErrInvalidLimit)
			}
		})
	}

	pool, err := NewPool[int, int](context.Background(), 1, 0, nil)
	if pool != nil || !errors.Is(err, ErrNilFunction) {
		t.Fatalf("NewPool(nil worker)=(%v,%v), want (nil,%v)", pool, err, ErrNilFunction)
	}
}

func TestPoolBoundsAdmissionAndRejectsAfterClose(t *testing.T) {
	release := make(chan struct{})
	started := make(chan int, 3)
	var calls atomic.Int32
	var running atomic.Int32
	var peak atomic.Int32
	pool, err := NewPool(context.Background(), 1, 1, func(_ context.Context, value int) (int, error) {
		calls.Add(1)
		current := running.Add(1)
		defer running.Add(-1)
		for old := peak.Load(); current > old && !peak.CompareAndSwap(old, current); old = peak.Load() {
		}
		started <- value
		<-release
		return value * 2, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	first, err := pool.Submit(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if got := waitForPoolValue(t, started); got != 2 {
		t.Fatalf("first worker value=%d, want 2", got)
	}
	second, err := pool.Submit(context.Background(), 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok := second.Try(); ok {
		t.Fatal("queued future resolved before a worker was available")
	}
	select {
	case value := <-started:
		t.Fatalf("queued value %d started above the worker limit", value)
	default:
	}

	wantCancellation := errors.New("stop waiting for admission")
	submitBase, cancelSubmit := context.WithCancelCause(context.Background())
	submitCtx := newPoolObservedContext(submitBase)
	thirdResult := make(chan poolSubmitOutcome[int], 1)
	go func() {
		future, err := pool.Submit(submitCtx, 4)
		thirdResult <- poolSubmitOutcome[int]{future: future, err: err}
	}()
	waitForPoolSignal(t, submitCtx.observed)
	select {
	case got := <-thirdResult:
		t.Fatalf("third Submit returned before admission was released: (%v,%v)", got.future, got.err)
	default:
	}
	cancelSubmit(wantCancellation)
	third := waitForPoolOutcome(t, thirdResult)
	if third.future != nil || !errors.Is(third.err, wantCancellation) {
		t.Fatalf("third Submit=(%v,%v), want (nil,%v)", third.future, third.err, wantCancellation)
	}

	pool.Close()
	pool.Close()
	waitStarted := make(chan struct{})
	waitDone := make(chan struct{})
	go func() {
		close(waitStarted)
		pool.Wait()
		close(waitDone)
	}()
	waitForPoolSignal(t, waitStarted)
	select {
	case <-waitDone:
		t.Fatal("Wait returned before admitted work drained")
	default:
	}
	close(release)
	waitForPoolSignal(t, waitDone)
	pool.Wait()

	assertPoolFutureValue(t, first, 4)
	assertPoolFutureValue(t, second, 6)
	if got := waitForPoolValue(t, started); got != 3 {
		t.Fatalf("second worker value=%d, want 3", got)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("worker calls=%d, want 2", got)
	}
	if got := peak.Load(); got != 1 {
		t.Fatalf("peak workers=%d, want 1", got)
	}

	closedCtx, cancelClosed := context.WithCancel(context.Background())
	cancelClosed()
	closedFuture, err := pool.Submit(closedCtx, 5)
	if closedFuture != nil || !errors.Is(err, ErrClosed) {
		t.Fatalf("closed Submit=(%v,%v), want (nil,%v)", closedFuture, err, ErrClosed)
	}
}

func TestPoolAdmittedWorkUsesParentContext(t *testing.T) {
	type contextKey struct{}
	key := contextKey{}
	parentBase := context.WithValue(context.Background(), key, "pool parent")
	parent, cancelParent := context.WithCancelCause(parentBase)
	defer cancelParent(nil)
	seenContext := make(chan context.Context, 1)
	pool, err := NewPool(parent, 1, 0, func(ctx context.Context, _ int) (int, error) {
		seenContext <- ctx
		<-ctx.Done()
		return 0, context.Cause(ctx)
	})
	if err != nil {
		t.Fatal(err)
	}

	submissionBase := context.WithValue(context.Background(), key, "submission")
	submissionCtx, cancelSubmission := context.WithCancelCause(submissionBase)
	future, err := pool.Submit(submissionCtx, 1)
	if err != nil {
		t.Fatal(err)
	}
	workerCtx := waitForPoolContext(t, seenContext)
	if got := workerCtx.Value(key); got != "pool parent" {
		t.Fatalf("worker context value=%v, want pool parent", got)
	}

	submissionCause := errors.New("submission canceled after admission")
	cancelSubmission(submissionCause)
	if _, _, ok := future.Try(); ok {
		t.Fatal("submission cancellation resolved admitted work")
	}
	parentCause := errors.New("pool parent canceled")
	cancelParent(parentCause)
	pool.Close()
	pool.Wait()
	if value, err := future.Await(context.Background()); value != 0 || !errors.Is(err, parentCause) || errors.Is(err, submissionCause) {
		t.Fatalf("Await=(%d,%v), want (0,%v)", value, err, parentCause)
	}
}

func TestPoolCloseRejectsSubmissionWaitingForCapacity(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	var calls atomic.Int32
	pool, err := NewPool(context.Background(), 1, 0, func(_ context.Context, value int) (int, error) {
		calls.Add(1)
		if value == 1 {
			close(started)
			<-release
		}
		return value, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := pool.Submit(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	waitForPoolSignal(t, started)

	submitCtx := newPoolObservedContext(context.Background())
	result := make(chan poolSubmitOutcome[int], 1)
	go func() {
		future, err := pool.Submit(submitCtx, 2)
		result <- poolSubmitOutcome[int]{future: future, err: err}
	}()
	waitForPoolSignal(t, submitCtx.observed)
	pool.Close()
	pool.Close()
	got := waitForPoolOutcome(t, result)
	if got.future != nil || !errors.Is(got.err, ErrClosed) {
		t.Fatalf("racing Submit=(%v,%v), want (nil,%v)", got.future, got.err, ErrClosed)
	}
	if _, _, ok := first.Try(); ok {
		t.Fatal("Close resolved admitted work before capacity was released")
	}
	close(release)
	pool.Wait()
	pool.Wait()
	assertPoolFutureValue(t, first, 1)
	if calls.Load() != 1 {
		t.Fatalf("worker calls=%d, want 1", calls.Load())
	}
}

func TestPoolFutureCarriesWorkerError(t *testing.T) {
	want := errors.New("worker failed")
	pool, err := NewPool(context.Background(), 1, 0, func(context.Context, int) (string, error) {
		return "", want
	})
	if err != nil {
		t.Fatal(err)
	}
	future, err := pool.Submit(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	pool.Wait()
	if value, err := future.Await(context.Background()); value != "" || !errors.Is(err, want) {
		t.Fatalf("Await=(%q,%v), want (\"\",%v)", value, err, want)
	}
}

type poolObservedContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func newPoolObservedContext(ctx context.Context) *poolObservedContext {
	return &poolObservedContext{Context: ctx, observed: make(chan struct{})}
}

func (ctx *poolObservedContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.observed) })
	return ctx.Context.Done()
}

func waitForPoolSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(poolTestTimeout):
		t.Fatal("timed out waiting for pool signal")
	}
}

func waitForPoolValue(t *testing.T, values <-chan int) int {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(poolTestTimeout):
		t.Fatal("timed out waiting for pool value")
		return 0
	}
}

func waitForPoolContext(t *testing.T, contexts <-chan context.Context) context.Context {
	t.Helper()
	select {
	case ctx := <-contexts:
		return ctx
	case <-time.After(poolTestTimeout):
		t.Fatal("timed out waiting for worker context")
		return nil
	}
}

func waitForPoolOutcome[T any](t *testing.T, outcomes <-chan poolSubmitOutcome[T]) poolSubmitOutcome[T] {
	t.Helper()
	select {
	case outcome := <-outcomes:
		return outcome
	case <-time.After(poolTestTimeout):
		t.Fatal("timed out waiting for pool submission")
		return poolSubmitOutcome[T]{}
	}
}

func assertPoolFutureValue[T comparable](t *testing.T, future *Future[T], want T) {
	t.Helper()
	value, err := future.Await(context.Background())
	if err != nil || value != want {
		t.Fatalf("Await=(%v,%v), want (%v,nil)", value, err, want)
	}
}
