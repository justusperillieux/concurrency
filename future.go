package concurrency

import (
	"context"
	"sync"
)

// Future is the result of one asynchronous operation: a value of type T and an
// error. It is resolved exactly once; later resolutions are ignored. The zero
// Future is not usable; create futures with Go, Completed or Failed, or obtain
// them from Pool.Submit. A Future is safe for concurrent use and may be awaited
// by any number of goroutines.
type Future[T any] struct {
	done  chan struct{}
	once  sync.Once
	mu    sync.RWMutex
	value T
	err   error
}

func newFuture[T any]() *Future[T] {
	return &Future[T]{done: make(chan struct{})}
}

func (f *Future[T]) resolve(value T, err error) {
	f.once.Do(func() {
		f.mu.Lock()
		f.value = value
		f.err = err
		f.mu.Unlock()
		close(f.done)
	})
}

// Go runs fn(ctx) in a new goroutine and returns a future for its result. ctx
// controls the work; Go itself never blocks. If fn is nil, the returned future
// is already resolved with ErrNilFunction. Panics in fn are not recovered.
func Go[T any](ctx context.Context, fn func(context.Context) (T, error)) *Future[T] {
	future := newFuture[T]()
	if fn == nil {
		var zero T
		future.resolve(zero, ErrNilFunction)
		return future
	}

	go func() {
		value, err := fn(ctx)
		future.resolve(value, err)
	}()
	return future
}

// Completed returns a future that is already resolved with value and a nil
// error.
func Completed[T any](value T) *Future[T] {
	future := newFuture[T]()
	future.resolve(value, nil)
	return future
}

// Failed returns a future that is already resolved with the zero value and
// err.
func Failed[T any](err error) *Future[T] {
	future := newFuture[T]()
	var zero T
	future.resolve(zero, err)
	return future
}

// Await blocks until the future is resolved or ctx is done. It returns the
// published value and error. If ctx is done first, Await returns the zero
// value and context.Cause(ctx); a result that is already published wins over
// the cancellation. Canceling ctx affects only this wait, never the work.
func (f *Future[T]) Await(ctx context.Context) (T, error) {
	if value, err, ok := f.Try(); ok {
		return value, err
	}

	select {
	case <-f.done:
		value, err, _ := f.Try()
		return value, err
	case <-ctx.Done():
		if value, err, ok := f.Try(); ok {
			return value, err
		}
		var zero T
		return zero, context.Cause(ctx)
	}
}

// Try reports the result without blocking. ok is false, with a zero value
// and nil error, while the future is pending.
func (f *Future[T]) Try() (value T, err error, ok bool) {
	select {
	case <-f.done:
		f.mu.RLock()
		defer f.mu.RUnlock()
		return f.value, f.err, true
	default:
		var zero T
		return zero, nil, false
	}
}

// Done returns a channel that is closed once the future is resolved, for use
// in select statements.
func (f *Future[T]) Done() <-chan struct{} {
	return f.done
}
