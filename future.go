package concurrency

import (
	"context"
	"sync"
)

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

func Completed[T any](value T) *Future[T] {
	future := newFuture[T]()
	future.resolve(value, nil)
	return future
}

func Failed[T any](err error) *Future[T] {
	future := newFuture[T]()
	var zero T
	future.resolve(zero, err)
	return future
}

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

func (f *Future[T]) Try() (T, error, bool) {
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

func (f *Future[T]) Done() <-chan struct{} {
	return f.done
}
