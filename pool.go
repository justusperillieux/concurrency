package concurrency

import (
	"context"
	"sync"
)

type Pool[I, O any] struct {
	ctx    context.Context
	worker func(context.Context, I) (O, error)

	slots   chan struct{}
	workers chan struct{}
	mu      sync.Mutex
	closed  bool
	wg      sync.WaitGroup
}

func NewPool[I, O any](ctx context.Context, workers, queue int, worker func(context.Context, I) (O, error)) (*Pool[I, O], error) {
	if workers <= 0 || queue < 0 {
		return nil, ErrInvalidLimit
	}
	if worker == nil {
		return nil, ErrNilFunction
	}

	return &Pool[I, O]{
		ctx:     ctx,
		worker:  worker,
		slots:   make(chan struct{}, workers+queue),
		workers: make(chan struct{}, workers),
	}, nil
}

func (p *Pool[I, O]) Submit(ctx context.Context, input I) (*Future[O], error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrClosed
	}
	p.mu.Unlock()

	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	select {
	case p.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		<-p.slots
		return nil, ErrClosed
	}
	if cause := context.Cause(ctx); cause != nil {
		p.mu.Unlock()
		<-p.slots
		return nil, cause
	}
	future := newFuture[O]()
	p.wg.Add(1)
	p.mu.Unlock()

	go func() {
		defer p.wg.Done()
		defer func() { <-p.slots }()

		p.workers <- struct{}{}
		defer func() { <-p.workers }()

		value, err := p.worker(p.ctx, input)
		future.resolve(value, err)
	}()
	return future, nil
}

func (p *Pool[I, O]) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
}

func (p *Pool[I, O]) Wait() {
	p.wg.Wait()
}
