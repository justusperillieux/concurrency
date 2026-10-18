package concurrency

import (
	"context"
	"sync"
)

// Pool processes typed inputs with one reusable worker function. At most
// workers worker calls run at once, and at most workers+queue submissions are
// admitted (running or queued) at a time. Each admitted submission gets its
// own Future; a worker error resolves only that future and does not affect
// other submissions. Call Close to reject new submissions and Wait to drain
// admitted work. A Pool is safe for concurrent use.
type Pool[I, O any] struct {
	ctx    context.Context
	worker func(context.Context, I) (O, error)

	slots    chan struct{}
	workers  chan struct{}
	closedCh chan struct{}
	mu       sync.Mutex
	closed   bool
	wg       sync.WaitGroup
}

// NewPool returns a pool that runs worker with ctx for every admitted input.
// It returns ErrInvalidLimit if workers is not positive or queue is negative
// and ErrNilFunction if worker is nil.
func NewPool[I, O any](ctx context.Context, workers, queue int, worker func(context.Context, I) (O, error)) (*Pool[I, O], error) {
	if workers <= 0 || queue < 0 {
		return nil, ErrInvalidLimit
	}
	if worker == nil {
		return nil, ErrNilFunction
	}

	return &Pool[I, O]{
		ctx:      ctx,
		worker:   worker,
		slots:    make(chan struct{}, workers+queue),
		workers:  make(chan struct{}, workers),
		closedCh: make(chan struct{}),
	}, nil
}

// Submit blocks until the pool admits input or ctx is done, and returns the
// future of the admitted job. ctx bounds only the admission wait: once
// admitted, the worker runs with the pool context and canceling ctx does not
// cancel it. Submit returns ErrClosed after Close (including while blocked
// waiting for capacity) and context.Cause(ctx) if ctx is done before
// admission. Panics in the worker are not recovered.
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
	case <-p.closedCh:
		return nil, ErrClosed
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

// Close rejects new submissions and unblocks submissions waiting for
// capacity with ErrClosed. Admitted work keeps running. Close is idempotent.
func (p *Pool[I, O]) Close() {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.closedCh)
	}
	p.mu.Unlock()
}

// Wait blocks until every admitted job has finished. Call Close first, or
// concurrent submissions may extend the wait.
func (p *Pool[I, O]) Wait() {
	p.wg.Wait()
}
