package concurrency

import (
	"context"
	"errors"
	"sync"
)

// Group runs a scoped batch of related callbacks with bounded concurrency.
// Callbacks share a context derived from the group's parent; the first
// callback error cancels it with that error as the cause. Wait closes the
// group to new work, drains admitted callbacks and joins their errors in
// submission order. A Group is single-use and safe for concurrent use.
type Group struct {
	ctx      context.Context
	cancel   context.CancelCauseFunc
	limit    chan struct{}
	closedCh chan struct{}

	mu     sync.Mutex
	closed bool
	next   int
	errs   map[int]error
	wg     sync.WaitGroup
}

// NewGroup returns a group whose callbacks run under a context derived from
// parent, with at most limit callbacks running at once. It returns
// ErrInvalidLimit if limit is not positive.
func NewGroup(parent context.Context, limit int) (*Group, error) {
	if limit <= 0 {
		return nil, ErrInvalidLimit
	}

	ctx, cancel := context.WithCancelCause(parent)
	return &Group{
		ctx:      ctx,
		cancel:   cancel,
		limit:    make(chan struct{}, limit),
		closedCh: make(chan struct{}),
		errs:     make(map[int]error),
	}, nil
}

// Go admits fn and runs it in a new goroutine with the group context. It
// blocks while limit callbacks are running. It returns ErrNilFunction for a
// nil fn, ErrClosed once Wait has been called (including while Go is blocked
// waiting for capacity), and the context cause if the group context is done
// before fn is admitted; in those cases fn does not run. Panics in fn are not
// recovered.
func (g *Group) Go(fn func(context.Context) error) error {
	if fn == nil {
		return ErrNilFunction
	}

	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return ErrClosed
	}
	g.mu.Unlock()
	if cause := context.Cause(g.ctx); cause != nil {
		return cause
	}

	select {
	case g.limit <- struct{}{}:
	case <-g.closedCh:
		return ErrClosed
	case <-g.ctx.Done():
		g.mu.Lock()
		closed := g.closed
		g.mu.Unlock()
		if closed {
			return ErrClosed
		}
		return context.Cause(g.ctx)
	}

	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		<-g.limit
		return ErrClosed
	}
	if cause := context.Cause(g.ctx); cause != nil {
		g.mu.Unlock()
		<-g.limit
		return cause
	}
	index := g.next
	g.next++
	g.wg.Add(1)
	g.mu.Unlock()

	go func() {
		defer g.wg.Done()
		defer func() { <-g.limit }()

		if err := fn(g.ctx); err != nil {
			g.mu.Lock()
			g.errs[index] = err
			g.mu.Unlock()
			g.cancel(err)
		}
	}()
	return nil
}

// Wait closes the group to new callbacks, blocks until every admitted callback
// has returned and then cancels the group context. It returns the callback
// errors joined with errors.Join in submission order, or nil if every
// callback succeeded. Wait may be called more than once.
func (g *Group) Wait() error {
	g.mu.Lock()
	if !g.closed {
		g.closed = true
		close(g.closedCh)
	}
	g.mu.Unlock()

	g.wg.Wait()
	g.cancel(nil)

	g.mu.Lock()
	errs := make([]error, 0, len(g.errs))
	for index := 0; index < g.next; index++ {
		if err := g.errs[index]; err != nil {
			errs = append(errs, err)
		}
	}
	g.mu.Unlock()
	return errors.Join(errs...)
}
