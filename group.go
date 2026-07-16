package concurrency

import (
	"context"
	"errors"
	"sync"
)

type Group struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	limit  chan struct{}

	mu     sync.Mutex
	closed bool
	next   int
	errs   map[int]error
	wg     sync.WaitGroup
}

func NewGroup(parent context.Context, limit int) (*Group, error) {
	if limit <= 0 {
		return nil, ErrInvalidLimit
	}

	ctx, cancel := context.WithCancelCause(parent)
	return &Group{
		ctx:    ctx,
		cancel: cancel,
		limit:  make(chan struct{}, limit),
		errs:   make(map[int]error),
	}, nil
}

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

func (g *Group) Wait() error {
	g.mu.Lock()
	g.closed = true
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
