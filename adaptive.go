package concurrency

import (
	"container/list"
	"context"
	"math"
	"sync"
	"sync/atomic"
)

// DefaultAdaptiveDecreaseFactor is the AdaptiveOptions.DecreaseFactor used
// when the option is zero.
const DefaultAdaptiveDecreaseFactor = 0.7

// Outcome reports how the work guarded by a Permit went.
type Outcome int

const (
	// Success counts toward an additive increase of the limit.
	Success Outcome = iota
	// Overload multiplicatively decreases the limit. Use it for signals that
	// the downstream is saturated, such as HTTP 429 or 503 responses and
	// timeouts.
	Overload
	// Failure leaves the limit unchanged. Use it for errors that say nothing
	// about downstream capacity.
	Failure
)

// AdaptiveOptions configures NewAdaptive.
//
// Max is required. Min defaults to 1 and Initial defaults to Min.
// IncreaseAfter is the number of Success releases that raise the limit by one;
// 0 uses the current limit, which grows the limit by about one per window of
// completed work. DecreaseFactor in (0, 1) scales the limit down on Overload
// and defaults to DefaultAdaptiveDecreaseFactor.
type AdaptiveOptions struct {
	Min            int
	Max            int
	Initial        int
	IncreaseAfter  int
	DecreaseFactor float64
}

// Adaptive is a concurrency limiter whose limit adapts with additive increase
// and multiplicative decrease (AIMD). Successes slowly raise the limit up to
// Max; overloads quickly lower it down to Min. Blocked Acquire calls are
// admitted in FIFO order whenever a permit is released or the limit grows. An
// Adaptive is safe for concurrent use.
//
// After the limit decreases, overloads reported by permits acquired before
// that decrease do not decrease it again, so one burst of concurrent overloads
// shrinks the limit once rather than once per in-flight request.
type Adaptive struct {
	min            int
	max            int
	increaseAfter  int
	decreaseFactor float64

	mu         sync.Mutex
	limit      int
	inFlight   int
	successes  int
	generation uint64
	waiters    list.List
}

type adaptiveWaiter struct {
	ready  chan struct{}
	permit Permit
}

// Permit is one unit of admitted concurrency. Release it exactly once when the
// guarded work finishes; later calls are no-ops. The zero Permit is a no-op.
type Permit struct {
	state *permitState
}

type permitState struct {
	adaptive   *Adaptive
	generation uint64
	released   atomic.Bool
}

// NewAdaptive returns an AIMD concurrency limiter. It returns ErrInvalidLimit
// unless 1 <= Min <= Initial <= Max after defaults, IncreaseAfter >= 0, and
// DecreaseFactor is in (0, 1).
func NewAdaptive(options AdaptiveOptions) (*Adaptive, error) {
	if options.Min == 0 {
		options.Min = 1
	}
	if options.Initial == 0 {
		options.Initial = options.Min
	}
	if options.DecreaseFactor == 0 {
		options.DecreaseFactor = DefaultAdaptiveDecreaseFactor
	}
	if options.Min < 1 || options.Max < options.Min ||
		options.Initial < options.Min || options.Initial > options.Max ||
		options.IncreaseAfter < 0 ||
		math.IsNaN(options.DecreaseFactor) || options.DecreaseFactor <= 0 || options.DecreaseFactor >= 1 {
		return nil, ErrInvalidLimit
	}
	return &Adaptive{
		min:            options.Min,
		max:            options.Max,
		increaseAfter:  options.IncreaseAfter,
		decreaseFactor: options.DecreaseFactor,
		limit:          options.Initial,
	}, nil
}

// Limit returns the current concurrency limit.
func (a *Adaptive) Limit() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.limit
}

// InFlight returns the number of acquired permits that are not yet released.
// It can exceed Limit briefly after a decrease.
func (a *Adaptive) InFlight() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.inFlight
}

// Acquire blocks until fewer than Limit permits are in flight and no earlier
// Acquire is waiting, then returns a permit. If ctx is done first, it returns
// context.Cause(ctx); a permit granted before the cancellation was observed
// wins and is returned with a nil error.
func (a *Adaptive) Acquire(ctx context.Context) (Permit, error) {
	if cause := context.Cause(ctx); cause != nil {
		return Permit{}, cause
	}

	a.mu.Lock()
	if a.waiters.Len() == 0 && a.inFlight < a.limit {
		permit := a.admitLocked()
		a.mu.Unlock()
		return permit, nil
	}
	waiter := &adaptiveWaiter{ready: make(chan struct{})}
	element := a.waiters.PushBack(waiter)
	a.mu.Unlock()

	select {
	case <-waiter.ready:
		return waiter.permit, nil
	case <-ctx.Done():
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	select {
	case <-waiter.ready:
		return waiter.permit, nil
	default:
	}
	a.waiters.Remove(element)
	return Permit{}, context.Cause(ctx)
}

// Release returns the permit and adjusts the limit for outcome. Unknown
// outcomes are treated as Failure.
func (p Permit) Release(outcome Outcome) {
	if p.state == nil || !p.state.released.CompareAndSwap(false, true) {
		return
	}
	a := p.state.adaptive
	a.mu.Lock()
	defer a.mu.Unlock()
	a.inFlight--
	switch outcome {
	case Success:
		a.successes++
		threshold := a.increaseAfter
		if threshold == 0 {
			threshold = a.limit
		}
		if a.successes >= threshold {
			a.successes = 0
			a.limit = min(a.limit+1, a.max)
		}
	case Overload:
		if p.state.generation == a.generation {
			a.generation++
			a.successes = 0
			a.limit = max(int(float64(a.limit)*a.decreaseFactor), a.min)
		}
	}
	a.grantLocked()
}

func (a *Adaptive) admitLocked() Permit {
	a.inFlight++
	return Permit{state: &permitState{adaptive: a, generation: a.generation}}
}

func (a *Adaptive) grantLocked() {
	for element := a.waiters.Front(); element != nil && a.inFlight < a.limit; element = a.waiters.Front() {
		waiter := element.Value.(*adaptiveWaiter)
		a.waiters.Remove(element)
		waiter.permit = a.admitLocked()
		close(waiter.ready)
	}
}
