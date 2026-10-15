package concurrency

import (
	"container/list"
	"context"
	"math"
	"sync"
	"time"
)

// tokenEpsilon absorbs floating-point error when a refill should have produced
// exactly the requested number of tokens.
const tokenEpsilon = 1e-9

// Limiter is a token bucket. It holds at most burst tokens and refills at rate
// tokens per second. It starts full. Waiters are served in FIFO order, so a
// large request is not starved by smaller ones. A Limiter is safe for
// concurrent use.
//
// A rate of 0 never refills: only the initial burst is available until SetRate
// raises the rate. A rate of math.Inf(1) is unlimited: every request succeeds
// immediately, including requests larger than burst.
type Limiter struct {
	mu      sync.Mutex
	rate    float64
	burst   int
	tokens  float64
	last    time.Time
	waiters list.List

	timer       *time.Timer
	timerID     uint64
	timerWaiter *limiterWaiter
	timerRate   float64
}

type limiterWaiter struct {
	n     int
	ready chan struct{}
}

// NewLimiter returns a full token bucket with the given refill rate in tokens
// per second and capacity burst. It returns ErrInvalidLimit if rate is
// negative or NaN, or if burst is not positive.
func NewLimiter(rate float64, burst int) (*Limiter, error) {
	if !validRate(rate) || burst <= 0 {
		return nil, ErrInvalidLimit
	}
	return &Limiter{
		rate:   rate,
		burst:  burst,
		tokens: float64(burst),
		last:   time.Now(),
	}, nil
}

func validRate(rate float64) bool {
	return !math.IsNaN(rate) && rate >= 0
}

// Rate returns the current refill rate in tokens per second.
func (l *Limiter) Rate() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rate
}

// Burst returns the bucket capacity.
func (l *Limiter) Burst() int {
	return l.burst
}

// SetRate changes the refill rate. Tokens accrued at the previous rate are
// kept, leaving an unlimited rate starts from a full bucket, and blocked
// waiters are re-evaluated immediately. It returns
// ErrInvalidLimit, leaving the rate unchanged, if rate is negative or NaN.
func (l *Limiter) SetRate(rate float64) error {
	if !validRate(rate) {
		return ErrInvalidLimit
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.advanceLocked(time.Now())
	l.rate = rate
	l.grantLocked()
	return nil
}

// TryAcquire takes n tokens if they are available now and no Wait call is
// queued ahead of it. It never blocks. It reports false for negative n and,
// unless the rate is unlimited, for n greater than burst.
func (l *Limiter) TryAcquire(n int) bool {
	if n < 0 {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if math.IsInf(l.rate, 1) || n == 0 {
		return true
	}
	if n > l.burst || l.waiters.Len() > 0 {
		return false
	}
	l.advanceLocked(time.Now())
	return l.takeLocked(n)
}

// Wait blocks until n tokens are available and takes them, or until ctx is
// done. It returns ErrInvalidLimit for negative n and, unless the rate is
// unlimited, for n greater than burst. If ctx is done first, Wait returns
// context.Cause(ctx) and takes no tokens; tokens granted before the
// cancellation was observed win, and Wait returns nil.
func (l *Limiter) Wait(ctx context.Context, n int) error {
	if n < 0 {
		return ErrInvalidLimit
	}
	l.mu.Lock()
	unlimited := math.IsInf(l.rate, 1)
	l.mu.Unlock()
	if !unlimited && n > l.burst {
		return ErrInvalidLimit
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if n == 0 {
		return nil
	}

	l.mu.Lock()
	if math.IsInf(l.rate, 1) {
		l.mu.Unlock()
		return nil
	}
	if n > l.burst {
		l.mu.Unlock()
		return ErrInvalidLimit
	}
	l.advanceLocked(time.Now())
	if l.waiters.Len() == 0 && l.takeLocked(n) {
		l.mu.Unlock()
		return nil
	}
	waiter := &limiterWaiter{n: n, ready: make(chan struct{})}
	element := l.waiters.PushBack(waiter)
	l.scheduleLocked()
	l.mu.Unlock()

	select {
	case <-waiter.ready:
		return nil
	case <-ctx.Done():
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	select {
	case <-waiter.ready:
		return nil
	default:
	}
	wasHead := l.waiters.Front() == element
	l.waiters.Remove(element)
	if wasHead {
		l.advanceLocked(time.Now())
		l.grantLocked()
	}
	return context.Cause(ctx)
}

func (l *Limiter) takeLocked(n int) bool {
	if l.tokens+tokenEpsilon < float64(n) {
		return false
	}
	l.tokens = max(l.tokens-float64(n), 0)
	return true
}

func (l *Limiter) advanceLocked(now time.Time) {
	elapsed := now.Sub(l.last)
	if elapsed > 0 {
		l.last = now
	}
	switch {
	case math.IsInf(l.rate, 1):
		l.tokens = float64(l.burst)
	case l.rate > 0 && elapsed > 0:
		l.tokens = min(l.tokens+elapsed.Seconds()*l.rate, float64(l.burst))
	}
}

func (l *Limiter) grantLocked() {
	unlimited := math.IsInf(l.rate, 1)
	for element := l.waiters.Front(); element != nil; element = l.waiters.Front() {
		waiter := element.Value.(*limiterWaiter)
		if !unlimited && !l.takeLocked(waiter.n) {
			break
		}
		l.waiters.Remove(element)
		close(waiter.ready)
	}
	l.scheduleLocked()
}

func (l *Limiter) scheduleLocked() {
	var head *limiterWaiter
	if element := l.waiters.Front(); element != nil {
		head = element.Value.(*limiterWaiter)
	}
	if l.timer != nil && head == l.timerWaiter && l.rate == l.timerRate {
		return
	}
	if l.timer != nil {
		l.timer.Stop()
		l.timer = nil
		l.timerWaiter = nil
	}
	if head == nil || l.rate <= 0 || math.IsInf(l.rate, 1) {
		return
	}

	l.timerID++
	id := l.timerID
	l.timerWaiter = head
	l.timerRate = l.rate
	l.timer = time.AfterFunc(durationForTokens(float64(head.n)-l.tokens, l.rate), func() {
		l.onTimer(id)
	})
}

func (l *Limiter) onTimer(id uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if id == l.timerID {
		l.timer = nil
		l.timerWaiter = nil
	}
	l.advanceLocked(time.Now())
	l.grantLocked()
}

func durationForTokens(tokens, rate float64) time.Duration {
	nanos := math.Ceil(tokens / rate * float64(time.Second))
	switch {
	case nanos < 1:
		return 1
	case nanos >= math.MaxInt64:
		return math.MaxInt64
	default:
		return time.Duration(nanos)
	}
}
