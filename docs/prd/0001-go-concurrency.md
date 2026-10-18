# PRD 0001: go-concurrency

| Field | Value |
|---|---|
| Status | Accepted |
| Implementation | Complete |
| Owners | Justus Perillieux |
| Related ADRs | ADR-0002, ADR-0003, ADR-0004, ADR-0005, ADR-0006, ADR-0007, ADR-0008, ADR-0009, ADR-0010 |
| Last updated | 2026-10-18 |

Recorded retrospectively for the first public release; describes the library as implemented.

## 1. Problem and context

Go's primitives (goroutines, channels, `sync`, `context`) are powerful but
low-level. Every service that fans work out ends up re-implementing the same
pieces: a typed handle for one asynchronous result, a bounded batch of tasks
that cancels its peers on the first error, an order-preserving parallel map,
a reusable worker pool with a bounded queue, a rate limiter, and a
concurrency limit that backs off when a downstream is overloaded. Hand-rolled
versions commonly leak goroutines, lose errors, block forever on shutdown or
ignore cancellation.

`go-concurrency` provides these building blocks as one small, typed,
standard-library-only package with precise, tested semantics for errors,
cancellation and shutdown. It is used by higher-level libraries (executors,
messaging fan-out, caches, lifecycle and reconciliation engines, multipart
object storage uploads) and by applications directly.

## 2. Users and use cases

- As a library author, I want to start one operation asynchronously and await
  its typed result later, so that I can overlap I/O without managing channels.
- As a service developer, I want to run a bounded batch of related tasks that
  share cancellation and report all their errors, so that one failure stops
  the batch cleanly.
- As a data-processing developer, I want to map a function over a slice with
  bounded concurrency and get results in input order, so that parallelism does
  not change my output.
- As a backend developer, I want a long-lived worker pool with separate worker
  and queue limits and a future per job, so that bursts are absorbed without
  unbounded memory and each caller gets its own result.
- As a client developer, I want to cap request or byte rates with a token
  bucket whose rate I can change at runtime, so that I respect quotas.
- As a client developer, I want a concurrency limit that grows on success and
  shrinks on overload signals (HTTP 429/503, timeouts), so that I find the
  downstream's capacity without configuring it.

## 3. Goals and non-goals

### Goals
- G1 Typed, generic primitives for futures, groups, ordered parallel maps,
  worker pools, token-bucket rate limiting and adaptive (AIMD) concurrency.
- G2 Precise cancellation semantics based on `context.Context` and
  `context.Cause`.
- G3 Deterministic, inspectable errors: stable sentinels, joined callback
  errors in a defined order.
- G4 No goroutine leaks: every goroutine started by the package exits when its
  work returns; shutdown never deadlocks.
- G5 Standard library only.

### Non-goals
- NG1 Retries and backoff. They are policy, not a concurrency primitive; use a
  dedicated library (ADR-0007).
- NG2 Distributed execution, persistence, or distributed/shared rate limits.
  The package is in-process only.
- NG3 Priorities and dynamic pool resizing. They complicate fairness and
  admission guarantees without a current user.
- NG4 Panic recovery. Panics in callbacks propagate; callers that need
  recovery wrap their callbacks.
- NG5 Logging or metrics. The package reports only through return values.

## 4. Functional requirements

| ID | Requirement | Priority |
|---|---|---|
| FR-CONC-01 | `Go(ctx, fn)` runs `fn(ctx)` exactly once in a new goroutine with the given context and returns a `*Future[T]` without waiting for `fn`. | MUST |
| FR-CONC-02 | `Completed(v)` and `Failed(err)` return already-resolved futures; `Go` with a nil function returns a future resolved with `ErrNilFunction`. | MUST |
| FR-CONC-03 | A future publishes exactly one result; later resolutions are ignored. | MUST |
| FR-CONC-04 | `Future.Try` reports the result without blocking (`ok=false` while pending) and `Future.Done` returns a channel closed on resolution. | MUST |
| FR-CONC-05 | `Future.Await(ctx)` returns the published result, or `context.Cause(ctx)` if `ctx` is done first; canceling the wait never cancels the work, and an already-published result wins over a canceled wait. | MUST |
| FR-CONC-06 | `NewGroup` rejects a non-positive limit with `ErrInvalidLimit`; `Group.Go` rejects a nil function with `ErrNilFunction`. | MUST |
| FR-CONC-07 | A group runs at most `limit` callbacks at once; `Group.Go` blocks while the group is at capacity; after `Wait`, `Go` returns `ErrClosed`. | MUST |
| FR-CONC-08 | `Group.Wait` closes the group, unblocks capacity-blocked `Go` calls with `ErrClosed` without running their callbacks, does not cancel admitted callbacks, and returns once they have all finished. | MUST |
| FR-CONC-09 | The first callback error cancels the group context with that error as the cause. | MUST |
| FR-CONC-10 | `Group.Wait` returns all callback errors joined in submission order. | MUST |
| FR-CONC-11 | `Group.Go` returns the parent context's cancellation cause, without running the callback, once the parent is done. | MUST |
| FR-CONC-12 | `ParallelMap` applies a function to every input with at most `limit` concurrent calls and returns results in input order. | MUST |
| FR-CONC-13 | `ParallelMap` rejects a nil function with `ErrNilFunction` and a non-positive limit with `ErrInvalidLimit`. | MUST |
| FR-CONC-14 | On a callback error `ParallelMap` stops admitting inputs, waits for admitted calls, and returns a nil slice with the callback errors joined in input order. | MUST |
| FR-CONC-15 | When the parent context is done, `ParallelMap` stops admitting inputs and returns a nil slice and the cancellation cause. | MUST |
| FR-CONC-16 | `NewPool` rejects non-positive workers or a negative queue with `ErrInvalidLimit` and a nil worker with `ErrNilFunction`. | MUST |
| FR-CONC-17 | A pool admits at most `workers+queue` submissions and runs at most `workers` worker calls at once; `Submit` waits for admission until its own context is done; after `Close`, `Submit` returns `ErrClosed`; `Wait` blocks until admitted work has drained. | MUST |
| FR-CONC-18 | `Pool.Close` unblocks submissions waiting for capacity with `ErrClosed`; admitted work keeps running. `Close` and `Wait` are idempotent. | MUST |
| FR-CONC-19 | Admitted pool work runs with the pool context; canceling the submission context after admission does not affect it. | MUST |
| FR-CONC-20 | Each admitted submission's future carries that worker call's value and error. | MUST |
| FR-CONC-21 | `NewLimiter` rejects a negative or NaN rate and a non-positive burst; `SetRate` rejects a negative or NaN rate without changing it; `Wait` rejects a negative `n` and, at a finite rate, `n > burst`, all with `ErrInvalidLimit`. | MUST |
| FR-CONC-22 | A limiter starts full, refills at `rate` tokens per second up to `burst`, and `TryAcquire(n)` takes tokens only if available now, never blocking. | MUST |
| FR-CONC-23 | `Limiter.Wait(ctx, n)` blocks exactly until `n` tokens are available and takes them; concurrent waiters share the rate. | MUST |
| FR-CONC-24 | Blocked `Wait` calls are served in FIFO order and `TryAcquire` never takes tokens ahead of a queued waiter. | MUST |
| FR-CONC-25 | A `Wait` whose context is done first returns `context.Cause(ctx)`, takes no tokens and releases its queue position; tokens granted before the cancellation was observed win. | MUST |
| FR-CONC-26 | `SetRate` changes the rate at runtime and re-evaluates waiters immediately; a rate of 0 never refills; a rate of `+Inf` admits every request, including requests larger than `burst`. | MUST |
| FR-CONC-27 | `NewAdaptive` applies defaults (`Min` 1, `Initial` `Min`, `DecreaseFactor` `DefaultAdaptiveDecreaseFactor`) and rejects options violating `1 <= Min <= Initial <= Max`, `IncreaseAfter >= 0` or `0 < DecreaseFactor < 1` with `ErrInvalidLimit`. | MUST |
| FR-CONC-28 | `Success` releases raise the limit by one after `IncreaseAfter` successes (or the current limit when 0), up to `Max`. | MUST |
| FR-CONC-29 | `Overload` scales the limit by `DecreaseFactor`, down to `Min`, and resets success progress; `Failure` and unknown outcomes leave the limit unchanged. | MUST |
| FR-CONC-30 | Overloads reported by permits acquired before the latest decrease do not decrease the limit again. | MUST |
| FR-CONC-31 | `Adaptive.Acquire` blocks while `InFlight >= Limit` or earlier callers wait, and admits waiters in FIFO order; `InFlight` may exceed a lowered limit until permits are released. | MUST |
| FR-CONC-32 | An `Acquire` whose context is done first returns `context.Cause(ctx)` and leaves the queue; a permit granted before the cancellation was observed wins. | MUST |
| FR-CONC-33 | Releasing a permit more than once, or releasing the zero `Permit`, has no effect. | MUST |
| FR-CONC-34 | The sentinel errors `ErrClosed`, `ErrInvalidLimit` and `ErrNilFunction` are distinct and have stable texts. | MUST |

## 5. Non-functional requirements

| ID | Requirement | Measure |
|---|---|---|
| NFR-CONC-01 | Standard library only. | `go.mod` has no `require` directives. |
| NFR-CONC-02 | Futures, groups, pools, limiters and permits are safe for concurrent use. | `go test -race -count=3 ./...` passes, including `TestAdaptiveConcurrentUseRespectsBounds`. |
| NFR-CONC-03 | No goroutine leaks: package goroutines exit once their work returns; the limiter uses timers, not background goroutines. | Tests run in `testing/synctest` bubbles, which fail if bubble goroutines stay blocked. |
| NFR-CONC-04 | Deterministic tests without real sleeps. | Time and coordination tests use `testing/synctest`; `-count=3` is stable. |
| NFR-CONC-05 | The primitives compose: futures from a pool can be mapped and grouped. | `TestPublicPrimitivesCompose`. |
| NFR-CONC-06 | Supported toolchains: Go 1.25 and the two latest Go releases. | CI matrix `1.25.x`, `oldstable`, `stable`. |
| NFR-CONC-07 | No logging and no global state beyond the sentinel error values. | Non-test code does not import `log` or call `fmt.Print*`, and declares no package-level variables other than the sentinels. |

## 6. API sketch

```go
package concurrency

var ErrClosed, ErrInvalidLimit, ErrNilFunction error

type Future[T any] struct{ /* … */ }
func Go[T any](ctx context.Context, fn func(context.Context) (T, error)) *Future[T]
func Completed[T any](value T) *Future[T]
func Failed[T any](err error) *Future[T]
func (f *Future[T]) Await(ctx context.Context) (T, error)
func (f *Future[T]) Try() (value T, err error, ok bool)
func (f *Future[T]) Done() <-chan struct{}

type Group struct{ /* … */ }
func NewGroup(parent context.Context, limit int) (*Group, error)
func (g *Group) Go(fn func(context.Context) error) error
func (g *Group) Wait() error

func ParallelMap[T, R any](ctx context.Context, values []T, limit int, fn func(context.Context, T) (R, error)) ([]R, error)

type Pool[I, O any] struct{ /* … */ }
func NewPool[I, O any](ctx context.Context, workers, queue int, worker func(context.Context, I) (O, error)) (*Pool[I, O], error)
func (p *Pool[I, O]) Submit(ctx context.Context, input I) (*Future[O], error)
func (p *Pool[I, O]) Close()
func (p *Pool[I, O]) Wait()

type Limiter struct{ /* … */ }
func NewLimiter(rate float64, burst int) (*Limiter, error)
func (l *Limiter) Rate() float64
func (l *Limiter) Burst() int
func (l *Limiter) SetRate(rate float64) error
func (l *Limiter) TryAcquire(n int) bool
func (l *Limiter) Wait(ctx context.Context, n int) error

const DefaultAdaptiveDecreaseFactor = 0.7
type Outcome int // Success, Overload, Failure
type AdaptiveOptions struct{ Min, Max, Initial, IncreaseAfter int; DecreaseFactor float64 }
type Adaptive struct{ /* … */ }
type Permit struct{ /* … */ }
func NewAdaptive(options AdaptiveOptions) (*Adaptive, error)
func (a *Adaptive) Limit() int
func (a *Adaptive) InFlight() int
func (a *Adaptive) Acquire(ctx context.Context) (Permit, error)
func (p Permit) Release(outcome Outcome)
```

## 7. Acceptance criteria

| Criterion | Requirement(s) | Test(s) |
|---|---|---|
| `Go` is asynchronous, runs once, passes the work context | FR-CONC-01 | `TestFutureGoRunsAsynchronouslyOnceWithWorkContext`, `ExampleGo` |
| Resolved constructors and nil function | FR-CONC-02 | `TestFutureConstructorsAndNilFunction` |
| First result wins | FR-CONC-03 | `TestFutureResolvePublishesOnlyFirstResult` |
| `Try` and `Done` report publication | FR-CONC-04 | `TestFutureTryAndDoneReportPublication` |
| Wait cancellation is independent; cause returned; ready result wins | FR-CONC-05 | `TestFutureValueErrorAndIndependentWaitCancellation`, `TestFutureAwaitReturnsCancellationCause`, `TestFutureReadyResultWinsCanceledWaitContext` |
| Group input validation | FR-CONC-06 | `TestGroupRejectsInvalidLimit`, `TestGroupRejectsNilFunction` |
| Group bound and closure | FR-CONC-07 | `TestGroupBoundAndClosure`, `ExampleGroup` |
| `Wait` unblocks capacity-blocked `Go` | FR-CONC-08 | `TestGroupWaitUnblocksCapacityBlockedGo` |
| First error cancels peers with cause | FR-CONC-09 | `TestGroupCallbackErrorCancelsPeersWithCause` |
| Errors joined in submission order | FR-CONC-10 | `TestGroupJoinsErrorsBySubmissionIndex` |
| Parent cancellation rejects `Go` | FR-CONC-11 | `TestGroupGoReturnsParentCancellationCause` |
| Ordered results | FR-CONC-12 | `TestParallelMapPreservesInputOrder`, `ExampleParallelMap`, `Example` |
| Map input validation | FR-CONC-13 | `TestParallelMapRejectsInvalidInputs` |
| No partial results; joined callback errors | FR-CONC-14 | `TestParallelMapReturnsCallbackErrorWithoutPartialResults`, `TestParallelMapPreservesCallbackErrorsWhenAdmissionStops` |
| Parent cancellation stops admission | FR-CONC-15 | `TestParallelMapStopsAdmissionOnParentCancellation` |
| Pool configuration validation | FR-CONC-16 | `TestPoolRejectsInvalidConfiguration` |
| Admission and execution bounds, close, drain | FR-CONC-17 | `TestPoolBoundsAdmissionAndRejectsAfterClose`, `ExamplePool` |
| `Close` unblocks waiting submissions | FR-CONC-18 | `TestPoolCloseRejectsSubmissionWaitingForCapacity` |
| Worker uses the pool context | FR-CONC-19 | `TestPoolAdmittedWorkUsesParentContext` |
| Future carries the worker error | FR-CONC-20 | `TestPoolFutureCarriesWorkerError` |
| Limiter validation | FR-CONC-21 | `TestLimiterRejectsInvalidConfiguration` |
| Refill up to burst; non-blocking `TryAcquire` | FR-CONC-22 | `TestLimiterTryAcquireRefillsUpToBurst`, `ExampleLimiter` |
| `Wait` timing; shared rate | FR-CONC-23 | `TestLimiterWaitBlocksUntilRefilled`, `TestLimiterConcurrentWaitersShareRate` |
| FIFO waiters | FR-CONC-24 | `TestLimiterServesWaitersInOrder` |
| Cancellation releases queue position | FR-CONC-25 | `TestLimiterWaitCancellationReleasesQueuePosition` |
| Runtime rate changes, zero and infinite rates | FR-CONC-26 | `TestLimiterZeroRateWaitsForSetRate`, `TestLimiterSetRateReschedulesWaiters`, `TestLimiterInfiniteRateIsUnlimited` |
| Adaptive defaults and validation | FR-CONC-27 | `TestAdaptiveRejectsInvalidOptions`, `TestAdaptiveDefaults` |
| Additive increase | FR-CONC-28 | `TestAdaptiveAdditiveIncrease`, `ExampleAdaptive` |
| Multiplicative decrease; failure neutral | FR-CONC-29 | `TestAdaptiveMultiplicativeDecreaseAndFailure`, `ExampleAdaptive` |
| One decrease per overload burst | FR-CONC-30 | `TestAdaptiveConcurrentOverloadsDecreaseOnce` |
| Blocking at the limit, FIFO admission | FR-CONC-31 | `TestAdaptiveBlocksAtLimitAndAdmitsInOrder`, `TestAdaptiveInFlightCanExceedLoweredLimit` |
| Acquire cancellation | FR-CONC-32 | `TestAdaptiveAcquireCancellation` |
| Idempotent release | FR-CONC-33 | `TestAdaptivePermitReleaseIsIdempotent` |
| Stable sentinel errors | FR-CONC-34 | `TestFutureSentinelErrorsHaveStableText` |
| Race-free concurrent use | NFR-CONC-02 | `TestAdaptiveConcurrentUseRespectsBounds`, whole suite under `-race` |
| Composition | NFR-CONC-05 | `TestPublicPrimitivesCompose` |

## 8. Source references

None.

## 9. Open questions

- Q1 Should `Group` and `Pool` offer opt-in panic recovery that converts a
  panic into an error? Currently a non-goal (NG4).
- Q2 Should `Limiter` expose a reservation API (reserve now, act later)?
  No current consumer needs it.
