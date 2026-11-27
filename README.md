# concurrency

`concurrency` is a small Go library of typed building blocks for
in-process concurrent work: futures, bounded structured groups, ordered
parallel maps, reusable worker pools, token-bucket rate limiting and adaptive
(AIMD) concurrency limits. It is for Go developers who fan work out across
goroutines and want precise, tested semantics for errors, cancellation and
shutdown without pulling in dependencies: it uses only the standard library.

## Features

- **Futures**: `Go` starts one function and returns a typed `*Future[T]`;
  `Await`, `Try` and `Done` read it. `Completed` and `Failed` build resolved
  futures.
- **Bounded groups**: `NewGroup` runs related callbacks with a concurrency
  limit, cancels peers on the first error and joins all errors in submission
  order.
- **Ordered parallel map**: `ParallelMap` applies one function to a slice
  with bounded concurrency and returns results in input order.
- **Worker pools**: `NewPool` processes typed inputs with one worker function,
  separate worker and queue limits, and a future per submission.
- **Rate limiting**: `NewLimiter` is a token bucket with FIFO waiters and a
  rate that can change at runtime (including paused and unlimited).
- **Adaptive concurrency**: `NewAdaptive` admits permits up to a limit that
  grows on `Success` and shrinks on `Overload`.
- Context-aware everywhere, safe for concurrent use, no goroutine leaks, no
  logging.

## Installation

```bash
go get github.com/justusperillieux/concurrency@latest
```

Requires Go 1.25 or newer. Import it as:

```go
import concurrency "github.com/justusperillieux/concurrency"
```

## Quick start

Square numbers with at most two concurrent calls; the results keep input
order. This is the package [`Example`](example_test.go), so it is compiled
and checked by `go test`:

```go
package main

import (
	"context"
	"fmt"

	concurrency "github.com/justusperillieux/concurrency"
)

func main() {
	ctx := context.Background()

	squares, err := concurrency.ParallelMap(ctx, []int{1, 2, 3, 4}, 2, func(ctx context.Context, n int) (int, error) {
		if err := context.Cause(ctx); err != nil {
			return 0, err
		}
		return n * n, nil
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(squares) // [1 4 9 16]
}
```

`example_test.go` has runnable examples for every entry point: `ExampleGo`,
`ExampleGroup`, `ExampleParallelMap`, `ExamplePool`, `ExampleLimiter` and
`ExampleAdaptive`.

### Choosing a primitive

| Use | When |
|---|---|
| `Future` (`Go`, `Completed`, `Failed`) | One asynchronous result you await later. |
| `Group` (`NewGroup`) | A scoped batch of related, possibly different callbacks that share cancellation and join their errors. |
| `ParallelMap` | The same function over a slice, results in input order. |
| `Pool` (`NewPool`) | An ongoing stream of typed inputs through one worker function, with back-pressure and a future per job. One job's error does not affect the others. |
| `Limiter` (`NewLimiter`) | Cap operations or bytes per second. |
| `Adaptive` (`NewAdaptive`) | The right concurrency is unknown and should follow downstream feedback such as HTTP 429 or 503. |

### Futures

The context passed to `Go` controls the work; the context passed to `Await`
controls only that wait. A result that is already published wins over a
canceled wait.

### Groups

`NewGroup(parent, limit)` creates a group; `Go` blocks while `limit`
callbacks run. The first callback error cancels the group context with that
error as the cause. `Wait` closes the group (blocked and later `Go` calls
return `ErrClosed`), waits for admitted callbacks and returns their errors
joined in submission order.

### Pools

`NewPool(ctx, workers, queue, worker)` admits at most `workers + queue`
submissions and runs at most `workers` at once. `Submit` waits for admission
under its own context and returns a `*Future[O]`; once admitted, the worker
runs with the pool context. Always `Close` the pool and then `Wait` for
admitted work:

```go
pool, err := concurrency.NewPool(ctx, 2, 2, func(_ context.Context, value int) (int, error) {
	return value * value, nil
})
if err != nil {
	return err
}
future, err := pool.Submit(ctx, 3)
// …
pool.Close()
pool.Wait()
value, err := future.Await(ctx) // 9, nil
```

### Token-bucket `Limiter`

`NewLimiter(rate, burst)` creates a full bucket holding at most `burst`
tokens that refills at `rate` tokens per second. `Wait(ctx, n)` blocks until
`n` tokens are available and takes them; `TryAcquire(n)` takes them only if
they are available now and no waiter is queued. `SetRate` changes the rate
and re-evaluates waiters immediately. A rate of `0` never refills; a rate of
`math.Inf(1)` grants every request, including requests above `burst`.

```go
// Limit uploads to 8 MiB/s with bursts of up to 1 MiB.
limiter, err := concurrency.NewLimiter(8<<20, 1<<20)
if err != nil {
	return err
}
if err := limiter.Wait(ctx, len(chunk)); err != nil {
	return err
}
```

### Adaptive concurrency

`NewAdaptive(AdaptiveOptions{Max: 32})` admits up to `Limit()` permits.
Release each permit with the outcome of its work: `Success` counts toward
raising the limit by one (up to `Max`), `Overload` scales it by
`DecreaseFactor` (down to `Min`), `Failure` leaves it unchanged. `Min`
defaults to 1, `Initial` to `Min`, `DecreaseFactor` to
`DefaultAdaptiveDecreaseFactor` (0.7); `IncreaseAfter` of 0 grows the limit
by about one per window of completed work.

```go
permit, err := adaptive.Acquire(ctx)
if err != nil {
	return err
}
err = call(ctx)
switch {
case errors.Is(err, errThrottled):
	permit.Release(concurrency.Overload)
case err != nil:
	permit.Release(concurrency.Failure)
default:
	permit.Release(concurrency.Success)
}
```

## Packages

| Package | Description |
|---|---|
| `github.com/justusperillieux/concurrency` (package `concurrency`) | Futures, groups, `ParallelMap`, pools, `Limiter`, `Adaptive` and the sentinel errors. |

This is the only public package. Anything under `internal/` (none today) is
not part of the API.

## Design and guarantees

- **Concurrency safety.** Futures, groups, pools, limiters and permits are
  safe for concurrent use. Every goroutine the library starts exits when its
  work returns; the limiter uses timers, not background goroutines.
- **Cancellation.** Blocking calls take a `context.Context` and return
  `context.Cause(ctx)` when it ends first. Waiting contexts never cancel shared
  work. When a grant (result, tokens, permit) and a cancellation race, a grant
  made first wins.
- **Bounds and fairness.** Groups run at most `limit` callbacks; pools admit
  at most `workers + queue` and run at most `workers`. `Limiter.Wait` and
  `Adaptive.Acquire` serve waiters in FIFO order, so large requests are not
  starved. An `Adaptive` limiter decreases at most once per overload burst.
- **Shutdown.** `Group.Wait` and `Pool.Close` unblock callers waiting for
  capacity with `ErrClosed`; admitted work runs to completion.
- **Errors.** Sentinels `ErrClosed`, `ErrInvalidLimit` and `ErrNilFunction`
  have stable texts; compare with `errors.Is`. `Group.Wait` and `ParallelMap`
  join every callback error in submission order with `errors.Join`;
  `ParallelMap` never returns partial results. Callback errors are not
  wrapped.
- **Scope.** In-process only. No retries or backoff (use a dedicated library
  such as `github.com/cenkalti/backoff/v5`), no distributed or persistent
  execution, no priorities, no dynamic pool resizing, no panic recovery.
- **Stability.** The module is `v0.x`: minor releases may break the API.
  Every break is listed in [CHANGELOG.md](CHANGELOG.md) and backed by an ADR.

## Documentation

- API reference: [pkg.go.dev/github.com/justusperillieux/concurrency](https://pkg.go.dev/github.com/justusperillieux/concurrency)
- Requirements and design decisions: [docs/README.md](docs/README.md)
  (PRD and ADRs)
- Changes: [CHANGELOG.md](CHANGELOG.md)

## Contributing

Contributions go through pull requests against `main`; nothing is pushed to
`main` directly, and a pull request merges only with green CI. Read
[AGENTS.md](AGENTS.md) first: it describes the docs-first workflow, the
public API rules and the checks to run locally.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) and
[NOTICE](NOTICE).
