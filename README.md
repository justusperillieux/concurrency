# go-concurrency

`go-concurrency` provides small, typed building blocks for asynchronous work,
bounded structured concurrency, ordered parallel mapping, reusable worker
pools, retries with backoff, token-bucket rate limiting, and adaptive
concurrency limits. The module is private, depends only on the Go standard library, and is
independent of product-specific resource types.

Module path: `github.com/justusschock/go-concurrency`

## Requirements and installation

Go 1.25 or newer is required.

Because the module is private, configure repository access through your normal
GitHub authentication before installing it. Keep credentials out of commands,
source files, and Go environment settings.

```bash
go env -w GOPRIVATE='github.com/justusschock/*'
go get github.com/justusschock/go-concurrency@v0.3.0
```

## API and examples

Choose an abstraction based on the shape and lifetime of the work:

- Use a `Future` for one asynchronous result that you may await later.
- Use a `Group` for a scoped batch of related tasks that may run different
  functions, share cancellation, and join their errors.
- Use `ParallelMap` to apply one function to a collection concurrently while
  preserving input order.
- Use a `Pool` for an ongoing stream of typed inputs processed by one reusable
  worker function, with explicit worker and queue limits and a future for each
  admitted job.
- Use `Retry` or `RetryValue` to repeat one fallible operation with
  exponential backoff until it succeeds, fails permanently, or runs out of
  attempts.
- Use a `Limiter` to cap the rate of operations or bytes per second with a
  token bucket.
- Use an `Adaptive` limiter when the right concurrency is unknown and should
  follow downstream feedback, such as HTTP 429 or 503 responses.

### Typed futures

Use a future when one asynchronous operation produces a value or error that
will be consumed later.

`Go` starts one function asynchronously and returns a `*Future[T]`.
`Completed` and `Failed` create already-resolved futures. A future exposes:

- `Await(ctx)` to wait for its value or error;
- `Try()` to inspect it without blocking; and
- `Done()` for integration with `select`.

The context passed to `Go` controls the work. The context passed to `Await`
controls only that wait:

```go
package main

import (
	"context"
	"errors"
	"fmt"

	concurrency "github.com/justusschock/go-concurrency"
)

func main() {
	workCtx, cancelWork := context.WithCancel(context.Background())
	defer cancelWork()

	release := make(chan struct{})
	future := concurrency.Go(workCtx, func(ctx context.Context) (int, error) {
		select {
		case <-release:
			return 42, nil
		case <-ctx.Done():
			return 0, context.Cause(ctx)
		}
	})

	waitCtx, cancelWait := context.WithCancel(context.Background())
	cancelWait()
	_, waitErr := future.Await(waitCtx)
	if !errors.Is(waitErr, context.Canceled) {
		panic("expected the wait to be canceled")
	}

	close(release) // The canceled wait did not cancel the work.
	value, err := future.Await(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println(value)
}
```

### Bounded structured groups

Use a group for fork-join work: submit one scoped batch of related callbacks,
cancel peers when a callback fails, and wait for the whole batch. Each `Go`
call may submit a different function.

`NewGroup(parent, limit)` creates a group with a positive concurrency limit.
`Go` admits callbacks up to that limit and blocks while capacity is full. The
first callback error cancels the shared group context; `Wait` drains admitted
callbacks, closes the group to new work, and joins callback errors in submission
order.

```go
package main

import (
	"context"
	"fmt"

	concurrency "github.com/justusschock/go-concurrency"
)

func main() {
	group, err := concurrency.NewGroup(context.Background(), 2)
	if err != nil {
		panic(err)
	}

	inputs := []int{1, 2, 3}
	results := make([]int, len(inputs))
	for index, value := range inputs {
		index, value := index, value
		if err := group.Go(func(ctx context.Context) error {
			if cause := context.Cause(ctx); cause != nil {
				return cause
			}
			results[index] = value * value
			return nil
		}); err != nil {
			_ = group.Wait()
			panic(err)
		}
	}

	if err := group.Wait(); err != nil {
		panic(err)
	}
	fmt.Println(results) // [1 4 9]
}
```

### Ordered `ParallelMap`

Use `ParallelMap` when every input needs the same transformation and the output
must remain in input order even though work runs concurrently.

`ParallelMap` applies a typed function with bounded concurrency. Successful
results always follow input order, regardless of completion order. On any
admission or callback error it waits for admitted work and returns a nil result
slice with the error.

```go
package main

import (
	"context"
	"fmt"
	"time"

	concurrency "github.com/justusschock/go-concurrency"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	values, err := concurrency.ParallelMap(
		ctx,
		[]int{1, 2, 3},
		2,
		func(ctx context.Context, value int) (int, error) {
			if cause := context.Cause(ctx); cause != nil {
				return 0, cause
			}
			return value * value, nil
		},
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(values) // [1 4 9]
}
```

### Reusable worker pool

Use a pool when jobs arrive over time and all typed inputs should pass through
the same reusable worker function. A pool combines bounded admission
(`workers + queue`) with a separate execution limit (`workers`) and returns an
independent future for each admitted job. Unlike a group, one worker error does
not cancel the other jobs or become part of one aggregate error.

`NewPool(ctx, workers, queue, worker)` reuses one typed worker function across
submissions. `Submit` waits for admission using its own context and returns a
future once admitted. The worker receives the pool context, so canceling a
submission context after admission does not cancel that work.

Always call `Close` to reject new submissions and `Wait` to drain admitted work:

```go
package main

import (
	"context"
	"fmt"

	concurrency "github.com/justusschock/go-concurrency"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := concurrency.NewPool(ctx, 2, 2, func(ctx context.Context, value int) (int, error) {
		select {
		case <-ctx.Done():
			return 0, context.Cause(ctx)
		default:
			return value * value, nil
		}
	})
	if err != nil {
		panic(err)
	}
	defer func() {
		pool.Close()
		pool.Wait()
	}()

	futures := make([]*concurrency.Future[int], 0, 4)
	for _, value := range []int{1, 2, 3, 4} {
		future, err := pool.Submit(ctx, value)
		if err != nil {
			panic(err)
		}
		futures = append(futures, future)
	}

	results := make([]int, 0, len(futures))
	for _, future := range futures {
		value, err := future.Await(ctx)
		if err != nil {
			panic(err)
		}
		results = append(results, value)
	}
	fmt.Println(results) // [1 4 9 16]
}
```

### Retries with backoff

`Retry(ctx, policy, fn)` calls `fn(ctx, attempt)` with attempts numbered from 1.
`RetryValue` does the same for functions that return a value. Retrying stops
when `fn` succeeds, the attempts are exhausted, `fn` returns an error marked
with `Permanent`, `RetryPolicy.Retryable` reports false, or `ctx` is done.

The delay before attempt `k+1` is `min(BaseDelay*Multiplier^(k-1), MaxDelay)`.
`Jitter` in `[0, 1]` subtracts a random fraction of up to `Jitter` of that
delay; `Jitter: 1` is full jitter. Zero fields use defaults:

| Field | Zero value means |
| --- | --- |
| `MaxAttempts` | `DefaultRetryMaxAttempts` (3) |
| `BaseDelay` | `DefaultRetryBaseDelay` (100ms) |
| `MaxDelay` | `DefaultRetryMaxDelay` (10s), or `BaseDelay` if larger |
| `Multiplier` | `DefaultRetryMultiplier` (2) |
| `Jitter` | no jitter |
| `Retryable` | retry every error not marked `Permanent` |

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	concurrency "github.com/justusschock/go-concurrency"
)

var errNotFound = errors.New("not found")

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	body, err := concurrency.RetryValue(ctx, concurrency.RetryPolicy{
		MaxAttempts: 5,
		BaseDelay:   200 * time.Millisecond,
		MaxDelay:    5 * time.Second,
		Jitter:      1,
	}, func(ctx context.Context, attempt int) (string, error) {
		status, body := fetch(ctx, attempt)
		switch {
		case status == http.StatusNotFound:
			return "", concurrency.Permanent(errNotFound)
		case status != http.StatusOK:
			return "", fmt.Errorf("status %d", status)
		}
		return body, nil
	})
	if errors.Is(err, errNotFound) {
		fmt.Println("missing")
		return
	}
	if err != nil {
		panic(err)
	}
	fmt.Println(body)
}

func fetch(_ context.Context, attempt int) (int, string) {
	if attempt < 3 {
		return http.StatusServiceUnavailable, ""
	}
	return http.StatusOK, "payload"
}
```

When attempts run out or an error is not retryable, `Retry` returns the last
error. A `Permanent` error returned directly is unwrapped to the error passed
to `Permanent`. If `ctx` is done while waiting between attempts, the result
joins `context.Cause(ctx)` with the last attempt error, so `errors.Is` matches
both.

### Token-bucket `Limiter`

`NewLimiter(rate, burst)` creates a full bucket that holds at most `burst`
tokens and refills at `rate` tokens per second. `Wait(ctx, n)` blocks until
`n` tokens are available and takes them. `TryAcquire(n)` takes them only if
they are available now. `SetRate` changes the refill rate at runtime and
re-evaluates blocked waiters immediately.

- A rate of `0` never refills: only the initial burst is available until
  `SetRate` raises the rate.
- A rate of `math.Inf(1)` is unlimited: every request succeeds immediately,
  including requests larger than `burst`.
- A negative or NaN rate, or a non-positive burst, is rejected with
  `ErrInvalidLimit`. So is a negative `n`, or an `n` above `burst` at a finite
  rate.

```go
package main

import (
	"context"
	"fmt"

	concurrency "github.com/justusschock/go-concurrency"
)

func main() {
	// Limit uploads to 8 MiB/s with bursts of up to 1 MiB.
	limiter, err := concurrency.NewLimiter(8<<20, 1<<20)
	if err != nil {
		panic(err)
	}

	chunk := make([]byte, 256<<10)
	for range 4 {
		if err := limiter.Wait(context.Background(), len(chunk)); err != nil {
			panic(err)
		}
		// Upload chunk.
	}
	fmt.Println(limiter.Rate())
}
```

### Adaptive concurrency

An `Adaptive` limiter admits up to `Limit()` concurrent permits and adjusts
that limit with additive increase and multiplicative decrease (AIMD). Release
each permit with the outcome of its work:

- `Success` counts toward raising the limit by one, up to `Max`;
- `Overload` scales the limit by `DecreaseFactor`, down to `Min`; and
- `Failure` leaves the limit unchanged.

`AdaptiveOptions.Max` is required. `Min` defaults to 1, `Initial` to `Min`, and
`DecreaseFactor` to `DefaultAdaptiveDecreaseFactor` (0.7). `IncreaseAfter`
is the number of successes that raise the limit by one; 0 uses the current
limit, which grows the limit by about one per window of completed work.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	concurrency "github.com/justusschock/go-concurrency"
)

var errThrottled = errors.New("throttled")

func main() {
	ctx := context.Background()
	adaptive, err := concurrency.NewAdaptive(concurrency.AdaptiveOptions{
		Min:     1,
		Max:     32,
		Initial: 4,
	})
	if err != nil {
		panic(err)
	}

	group, err := concurrency.NewGroup(ctx, 32)
	if err != nil {
		panic(err)
	}
	for part := range 16 {
		permit, err := adaptive.Acquire(ctx)
		if err != nil {
			panic(err)
		}
		if err := group.Go(func(ctx context.Context) error {
			err := upload(ctx, part)
			switch {
			case errors.Is(err, errThrottled):
				permit.Release(concurrency.Overload)
				return nil // Requeue the part instead of failing the batch.
			case err != nil:
				permit.Release(concurrency.Failure)
				return err
			default:
				permit.Release(concurrency.Success)
				return nil
			}
		}); err != nil {
			permit.Release(concurrency.Failure)
			panic(err)
		}
	}
	if err := group.Wait(); err != nil {
		panic(err)
	}
	fmt.Println(adaptive.Limit())
}

func upload(_ context.Context, part int) error {
	if part%5 == 4 {
		return fmt.Errorf("part %d: status %d: %w", part, http.StatusTooManyRequests, errThrottled)
	}
	return nil
}
```

## Behavioral guarantees

- Canceling an `Await` context never cancels the future's underlying work. A
  result that is already published wins over wait cancellation.
- A group runs at most `limit` callbacks at once. Callback errors are collected
  by submission index and joined in that deterministic order.
- `ParallelMap` returns results by input index, not completion order, and does
  not expose partial results on failure.
- A pool's admission budget is exactly `workers + queue`, counting running and
  queued submissions. Later `Submit` calls wait for capacity or their submission
  context.
- A pool executes at most `workers` worker calls concurrently. Admitted work
  continues to completion after `Close`; `Wait` blocks until it has drained.
- Future results are published once. `Try` reports pending work without blocking,
  and `Done` closes when the result is available.
- `Retry` never sleeps after the final attempt and stops waiting as soon as its
  context is done.
- `Limiter.Wait` and `Adaptive.Acquire` serve blocked callers in FIFO order, so
  large token requests are not starved by smaller ones. `TryAcquire` never
  jumps ahead of queued waiters. A canceled wait takes no tokens or permit,
  unless they were granted before the cancellation was observed, in which case
  the grant wins.
- An `Adaptive` limiter decreases at most once per overload burst: overloads
  reported by permits acquired before the latest decrease do not decrease the
  limit again. `InFlight` can briefly exceed `Limit` after a decrease; new
  permits wait until it drops below the limit. Releasing a permit more than
  once has no effect.

## Errors

The package exports stable sentinel errors:

| Error | Returned when |
| --- | --- |
| `ErrClosed` | `Group.Go` is called after `Wait`, or `Pool.Submit` is called after `Close` |
| `ErrInvalidLimit` | A group/map limit, pool worker/queue configuration, retry policy, limiter rate/burst/token count, or adaptive option is invalid |
| `ErrNilFunction` | A required callback or worker function is nil |

Use `errors.Is` for these sentinels, context cancellation causes, and callback or
worker errors. In particular, `Group.Wait` and `ParallelMap` can return errors
joined from multiple callbacks, so callers must not compare errors by string or
with `==`.

For `Go[T](ctx, nil)`, the returned future is already resolved with
`ErrNilFunction`; observe it through `Await` or `Try`. `Retry` and `RetryValue`
return `ErrNilFunction` for a nil callback.

## Scope and non-goals

This module provides in-process concurrency mechanisms. It deliberately does
not provide distributed execution, persistence, distributed or shared rate
limits, priorities, dynamic pool resizing, or panic recovery. Callers own
context lifetimes, which errors count as overload or are permanent, and
application-level shutdown.

## Development

Run the local checks with Go 1.25 or newer:

```bash
go test ./...
go test -race ./...
go vet ./...
```

CI runs module verification, vet, and the race-enabled test suite on each Go
minor version from 1.25 through 1.27. Time-dependent tests run inside
`testing/synctest` bubbles, so backoff, refill, and wait timing is checked
against a fake clock without real sleeps.

## License

Licensed under the [Apache License 2.0](LICENSE).
