# go-concurrency

`go-concurrency` provides small, typed building blocks for asynchronous work,
bounded structured concurrency, ordered parallel mapping, and reusable worker
pools. The module is private, depends only on the Go standard library, and is
independent of product-specific resource types.

Module path: `github.com/justusschock/go-concurrency`

## Requirements and installation

Go 1.22 or newer is required.

Because the module is private, configure repository access through your normal
GitHub authentication before installing it. Keep credentials out of commands,
source files, and Go environment settings.

```bash
go env -w GOPRIVATE='github.com/justusschock/*'
go get github.com/justusschock/go-concurrency@v0.1.0
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

## Errors

The package exports stable sentinel errors:

| Error | Returned when |
| --- | --- |
| `ErrClosed` | `Group.Go` is called after `Wait`, or `Pool.Submit` is called after `Close` |
| `ErrInvalidLimit` | A group/map limit or pool worker/queue configuration is invalid |
| `ErrNilFunction` | A required callback or worker function is nil |

Use `errors.Is` for these sentinels, context cancellation causes, and callback or
worker errors. In particular, `Group.Wait` and `ParallelMap` can return errors
joined from multiple callbacks, so callers must not compare errors by string or
with `==`.

For `Go[T](ctx, nil)`, the returned future is already resolved with
`ErrNilFunction`; observe it through `Await` or `Try`.

## Scope and non-goals

This module provides in-process concurrency mechanisms. It deliberately does
not provide distributed execution, persistence, retries, rate limiting,
priorities, dynamic pool resizing, or panic recovery. Callers own context
lifetimes, retry policy, and application-level shutdown.

## Development

Run the local checks with Go 1.22 or newer:

```bash
go test ./...
go test -race ./...
go vet ./...
```

CI runs module verification, vet, and the race-enabled test suite on each Go
minor version from 1.22 through 1.26.

## License

Licensed under the [Apache License 2.0](LICENSE).
