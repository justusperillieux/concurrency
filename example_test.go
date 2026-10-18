package concurrency_test

import (
	"context"
	"errors"
	"fmt"

	concurrency "github.com/justusschock/concurrency"
)

// Square numbers with at most two concurrent calls; results keep input order.
func Example() {
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
	fmt.Println(squares)
	// Output: [1 4 9 16]
}

func ExampleGo() {
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

	// Canceling the context of one Await does not cancel the work.
	waitCtx, cancelWait := context.WithCancel(context.Background())
	cancelWait()
	_, err := future.Await(waitCtx)
	fmt.Println(errors.Is(err, context.Canceled))

	close(release)
	value, err := future.Await(context.Background())
	fmt.Println(value, err)
	// Output:
	// true
	// 42 <nil>
}

func ExampleGroup() {
	group, err := concurrency.NewGroup(context.Background(), 2)
	if err != nil {
		panic(err)
	}

	inputs := []int{1, 2, 3}
	results := make([]int, len(inputs))
	for index, value := range inputs {
		if err := group.Go(func(ctx context.Context) error {
			if err := context.Cause(ctx); err != nil {
				return err
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
	fmt.Println(results)
	// Output: [1 4 9]
}

func ExampleParallelMap() {
	values, _ := concurrency.ParallelMap(context.Background(), []int{1, 2, 3}, 2, func(_ context.Context, value int) (int, error) {
		return value * value, nil
	})
	fmt.Println(values)
	// Output: [1 4 9]
}

func ExamplePool() {
	ctx := context.Background()
	pool, err := concurrency.NewPool(ctx, 2, 2, func(_ context.Context, value int) (int, error) {
		return value * value, nil
	})
	if err != nil {
		panic(err)
	}

	futures := make([]*concurrency.Future[int], 0, 4)
	for _, value := range []int{1, 2, 3, 4} {
		future, err := pool.Submit(ctx, value)
		if err != nil {
			panic(err)
		}
		futures = append(futures, future)
	}
	pool.Close()
	pool.Wait()

	for _, future := range futures {
		value, err := future.Await(ctx)
		fmt.Println(value, err)
	}
	// Output:
	// 1 <nil>
	// 4 <nil>
	// 9 <nil>
	// 16 <nil>
}

func ExampleLimiter() {
	limiter, err := concurrency.NewLimiter(1000, 2)
	if err != nil {
		panic(err)
	}
	fmt.Println(limiter.TryAcquire(2), limiter.TryAcquire(1))
	fmt.Println(limiter.Wait(context.Background(), 1))
	// Output:
	// true false
	// <nil>
}

func ExampleAdaptive() {
	adaptive, err := concurrency.NewAdaptive(concurrency.AdaptiveOptions{
		Min:            1,
		Max:            8,
		Initial:        4,
		IncreaseAfter:  1,
		DecreaseFactor: 0.5,
	})
	if err != nil {
		panic(err)
	}

	permit, err := adaptive.Acquire(context.Background())
	if err != nil {
		panic(err)
	}
	permit.Release(concurrency.Success)
	fmt.Println(adaptive.Limit())

	permit, err = adaptive.Acquire(context.Background())
	if err != nil {
		panic(err)
	}
	permit.Release(concurrency.Overload) // For example, the server answered 429.
	fmt.Println(adaptive.Limit())
	// Output:
	// 5
	// 2
}
