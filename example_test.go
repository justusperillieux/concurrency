package concurrency_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	concurrency "github.com/justusschock/go-concurrency"
)

func ExampleParallelMap() {
	values, _ := concurrency.ParallelMap(context.Background(), []int{1, 2, 3}, 2, func(_ context.Context, value int) (int, error) {
		return value * value, nil
	})
	fmt.Println(values)
	// Output: [1 4 9]
}

func ExampleRetry() {
	attempts := 0
	err := concurrency.Retry(context.Background(), concurrency.RetryPolicy{
		MaxAttempts: 5,
		BaseDelay:   time.Millisecond,
		Jitter:      0.5,
	}, func(_ context.Context, attempt int) error {
		attempts = attempt
		if attempt < 3 {
			return errors.New("temporarily unavailable")
		}
		return nil
	})
	fmt.Println(attempts, err)
	// Output: 3 <nil>
}

func ExamplePermanent() {
	errNotFound := errors.New("not found")
	err := concurrency.Retry(context.Background(), concurrency.RetryPolicy{}, func(context.Context, int) error {
		return concurrency.Permanent(errNotFound)
	})
	fmt.Println(errors.Is(err, errNotFound))
	// Output: true
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
