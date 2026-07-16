package concurrency_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	concurrency "github.com/justusschock/go-concurrency"
)

func TestPublicPrimitivesCompose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	started := make(chan int, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWork := func() { releaseOnce.Do(func() { close(release) }) }

	pool, err := concurrency.NewPool(ctx, 1, 1, func(ctx context.Context, value int) (int, error) {
		started <- value
		select {
		case <-release:
			return value * value, nil
		case <-ctx.Done():
			return 0, context.Cause(ctx)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		releaseWork()
		pool.Close()
		pool.Wait()
	}()

	first, err := pool.Submit(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case value := <-started:
		if value != 2 {
			t.Fatalf("first worker value=%d, want 2", value)
		}
	case <-ctx.Done():
		t.Fatalf("first worker did not start: %v", context.Cause(ctx))
	}
	second, err := pool.Submit(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}

	pool.Close()
	releaseWork()
	pool.Wait()

	values, err := concurrency.ParallelMap(ctx, []*concurrency.Future[int]{first, second}, 2, func(ctx context.Context, future *concurrency.Future[int]) (int, error) {
		return future.Await(ctx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(values, []int{4, 9}) {
		t.Fatalf("mapped values=%v, want [4 9]", values)
	}

	group, err := concurrency.NewGroup(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	observed := make(chan int, len(values))
	for _, value := range values {
		value := value
		if err := group.Go(func(ctx context.Context) error {
			select {
			case observed <- value:
				return nil
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}); err != nil {
			_ = group.Wait()
			t.Fatal(err)
		}
	}
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
	close(observed)

	count, sum := 0, 0
	for value := range observed {
		count++
		sum += value
	}
	if count != 2 || sum != 13 {
		t.Fatalf("observed (count=%d,sum=%d), want (2,13)", count, sum)
	}
}
