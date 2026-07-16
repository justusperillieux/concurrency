package concurrency_test

import (
	"context"
	"fmt"

	concurrency "github.com/justusschock/go-concurrency"
)

func ExampleParallelMap() {
	values, _ := concurrency.ParallelMap(context.Background(), []int{1, 2, 3}, 2, func(_ context.Context, value int) (int, error) {
		return value * value, nil
	})
	fmt.Println(values)
	// Output: [1 4 9]
}
