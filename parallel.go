package concurrency

import "context"

func ParallelMap[T, R any](ctx context.Context, values []T, limit int, fn func(context.Context, T) (R, error)) ([]R, error) {
	if fn == nil {
		return nil, ErrNilFunction
	}

	group, err := NewGroup(ctx, limit)
	if err != nil {
		return nil, err
	}
	results := make([]R, len(values))
	for index, value := range values {
		index, value := index, value
		if err := group.Go(func(ctx context.Context) error {
			result, err := fn(ctx, value)
			if err == nil {
				results[index] = result
			}
			return err
		}); err != nil {
			_ = group.Wait()
			return nil, err
		}
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}
