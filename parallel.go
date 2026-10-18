package concurrency

import "context"

// ParallelMap calls fn for every element of values with at most limit calls
// running at once and returns the results in input order. Calls share a
// context derived from ctx that is canceled by the first error. On any error
// ParallelMap stops admitting new elements, waits for the admitted calls and
// returns a nil slice with the callback errors joined in input order, or with
// the admission error (such as the cancellation cause of ctx) if no callback
// failed. It returns ErrNilFunction for a nil fn and ErrInvalidLimit for a
// non-positive limit.
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
			if callbackErr := group.Wait(); callbackErr != nil {
				return nil, callbackErr
			}
			return nil, err
		}
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}
