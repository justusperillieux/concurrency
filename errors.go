package concurrency

import "errors"

// Sentinel errors returned by this package. Their values and texts are part of
// the API; compare with errors.Is.
var (
	// ErrClosed is returned by Group.Go after Group.Wait was called and by
	// Pool.Submit after Pool.Close was called.
	ErrClosed = errors.New("concurrency: closed")
	// ErrInvalidLimit is returned for an invalid group or map limit, pool
	// worker or queue size, limiter rate, burst or token count, or adaptive
	// option.
	ErrInvalidLimit = errors.New("concurrency: invalid limit")
	// ErrNilFunction is returned when a required callback or worker function
	// is nil.
	ErrNilFunction = errors.New("concurrency: nil function")
)
