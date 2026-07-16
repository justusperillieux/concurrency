package concurrency

import "errors"

var (
	ErrClosed       = errors.New("concurrency: closed")
	ErrInvalidLimit = errors.New("concurrency: invalid limit")
	ErrNilFunction  = errors.New("concurrency: nil function")
)
