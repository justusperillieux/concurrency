// Package concurrency provides small, typed building blocks for in-process
// concurrent work: futures, bounded structured groups, ordered parallel maps,
// reusable worker pools, a token-bucket rate limiter and an adaptive (AIMD)
// concurrency limiter.
//
// Choose an abstraction by the shape and lifetime of the work:
//
//   - [Future], created by [Go], [Completed] or [Failed]: one asynchronous
//     result that is awaited later.
//   - [Group]: a scoped batch of related callbacks that share cancellation and
//     join their errors.
//   - [ParallelMap]: one function applied to a slice with bounded concurrency,
//     results in input order.
//   - [Pool]: an ongoing stream of typed inputs processed by one reusable
//     worker function, with separate worker and queue limits.
//   - [Limiter]: a token bucket that caps the rate of operations or bytes.
//   - [Adaptive]: a concurrency limit that follows downstream feedback.
//
// Futures, groups, pools, limiters and permits are safe for concurrent use.
// Operations that block take a [context.Context] and return [context.Cause]
// when it is done first.
// Configuration and state errors are the sentinels [ErrClosed],
// [ErrInvalidLimit] and [ErrNilFunction]; compare them with [errors.Is].
//
// The package depends only on the Go standard library and does no logging.
package concurrency
