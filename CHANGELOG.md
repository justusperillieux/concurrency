# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the major version is `0`, a minor release may break the API.

## [Unreleased]

## [0.1.0] - 2026-10-18

First public release.

### Added

- Futures: `Go`, `Future[T]` with `Await`, `Try` and `Done`, and the resolved
  constructors `Completed` and `Failed`.
- Bounded structured groups (`NewGroup`) that cancel peers on the first error
  and join all errors in submission order.
- `ParallelMap`: ordered parallel map with bounded concurrency.
- Worker pools (`NewPool`) with separate worker and queue limits and a future
  per submission.
- `Limiter`: token-bucket rate limiter with FIFO waiters and runtime
  `SetRate`.
- `Adaptive`: AIMD concurrency limiter with `Permit` and `Success`,
  `Overload` and `Failure` outcomes.
- Requirements and architecture decision records under `docs/`.

Requires Go 1.25 or newer.

[Unreleased]: https://github.com/justusschock/concurrency/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/justusschock/concurrency/releases/tag/v0.1.0
