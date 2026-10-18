# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the major version is `0`, a minor release may break the API.

## [Unreleased]

Preparation for the first public release.

### Removed

- **Breaking:** `Retry`, `RetryValue`, `RetryPolicy`, `Permanent` and the
  `DefaultRetry*` constants. Retries are not a concurrency primitive; use a
  dedicated library such as `github.com/cenkalti/backoff/v5`
  ([ADR-0007](docs/adr/0007-retries-out-of-scope.md)).

### Changed

- Public API boundary reviewed: every remaining exported identifier is part of
  the public API; nothing moved to `internal/` or unexported
  ([ADR-0008](docs/adr/0008-public-api-boundary.md)).
- Minimum Go version is 1.25, required by `testing/synctest`; CI tests Go
  1.25, `oldstable` and `stable`
  ([ADR-0009](docs/adr/0009-supported-go-versions.md)).
- Future, group, `ParallelMap` and pool tests run in `testing/synctest`
  bubbles instead of using wall-clock timeouts.
- CI pins every action to a commit SHA, adds golangci-lint, and Dependabot
  keeps Go modules and actions current.

### Added

- Doc comments for every exported identifier and runnable examples for every
  entry point.
- `NOTICE`, `AGENTS.md`, a requirements document and architecture decision
  records under `docs/`.

## [0.3.0] - 2026-10-15

### Added

- `Limiter`: token-bucket rate limiter with FIFO waiters and runtime
  `SetRate`.
- `Adaptive`: AIMD concurrency limiter with `Permit` and `Success`,
  `Overload` and `Failure` outcomes.
- `Retry`, `RetryValue` and `Permanent` (removed again before the first
  public release).

### Changed

- Minimum Go version raised to 1.25 for `testing/synctest`.

## [0.2.0] - 2026-07-17

### Changed

- Minimum Go version lowered to 1.22; CI tests the supported Go versions.

## [0.1.0] - 2026-07-17

### Added

- `Future` with `Go`, `Completed` and `Failed`.
- `Group` for bounded structured concurrency and `ParallelMap`.
- `Pool` for bounded reusable workers.

### Fixed

- `Group.Wait` unblocks `Go` calls waiting for capacity; `Pool.Close`
  unblocks waiting submissions; `ParallelMap` preserves callback errors when
  admission stops.

[Unreleased]: https://github.com/justusschock/go-concurrency/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/justusschock/go-concurrency/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/justusschock/go-concurrency/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/justusschock/go-concurrency/releases/tag/v0.1.0
