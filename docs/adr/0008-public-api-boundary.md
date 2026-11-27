# ADR 0008: Public API boundary

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0002, ADR-0007 |

## Context

Recorded retrospectively. Before the first public release, every exported
identifier was reviewed to decide whether it is a contract users need or an
implementation detail that should be unexported or moved under `internal/`.
The module is one package with no sub-packages. Consumers (other
justusperillieux libraries and applications) use `Future`, `Go`, `Failed`,
`ParallelMap`, `NewGroup`, `NewPool`, `NewLimiter`, `NewAdaptive`,
`Adaptive`, `AdaptiveOptions`, `Success`, `Overload`, `Failure`,
`ErrClosed` and `ErrInvalidLimit`.

## Decision drivers

- D1 The public surface is exactly what users need.
- D2 Nothing a consumer uses may be removed or hidden.
- D3 Implementation details must be free to change.

## Considered options

1. **Keep the current exported set; keep helpers unexported; no `internal/` package yet**
   - Pro: every exported identifier is a user-facing contract (types,
     constructors, methods, options, outcomes, sentinel errors, the
     documented default decrease factor).
   - Pro: helpers (`newFuture`, `resolve`, `limiterWaiter`, `permitState`,
     `advanceLocked`, `grantLocked`, `scheduleLocked`, `durationForTokens`,
     `validRate`, `tokenEpsilon`, …) are already unexported.
   - Con: none identified.
2. **Hide `Limiter.Burst`/`Rate`, `Adaptive.InFlight` or `DefaultAdaptiveDecreaseFactor`**
   - Con: they are documented, cheap to keep and useful for observability and
     configuration; hiding them gains nothing.
3. **Introduce a `Clock` abstraction for tests**
   - Con: unnecessary; tests use `testing/synctest` (ADR-0010).

## Decision

The public API is every exported identifier of package `concurrency`:

- Futures: `Future[T]` (`Await`, `Try`, `Done`), `Go`, `Completed`, `Failed`.
- Groups: `Group` (`Go`, `Wait`), `NewGroup`.
- `ParallelMap`.
- Pools: `Pool[I, O]` (`Submit`, `Close`, `Wait`), `NewPool`.
- Rate limiting: `Limiter` (`Rate`, `Burst`, `SetRate`, `TryAcquire`, `Wait`), `NewLimiter`.
- Adaptive limiting: `Adaptive` (`Limit`, `InFlight`, `Acquire`),
  `NewAdaptive`, `AdaptiveOptions`, `DefaultAdaptiveDecreaseFactor`,
  `Permit` (`Release`), `Outcome` with `Success`, `Overload`, `Failure`.
- Errors: `ErrClosed`, `ErrInvalidLimit`, `ErrNilFunction`.

Nothing is moved to `internal/` and nothing is unexported for the first
release. Future implementation-only packages go under `internal/`; test
helpers stay in `_test.go` files. `Retry`, `RetryValue`, `RetryPolicy`,
`Permanent` and the `DefaultRetry*` constants were removed beforehand
(ADR-0007).

## Consequences

### Positive
- No consumer changes are needed.
- The exported set is small enough to document fully on pkg.go.dev.

### Negative
- Every listed identifier is now a compatibility commitment under semver.

### Follow-ups
- API changes require a new ADR and a `CHANGELOG.md` entry.

## Links

- [AGENTS.md](../../AGENTS.md)
