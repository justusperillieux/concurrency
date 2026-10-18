# ADR 0002: One flat package with generic, concrete types

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0004, ADR-0008 |

## Context

Recorded retrospectively. The library offers six related primitives
(futures, groups, ordered parallel map, worker pools, a token-bucket limiter
and an adaptive limiter). They compose: `Pool.Submit` returns a `Future`,
`ParallelMap` is built on `Group`, and all share the same sentinel errors.
Callers pass typed values and functions; results must be typed without
`any` casts.

## Decision drivers

- D1 Type safety for values flowing through futures, maps and pools.
- D2 A small surface that is easy to discover on pkg.go.dev.
- D3 Primitives must compose without adapter code.
- D4 Configuration that is validated once, at construction.

## Considered options

1. **One package `concurrency`, generic concrete types, constructors returning `(T, error)`**
   - Pro: one import; types compose directly (`*Future[O]` from `Pool[I, O]`).
   - Pro: generics give typed results with no reflection or casts.
   - Pro: invalid configuration is rejected by the constructor with `ErrInvalidLimit`/`ErrNilFunction`.
   - Con: the package grows as primitives are added.
2. **One sub-package per primitive (`future`, `group`, `pool`, …)**
   - Pro: smaller packages.
   - Con: cross-package composition and shared errors need an extra common package; more imports for callers.
3. **Interfaces for every primitive (e.g. `type Limiter interface`)**
   - Pro: callers could swap implementations.
   - Con: one implementation each; interfaces would freeze method sets and hinder adding methods. Callers can define narrow interfaces themselves.
4. **Functional options (`NewPool(ctx, WithWorkers(2), …)`)**
   - Pro: extensible without breaking signatures.
   - Con: most constructors take one to three required numbers; options hide required parameters and defer validation errors.

## Decision

We will keep a single package `concurrency` at the module root with
concrete, generic types (`Future[T]`, `Pool[I, O]`, `ParallelMap[T, R]`).
Constructors take required parameters positionally and return an error for
invalid configuration. When a primitive has several optional knobs with
defaults, it takes an options struct whose zero fields mean "default"
(`AdaptiveOptions`, `DefaultAdaptiveDecreaseFactor`). Results are reported
through futures and return values, never through callbacks registered on
the type.

## Consequences

### Positive
- Callers import one package and get typed results.
- Composition is direct: a pool's futures can be passed to `ParallelMap`
  (`TestPublicPrimitivesCompose`).

### Negative
- Positional constructor parameters cannot gain optional settings without a
  new constructor or an API break.
- No interfaces are provided for mocking; callers define their own.

### Follow-ups
- New optional settings on an existing primitive are added through an
  options struct, recorded in a new ADR.

## Links

- [PRD-0001 §6](../prd/0001-concurrency.md#6-api-sketch)
