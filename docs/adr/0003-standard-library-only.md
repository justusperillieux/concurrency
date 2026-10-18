# ADR 0003: Depend on the standard library only

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (NFR-CONC-01) |

## Context

Recorded retrospectively. concurrency sits at the bottom of a dependency
graph: other libraries (executors, messaging, caches, lifecycle and
reconciliation engines, object storage) import it. Every dependency it adds
is inherited by all of them. `go.mod` has no `require` directives.
Comparable functionality exists outside the standard library, for example
`golang.org/x/sync/errgroup`, `golang.org/x/sync/semaphore` and
`golang.org/x/time/rate`.

## Decision drivers

- D1 Foundation libraries should not impose transitive dependencies.
- D2 Semantics must be exactly as specified (submission-ordered joined
  errors, ErrClosed on wait, FIFO limiter waiters, runtime rate changes).
- D3 Supply-chain and licence review should be trivial.

## Considered options

1. **Standard library only**
   - Pro: zero transitive dependencies; nothing to audit or update.
   - Pro: full control over semantics.
   - Con: the library maintains its own limiter and group code.
2. **Build on `golang.org/x/sync` and `golang.org/x/time`**
   - Pro: well-tested implementations.
   - Con: semantics differ from those specified here (errgroup reports only
     the first error and has no `ErrClosed` admission after `Wait`;
     `rate.Limiter` reserves future tokens at call time instead of queueing
     waiters that are re-evaluated on a rate change); wrappers would still be
     needed.
   - Con: adds dependencies to every consumer.
3. **Depend on other justusschock libraries**
   - Con: this library is the lowest level; any such dependency would be circular.

## Decision

We will use only the Go standard library in non-test and test code. Adding
any third-party or other justusschock module requires a new ADR that
supersedes this one.

## Consequences

### Positive
- Consumers inherit no dependencies; `go mod verify` is trivial.

### Negative
- Rate limiting and admission algorithms are maintained here (ADR-0006).

### Follow-ups
- None.

## Links

- [go.mod](../../go.mod)
