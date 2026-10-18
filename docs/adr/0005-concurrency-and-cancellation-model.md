# ADR 0005: Concurrency and cancellation model

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (FR-CONC-05, -07, -08, -09, -17, -18, -19), ADR-0004 |

## Context

Recorded retrospectively. Each primitive involves two lifetimes: the
lifetime of the work and the lifetime of a caller waiting for it (awaiting a
future, waiting for admission, waiting for tokens). Conflating them causes
classic bugs: a timed-out waiter that cancels shared work, or shutdown that
deadlocks because a submitter is blocked on capacity. Two fix commits
("unblock group admission on wait", "unblock pool submissions on close")
established the shutdown rules.

## Decision drivers

- D1 Canceling a wait must never cancel work other callers depend on.
- D2 Shutdown must never deadlock and must not leak goroutines.
- D3 Admission must be fair and bounded.
- D4 Behaviour must be predictable under races between grant and cancellation.

## Considered options

1. **Separate work and wait contexts; explicit close; FIFO admission**
   - Pro: each context has one meaning.
   - Pro: close/wait unblocks every pending admission with `ErrClosed`.
   - Con: callers must understand two contexts per primitive.
2. **One context per primitive controlling both work and waits**
   - Pro: simpler signatures.
   - Con: a caller giving up on a wait would cancel shared work.
3. **Unbounded admission (spawn a goroutine per submission)**
   - Pro: callers never block.
   - Con: no back-pressure; memory grows without bound under load.

## Decision

We will separate the work context from wait contexts:

- `Go(ctx, fn)` passes `ctx` to the work; `Await(ctx)` bounds only that wait.
- `NewGroup(parent, …)` derives the group context that callbacks receive;
  the first callback error cancels it with that error as the cause.
- `NewPool(ctx, …)` gives workers the pool context; `Submit(ctx, …)` bounds
  only admission.
- `Limiter.Wait(ctx, …)` and `Adaptive.Acquire(ctx)` bound only the wait.

Admission is bounded and blocking: `Group.Go` blocks at `limit`;
`Pool.Submit` blocks at `workers+queue` and execution is bounded separately
by `workers`; limiter and adaptive waiters queue in FIFO order. Shutdown is
explicit: `Group.Wait` and `Pool.Close` close a channel that every blocked
admission selects on, so blocked callers return `ErrClosed` and admitted work
keeps running until it finishes; `Group.Wait` and `Pool.Wait` then drain it.
When a grant and a cancellation race, a grant made before the cancellation is
observed wins. Every exported type with state is safe for concurrent use and
guards state with a mutex; the limiter wakes waiters with `time.AfterFunc`
rather than a background goroutine. Panics in callbacks are not recovered.

## Consequences

### Positive
- No goroutine outlives its work; tests in synctest bubbles would fail on a leak.
- Shutdown is deterministic and race-free.

### Negative
- Callers must call `Wait` (group) or `Close` then `Wait` (pool).
- A panic in a callback crashes the process, as with a bare goroutine.

### Follow-ups
- PRD-0001 Q1 tracks opt-in panic recovery.

## Links

- [group.go](../../group.go), [pool.go](../../pool.go), [future.go](../../future.go)
