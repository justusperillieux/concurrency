# ADR 0006: Token bucket and AIMD limiting algorithms

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (FR-CONC-21 to FR-CONC-33), ADR-0003, ADR-0005 |

## Context

Recorded retrospectively. Two kinds of limiting are needed: capping the
rate of operations or bytes (for example upload bandwidth) and capping
concurrency when the right limit is unknown and must follow downstream
feedback (HTTP 429/503, timeouts). ADR-0003 rules out `golang.org/x/time/rate`.

## Decision drivers

- D1 Large requests (many tokens) must not be starved by small ones.
- D2 Rates must be adjustable at runtime, including "paused" and "unlimited".
- D3 A burst of concurrent overload responses must not collapse the limit to
  its minimum.
- D4 No background goroutines; waiting must be cancellable.

## Considered options

1. **Token bucket with fractional tokens, FIFO waiters and one timer for the head waiter; AIMD with generations**
   - Pro: FIFO prevents starvation; a single `time.AfterFunc` wakes the head
     waiter exactly when enough tokens have accrued.
   - Pro: generations make one overload burst count once.
   - Con: more code than a polling loop.
2. **Polling (sleep and retry) for limiter waits**
   - Con: wastes CPU, imprecise, untestable without real time.
3. **Leaky bucket / fixed window**
   - Con: no burst capacity, or bursts at window edges.
4. **Gradient or latency-based concurrency limits (Vegas, gradient2)**
   - Pro: react to latency before errors.
   - Con: need latency measurement and tuning; AIMD on explicit outcomes is
     simpler and matches the available signals.

## Decision

We will implement `Limiter` as a token bucket that starts full, holds up to
`burst` tokens and refills at `rate` tokens per second using fractional
tokens with a small epsilon for floating-point error. `Wait` queues in FIFO
order and `TryAcquire` never takes tokens while a waiter is queued. One
`time.AfterFunc` timer targets the head waiter and is rescheduled on
`SetRate` and cancellation. A rate of 0 never refills; `+Inf` grants every
request, including requests above `burst`.

We will implement `Adaptive` as an AIMD limiter: `Success` adds one to the
limit after `IncreaseAfter` successes (default: the current limit, about one
per window), `Overload` multiplies the limit by `DecreaseFactor` (default
`DefaultAdaptiveDecreaseFactor` = 0.7) and resets success progress, and
`Failure` is neutral. Each permit records the limit generation at
acquisition; only overloads from the current generation decrease the limit.
Waiters are admitted in FIFO order whenever a permit is released or the limit
grows. `Permit` is a value type whose `Release` is idempotent.

## Consequences

### Positive
- Deterministic timing that synctest verifies to the nanosecond
  (`TestLimiterWaitBlocksUntilRefilled`).
- One overload burst lowers the limit once (`TestAdaptiveConcurrentOverloadsDecreaseOnce`).

### Negative
- `InFlight` can exceed `Limit` after a decrease until permits are released.
- No reservation API; callers wait or try.

### Follow-ups
- PRD-0001 Q2 tracks a reservation API.

## Links

- [limiter.go](../../limiter.go), [adaptive.go](../../adaptive.go)
