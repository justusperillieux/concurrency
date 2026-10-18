# ADR 0010: Testing strategy

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0009 |

## Context

Recorded retrospectively. Nearly every behaviour of this library involves
goroutines, blocking and time: admission limits, shutdown while callers are
blocked, cancellation races, token refill timing and FIFO queues. Tests
based on real time (`time.Sleep`, `time.After` timeouts, "has not returned
after 20ms" checks) are slow, flaky under load and cannot assert exact
timing. The limiter and adaptive tests were written with
`testing/synctest`; the older future, group, map and pool tests used
wall-clock timeouts and a context wrapper that signalled when `Done` was
called.

## Decision drivers

- D1 Deterministic tests that pass with `-race -count=3`.
- D2 Exact timing assertions for the limiter.
- D3 Negative assertions ("still blocked") must be meaningful, not timing guesses.
- D4 Every requirement is traceable to a test.

## Considered options

1. **`testing/synctest` for all time and coordination tests**
   - Pro: virtual time; `synctest.Wait` settles all goroutines before a
     negative assertion; a hang fails immediately as a bubble deadlock.
   - Con: requires Go 1.25 (ADR-0009).
2. **Injectable fake clock**
   - Con: adds a `Clock` to the public API only for tests; does not help with
     goroutine settling.
3. **Real time with generous timeouts**
   - Con: slow and flaky; cannot assert exact durations.

## Decision

We will:

- write white-box unit tests in package `concurrency` and black-box
  composition tests and examples in package `concurrency_test`;
- run every test that involves time, blocking or goroutine coordination
  inside `synctest.Test`, use `synctest.Wait` before asserting that something
  has not happened, and never sleep on the real clock;
- keep tests without time or blocking (validation, sequential state machines,
  the race stress test `TestAdaptiveConcurrentUseRespectsBounds`) outside
  bubbles;
- provide a runnable `Example` for every public entry point and keep the
  README quick start identical to the package `Example`;
- run `go test -race` in CI on every supported Go version and lint with
  golangci-lint;
- cite requirement IDs (`// FR-CONC-NN`) on the tests listed in PRD-0001's
  acceptance table.

There are no external services, so there is no integration job, and no
fuzz tests: inputs are small integers and functions, covered by table tests.

## Consequences

### Positive
- The full suite runs in about a second and is deterministic.
- Hangs and goroutine leaks fail fast with a deadlock report.

### Negative
- Code under test must not block on real I/O inside a bubble; the library
  does none.

### Follow-ups
- New behaviours add tests that cite their requirement ID.

## Links

- [testing/synctest](https://pkg.go.dev/testing/synctest)
