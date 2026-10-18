# ADR 0004: Error semantics

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (FR-CONC-05, -10, -14, -34), ADR-0005 |

## Context

Recorded retrospectively. The primitives report three kinds of errors:
misuse or invalid configuration, use after shutdown, and errors produced by
caller callbacks or by context cancellation. Batch primitives (`Group`,
`ParallelMap`) can see several callback errors at once. Fix commits in the
history ("preserve parallel map callback errors") show that losing a callback
error behind an admission error was treated as a bug.

## Decision drivers

- D1 Callers must branch on error kinds with `errors.Is`, not strings.
- D2 No error may be silently dropped.
- D3 Error output must be deterministic for tests and logs.
- D4 Cancellation must carry the caller's cause.

## Considered options

1. **Sentinels for library errors, `context.Cause` for cancellation, `errors.Join` in submission order for batches**
   - Pro: `errors.Is` works for sentinels, causes and every callback error.
   - Pro: deterministic order independent of completion order.
   - Con: joined errors produce multi-line messages.
2. **Typed error structs (e.g. `*LimitError{Field: …}`)**
   - Pro: more detail.
   - Con: more API for little gain; callers mostly need "invalid" vs "closed".
3. **First error only (errgroup style)**
   - Pro: simple.
   - Con: drops information; which error is "first" depends on scheduling.
4. **Return `ctx.Err()` instead of `context.Cause(ctx)`**
   - Con: loses the caller-supplied cause from `context.WithCancelCause`.

## Decision

We will export exactly three sentinel errors with stable texts:
`ErrClosed`, `ErrInvalidLimit` and `ErrNilFunction`. Blocking operations
return `context.Cause(ctx)` when their context ends first. `Group.Wait` and
`ParallelMap` join all callback errors with `errors.Join` in submission
(input) order; when `ParallelMap` stops admitting because of an error and
callbacks also failed, the callback errors are returned. A result that is already available
(published future value, granted tokens or permit) wins over a cancellation
observed at the same time. Errors are never wrapped with extra text by the
library, so callback errors reach the caller unchanged. Panics are not
converted to errors.

## Consequences

### Positive
- `errors.Is(err, ErrClosed)`, `errors.Is(err, myCause)` and
  `errors.Is(err, callbackErr)` all work.
- Tests can assert exact joined messages (`TestGroupJoinsErrorsBySubmissionIndex`).

### Negative
- Callers must not compare errors with `==` or by string.
- Sentinel texts are part of the API (`TestFutureSentinelErrorsHaveStableText`).

### Follow-ups
- None.

## Links

- [errors.go](../../errors.go)
