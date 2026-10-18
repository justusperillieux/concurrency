# ADR 0007: Keep retries and backoff out of the library

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 (NG1) |

## Context

Recorded retrospectively. Earlier development added `Retry`, `RetryValue`,
`RetryPolicy`, `Permanent` and `DefaultRetry*` constants (exponential backoff
with jitter). Nothing else in the module used them, and no consumer of the
module imports them. They were removed before the first public release
(commit "refactor!: remove Retry, RetryValue, and Permanent").

## Decision drivers

- D1 Keep the module focused on concurrency primitives.
- D2 Retry policy (which errors are retryable, budgets, jitter, logging)
  varies by application and is well served by dedicated libraries.
- D3 Every exported identifier is a long-term compatibility commitment.

## Considered options

1. **Remove retries; point callers to a dedicated library**
   - Pro: smaller, focused API; no policy decisions embedded here.
   - Con: callers that wanted a one-stop package add another dependency.
2. **Keep retries in this package**
   - Pro: convenience.
   - Con: mixes policy with primitives; grows API that must stay stable.
3. **Move retries to a sub-package**
   - Con: same maintenance burden with no user.

## Decision

We will not provide retries or backoff. Callers that need them use a
dedicated library such as `github.com/cenkalti/backoff/v5`, combined with
this package's primitives where needed.

## Consequences

### Positive
- The public API is limited to futures, groups, maps, pools and limiters.

### Negative
- Code written against pre-release versions that used `Retry` must migrate;
  the change is listed in `CHANGELOG.md`.

### Follow-ups
- None.

## Links

- [CHANGELOG.md](../../CHANGELOG.md)
