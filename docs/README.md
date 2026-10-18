# concurrency documentation

Requirements (PRDs) say *what* the library does; architecture decision
records (ADRs) say *how and why*. The process is described in
[ADR-0001](adr/0001-record-architecture-decisions.md) and
[AGENTS.md](../AGENTS.md). New documents start from the templates:
[PRD template](prd/TEMPLATE.md), [ADR template](adr/TEMPLATE.md).

Requirement area code: `CONC` (`FR-CONC-NN`, `NFR-CONC-NN`).

## Product requirements

| PRD | Title | Status | Implementation |
|---|---|---|---|
| [0001](prd/0001-concurrency.md) | concurrency | Accepted | Complete |

## Architecture decisions

| ADR | Title | Status |
|---|---|---|
| [0001](adr/0001-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [0002](adr/0002-single-package-generic-api.md) | One flat package with generic, concrete types | Accepted |
| [0003](adr/0003-standard-library-only.md) | Depend on the standard library only | Accepted |
| [0004](adr/0004-error-semantics.md) | Error semantics | Accepted |
| [0005](adr/0005-concurrency-and-cancellation-model.md) | Concurrency and cancellation model | Accepted |
| [0006](adr/0006-rate-and-adaptive-limiting-algorithms.md) | Token bucket and AIMD limiting algorithms | Accepted |
| [0007](adr/0007-retries-out-of-scope.md) | Keep retries and backoff out of the library | Accepted |
| [0008](adr/0008-public-api-boundary.md) | Public API boundary | Accepted |
| [0009](adr/0009-supported-go-versions.md) | Supported Go versions | Accepted |
| [0010](adr/0010-testing-strategy.md) | Testing strategy | Accepted |
