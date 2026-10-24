# Configure existing ISCP cadences for CI

Revision: 1. Owner: #29249. Implementation series:
`codex/ut-bvt-quality-20261023` (PR link to be added when published).

## Problem and evidence

CN constructs the ISCP daemon through `ISCPTaskExecutorFactory`, which always
passes nil options to `NewISCPTaskExecutor`. The constructor already owns default
resolution and supports explicit task-discovery and index-watermark intervals.
The CN configuration cannot currently supply those existing options.

Ordinary BVT proves eventual index maintenance through durable readiness checks.
Its elapsed time need not include the production discovery and persistence
cadences. A private constructor overlay, using the same service binary for both
arms, kept all SQL, results, waits and outer deadlines unchanged. Two single-CN
runs per arm gave:

| Case | Default discovery/watermark | 1s/1s | Service CPU seconds |
| --- | ---: | ---: | ---: |
| `fulltext2_async` | 79–81s | 17s | 8.4 → 2.5 |
| `fulltext2_hnsw_clone` | 49s | 7s | 5.7 → 1.7 |

All comparisons and teardown checks passed. Container memory peaks were about
0.42 GiB in both arms. These are local case measurements; full-CI savings remain
unproven. Two-CN validation and validation of the actual configuration wiring
remain required. The private environment hook will not enter repository code.

## Responsibilities and interface

1. Add two startup-only CN TOML settings under `[cn.iscp]`:
   `sync-task-interval` and `index-flush-watermark-interval`.
2. Represent input with the existing `toml.Duration` type. Zero means inherit the
   ISCP default. Reject negative values in CN configuration validation.
3. Pass an `ISCPExecutorOption` value through the existing factory. Update its
   two repository callers. Each factory invocation copies that value before
   passing it to the constructor, whose existing `fillDefaultOption` remains
   the sole owner of production defaults.
4. Set both values to `1s` in the two dedicated compose BVT CN configurations.
   Other launch configurations and embedded test fixtures retain their defaults.

The factory receives configuration; the executor owns reconciliation and
persistence. No alternate execution path, live setter, environment switch,
compatibility wrapper, notification queue or scheduler is introduced.

## Invariants and failure paths

- Omitted settings retain discovery at 10s and index watermark persistence at
  5s. General watermark persistence, GC, retry and worker settings are inherited
  unchanged from the existing defaults.
- Configuration is copied at construction. Default filling and factory retries
  cannot mutate the caller's option value or share a mutable option instance
  between executor generations.
- Only timer periods and the existing index watermark threshold change when
  explicitly configured. Transaction completion, persisted watermark publication,
  rollback reservations, job fences and index freshness checks remain authoritative.
- The existing lifetime context, Start/Pause/Resume/Cancel/Stop paths, process
  ownership and cleanup continue to own all resources and waits.
- BVT still waits for actual durable conditions. Its result oracles and timeout
  budgets are retained. Faster ticking must not replace a readiness condition.
- Remove the executor's unused packer field and its constructor allocation.
  Real packer consumers already own their local packers; adding cleanup for an
  unused buffer would retain unnecessary state.

## Costs, compatibility and alternatives

There is one small value copy per daemon invocation and no additional per-query,
per-row or per-batch branch. Production instances using omitted settings retain
their existing cadence. Configured faster discovery increases metadata scan
frequency, and a shorter index threshold can increase catalog writes; total CPU
and memory must therefore be measured with the whole owning BVT group.

The change adds no persistent, SQL, wire or index format. The factory's internal
Go signature changes with both callers updated. An operator rolling back the
binary should also use that version's configuration. CI builds its binary and
configuration from the same checkout.

Changing global production constants would alter every deployment. Forcing a
flush from BVT would bypass the periodic path being tested. Shortening polling
or assertion deadlines would not remove executor cadence costs. A new event
delivery protocol would add ownership and recovery obligations beyond this need.

## Validation and delivery gates

- Parse the real TOML names and verify zero, positive and negative inputs without
  a cluster. Reuse the existing CN configuration tests.
- Extend the existing factory attach-failure test: default and distinct explicit
  intervals, two invocations, unchanged caller value, existing error propagation
  and lifetime cleanup. No additional heavy fixture is needed.
- Run incremental static checks and owning ISCP/CN race tests; retain the existing
  default, watermark rollback, cancellation and generation tests.
- Build an unmodified service from the implementation. Its startup logs must show
  10s/5s with omitted settings and 1s/1s from the actual TOML settings. The private
  experiment hook is insufficient evidence for configuration delivery.
- Run the owning Fulltext2 BVT group on two CNs with normal comparison, including
  both frontend endpoints, same-instance repetition and teardown checks. Compare
  time, CPU, memory and OOM counters with the unchanged configurations.
- Resolve any test depending on an incidental delay through its real behavior
  and owner. Do not weaken an assertion or add a retry to hide a failure.
