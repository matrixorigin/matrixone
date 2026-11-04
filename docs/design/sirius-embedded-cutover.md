# E: embedded Sirius and complete MO Flight removal

Version: 2. Status: owner-approved for implementation on 2026-10-15.
Tracking: #28966. Base: `17a6200f59e764729e5544908fffd5480ae88279`, including
merged #30021 (`f1f52747`). Native source remains pinned at `06400cf4`.

The owner approved this revised coding plan after requiring complete removal
with no compatibility bridges. This supersedes the earlier local version 1:
there are no deprecated configuration fields, legacy runtime adapters, retained
lease/recovery providers, drain-verification APIs or new file-service APIs.

## Contract

Enabled Sirius has one implementation: the embedded C ABI with normal MO
readers. Sirius remains disabled by default and uses the existing explicit SQL
selector. Exact numeric semantics, snapshot ownership, bounded input/results,
one active GPU query and sixteen waiting queries retain their existing contracts.

The only Sirius configuration fields are `enabled`, `native-config-path`,
`gpu-streams`, `max-waiting-queries`, `request-timeout` and `cleanup-timeout`.
Both TOML loaders reject undecoded keys in the Sirius section, regardless of
whether it is enabled. No legacy-key allowlist or tombstone fields are retained.
Defaults remain two streams, fifteen minutes request time and thirty seconds
cleanup time. An ordinary build rejects enabled Sirius without native support.

## Removal and ownership

Remove Flight client/resolver/TLS/tickets, compiler Flight admission and fallback,
lease journals/protection/auditing/reconciliation, no-GC benchmark launch hooks,
the obsolete TAE snapshot provider and engine hooks used only by that provider.
Remove legacy exporter modes and request fields. Keep only the active MO-reader
Substrait lowering, C ABI conversion and query/result ownership paths.

CN owns the native runtime; the compiler owns each query. Configuration and
native initialization precede runtime publication. Query cleanup cancels and
joins readers/native work before releasing their resources; failed bounded
cleanup retains ownership and seals admission. Service shutdown unpublishes
only its own successfully closed generation. Explicit offload errors never
silently execute through a different engine.

Shared storage helpers, normal MO GC, numeric lowering and independent DuckDB
HTTP tooling remain only where active callers require them. Removed transport
tests disappear with their code; meaningful semantic/ownership assertions map
to the retained path. The abandoned sidecar repository remains untouched.

## Tooling and rollout

The current campaign generates only the supported embedded configuration.
There is no backend-selection switch or new compatibility profile. Historical
captures remain immutable and use their recorded tool revision for inspection.

Migration is offline: the previous release stops admission, drains old consumers
and completes old-state recovery before this release starts. GC protection stays
with that old owner until consumers quiesce. The new binary does not open,
interpret or repair old Flight state. Rollback restores the previous binary and
its matching configuration. Unknown or incomplete old-state recovery blocks
deployment rather than adding a new recovery mechanism.

## Validation and delivery

Validate both TOML loaders, minimal/disabled configuration, missing native
capability, unknown/removed keys including explicit empty/zero values, initialization
failure, sealed admission, cancellation, failed cleanup and service-generation
ownership. Reuse the small public SQL fixture for metadata/results/errors,
transaction visibility and default-route execution. Run affected owning suites,
targeted lifecycle race checks, CPU/Sirius/combined builds and actual GPU/cuVS
coexistence. Complete fresh matched all-22 SF1/SF10 streams 1/2/4 acceptance,
744 timing/correctness runs, 264 separate resource runs and unchanged performance
gates. Never relabel historical evidence as a new run.

One MatrixOne PR uses the repository template and a normal newest-main merge
before each push. Close #28966 only after merge, acceptance and verified release
availability. F, dependency upgrades, Docker changes and new query optimizations
are outside this delivery.
