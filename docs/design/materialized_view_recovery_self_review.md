# Upgrade and crash validation follow-up

Review target: PR #30085, baseline
`d50c4c77513c08837cf6ba351fae047ff9728bd2`. Latest inspected main is
`ac55588b8d4bf19c803a31bbd8248b7f63ad8122`; its lockservice/window changes do
not conflict with these files. The earlier exact baseline's ALL CI
38843251708, Connector/J 38843254257 and Iceberg 38843254214 all completed
successfully. Their optional compatibility jobs were skipped. Review
5543067251 requires additional native upgrade/crash validation and accepts
revision 3's lifecycle policy; this follow-up closes those validation boundaries
without changing the selected feature scope or claiming external approval.

## Change map and invariants

| Closure | Owner and consumers | Risk/proof |
| --- | --- | --- |
| MV catalog migration | 4.0.15 handler and existing common-protocol inventory | R3: no persistent MV catalogs before all inventoried nodes support 117; zero DDL on old-node/query-error paths; real mixed-node rejection, interrupted transaction and restart/repetition |
| Refresh fault boundaries | Native refresh transaction callback and return to task-service executor | R3: no visible partial tuple before commit; durable success survives response loss and CN death; same task/run identity acknowledges without repeated post-commit publication |
| Explicitly tagged startup initializer | mo-service build and existing fault owner | R2: environment startup injection exists only with `matrixone_mv_faults`; production build excludes it; native upgrade phase proves tagged entry, normal production build proves ordinary delivery path |
| Native probe/driver | SQL connection/result owner and process-owning Python driver | R3 fixture: two rows, normal authentication, real LOG/TN/CN, old/new CN overlap, immediate resource ownership/finally cleanup and bounded SQL/phase waits; no manually edited upgrade/run/task state |
| Documentation and frozen ledger | C07/C10/C12, reproduction and PR evidence | R0: separate historical performance, local instrumented runs, production build and final-head remote checks |

The upgrade metadata's required protocol was previously not consumed: the
framework checks it before tenant enumeration, but this handler has no tenant
phase. The handler now calls the existing inventory gate before its first DDL.
There is no new capability namespace, retry owner, catalog version, protocol
allocation, scheduler, persistent state or downgrade bypass.

Two disabled fault lookups per full refresh and four per migration add constant
work outside per-row processing. ECHO injects an error only after a real native
commit and routes it through the existing durable success resolution. WAIT uses
the existing context; the native driver kills the exact owned CN while keeping
LOG/TN alive. There is no added production goroutine or unbounded collection.
No new performance benchmark is needed for these constant, disabled lookups;
the historical performance results remain explicitly historical.

Test fixtures reuse the existing refresh executor and upgrade suite. The mock
records its committed last-run identity on actual successful callback commit,
so the new post-commit fault case exercises the real resolution branch. Fault
state is removed in cleanup, with deferred disable even if a cleanup assertion
fails; memory-pool deletion likewise runs on an assertion failure. No selected
test is parallel with another owner of the package's fault registry.

The native suite cannot reuse an ordinary BVT service: it must kill a CN and
upgrade persistent old storage while asserting actual task replay. Three
separate processes keep durability owners alive; only the compatibility phase
adds a fourth old/new CN. Two source rows distinguish retained and newly
published values. Startup/inventory/scheduler time dominates; no load or
wall-clock speed assertion is introduced. Existing ordinary SQL and multi-CN
CI tests remain necessary and retain all their assertions.

## Validation and evidence boundaries

- Owning MV and 4.0.15 suites pass in normal mode; the complete bootstrap suite
  passes with the new gate. Final test-cleanup edits are rerun separately.
- `TestMaterializedViewRefreshFailureBoundaries` is selected by named JSON test
  events, including the injected native-boundary analogue. Measured race body
  T=0.010 s yields N=clamp(floor(30/T),1,100)=100; the exact root passes 100
  repetitions. The full MV owning package passes under race. No skips or race
  reports are accepted.
- Five-package vet and configured incremental lint cover MV, 4.0.15,
  bootstrap, mo-service and native_probe, including the startup build tag;
  configured lint reports zero issues. Both tagged and ordinary service builds
  succeed. Python syntax and the actual native driver are checked separately.
- Native results and the exact source/configuration/binary fingerprints are in
  `materialized_view_native_recovery_results.json`. Old source matches all
  11,132 tracked blobs of fcad62a8. Normal login is used throughout. Pre-delivery
  current binaries report d50c4c77 plus `vcs.modified=true`, explicitly distinct
  from an unmodified publication commit.
- Native startup troubleshooting exposed a storage-only `-launch` restriction
  and shared status-port conflict; use separate `-cfg` processes/ports. Old-node
  refusal can happen at the earlier persisted protocol fence, before the
  cluster-version fence. A completed task row can survive until the existing
  HAKeeper GC cycle; teardown verifies terminal state before waiting for actual
  zero rows. These observations are retained, not reported as product CI failures.

The final-head remote CI and external re-review remain separate. These native
cases cover one pinned old revision and one local topology, with deterministic
before/after-commit injection. They do not claim all release pairs, arbitrary
network partitions/WAL failures, operator downgrade preflight, scale, cold-cache
latency or current-head speedup against StarRocks. No Request changes is
dismissed by the author and the PR is not merged by this follow-up.
