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


## Covered bridge consumer and license-only follow-up

Exact a9bff0c9 SCA evidence identifies one invalid Apache notice before analyzer
execution; expand that comment and verify the changed path with the same
license-eye policy (one valid, zero invalid). No executable initializer bytes
or build tags change. The native recovery capture is reused by exact production
source equality and exact equality from the initializer's build tag onward;
the original source hashes remain labeled with their capture revision.

The new bridge package otherwise contributes zero-hit UT blocks. Reuse the
existing native BVT deployment to validate its actual JSON stream contract,
SQL NULL/empty distinction, DSN failure, Exec/Query errors and session lifetime.
Two independent bridge processes each handle nine requests; only connection-
local state changes. The shell owns the temporary binaries, clears stale probe
reports, verifies publication and collects coverage through the existing owner.
This adds no cluster, production state or coverage-policy exception. Bridge
coverage is 28/32 blocks; aggregate coverage remains a separate remote gate.
Full-entry consumer evidence is retained separately, including repeated native
checks and independent teardown. No additional kernel/race suite is required
for Python/shell checks and an unchanged Go initializer body; normal Go/race
proofs remain valid for their unchanged semantic inputs.


Final covered-consumer validation passes the exact immutable entry script twice
on the same two-CN native instance: each run has 2 x 9 bridge requests and
70 functional + 5 failure + 9 cross-CN checks. Independent post-run database,
definition, dependency, run and task counts are zero before repetition. All
owned services exit. `materialized_view_ci_bridge_results.json` retains both
runs, exact changed input hashes, license proof and component coverage. The
production Go service/bridge inputs are unchanged from the 12-case capture;
only the startup initializer license comment changes. Final-head remote SCA,
UT, coverage and BVT remain required separately.


## Mixed-version task metadata: first-publisher correction

The old native log exposed an omitted contract: the old CN claimed executor 17
while catalog migration was blocked. The old 12-case result is superseded, not
reused as proof of task exclusion. The current 13-case suite observes the real
admission attempt and zero cron/async metadata, before proving normal task
execution and same-run replay after upgrade.

R3 ownership: MV service owns the catalog/protocol admission and task-service
publication; CN stopper retains the same 30-second attempt/10-second retry and
shutdown authority. The shared retry function accepts the create operation for
two existing consumers, so MV admission and task-store errors have one lifetime;
lineage GC's operation and identities are unchanged. No per-row work, new worker,
state map or unbounded admission is added. Catalog queries and one existing
inventory check run only during bounded startup registration attempts.

Eight cheap component cases cover ready/missing/partial/error catalogs,
old/empty/error protocol inventory and task-store failure. The channel-driven
retry test retains system-account assertions and proves both admission and
store failures before success. Both roots pass 100 exact-name race repetitions,
and both owning packages pass normally and under race. Vet/lint/builds pass.
A local author-set memory limit conflicted with an unrelated default-limit
assertion; removing that execution-only override restores the full CN race pass
without changing any test assertion. Native 13/13 passes with exact frozen input
identity and normal authentication. The full existing two-CN BVT consumer is
validated independently against the new production build.


Final post-admission production-consumer proof: the exact entry passes twice
on the same two-CN instance, each with 18 bridge requests and 70 functional,
5 failure and 9 cross-CN checks. Independent cleanup counts are zero after
each run and all services exit. Bridge counters remain 28/32 blocks; no
coverage-policy change. All 11,042 frozen inputs match. The new source/result
ledger supersedes the earlier consumer-only capture. Final-head remote CI
remains separate.


## Upstream protocol 117 allocation collision

While cd5de147 ALL CI was running, upstream main 9f873692e0 added protocol
117 for collation-aware LEAST/GREATEST semantics and made this PR conflict.
Retain upstream's 117 contract and allocate 118 to native MVs across the
4.0.15 migration, coordinator publication, frontend DDL, plan authoring and
compile registration/scan consumers. A 117 node is the nearest unsupported
MV consumer even though its collation feature is supported. Catalog version
4.0.15, task executor 17, retry ownership and feature scope do not change.
The combined parser is regenerated from grammar; its one changed action
preserves upstream COLLATE expression nodes.

R3 compatibility closure: typed tests reject MV protocol/floor 117 and admit
118 while ordinary scans and collation's 117 contract retain their controls.
Reuse the native destructive driver with a clean real upstream 117 binary
and verify its actual protocol inventory via normal authenticated SQL. This
changes no concurrency primitive, state owner or hot path; previous race
proof remains evidence for those unchanged mechanisms. Final normal suites,
static checks, new binaries and native/BVT results are recorded separately.
Pre-merge 117 captures remain historical evidence for their source, not the
new allocation. The old cd5 CI cannot establish the subsequent merge head.


Final protocol-118 validation: eight complete owning suites pass (defines,
4.0.15, MV, frontend, plan, compile, mysql parser and CN); upstream 117
collation controls in function/PB plan pass. Six-package vet and configured
incremental lint pass with zero issues. Clean real upstream 117 and tagged/
normal 118 service builds succeed. The native run passes 13/13 groups in
324.495 seconds, including actual 117 inventory, no premature catalog/tasks,
interrupted/repeated upgrade, 118-vs-117 downgrade refusal and both commit/ack
crash windows. Its committed JSON includes the upgrade wait stack and native
protocol/admission diagnostic excerpts, not only local filesystem references.

The exact covered BVT entry passes twice on the same two-CN production
instance: each has 18 bridge requests plus 70/5/9 functional/failure/cross-CN
checks, independent zero cleanup counts, and all service processes reaped.
Bridge component counters are 28/32 regions and 50/56 statements; aggregate
coverage is a separate remote gate. All 11,061 frozen tracked non-documentation
inputs match. Pre-delivery new binaries report cd5de147 with modified=true;
the real upstream old binary reports clean 9f873692.

One focused local link attempt exhausted the author cache disk. Old generated
cache entries were removed while protecting active import inputs, then full
owning suites passed; no source/assertion workaround was made. The cd5 remote
SCA/coverage-UT/Proxy results passed before this merge, but cannot establish
its final-head CI. Whitespace checking uses upstream as the delta base; main's
existing trailing-space CHAR collation golden values are preserved unchanged.

The combined EXPLAIN consumer suite also passes (0.326 s), with its own
vet and configured incremental lint reporting zero issues. This closes the
additional auto-merged test consumer without changing source or existing
semantic input fingerprints.
