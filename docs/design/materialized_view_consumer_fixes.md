# MV native consumer corrections (review 5562448876)

This focused follow-up is based on `79484a43c2a4c984b9cb9e0290805822d6d6a275`,
whose main CI and both connector workflows passed. It preserves that revision's
format-2 semantic context, migration4.0.17/offset5, protocol119, task executor17,
and existing transaction/task owners. It addresses the three public SQL defects
reproduced by review5562448876; no new feature design or allocation is introduced.

## Requirements and ownership

| Reproducer | Required behavior | First owner / correction |
| --- | --- | --- |
| Native LOAD TABLE installs a previously dumped MV's objects without changing its recorded source publication | An MV remains read-only at every native import boundary | Shared table-dump schema admission rejects relation kind `m` for DUMP/LOAD before opening files, enumerating objects or calling native Write. Logical export remains available. |
| DELETE FROM an MV source is optimized to physical TRUNCATE and rejected by the dependency guard | Source DELETE remains valid row DML; source identity, last-good MV rows and provenance survive | Physical delete eligibility probes account-qualified source dependencies within the current statement transaction. A dependent source uses row DELETE. Index deletion receives the same eligibility decision. Explicit TRUNCATE keeps its existing guard. |
| SHOW CREATE TABLE emits internal MV storage properties that CREATE TABLE refuses | Standard export emits replayable CREATE MATERIALIZED VIEW and its current refresh policy | Both SHOW consumers call `mv.State.CreateSQL`. Standard SHOW obtains authoritative State; ALTER policy changes cannot be lost through stale relation properties. Snapshot export fails explicitly instead of combining historical identity with current policy. |

The modern DELETE planner's truncate hint only delegates to legacy planning;
it never performs a physical replacement. Dependency lookup therefore belongs
at the physical eligibility owner, once per otherwise eligible unrestricted
DELETE. It must not run once in that hint and again during legacy planning, or
once more for index maintenance. Ordinary SELECT, filtered DELETE, temporary
relations and MV refresh target deletes gain no dependency SQL. An eligible
ordinary unrestricted DELETE gains a bounded indexed `LIMIT 1` lookup and cached
catalog resolution. No zero-overhead claim is made.

Pre-migration catalogs remain usable: an unavailable MV runtime or absent
reverse-dependency relation preserves existing ordinary DELETE behavior. Other
metadata/query errors propagate. There is no new background worker, persistent
flag, dependency cache, retry loop or source metadata format.

## Test cases

| Layer | Case and independent oracle |
| --- | --- |
| Frontend UT | Existing native LOAD fixture adds kind `m` and requires exact NotSupported admission, before file reading; its mock permits no native Write. Existing unsupported-kind controls retain their typed error. |
| Planner UT | Dependent and independent sources, absent pre-migration catalog (nil and NoSuchTable), catalog error and query error. The delegation hint performs zero probes; physical eligibility performs exactly one, while the dependent DELETE node retains its relation identity. Empty/batched executor Results are closed and the pool is empty afterward. |
| Formatter UT | Manual and authoritative120-second policy with deliberately stale definition policy60; quoted database/view names. |
| Native BVT | Prepare DELETE before creating the MV, then execute after creation with experimental creation disabled. Require two affected rows, empty source, two last-good MV rows and unchanged successful sequence/run/full source TS. |
| Native BVT | Secondary lookup is empty after DELETE; reinserting the same unique keys succeeds. Direct unrestricted DELETE returns two affected rows, repeated empty DELETE returns zero, and explicit TRUNCATE still reports the dependency guard. |
| Native BVT | DUMP/LOAD with a file URI return the MV-kind rejection before file admission; full last-good rows/publication remain unchanged. The review's pre-fix valid-dump reproduction establishes the original bypass. |
| Native BVT | Standard and explicit SHOW DDL strings agree; replay a manual MV under another target and refresh it against the empty source. Read periodic policy60 first, ALTER to120, then repeat the same standard SHOW and replay; stored policy is120. |
| CI consumer | `-consumer-check` is in the existing materialized-view BVT entrypoint, with nonempty successful JSON admission and the existing coverage producer. No additional service deployment or threshold/exclusion change. |

The native mode owns one two-row indexed source and at most four views in its
own database. It reuses the existing authenticated deployment. Statements and
rows close immediately; cleanup drops views before database RESTRICT, using an
independent30-second context and retaining the original failure. The existing
client15-minute context bounds execution. A prepared statement predating MV
creation is a causal cache-boundary check, not scheduler timing speculation.

## Evidence and limits

`materialized_view_consumer_results.json` records exact local selections,
terminal statuses, source/binary identity, native assertions and cleanup. Native
binaries identify the pre-delivery79484a43 working tree with `vcs.modified=true`;
they are not represented as pristine binaries of the later publication commit.
A complete tracked input fingerprint bridges that publication boundary.

The previous experiment under `/workspace/matrixone-mv-pr` predates the remote
79484a43 update and is preserved separately. Its alternative semantic-context
patch, old native runs and prepared publication objects are not delivered or
claimed as proof for this follow-up. The first follow-up fixture compiler error
(missing test import) and disk-cache relocation diagnostics are retained. Later
terminal build/UT results decide success; an interrupted/missing result is never
counted as a pass.

These changes do not alter refresh transaction publication, catalog migration,
cron admission, task replay or allocation. Prior evidence for those unchanged
owners remains historical and is not relabeled as a new crash/upgrade run.
No new throughput, speedup, cold-cache, scale or zero-overhead claim is made.
New-head CI and reviewer acceptance are separate from local validation; no
review dismissal, self-approval or merge is performed.


Final local validation passed: complete MV/plan/frontend/compile suites; the
final single-probe physical path also passed the complete planner suite. Owning
package and final planner/client vet checks passed. Native final smoke passed
9/9 and the existing entrypoint passed111 checks (70 functional,5 failure,9
consumer,9 cross-CN,18 SQL bridge), with both CN processes exiting0 and zero
owned database/definition/dependency/history/per-view-task residue. Fresh main
`799523418e` adds four upstream commits without changing these allocations; a
local merge-tree preflight is conflict-free. That preflight is not a claim of
executing the unmerged main tree. Complete source fingerprints have zero delivery
mismatches. Publication-head CI remains a separately observed result.


## Integration with two-phase ALTER COPY

Main `e97e54ff72957f05abee2c1676218f19b7493f02` includes the two-phase COPY
implementation from #29798. Physical preparation precedes publication admission.
An MV can be created in that interval without changing the source definition or
data commit timestamp. Those upstream source checks therefore cannot substitute
for checking durable MV dependencies.

Keep the existing fail-fast check before creating/copying the private relation,
and check dependencies again after publication locks, RC snapshot advancement
and source identity/schema validation. The existing source-name locks exclude
subsequent pessimistic MV creation; optimistic SI retains its existing
transactional dependency write fence. This adds no gate, worker, cache or per-row
operation. Each check uses the existing statement transaction and closes its
bounded result; query cancellation aborts before lineage/source publication.

`TestMaterializedViewAlterCopyRechecksDependencies` reuses the upstream COPY
phase fixture. It covers an existing dependency, a dependency published during
preparation while source ID/version/data timestamp remain unchanged, an
independent source, and cancellation of the publication probe. The late cases
must reach physical preparation and publication admission; rejected cases must
publish no lineage, drop no source and rename no replacement. A source overlay
removing only the late guard provides the corresponding failing counterexample.
This is a database transaction interleaving modeled at an existing phase boundary;
it introduces no Go shared-memory synchronization. Native consumer check ten
also verifies public COPY rejection and unchanged relation inventory/publication.
The existing authenticated two-CN BVT entrypoint remains the execution owner.

Earlier 9/111-check captures above describe the pre-integration source only.
Fresh integration results are recorded separately; historical upgrade, recovery
and performance evidence is not relabeled as a new run.

Fresh integration validation: complete compiler suite **1,499 root tests**, exit0;
focused MV/planner/frontend/compiler **36 root tests**, exit0; owning compiler
and client `vet=all`, exit0; configured incremental golangci-lint2.14.0 reports
zero issues. The four COPY phase cases pass; removing only the
late guard makes the late-dependency subcase fail at its independent publication
oracle. A fresh production service and authenticated two-CN entrypoint pass
**112 checks** (70 functional,5 failure,10 consumer,9 cross-CN,18 SQL bridge),
plus separate10/10 consumer smoke. Entrypoint elapsed214.634s, both service
processes exit0, all five independent metadata/task cleanup queries return zero.
The11,214-file tracked input map outside `docs/` is unchanged after execution;
canonical SHA256 is `928d2d01820842e8100fac336eac67ebb12854fd7513fecfefe587691df4fd4d`.
See [integration receipts](materialized_view_copy_integration_results.json).
