# Race UT critical path: bounded execution at existing owners

Design revision: `pr29760-design-v4` with `pr29795-opt-in-v1` and bounded
embedded-wave amendment (2026-10-09).
Tracking: [#29562](https://github.com/matrixorigin/matrixone/issues/29562).
Baseline implementation: [#29760](https://github.com/matrixorigin/matrixone/pull/29760).
Current bounded embedded-wave implementation: [#29807](https://github.com/matrixorigin/matrixone/pull/29807).

The correction and single-lock cleanup amendment were approved for implementation
by `gpt-6.1-sol`, reasoning `xhigh`, in design session
`/root/design_pr29760_delivery`, against source
`817a2e053e4e89dda4a61af0ebd7801818e5008e` and base
`aee2a0b3764781a8ca908618548dfef75e336263`. The final implementation requires
its own overall review and validation. This revision supersedes the scheduling
snapshot assumption in [UT racing owner costs](ut-race-owner-costs.md).

## Eight-batch issues execution (2026-10-14)

This amendment changes the Make/CI issues default to eight round-robin batches,
with at most two test processes. Each process retains a complete two-CN cluster
and all selected roots, subtests, assertions and race instrumentation. One or
four batches remain serial by default; direct script invocation still defaults
to one batch. Embedded execution remains serial. The new Make default disables
embedded prebuild and uses its existing full-scope `go test -race -p1` path.
One/four-batch rollback modes and explicit embedded-pool users retain their
previous prebuild default; callers can still override `UT_PREBUILD_EMBEDDED`.

The existing partitioner, process admission, dispatcher, shared package deadline,
TestMain guard and failure/drain owners remain authoritative. Eight batches
must enter the existing batched dispatch, rather than the ordinary package path.
The eight-batch pool scopes `GOMAXPROCS=2` to issues execution after preparation.
It restores the caller's environment on return and does not cap compilation.
The default no longer overlaps an embedded compiler with two issues clusters.
Four-batch pool opt-in retains its previous CPU policy.

The cost being addressed is long process lifetime plus serialized independent
root groups. Four pooled batches alone improved time but retained more memory.
Shorter generations reduced measured total CPU and memory occupancy enough to
offset four additional cluster starts in the local experiment:

| Issues execution | Wall seconds | CPU seconds | Peak PSS GiB | PSS integral GiB-seconds |
|---|---:|---:|---:|---:|
| Four serial, GOMAXPROCS=8 | 570.93 | 926.20 | 2.84 | 939.77 |
| Four pooled, GOMAXPROCS=2 | 304.36 | 761.96 | 4.14 | 833.49 |
| Eight pooled, GOMAXPROCS=2 | 301.21 | 717.60 | 3.27 | 677.00 |

These are standalone same-machine race measurements, not completed CI timings.
The eight-batch phase peak still exceeds the serial phase peak. Overall memory
acceptance therefore requires the combined issues/prebuild peak to stay below
the unchanged whole-run peak, with lower CPU and anonymous/PSS occupancy. The
unchanged same-machine SQL package supplies a measured peak floor; comparing
different runners or anonymous memory against total cgroup memory is invalid.

The first combined experiment included ten-package prebuild in both arms,
identical starting Go caches, an actual eight-CPU quota, an 8-GiB memory limit
and no swap. It improved elapsed time from 673.10 to 367.37 seconds and CPU
from 1040.61 to 848.50 seconds, but failed the memory gate: process PSS peak
rose from 4.41 to 5.72 GiB and cgroup max events rose from zero to 965. Neither
arm had an OOM. The peak occurred during compilation; after compilation the
candidate PSS peak was 3.13 GiB. That overlap is rejected for the new default.

The revised resource experiment runs issues first and then compiles all ten
embedded packages, retaining their binaries as a conservative resource proxy.
It is not a timing measurement of complete embedded test execution. Production
uses the existing ordinary embedded command, which compiles and executes with
`-p1` and removes completed test work directories. No new scheduling state,
resource fallback or cleanup path is introduced. Compiler budget stays eight.
The revised experiment passed the resource gate at base `a33dcfb2e5`:

| Issues plus ten-package compilation | Wall seconds | CPU seconds | Peak PSS GiB | Peak cgroup GiB | PSS integral GiB-seconds |
|---|---:|---:|---:|---:|---:|
| Four serial, overlapping prebuild | 673.10 | 1040.61 | 4.41 | 7.51 | 1277.67 |
| Eight pooled, overlapping prebuild (rejected) | 367.37 | 848.50 | 5.72 | 8.00 | 976.67 |
| Eight pooled, compilation afterward | 385.92 | 802.20 | 3.31 | 5.48 | 812.60 |

The final arm had zero max/OOM events and zero CPU throttling. Anonymous memory
occupancy fell 32.4%; some/full memory PSI fell from 11.60/11.51 seconds to
1.08/1.08 seconds. Its issues-phase anonymous peak exceeds the serial issues
phase, but remains below the unchanged SQL package's measured anonymous peak.
The process and cgroup peaks both decrease against the combined baseline.
Inventory comparison passed all 179 roots and 1548 terminal tests, with no
failures, skips, duplicate terminals or malformed JSON. All ten compiles passed.

Recent CI issues phases took 28–35 minutes, while prebuild overlapped about
two minutes. Applying the standalone local improvement and subtracting that
lost overlap projects roughly 11–14 minutes of CI saving. This is an estimate;
no completed optimized CI run or full embedded-wave time is claimed. Design
and source review used `gpt-6.1-sol` with `xhigh` reasoning.

All 179 roots and 1548 terminal test results must remain; four subtest names contain a runtime PITR timestamp, which is the
only normalization allowed in result-name comparison. No runtime failure reruns
completed roots. `UT_ISSUES_BATCHES=4 UT_ISSUES_BATCH_PARALLEL=1` restores the
previous execution policy.

The following amendments retain the historical decisions and measurements;
their serial Make default is superseded only by this bounded eight-batch mode.

## Audited DML/SQL generations (2026-10-14, design v1)

`gpt-6.1-sol`, reasoning `xhigh`, reviewed this design in
`/root/ut_race_design_20261014`. Adoption of a new default requires the complete
embedded-wave resource comparison below; isolated package results are insufficient.

The target is the ordinary embedded stage's long process lifetimes and serial
independent roots. DML has 44 roots and 200 terminal tests; SQL has 41 roots and
270 terminal tests. Keep every topology, datum and oracle. DML's existing
intra-root parallelism is preserved. SQL's real TestMain must execute for inventory
and every batch, close its canonical fixture and propagate cleanup failure.

Reuse the issues batching executor, partitioner, shared package deadline, process
admission, report consumption and drain protocol. Its caller supplies batch
count, process limit and execution CPU budget; preparation retains the caller's
budget. Derive checkpoint stage from the current command. Unknown TestMain
contracts remain ineligible; the exact SQL package receives an internal
cleanup-only permission following its lifecycle audit. There is no environment
override for this permission and no inferred parsing of a TestMain body.

One `UT_EMBEDDED_BATCHES=1|4` option governs audited DML/SQL roots on the ordinary
path. `1` restores the exact original full-scope Go command and order. `4` removes
only those audited packages present in the scope, runs all remaining packages in
one ordinary command, then compiles and executes each audited package in order:
four round-robin batches, at most two processes, GOMAXPROCS=2 per process.
Do not invoke Go with an empty ordinary scope. Explicit prebuild execution keeps
its existing package policy. Make defaults to `4` after the complete-wave and
full-UT gates below passed; direct script invocation retains `1`.

Publish a batched report/binary only after the preceding command joins; consume
and release them before the next package. Ordinary test failure still executes
the remaining selected packages and retains failure. Status 125 or failed drain
stops admission and preserves owned diagnostics. Preparation may fall back to
the current package before its first root; runtime failure never retries roots
or reruns the complete scope. No new scheduler, background owner or resource
fallback is introduced.

The SQL experiment exposed account PITR retained by CDCNoPrimaryKeyRejected.
LegacyPartitionColumnLimitCopy passes alone but fails immediately after CDC:
the retained PITR enables historical lineage's intentional DROP/ADD restriction.
Clean CDC and PITR in the existing deferred cleanup before dropping its database;
do not alter production lineage policy, reset the fixture or reorder roots.
Also check DML's streaming result error after iteration, preserving both EXCEPT
directions and the zero-row oracle.

Before adoption, compare issues plus the complete ten-package embedded wave,
including compilation, with identical cache starting conditions, eight CPU
cores, 16 GiB, and no swap. CI run `38538693413` records an eight-CPU quota and
`memory.max=17179869184`; use that actual runner budget for both variants. The
initial 8 GiB baseline encountered memory-limit events and was stopped; its
partial timing is not performance evidence.

While the issues optimization remains in this unmerged PR, compare the original
four serial batches with overlapping prebuild against the combined proposed
policy. Count that complete difference once. Historical issues savings cannot
be added again; after their merge, any further optimization needs the new
baseline. Run variants sequentially. Require identical root/subtest terminal
maps, retained failure propagation, lower CPU and memory occupancy, and no
increase in whole-wave memory peak or max/OOM events. Tests must cover rollback,
scope subsets, budgets, TestMain cleanup failure, preparation fallback and
undrained ownership through the existing runner harness. Before delivery, also
run the complete repository race UT through the real Make entry point and
incremental SCA for the changed package closure.

The matched eight-CPU/16-GiB pair saves 454.06 seconds (1717.49 to 1263.43)
for issues plus all ten embedded packages, with identical 475 roots and 2636
terminal tests. CPU falls 13.38%, cgroup peak 15.42%, and PSS occupancy 16.65%.
This measures the changed wave; the whole-CI ten-minute target is unverified.
The complete local Make race selection subsequently passes with explicit batch
count `4`, and incremental SCA passes for all five changed Go packages.

## PR29795 amendment: optional bounded process waves

Implementation: [#29795](https://github.com/matrixorigin/matrixone/pull/29795).
The correction design was approved before implementation by `gpt-6.1-sol`,
reasoning `xhigh`, in CLI session `01a12177-99a4-71b1-804b-41dc42f57512`,
against source `0094ef382a04a50773948cd8b5ab585b3e3ce53d` and base
`d2a0055ebe1a0b2423368e046c1812c18f6face8`. Final implementation review and
validation are separate gates.

This amendment supersedes only v4's removal of optional two-process execution
and exclusive-only process admission below. The inventory, deadline, report,
cleanup, CDC, HAKeeper and engine-fixture contracts remain. Four serial issues
batches measured 515.1 -> 407.8 seconds in PR29760; that benefit does not
belong to the new pool. A historical embedded prototype subset (embed and
sqlintegration) measured 545.5 -> 372.3 seconds with two processes. The
historical 1057-second full serial wave included an arrowload failure.

The latest successful single-runner trace (2026-10-09) measured issue batches
at 303.2, 143.6, 220.8 and 335.3 seconds, or 1002.9 seconds serialized.
The explicit two-process option uses round-robin root partitioning; the serial
default remains contiguous. Replaying
the 159 top-level issue roots from that trace through the actual partition and
two-process refill rules gives round-robin group totals of 290.7, 212.7, 310.1
and 183.8 seconds, with a 522.8-second pool makespan. The same roots in the
contiguous layout produce a 635.7-second pool makespan. Against the 997.3
seconds of serialized root time, the round-robin estimate removes about 474.5
seconds (7.9 minutes) before process startup, CPU contention and cluster
admission overhead. This is a schedule estimate, not a CI result; the first
matched Linux run must record wall time, CPU throttling, peak memory and OOM/max
events before any further parallelism is considered.

The same trace recorded 1705.7 seconds for the embedded package wave when run
serially. The Linux embedded pool keeps `pkg/embed` as the first, exclusive
package, then admits at most two ordinary package processes. The known
high-footprint packages (`pkg/tests/issues/isolated`,
`pkg/tests/sqlintegration`, and `pkg/tests/sqlintegration/multicn`) also run
alone, so no second process is admitted while one is active. Replaying those
durations through that admission policy gives a 1537.6-second makespan, or
about 168.1 seconds (2.8 minutes) of estimated saving. These figures are
schedule estimates, not a CI result.

The classification currently uses whole-cgroup samples. At the largest sampled
current usage for `pkg/embed`, `isolated`, and `sqlintegration`, respectively,
the cgroup contained 13.96, 13.22, and 12.80 GiB. File cache accounted for 9.19,
9.43, and 9.61 GiB of those samples; anonymous memory accounted for 4.03, 2.99,
and 2.35 GiB. These totals measure runner pressure, not individual package
footprint, and do not establish which package combinations require exclusion.
The current classification remains a conservative restriction while matched
complete-wave measurements determine which exclusions are necessary.

Current local issues measurements use the same race binary and complete scope,
with sequential arms in separate 8-CPU/16-GiB cgroups and swap disabled:

| Four issues batches | Elapsed seconds | CPU seconds | Peak GiB |
|---|---:|---:|---:|
| Contiguous, serial | 420.05 | 581.89 | 2.16 |
| Round-robin, two processes | 258.21 | 758.03 | 3.74 |

Both arms passed all 159 roots with equivalent 1424 terminal test results and
no OOM or memory-limit events. The pool saves 161.84 seconds but uses 30.27%
more CPU time and 73.02% more peak memory in this wave. This establishes a wall
time benefit and a resource tradeoff, rather than completion of the combined
issues/embedded optimization. Complete-wave results must justify the runtime
budget and exclusion policy before resource acceptance is closed. CPU totals
are summed across the sequential waves; their overall peak is the largest
wave peak. Memory occupancy over time is reported separately from peak memory.

This change does not merge embedded packages into one fixture or remove the
multi-CN package. Embedded package `TestMain` and lifecycle hooks are
process-scoped, while the multi-CN cases prove routing, metadata, index, and
cancellation contracts that a single-CN fixture cannot cover; that package was
about 78 seconds in the same trace and is not the critical path.

| Choice | Decision |
|---|---|
| Serial batching only | Lowest complexity; retains the established batching benefit. |
| Serial default with optional bounded pool | Selected until matched full-wave CPU/memory evidence passes the adoption gate. |
| Linux default with bounded two-process pool | Deferred: resource acceptance has not passed. Explicit opt-in retains the bounded scheduler for experiments. |

For Makefile/CI and direct runner use, `UT_ISSUES_BATCH_PARALLEL` and
`UT_EMBEDDED_PACKAGE_PARALLEL` both default to one on every platform.
Two-process execution requires an explicit opt-in for each wave; one wave's
choice does not implicitly enable the other. The issues pool requires four
batches. `UT_ISSUES_BATCHES=1` retains single-process issues execution.
Invalid and explicitly empty values fail before preparation. Preparation failure
falls back before execution; runtime failure never reruns completed roots. Pool
mode uses round-robin roots to avoid a contiguous long-tail batch; serial mode
retains the historical contiguous partition.

The existing dispatcher owns children, watchdogs and reports for both wave
types. It admits at most the configured number of commands, reaps completed
owners before refilling, and preserves the existing package deadlines. Pooled
children receive `MO_TEST_CLUSTER_ADMISSION_POOL_SIZE=2`. The admission manager
uses that environment as its only pool-size source: it takes a shared gate and
one exclusive slot, then publishes the lease. Ordinary exclusive owners exclude
the whole pool. Unsuccessful attempts close their handles; cancellation ends
retry waits. Process admission and same-process borrowing are independent
permissions. A second cluster must explicitly opt into local borrowing even
inside the pool; borrowing retains the active lock mode and increments
references, so the bound counts processes rather than clusters. Final release closes the slot and gate; lock
files are not unlinked while a holder can exist.

`cluster.Start` is the single admission boundary before services start. Close
and failed-Start rollback retain admission until service cleanup finishes. Each
wave uses one lock namespace and fixed pool size while leases live; manual
mixed-size pools and live reconfiguration are outside this contract. Process
exit releases OS locks. There is no new persistent, wire or SQL contract.
Cancellation and timeout keep bounded TERM/KILL drainage; unproven drainage
preserves status 125 and artifacts. Scheduler diagnostics go to stderr while
the authoritative report remains Go test NDJSON.

Acceptance uses the real Make-to-runner boundary for defaults, rollback and
invalid overrides. The existing three-package mock uses phase barriers to prove
two distinct live children, refill while the second remains blocked, and a
checkpoint-derived maximum of two outstanding commands. It verifies exactly
three terminal pass events and artifact cleanup; forcing serial dispatch must
fail the regression. Existing admission tests use the real environment parser
and cover cross-process slots, exclusive competition, cancellation and reuse.
Run affected normal/race packages and incremental static checks; reuse unchanged
embed lifecycle evidence. No new cluster fixture or SQL BVT is needed.

The extra slot locks and retry work apply to either explicitly enabled process
pool; other test lifecycles keep the existing exclusive admission path. Existing
subset measurements motivate testing the two-process candidate. Matched complete
waves must record elapsed time, CPU throttling, peak memory, and OOM/max events,
and establish the combined resource benefit. If evidence violates the runner budget,
`UT_ISSUES_BATCH_PARALLEL=1 UT_EMBEDDED_PACKAGE_PARALLEL=1` is the immediate
serial rollback.

## Original PR29760 design (v4)

The following sections retain the original decision and evidence; the scoped
PR29795 amendment above governs optional scheduling and process admission.

## Problem and selected boundaries

Long-lived issues race processes retain fixture and runtime resources. The
reported matched issues-only measurement is wall 515.1 -> 407.8 seconds, CPU
792.2 -> 512.7 seconds, sampled RSS 3.42 -> 2.26 GiB with four serial processes.
These are author measurements, not a whole-job or general memory claim.
HAKeeper scheduling unnecessarily copies display configuration. CDC ACK also
previously modified scratch owned by the queue worker, creating a race and
allowing delayed unversioned reads to replace acknowledged progress.

Keep four issues batches after one binary build, the common prebuilt executor,
serialized cluster admission, the local scheduling projection, CDC publication
corrections, and the engine fixture notifier join. Remove optional two-process
embedded execution: it has no established whole-job or memory-headroom benefit,
but adds shared-lock modes, slot handles, retry/rollback states and runtime
concurrency overrides. A future concurrent mode requires its own evidence and
review; this revision defines no participant environment variable or slot files.

Alternatives considered: a full runner revert loses the measured batching
benefit; separate executors duplicate process/report ownership; retaining unused
participant admission leaves an extra protocol to maintain; caching scheduling
snapshots introduces stale state and another lifetime owner. The selected
changes reduce work and state at the existing owners.

## Execution, reports and failure boundaries

The compiled binary owns the runnable inventory. Every Test/Fuzz/Example root
appears once; a root retains all subtests, fuzz seeds and internal stress rounds.
Issues roots are partitioned contiguously into four serial batches. TestMain or
GOFLAGS disables batching before execution. Inventory begins one absolute
package deadline after compilation; preparation fallback uses its remaining
budget. Runtime failures never rerun earlier roots. `UT_ISSUES_BATCHES=1` retains
the single-process rollback option. Compile time remains inside the existing
outer runner budget, rather than the package execution deadline.

The common helper executes serial issues and embedded packages and the existing
engine shard wave. It publishes each child/watchdog PID before replaying TERM.
Each owned group is stopped with bounded TERM/KILL escalation before joining.
Failure to drain returns status 125, preserves diagnostics and stops subsequent
stages. Watchdog expiry remains a failure even if the test handles TERM and
exits zero. Ordinary failures remain visible in both status and JSON reports.

Report writers retain ownership until stopped. The existing transactional report
consumer chooses a complete ready report or recoverable shard files and appends
one representation. Pending TERM is held across ownership transfer; a failed
append retains its source. The correction adds no second report store or process
scheduler. Prebuilt CURRENT, engine, plan and prebuild helpers publish one
completion status per existing owner after normal return or fully drained
cancellation. The parent resets that acknowledgment before admission and checks
it against the reaped status and drains the helper group before releasing
ownership, including preparation/fallback descendants. Missing or inconsistent
completion is terminal 125, even when an abrupt exit numerically matches an
ordinary test failure. Compiler/discovery/shard groups must drain before their
PID slots are cleared; final engine/plan artifact deletion belongs to the existing
parent report consumer. Ordinary CURRENT and LIGHT exit contracts are unchanged.
An uncatchably dead helper cannot clean its independent groups; this gate retains
evidence and forbids later admission but does not claim immediate reclamation
without a child-group handoff. ARM cancellation timeout diagnosis remains open.
No lock files are unlinked while an owner can still hold them.

## Cluster and fixture lifecycle

One process owns one exclusive OS-backed cluster-lifecycle lock. Existing
AllowConcurrent permits intentional same-process borrowing, never a second
process. Acquisition checks cancellation before changing references. The pinned
flock v0.8.1 TryLockContext either acquires the fresh lock or returns an error
with no held lock; failed attempts close their unheld handles. Only successful
acquisition publishes a lock and lease to the manager and embed/service callers.
Failed release retains ownership for retry; the final successful release or
process exit releases the lock.

Embedded execution stays serial at its original CPU and test parallelism.
Compile-only overlap starts no cluster. The engine fixture owns its notifier
context and ticker, cancels and joins it on success/error before dependent cleanup,
and uses the existing fileservice constructor rather than discarded caller setup.

## HAKeeper and CDC authority

Dragonboat serialization and state-machine Lookup remain the HAKeeper authority.
Scheduling copies store records without CN/TN/proxy display configuration and
retains the LOG WAL recovery status item. Existing deep copy detaches retained
maps, items and protobuf unknown fields. Store state, recovery generations,
replicas and scheduler decisions remain unchanged. StateOnly assertions and
ordinary full/nil queries keep their separate existing contracts; public full
getters and GET_CLUSTER_STATE are complete. This local query adds no Raft command,
RPC schema, persisted format or upgrade/mixed-version protocol. Cost is bounded
by store cardinality; temporary store maps replace configuration traversal.

The CDC queue worker exclusively owns batch scratch. The updater mutex owns
published timestamp/source-generation tuples; ACK does not mutate scratch.
Legacy read, insert and stopped-queue fallback preserve owned progress.

A successful guarded checkpoint can be a durable no-op after a remote owner
claim, so its completed-write cache is optimistic. Fresh typed reads and owner
claims publish the database tuple rather than taking a maximum with that cache.
Outstanding reads share a reader count and publication revision; map entry
identity defines their lifetime. ACK and non-retiring local owner transitions
advance revision under the publication mutex. A revision conflict permits one
additional SELECT within the original deadline, without repeating the claim
UPDATE; a second conflict returns a retryable error. Retirement and claim-loss
eviction detach observations. Identity mismatch is terminal and never rereads.
Task deletion blocks admission and publication. Final release removes only the
matching current entry; no idle-key history or second progress store is retained.
Normal reads, claims and checkpoint flushes add no SQL statements.
SQL and fence callbacks execute outside the cache mutex.

This amendment was approved by `gpt-6.1-sol / xhigh` against head
`79e10abff459f4d3a4121194639052e8caa3991c` in CLI session
`01a11f9f-c469-7a63-be78-f3b489af1d8e`. It replaces v2's assumption that
source-generation/timestamp ordering establishes durability. The regression
control models another CN winning ownership between the fence check and guarded
SQL: durable progress stays 100 while the old updater caches attempted 200.
Both a fresh read and replacement claim must return and install 100.

The v4 cleanup removes the redundant retirement flag and unobservable retirement
revision update. Map identity already supplies the terminal check, and outstanding
readers retain the old object while replacement admission allocates a fresh one.
This cleanup was approved by `gpt-6.1-sol / xhigh` against head
`755216934e9917aca9bd66fb7d7333401fe64349` in CLI session
`01a11ffd-0e30-7023-a940-fd129e8bd155`.

## Acceptance and validation

- Runner: exact root/seed/example inventory; remaining-budget fallback; discovery
  cancellation; expiry; failed drain; interrupted report publication and helper
  status 125, using the existing isolated harness and `optools -race`.
- Admission: process exclusion/death, borrowed lease retention, cancelled
  acquisition and repeated release; embed/service Start/Close rollback owners.
- CDC: barrier-controlled ACK versus legacy publication, malformed/absent durable
  tuples, remote guarded no-op, delayed typed read/claim versus ACK, retirement,
  bounded conflicts and owner replacement; existing real generation
  replacement consumer under race mode.
- HAKeeper: independent full/projected snapshots and unknown fields, equal WAL
  pending/coordinator decisions, leader checker and remote recovery bootstrap.
  Paired full/scheduling benchmark measures cost without starting services.
- Engine fixture: existing InitEnginePack transfer consumers exercise notifier
  completion and cleanup. Preserve original positive and unhappy-path oracles.
- Complete PR delta: configured incremental SCA including vet and lint, relevant
  owning/consumer UT with race and controlled native prerequisites. Blocked or
  partial runs are not passes. Reuse unchanged evidence by semantic fingerprint.

Delivery links the exact committed design revision and records a separate
`gpt-6.1-sol / xhigh` overall review. Full CI savings and new embedded concurrency
remain outside acceptance; they require fresh measurements before adoption.
