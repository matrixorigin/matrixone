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
