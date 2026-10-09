# Race UT critical path: bounded execution at existing owners

Design revision: `pr29760-design-v1` (2026-10-09).
Tracking: [#29562](https://github.com/matrixorigin/matrixone/issues/29562),
[#29752](https://github.com/matrixorigin/matrixone/issues/29752).
Implementation: [#29760](https://github.com/matrixorigin/matrixone/pull/29760).

The correction was approved for implementation by `gpt-6.1-sol`, reasoning
`xhigh`, in design session `/root/design_pr29760_delivery`, against source
`1c35457140ae46492069eaa44f2d20a0b27b6867` and base
`aee2a0b3764781a8ca908618548dfef75e336263`. The final implementation requires
its own overall review and validation. This revision supersedes the scheduling
snapshot assumption in [UT racing owner costs](ut-race-owner-costs.md).

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
scheduler. Earlier compiler/helper cancellation paths remain under their existing
outer ownership contract; this is not a claim that every preexisting helper has
been redesigned. No lock files are unlinked while an owner can still hold them.

## Cluster and fixture lifecycle

One process owns one exclusive OS-backed cluster-lifecycle lock. Existing
AllowConcurrent permits intentional same-process borrowing, never a second
process. Acquisition checks cancellation before changing references. A lease
without an acquired lock cannot authorize borrowing. If acquisition and rollback
both fail, Acquire returns a cleanup-only lease with the error; embed and service
callers retain it so Close can retry. Failed release retains ownership; the final
successful release or process exit releases the lock.

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
Legacy read, insert and stopped-queue fallback preserve owned progress. Delayed
typed reads/claims compare the existing generation/timestamp ordering before
publishing. Durable SQL and owner fences retain their existing authority. SQL,
callbacks and durable fallback reads execute outside the cache mutex. There is
no new worker, cache, queue, schema, transaction owner or retention budget.

## Acceptance and validation

- Runner: exact root/seed/example inventory; remaining-budget fallback; discovery
  cancellation; expiry; failed drain; interrupted report publication and helper
  status 125, using the existing isolated harness and `optools -race`.
- Admission: process exclusion/death, borrowed lease retention, cancelled
  acquisition and incomplete cleanup; embed/service Start/Close rollback owners.
- CDC: barrier-controlled ACK versus legacy publication, malformed/absent durable
  tuples, generation ordering and owner replacement; existing real generation
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
