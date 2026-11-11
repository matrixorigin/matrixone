# Transaction workspace memory admission on 4.2

Design revision: 1. Target 42aac8161cd0e2240966c610fb03f19ba6e6313e.
Reference main merge: 56ad29d8a5ebcd7359febb3e5c5575b2fce8eee6 (#32458).
Owning issue: #32008. Delivery: independent draft PR against 4.2-dev.

This crosses allocator, execution budget, workspace publication and object GC
ownership boundaries and therefore requires distinct design-first review.

## Contract and ownership

Every retained workspace copy acquires physical allocation capacity before backing
allocation on the existing per-CN HashBuild ledger. Physical Free/lease release
alone refunds the charge. Workspace size estimates remain spill decisions only.
Independent transaction generations share the ledger/registry, never the statement
lifetime. Snapshot workspaces sharing a Process have independent terminal states.
Statement rollback frees only new entries, keeping the account and prior writes.
Finalization closes accounts only after producers/borrowers quiesce and buffers
are freed. Sealing does not forgive live allocations; tombstones retain diagnostics.

Preserve HashBuild APIs and recovery floor/borrowed semantics. New backing grows
through existing acquireMemory; borrowing an admitted recovery floor does not pay
twice. Add committed/retired allocator snapshots and main's cached live headroom
observation. Under the budget lock, take backing checkpoint before sampling OS
headroom. Admission subtracts all aggregate promises not covered by retained
pre-sample backing and runtime safety. Non-MPool leases use the same observation.
This remains sampled protection, not a proof that all CN memory is accounted.

Add explicit CopyBatchWithAllocation; generic CopyBatch stays unchanged. Account
selection precedes allocations, never relabels backing. Use TxnWorkspace owner16,
preserve existing IDs. Rowid buffers transfer directly for matching provenance.
Foreign/raw WriteBatch provenance remains caller-owned. Copy/append/sort failure
cleans temporary destinations without changing sources or silently falling back.

## Publication and persistence

Stage merged payload and regenerated rowids before replacing entries. INSERT
skips obsolete old rowids. Maintain statement offsets and approximate counters.
Compaction stages clean republication and dirty rewrite records before publishing,
waits for all workers, returns errors rather than panicking, and keeps old records,
summaries and deletion marks on failure. No transaction lock across cleanup I/O.

Persisted-but-unrecorded objects retain canonical names and a cleanup owner.
Port the necessary main local unpublishedPending claim/reservation/drain closure,
including capacity admission before persistence hooks, failed publication handoff, shared
claims, GC producer fencing and bounded Engine.Close. Reuse GC pool/maintenance
loop and main defaults (500000 object obligations); do not invent a second queue.
A failed deletion/submission retains ownership; saturation refuses new obligations
before persistence. Clean objects and clone/CCPR shares are never newly-owned
orphans. Unknown commit results never delete possibly committed objects.
No network/on-disk format, user setting, general spill or production rollout changes.

## Validation and delivery

Deterministic tests cover copy/rowid/growth failures, backing races and recovery,
shared-CN competition, independent snapshot closure, real statement rollback with
prior writes and later successful writes, commit/unknown/cancel/repeated cleanup.
Publication tests cover partial merge, clean/dirty compaction failures, post-Sync
record refusal, deletion/submission failure, shared claims and close/producers.
SQL low-budget injection acts before account creation; the same fixed account
must fit the first/last small writes while refusing the middle large statement.

UT, focused race, SQL BVT, incremental static checks and final independent review
precede draft PR delivery. Exact-head CI, fresh admitted Linux 4CPU/16GiB cgroup QA
and performance are separate gates. Compare identical baseline/candidate builds
for serial DML, concurrent DML and mixed HashBuild: three paired runs, 30s warmup,
120s minimum measurement. TPS regression and p95 increase must each be <=5% per
scenario; unstable evidence is INCONCLUSIVE. Never disable accounting to pass.
No original experiment/PVC/receiver operations. Historical OOM closure is unproven.

## Design review

Revision 1 (2f84010c8ed5) independently reviewed and approved before implementation.
Mandatory implementation checks: headroomMu -> budget.mu refresh ordering; no
worker reacquisition of txn lock during compaction join; balance WaitGroup on
Submit failure; no delete/blocking Submit under queue locks; preserve staged
claims and object owners on every failure. Approval covers design only, not code.
