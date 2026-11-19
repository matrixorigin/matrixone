# Transaction workspace allocation admission, revision 1

Owner: issue #32008; implementation PR pending. Base: 745d25375de6.
Design-first review: PASS, reviewed before implementation in this task.

Invariant: a workspace-owned physical vector allocation is admitted against its
own lifetime account and the existing CN ledger before backing is allocated.
Free is the sole refund owner. Source and destination copies coexist and are
both charged. A workspace never closes the Process generation it borrowed.

Each workspace lazily opens an independent generation on the Process generation's
CN ledger, including snapshot workspaces sharing a Process. A separate mutex
serializes creation and terminal closure without reentering the workspace mutex.
States: absent -> open -> terminal. Terminal cannot reopen. Partial construction
closes its private generation. Final cleanup seals/publishes the account only
after workspace batches are cleaned; nonzero snapshots retain the existing
release-capable tombstone semantics. Statement rollback retains earlier writes.

Use the existing allocator-backed off-heap vector APIs, destination-selected
CopyBatch, and append-only owner catalog. Preserve generic CopyBatch semantics.
Account rowids before filling; append/merge/ALTER copies preserve provenance.
Keep existing approximate workspace/spill counters: they are not refund owners.
Capacity errors propagate; no unaccounted fallback or automatic retry is added.
No new configuration, protocol, storage format, worker or controller.

Alternatives rejected: source operator accounts have the wrong lifetime;
estimated reservation tokens duplicate physical lease refunds; a new RSS
controller duplicates main's existing dynamic CN headroom guard. Cost is one
lazy account per writing workspace, allocation metadata and existing admission
locking, with no per-row global lookup. Read-only workspaces allocate no account.

Tests: deterministic low-budget copy/growth failures and source independence;
independent generation close and CN competition; workspace terminal/rollback;
SQL low-budget rejection with unchanged rows and subsequent connection reuse.
Native build, UT, race, BVT and fresh-capacity isolated QA are separate evidence.
4.2 lacks these main mechanisms: assess dependencies separately, no blind cherry
pick. Original incident SQL/concurrency remain unknown; this closes a proven
admission gap and does not claim incident reproduction. Old source resources and
receiver are out of scope. Rollback is code rollback, with no data migration.

## Implementation review and validation

Closures reviewed: explicit downstream copy ownership (R2), independent generation
and close/concurrent lazy initialization (R3), workspace rowid/copy/merge cleanup
(R3), append-only owner compatibility (R1), inert database-keyed test limit (R1).
The raw WriteBatch transfer interface retains caller-owned batch provenance;
normal table INSERT/DELETE select the workspace owner at the copy boundary.
Generic CopyBatch remains unchanged. Existing spill staging and ownership
rollback remain authoritative; no release is inferred from approximate sizes.

Normal tests passed for mpool, util and process; disttae final full package
passed after multi-block fixtures explicitly selected an appropriate query
limit and the mirror-memory oracle included off-heap backing. Selected race
checks passed, including independent close and concurrent lazy initialization.
Incremental lint reported zero issues; vet-all passed for the changed packages.
The new BVT SQL passed direct MySQL execution on the built local service:
three capacity errors, unchanged rows after each refusal, and successful writes
on the same connection after removing the limit. It passed twice on the same
instance; cn_spill_byte_bound also passed exact structured output checks. The
mo-tester result-file comparison remains for CI. Initial service build attempts
hit toolchain switching and disk pressure; the explicit toolchain and completed
cache allowed the final build to pass. All local SQL services were stopped and
only their owned data was removed; logs and checksum manifests are retained.
Isolated 16 GiB/cgroup acceptance is NOT_RUN.
Microbenchmark (32 varlen rows, 1000 copies): generic 1163 ns/op, 3196 B/op,
6 allocations; workspace 3725 ns/op, 616 B/op, 4 allocations. This diagnostic is
not a transaction throughput acceptance result; QA must assess the hot-path cost.

4.2 assessment: the incident version lacks main's unified execution generation,
physical-growth headroom and destination-account lifecycle closure. This patch
must not be mechanically cherry-picked without a separate dependency and
release-specific failure-path review. No 4.2 code is changed here.

Local SQL evidence: /private/var/folders/_7/28p3df355x96blk88tp_0w4r0000gn/T/txn-workspace-bvt-7o0zgimb (evidence-manifest.json, sql-oracle.json, repeat-oracles.json, cleanup.json).
