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
