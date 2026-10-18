# Prepared metadata and materialized-view retirement

This follow-up to PR #30085 fixes two failures found during cross-CN validation
of 14a213e5. It introduces no catalog fields, protocol change or scheduler.

## Prepared SHOW CREATE

SHOW CREATE TABLE lowers to a SELECT containing literal DDL. A prepared handle
therefore retained the old refresh policy after another CN executed ALTER
MATERIALIZED VIEW, although direct SHOW returned the new policy. The source
table schema version and preparing session's DDL generation do not describe
this metadata change.

The original SHOW CREATE TABLE AST now requires binding on every EXECUTE.
Both physical-compile admission sites honor the same predicate. Ordinary SHOW
already bypasses the SQL plan cache; ordinary SELECT keeps its cache policy.
The native consumer prepares the 60-second policy, changes it to 120 on the
second CN, then checks that the same prepared handle equals fresh direct SHOW.
An independent probe also checks manual-to-120 and the previous prepared DELETE
regression without intervening DDL on the preparing connection.

## Metadata lifecycle owner

An SI native run passed the 11 consumer assertions but failed database cleanup:
all four DROP MATERIALIZED VIEW calls returned successfully, yet a stored_copy
definition survived with no physical MV or dependency rows. Restart and
synccommit preserved the orphan. Its advanced next_cleanup value identified
the unfenced maintenance cursor update. This failed run is retained as a
negative result, not counted as passing validation.

All service writers now participate in Service.metadataTxn. It locks the
existing account lifecycle row using SELECT revision ... FOR UPDATE in
PESSIMISTIC/RC. DROP already uses this row in PESSIMISTIC/RC mode. The lock
covers refresh reservation, failure status and rescheduling, task admission
failures, expired-run recovery, history cursor advancement and ALTER policy.
ALTER loads the live definition inside that transaction and preserves captured
refresh semantics. Frontend authorization remains the admission owner.

Data publication holds an outer read-only RC fence transaction while the
original, separate optimistic SI transaction replaces rows and publishes
provenance. Its minimum snapshot is the admitted fence snapshot. SI source
reads never advance: the pre/post snapshot equality assertion remains in place.
The outer retry owner allows at most three attempts; there is no inner retry
loop. If inner commit succeeds but the outer completion fails, existing run-ID
idempotence and committed-state recovery recognize the published result.

The fence does not INSERT account state or change dependency revisions. A
revision-writing trial caused unrelated SI CREATE conflicts. A trial that
locked only metadata work produced duplicate definitions during concurrent
publication. A same-transaction pessimistic SI trial could reject both
publishers while acquiring the account lock. These negative results explain
why final lock admission occurs in RC before creating the fixed SI snapshot.
Bounded terminal-history deletion only removes terminal rows and retains its
existing status predicate.

## Ownership, failure and cost review

The SQL executor owns transaction commit/rollback and lock release. Existing
timeouts bound lock waits, cancellation aborts the transaction, and the existing
retry owner allows at most three attempts. No extra worker, queue, cache or
retry layer is added. Fence failure prevents downstream metadata mutation;
failure recording cannot overwrite a completed refresh. Missing retired rows
are never recreated by maintenance. The added work is one scalar row lock per
metadata transaction and one additional read-only transaction around SI
publication. Refreshes for an account serialize across CNs, extending the
existing per-CN account admission rule. History's 16-key and two 128-row page
bounds are unchanged.
The physical refresh fixture asserts optimistic SI, a nonempty minimum
snapshot and an outer lifecycle lock held until data replacement completes.

The new lifecycle fixture covers success, guard failure, cancellation and
cursor failure, verifies the same transaction, tenant qualification, mode and
isolation, and checks zero outstanding pool allocations. Existing admission,
recovery, retention and backlog fixtures retain their distinct assertions.
The 11th consumer reuses the existing topology, rows and statement helper; it
adds no sleep, cleanup retry or catalog repair. See the adjacent results file
for final source identity, exact execution modes and evidence limitations.

## Merge and evidence scope

The main merge includes the empty-CTE-body parser fix. The grammar merged
cleanly; the conflicted generated mysql_sql.go was regenerated with the
repository goyacc command. Parser, planner and frontend package tests validate
that merged source. Native RC/SI and crash tests use the pre-merge candidate
binary (14a213e5 plus the production fixes); all five changed production Go
files and the consumer client are byte-identical to the merged delivery. This
is scoped evidence, not a claim that the candidate binary is a pristine final
merge build. The final GitHub build/BVT validates the merged server.

The new --refresh-only mode reuses the existing recovery fixture and crash
oracles, including durable response loss, both kill points, same-run task
acknowledgement and independent cleanup. Full upgrade mode remains available
and still requires its old binary. No migration/protocol inputs changed in
this follow-up, so the earlier upgrade results remain historical evidence.
No new benchmark is claimed. Query-read paths retain their earlier behavior;
the extra fence transaction and cross-CN account serialization can increase
refresh latency under concurrent load.

| Closure | Risk and contract | Evidence |
| --- | --- | --- |
| Prepared SHOW | R2: rebind live metadata; retain normal cache behavior | Same prepared handle, other-CN ALTER, fresh SHOW equality; frontend tests |
| Metadata/publication | R3: shared retirement lock, fixed SI source, bounded release/retry | Failure/cancel fixtures, transaction options, snapshot drift, lost outer response, race, two-CN native tests and crash/replay |
| Main parser merge | R2: retain both grammar changes | Regeneration and parser/planner/frontend package tests |
| Recovery selection | R1: same fixture/oracles; skip only upgrade in refresh-only mode | Real refresh-only native run and Python syntax/help |

The SQL executor retains commit/rollback ownership of both transactions. The
outer fence never requests a resource held by its inner SI transaction, and
inner optimistic DML does not acquire the account fence, avoiding a lock cycle.
DDL waits behind the outer fence and release follows callback completion,
error, cancellation or CN recovery. There is no new Go goroutine/container,
operator signal or pipeline protocol in this change. The account lock is an
intentional correctness/throughput tradeoff; no unbounded waiter map or new
persistent lifecycle state is introduced.
