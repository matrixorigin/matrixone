# Database branch identity on 4.2

Revision: `PR32723-BRANCH-IDENTITY-42-2026-11-13-r1`.
Tracking: [#26068](https://github.com/matrixorigin/matrixone/issues/26068),
[PR #32723](https://github.com/matrixorigin/matrixone/pull/32723), and SDK
[PR #32671](https://github.com/matrixorigin/matrixone/pull/32671).
Reference implementation: main commit `8ab6ea6e6f53e7901729a4f0c28e5e805b75e2cc`
([#28272](https://github.com/matrixorigin/matrixone/pull/28272)). This is a
separate 4.2 adaptation; the upstream document's approval is not claimed for it.

## Problem and invariant

The 4.2 delete validator recognizes a branch database only through active
ordinary-table receipts. A successful empty clone, view/sequence-only clone, or
branch whose last ordinary table was dropped cannot be deleted with its matching
DATA BRANCH statement. This was reproduced by the SDK online suite on v4.2.8.

Every database created by DATA BRANCH CREATE DATABASE must retain durable branch
identity until DROP, independently of ordinary-table lifetime. An unmarked empty
database must remain rejected. Every present ordinary table must still have an
active branch receipt, including locally added tables in a marked database.
Existing unmarked table-backed branches remain recognizable through their
receipts; no migration may guess the identity of old empty databases.

The database catalog row owns categorical identity; mo_branch_metadata owns
table lineage and protection snapshots. The marker does not grant privileges,
replace receipts, or authorize a fallback DROP of an ordinary database.

## Representation and compatibility

Use `data-branch` in the existing `mo_database.dat_type` column. The internal
CREATE DATABASE context passes this value to the existing engine call, atomically
with catalog creation. Subscription creation retains precedence over that
internal value. No schema, checkpoint, WAL, or RPC format changes are needed.

4.2 upgrades are shutdown-only. Do not introduce main's per-feature MORPC v75
gate or a synthetic 4.2 protocol number. Existing unrelated gates remain intact.
Fresh installation and an upgrade from the pinned v4.2.8 release must both work.
An older release's empty/unmarked database remains unmarked. Starting an older
binary on data written by this change is not a supported downgrade path.

Physical backup/replay already stores dat_type. Logical snapshot, timestamp,
PITR, database/account/cluster restore must read the authoritative type alongside
CREATE SQL and propagate only the branch marker into each internal CREATE.
Subscription restoration continues through its existing SQL path. Identity-read
errors must precede replacement of the corresponding database; account/cluster
entry preflight validates source definitions before irreversible account effects.
Table-only restore must not change the existing destination database's identity.

## Ownership and transitions

1. Existing privilege/quota/source validation remains authoritative. Use the
   existing background transaction; failures roll back catalog, object, receipt,
   and protection-snapshot work together.
2. Derive clone-only context for one statement and restore ExecCtx.reqCtx on
   every exit. A multi-statement request must not mark a following ordinary DDL.
3. DELETE takes the same exclusive mo_database catalog-key lock as object DDL,
   within the transaction that validates and drops the database. Lock the
   effective account/name key before reading identity and the complete object
   set. A waiting lock must refresh the RC view through the existing LockRows
   owner; transaction completion releases it on success/error/cancellation.
4. Reject missing, subscription and unknown-type targets. For zero ordinary
   tables require the marker. Views/sequences have no ordinary-table receipts;
   all present ordinary tables retain batched active-receipt validation.
5. Publication CREATE and ALTER classify `data-branch` as a user database, while
   subscription/unknown types remain rejected. ALTER without DATABASE resolves
   the stored database ID/name and owner account; it cannot silently bind to a
   same-named database in another account.
6. Every restore source reader returns CREATE SQL plus dat_type as one checked
   definition. A fresh derived context is used for database creation, with no
   marker leakage into sibling databases or later statements. Existing restore
   transaction/lineage owners remain responsible for commit and rollback.

No new goroutine, queue, cache, retry policy, session flag or background worker
is introduced. Catalog results and temporary lock vectors are released by the
same lexical owner on every return. Lock waits use the existing transaction
context; no cleanup waits on a new resource. Per-database work remains O(number
of ordinary tables), with existing bounded receipt batches. Account/cluster
preflight is O(number of source databases), with scalar definitions and no table
data materialization. Any extra catalog reads must be recorded separately from
test timing claims.

## Alternatives

| Alternative | Assessment |
|---|---|
| Keep table receipts only | Leaves the demonstrated empty/all-dropped bug. |
| Guess from database names or fall back to DROP | Misclassifies ordinary databases and crosses the authorization boundary. |
| Synthetic table receipt | Overloads table IDs, lineage and snapshot reclamation for a database fact. |
| New catalog column/table | Adds migration and ownership for a fact already represented by dat_type. |
| Existing categorical type | Atomic database-owned identity with existing persistence; selected. |

## Required proof and delivery gate

Maintain a producer/consumer/restore coverage map. Focused tests must cover
marked empty/view/sequence/all-dropped branches, legacy table-backed branches,
unmarked empty and mixed ordinary tables, missing/subscription/unknown types,
cross-account publication and ALTER without DATABASE, statement context reset,
catalog-read failure, transaction rollback, and concurrent object DDL.

Run the owning frontend/compile packages, scoped vet/lint, focused lifecycle race
checks and the relevant owning race package. Actual BVT and public SDK clients
must preserve exact catalog identity, values, ordinary-database rejection,
restore behavior and cleanup; repeat stateful BVT on the same instance. No skip
or negative-only oracle substitutes for the newly required positive behavior.

Review this exact document before implementation. Record the decision and digest
separately. Final implementation review and delivery remain blocked until the
mapped proof passes. The immutable v4.2.8 image is a reproduction/upgrade source;
only a binary containing this change can qualify the corrected 4.2 behavior.
