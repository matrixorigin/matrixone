# ALTER COPY refresh after concurrent source writes

Issue: #31401. This extends the speculative preparation and publication model
in [ALTER COPY pre-publication](20261010-alter-copy-pre-publication.md).

The scalar ADD COLUMN admission refinement is specified in
[Scalar ALTER COPY admission](20261028-scalar-alter-copy-admission.md).
It retains this refresh mechanism while removing the exclusive global gate
from certified ordinary scalar replacements. The broad path below still
applies to protected and complex tables.

## Contract

COPY can prepare an old source snapshot while a LOAD is running. Publication
already waits for LOAD's metadata lock, but its commit invalidates that private
copy. Pessimistic RC ALTER now rebuilds the private replacement once after the
wait, so a data-only change does not require an external transaction retry.

Source replacement, definition changes, unavailable freshness proof and any
source/index lock error remain terminal. Temporary tables, optimistic and
non-RC transactions keep their existing behavior. ALTER remains transactional;
no frontend transaction restart or implicit commit is introduced.

## State and ownership

1. Capture source identity/version and caller workspace history, before private
   CREATE changes the transaction's DDL/write history. Borrowed transactions may
   have writes without a frontend BEGIN flag.
2. Prepare generation 0 with the existing lock-free CREATE, row copy, allocator
   reconciliation and unaffected-index clone path.
3. Acquire existing lifecycle admission, endpoint pins and source/index locks;
   advance the workspace snapshot, then re-resolve and validate the source.
4. An unchanged frontier publishes generation 0 at its original timestamp.
5. Only a successful pessimistic-RC admission with unchanged source ID/version
   and an advanced data frontier can rebuild. Recompute authoritative lineage,
   FK admission and fixed-copy policy at the fresh timestamp first.
6. Verify every replica physical ID, hidden index and partition member belongs
   to this transaction and excludes the public source. Discard old offset resets
   explicitly; a successful overall ALTER would not run failure-only cleanup.
7. Under the already-held broad lifecycle receipt, DROP the complete private
   generation with DisableLock, IgnoreForeignKey, IgnorePublish and
   SkipDataBranchReclaim. Recreate the same private name with new physical IDs
   and run the same preparation path at the fresh frontier.
8. Revalidate identity/version and freshness. Publish only generation 1. There
   is no third generation or ordinary statement retry under retained locks.

IgnoreForeignKey also preserves logical-ID role privileges. The private DROP
still uses existing index, partition, allocator, shard and catalog owners;
skipping task registration during CREATE does not bypass DROP's task checks.
Every task lookup/deletion must address the private name or physical ID.

All nested SQL remains in the caller's transaction and workspace statement.
DisableLock propagates through the statement tree, including partition and
allocator service SQL. The synchronous receipt expires on return; it borrows
successful admission and cannot grant it to an arbitrary caller.

Old create caches, private objects and locks can remain until transaction close
or GC. New physical IDs prevent old callbacks from operating on the successor.
Recovery and subsequent publication failures retain the former stale-copy
whole-transaction failure semantics, with the cause recorded. This includes
ordinary RC retry errors from source task teardown: they must never restart
the full COPY after a refresh. Cancellation follows the caller context;
nested SQL must not synchronously wait for its own token to drain.

## Index partitions and snapshot consistency

A partitioned index parent has no data rows. Unaffected-index cloning must
visit the source and target physical partitions matched by name, position and
ID, using the same algorithm policies for each member. Cloning only the parent
while INSERT skips that index would publish empty partition indexes.

Rows, index reads and lineage history use the refreshed source frontier. Caller
local writes remain visible; explicit/borrowed workspace history cannot be
replaced by a fixed historical snapshot. Workspace-owned advancement preserves
relocated tombstones; never rewind only the transaction timestamp.

## Cost and validation

The normal path still prepares once before the global gate. Recovery performs
one extra full copy and index preparation while holding that gate, delaying
other lifecycle operations. Two generations bound attempts, not bytes or IO
duration; capacity and storage failures terminate rather than loop.

Focused tests cover ownership and reset cleanup, PK/no-PK LOAD interleavings,
commit/rollback/borrowed caller writes, failures and cancellation, partitions,
allocator values, regular/unique indexes, plugin task isolation, lineage
participation before or after preflight, and FK/GRANT preservation. Public SQL
BVT separately checks LOAD defaults, transactional COPY and partition indexes.
Existing independent ALTER, DROP/TRUNCATE ordering and retained-lock FastFail
controls remain applicable.
