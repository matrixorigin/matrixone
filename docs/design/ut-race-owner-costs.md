# UT racing owner cost reductions

Tracking: [#29562](https://github.com/matrixorigin/matrixone/issues/29562).

Remove repeated work at existing owners. Preserve transaction boundaries,
persistence and wire formats, worker lifetimes, and public SQL behavior.
This document consolidates the six approved designs without adding a framework.

## Branch hashmap CPU budget

The measured CI runner exposes 96 CPUs with an eight-CPU execution budget.
The old default selects 48 shards; each shard can own temporary batches,
LCA/reader work, and a final partial tombstone batch. Reuse `system.GoMaxProcs`,
the existing application execution-budget owner, without a new scheduler.

Default shards are `min(visibleCPUs / 2, max(1, system.GoMaxProcs()))`, followed
by the existing [4, 128] clamp. Visible16/budget8 keeps eight shards; bare-metal
behavior is preserved. Explicit positive counts retain their original clamp.
Default iteration uses the execution budget, capped at shard count; explicit
positive parallelism is unchanged. Project/Migrate retain source topology.
Explicit four-shard visible-state stores are unaffected.

Preserve keys, duplicates, routing format, combined memory/spill budget,
failure sealing, rollback, cursor mutation and Close. Fewer shards increase the
existing per-shard tombstone threshold; measure resulting resident cost and
partial-batch work. Execution budget need not be optimal for I/O concurrency,
so retain explicit controls. The mixed frontend CSV worker pool has two
long-lived coordination roles: mechanically capping it can deadlock at budget
one or two. Its capacities are outside this change.

## HAKeeper assertion reads

Health/task assertions consume only State, while full CheckerState snapshots
copy all store configurations. The state-machine lookup owns this cost.
An explicit local `StateQuery.StateOnly` returns a new CheckerState containing
only State. Zero-value and typed-nil queries retain full independent snapshots.
The query is neither replicated state nor an RPC schema.

`assertHAKeeperState` requests StateOnly. Every assertion still performs its
own authoritative SyncRead with existing timeout, cause attachment, retry and
error behavior. PR #29760 additionally introduces a scheduling projection,
retaining store/runtime state and LOG WAL recovery status while omitting display
configuration. Public/private full getters, GET_CLUSTER_STATE and existing full
truncation queries remain complete. The current consumer and failure contracts
are defined in [Race UT critical path](pr29760-race-ut-critical-path.md).
There is no cache, aliased mutable map, assertion reuse or cadence change.
Dragonboat retains lookup serialization ownership.

## Initial role privileges

The existing initialization transaction previously executes 30/34 admin
privilege INSERTs plus a separate public INSERT. Use one bounded multi-row
statement per role at the existing frontend owner; public keeps its position.
The default lists and privilegeEntriesMap remain authoritative. Preserve all
ten columns, IDs, names, levels, operation user, grant option, order, and each
row's existing clock/UTC precision. Upgrade and runtime GRANT/REVOKE retain
single-row semantics through the shared row format.

The SQL builder adds no executor or transaction owner. A rejected batch returns
the original error, stops later statements, and uses existing rollback and
cancellation semantics. No partial default privilege set is visible before
commit. Inputs are fixed internal constants and numeric IDs, bounded at 34 rows;
empty input produces no statement. No migration or new quoting surface.

## Logtail object-name classification

Preserve the exact unanchored RE2 languages `_\d+_data_meta` and
`_\d+_tombstone_meta`: containing matches, ASCII digits, leading zeros,
unbounded digit lengths, invalid UTF-8 and arbitrary surrounding bytes.
The private matcher finds each fixed suffix, scans backward over ASCII digits,
and accepts a nonempty digit run preceded by underscore. A failed occurrence
must not hide later valid occurrences. Nonoverlapping suffixes and their digit
runs bound total work linearly; no regexp, numeric parsing, cache or pool.
Keep public classifiers and IsMetaEntry short-circuit behavior.

## Logtail segment capacity

The existing segment pool previously allocates maximum-sized payloads even
for small responses. Create empty payloads and grow at the existing writer:
`min(chunkLimit, max(chunkLength, 2*oldCapacity))` when capacity is insufficient,
then set actual length and copy. Capacity stays bounded by the valid limit.
Cold full-size writes retain one allocation/copy; gradual increases can add
allocations, bounded geometrically, and must be measured.

Preserve serialization buffer, Split, headers, sequence IDs, message size,
wire codec, transport ownership transfer and release callback. Each queued
segment owns independent bytes. Release resets metadata while retaining
capacity. The writer is the sole production Acquire consumer; pool callers
must not assume eager maximum capacity. No new size classes or lifetime state.

## Clone-owned catalog restore

Snapshot, PITR and cross/dropped-account timestamp restore all use the fixed
CREATE TABLE CLONE format. Remove unreachable manual CREATE fallbacks,
sole-purpose regexp and unused ordinary catalog SHOW CREATE work. Clone owns
source schema/data; sequences still need historical createSql, and UDF uses
its current-schema owner. Historical user-table CREATE enumeration remains
necessary for FK topology and views. Keep source/target identity, protocol,
transaction and cleanup checks; recreate empty objects and catalog generations.

Cluster cleanup uses existing showFullTables name/kind enumeration, preserves
system-account-only selection, both special view DELETEs and ordered failure
propagation. Even missing special tables must propagate errors. Snapshot's
existing missing-other-object FK tolerance remains for user databases only;
mandatory catalog cloning fails closed. All three families preserve enumeration,
sequence read, cancellation and downstream DROP/DELETE/CLONE failure boundaries.
No lazy metadata provider, inventory cache or table-name whitelist is introduced.

## Validation and delivery limits

Keep independent privilege/catalog SQL results, complete snapshot isolation,
exact legacy-RE2 differential cases, sequence/UDF exceptions, fail-closed restore,
wire decoding, queued-byte independence and bounded capacity reuse. Share only
compatible fixtures with scoped mutable state and cleanup on assertion failure.
Retain normal/race owner checks and existing real SQL consumer coverage.

Report wall, CPU, cumulative allocation and peak/resident memory separately.
Local resource results are mixed, including matched controls with regressions;
no general memory reduction or CI-minute saving is established. Microbenchmarks
and split test runs do not prove a complete CI workload improvement. Retain
adverse results in PR evidence; the original CI saving goal remains open.
