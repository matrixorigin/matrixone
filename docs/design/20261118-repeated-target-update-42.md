# Repeated-target UPDATE on 4.2

## Invariant and scope

For aliases of the same physical table, select one complete source tuple per
alias and physical row. Apply alias contributions to a single row image. A
rejected `UPDATE IGNORE` candidate must not reserve any unique key or discard a
valid contribution from another alias. Ordinary `UPDATE` must fail atomically
on duplicate keys. Single-writable-alias joins and updates of distinct tables
continue through the existing 4.2 route.

The legacy path generated separate writes from a shared positional projection;
repeated targets could reference a pruned column, panic, or leave the base and
unique indexes inconsistent. Fixing only the projection did not provide one
physical-row owner. A sequential unique-index filter was also insufficient: if
row 1 proposes `(500, 300)` and conflicts on its second key, row 2 must still
be able to propose `(500, 400)`.

## Ownership and execution

The planner materializes complete alias contributions and normalizes them to a
single physical-row image before the existing MULTI_UPDATE writers. Ordinary
updates reuse their existing lock and uniqueness checks. For IGNORE, one
ordered PRE_INSERT_UK instance per affected alias stage checks all affected
unique keys before publishing any release or reservation. Existing mark joins
supply snapshot, current-stage and released-key facts. The owner keeps an
execution-local key overlay across input batches; a failed candidate does not
modify it. NULL, composite, generated and prefix keys use the planner's existing
index expressions. The operator is merged to one decision owner if its input
has parallel scopes.

The key hash tables, iterator buffers and output vectors use the execution
allocation account. Reset and Free release the owner state, and cancellation or
allocation failure returns an error without publishing a partial output batch.
The surrounding transaction owns rollback. Small Go slices and planner metadata
are still ordinary Go allocations. This path has no new persistent storage or
spill implementation; large updates can hit the existing allocation limit.

Partitioned windows emit many small batches. A receiver now signals the
existing pipeline edge when it consumes a queued signal, so a producer waiting
for capacity can recheck immediately instead of waiting for the next 1 ms poll.
The notification is only a bounded, coalesced hint, not a capacity reservation:
Ch2 remains authoritative. The existing poll remains for direct channel readers
and for coalesced notifications with multiple producers. The existing import
memory bound accounts for the extra channel in both edge generations.

| Lifecycle | Termination or bound |
| --- | --- |
| Edge generation | One capacity-hint channel with at most one pending signal; reuse replaces it only after senders and receivers stop. |
| Capacity wait | A receive, caller cancellation, terminal edge or the existing poll causes a recheck; stale hints do not grant capacity. |
| Owner cleanup | PRE_INSERT_UK releases key state and output on reset/free; transaction rollback owns failed writes. |

Foreign-key RESTRICT checks execute at each relevant parent-key transition,
including an intermediate value that a later alias would restore. A parent
CASCADE that overlaps direct writes to the same physical child is rejected
before writes: 4.2's previous path also failed, and support for this operation
is outside this change. The existing 4.2 foreign-key error code is retained.

## Cost and compatibility

For each alias stage and affected unique index, the planner adds three mark
joins over current rows, released keys and the snapshot index. Expected hash
work is proportional to the staged rows and scanned index rows per key and
alias; both materialization and the decision overlay consume memory. Existing
repeated-row selection also uses multiple partitioned windows.

On 57, an embedded three-CN run with 1024 rows and one changed unique key had a
five-sample median of 4.82 s before the capacity notification and 266 ms after
it. With two changed unique keys the latter median was 352 ms. The unaffected
single-writable-alias control was 14.4 ms on the clean 4.2 baseline and 9.9 ms
with the change. These are short local measurements, not throughput, tail
latency or production-scale bounds; repeated-target statements perform more work
and the old path did not successfully implement the affected behavior. A
ready-receiver microbenchmark showed five-sample medians of 111.4 ns before and
111.8 ns after, with the same 80 bytes and one allocation per operation; it does
not measure contended pipeline throughput.

The protocol additions are query-local and do not change the on-disk format.
Remote encoding/decoding checks the local runtime's MORPC capability version;
these checks do not negotiate the capabilities of an arbitrary older peer. The
4.2 upgrade contract uses full service shutdown, so all peers must run the new
binary before the new protocol is used. Rolling mixed-version execution has not
been validated.

## Validation

The regression set exercises the original panic, ordinary duplicate rollback,
multiple-key arbitration, key release across batches, NULL/composite/prefix and
generated keys, prepared statements, CHECK, RESTRICT, memory-allocation failure,
reset, and simultaneous writes from two CNs. Base-table and forced-index reads
are compared. A real mo-tester SQL case verifies the multi-key counterexample,
ordinary duplicate rollback and index views. Tests do not establish correctness
for overlapping CASCADE writes or a mixed-version rolling upgrade.
