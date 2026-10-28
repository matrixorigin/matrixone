# General hash aggregation throughput

## Problem

Integer hash insertion planning and key publication consume substantial CPU in
large hash aggregations. Reduce repeated work in those common paths without
changing the planner, local aggregation policy, memory admission or spill.
The initial profile was collected on the 99,997,497-row ClickBench Q5 input
against main `e9dc8c420818732d6a5691bef1b7ebf6e4d8a87f`; measurements must be
repeated against the actual base and a range of input distributions.

## Design and invariants

1. Simplify integer insertion planning by hoisting immutable address masks and
   advancing directly when a mapping resolves. Preserve the tentative-slot
   overlay, encounter-order group IDs, immutable preview, commit validation and
   capacity rejection behavior.
2. Copy a single non-null, non-constant eight-byte key into iterator-owned
   scratch when its representation matches the existing H8 encoding. Floats,
   NULLs, constants and composite keys retain the generic encoding. Plans do not
   borrow input storage across preview and commit.
3. Pass the selected-row count already validated during preflight through
   group-key publication and output-batch splitting. Preserve string provenance
   preflight, application and finalization, including duplicate-only chunks;
   preserve all aggregate updates and allocation accounting.

Exact equality, NULL behavior, insertion flags, row counts and output values
remain unchanged. Fallible resource admission precedes publication; a rejected
batch is retried at the same input offset. This adds no ownership, concurrency,
serialization, persisted-data, or SQL behavior changes.

## Alternatives and validation

A fixed-size adaptive local-reduction prototype was rejected: at 131,072 keys
repeated 32 times, its short observation window repeatedly lost cross-window
reduction and made a directly exercised local/final Group pair about 17% slower.
Raising the threshold would move this failure to another cardinality; it is not
part of the implementation. String probing and other experimental rewrites were
also excluded when controls regressed.

Validate the owning hash table, hash map and group packages, including preview,
commit, rejected admission, NULLs, constants, floating-point canonicalization,
string provenance and batch-boundary cases. Compare baseline and candidate with
identical inputs and sequential ABBA runs, exact result checks, several key
cardinalities and distributions, and spill and process-swap monitoring. Do not
put timing assertions in unit tests. Shared-host interference limits timing
precision; retain raw benchmark artifacts outside the source tree.
