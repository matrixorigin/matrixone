# Branch hashmap default CPU budget

Related issue: https://github.com/matrixorigin/matrixone/issues/29562.
Implementation: the CI UT racing optimization PR containing this document.

## Owner and problem

Branch hashmap construction defaults to half the visible CPUs and shard
iteration defaults to all visible CPUs. The latest measured CI runner exposes
96 CPUs but allows eight CPUs of execution. This selects 48 shards and up to
48 simultaneous shard callbacks, despite the application's existing eight-CPU
budget. A shard callback owns temporary batches and can perform LCA SQL/reader
work; every shard also flushes its final partial tombstone batch. Excess shards
can duplicate partial-batch work, not merely create idle workers.

The existing `system.GoMaxProcs` owner tracks the application's execution
budget, including environment/runtime limits and ADMIN updates. Compilation
and MORPC already use this owner. Reuse it without changing global CPU-count
semantics or introducing a controller, cache or scheduler.

## Selected scope and invariants

For an unspecified shard count, cap the old `visibleCPUs / 2` result at
`max(1, system.GoMaxProcs())`, then retain the existing [4, 128] clamp.
This preserves the old bare-metal default and eight shards on visible16/quota8;
it does not halve the eight-CPU budget again. Explicit positive shard counts
retain their existing clamp and are not capped to the CPU budget.

For unspecified iteration parallelism, use `max(1, system.GoMaxProcs())` and
retain the existing clamp to shard count. Explicit positive parallelism stays
unchanged. Project and Migrate retain their source's shard count, storage
ownership and caller-specified parallelism. Visible-state stores' explicit
four-shard topology remains unchanged.

All keys and duplicate rows remain present. Hash routing changes only the
internal process-local partition, with no durable/wire format change. Preserve
allocator and spill limits, per-bucket format, spill failure sealing, rollback,
cursor mutation and Close cleanup. Fewer default shards cause the existing
memory-budget-derived tombstone batch threshold to increase; the combined
budget formula stays unchanged. Validate single-query parse/plan and resident
cost rather than assuming the old per-shard threshold stays constant.

Do not change the frontend's mixed worker pool or channel capacities in this
patch. CSV output has two long-lived coordination roles in that pool; replacing
its capacity mechanically can deadlock when the execution budget is one or two.
Its ownership and liveness need a separate proved design.

## Validation and performance gate

Extend existing hashmap fixtures for defaults at one/two/eight/large execution
budgets, visible96/budget8 default calculation, bare-metal controls and explicit
options. Cover default and explicit iteration, all keys/duplicates, mutation,
Project/Migrate, spill, original callback errors and Close using existing
deterministic barriers and allocators. Restore scoped runtime/application
settings after each test; do not add global CPU mocks or timing-based oracles.

Use explicit 48- and eight-shard diagnostic fixtures to model CI topology with
identical keys, byte budget and data. Measure partial-batch/probe counts and
decode/results, CPU and allocation for small/medium, large and spilled cases.
Iteration callbacks include CPU work and I/O/LCA work, so execution budget is
not automatically the best I/O concurrency. Keep explicit controls and reject
this default change if realistic controls show material performance regression.
Run whole owning normal/race packages and real branch SQL consumers.

Local visible16/budget8 should preserve the old default and demonstrates the
nearest no-regression control, not the CI saving. CI-minute savings remain
unconfirmed until a real CI run. No case removal, weakening or special SQL
selection is permitted.
