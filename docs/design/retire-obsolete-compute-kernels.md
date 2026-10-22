# Retire obsolete computation kernels

Version 1, 2026-10-22. Owner: engineering-quality issue #29249.
Implementation: `cleanup/29249-modules-20261022`.
Base: `b90e839af241184d14ac819ddc894e9dee097262`.

## Boundaries

The old `ivfflat/kmeans/elkans` package has no importers under any checked-in
build tag. CPU device dispatch uses balanced k-means; GPU dispatch uses cuVS or
the balanced fallback. Remove Elkan's implementation, initializers, and isolated
tests/benchmarks without replacing the active clustering algorithm.

The old `vectorize/floor` constants duplicate the live SQL function constants,
and `vectorize/instr` is disconnected from the collation-aware SQL implementation.
Remove these unreferenced packages and the old INSTR helper's isolated test.
Preserve SQL registration, result/error semantics, and active function tests.

The shared initialization selector, configuration fields, producer assignments
and CPU/GPU device signatures remain outside this retirement. Both constructors
ignore the selector, but changing their shared interface requires serialization
compatibility proof and genuine GPU build evidence. Deleting the disconnected
algorithm does not change these interfaces.

## Alternatives and invariants

Keeping obsolete implementations or moving them into tests leaves duplicate
algorithms without a live consumer. Select direct removal, with no replacement
state or abstraction. Independent planner/engine test entrypoints, build-selected
adapters, and executable tools are excluded despite having no ordinary importers.
This is an internal Go implementation cleanup, not an index format, SQL option,
algorithm-registration, or cluster compatibility change. Unknown out-of-tree Go
importers are outside the repository's source-compatibility evidence.

## Validation

The production-line deletion triggers the design size gate. Require a complete
reference/build-tag audit and verify that active dispatch and initialization are
unchanged. Reuse balanced-kmeans and SQL-function tests for active behavior,
including validation, failure cleanup, NULLs, collation and numeric boundaries;
run normal/race checks and incremental configured vet/lint. Compile the real SQL
consumer and main binary. Deleted self-tests exercise only retired kernels, not
the live implementation or an independent live-path reference oracle.

There are no new hot-path operations, resources, waits, or allocations. Do not
claim runtime speedups or reduced CI time without measurements. Review logs,
candidate inventories, and temporary validation artifacts stay outside source.
