# Retire obsolete computation and bookkeeping

Version 3, 2026-10-22. Owner: engineering-quality issue #29249.
Implementation: [PR #30861](https://github.com/matrixorigin/matrixone/pull/30861).
Current frozen base: `3ad9adbb4ce6c9f8e421917233cc6f6d40ab5f25`.

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

## Unconsumed runtime bookkeeping

Remove state only after tracing definitions, all writes, consumers, implicit
comparison/serialization, and existing test oracles. The bounded candidates are:

| Owner | State to retire | Existing responsibility to preserve |
| --- | --- | --- |
| Merge-order spill reader | `heapIdx` and its maintenance loop | Heap slice/order and reader ownership |
| External writer | `expandedPath` copy | Local expanded URL and file writer |
| Hash-build | `hashMapRowCount`, `hashMapRowCountSet` | Actual row count, map/selection admission, dedup rebuild |
| Sampling | `isMultiSample` | Sample pool construction and output |
| Time windows | `partEnd` and stale comment | Local partition break and cursor restart |
| Planner binding | `refCnts`, including unused slice allocation | Column resolution and real projection reference-count maps |
| RETURNING | `returningObjRef` and unused source-helper arguments | Live irregular-maintenance object reference and row-image source |
| IVF planning | `initialStrategy` | Selected search mode and auto-mode behavior |
| DML planning | `derivedTableId`, `onDuplicateIsIgnore` | Current root node and INSERT/IGNORE handling |
| Hint parsing | Duplicate `vectorLocalDOP` cache | Canonical `VectorLocalDOPEnabled`, exact parser rules and allocation ordering |

Update existing test literals and replace dead heap-index assertions with
observable heap contents/order. Preserve distinct SQL, failure and cleanup
assertions. An unused-in-production flag that proves a necessary regression
path is retained unless equivalent observable assertions demonstrate sensitivity
to the original mechanism through a bounded mutation experiment.

The Substrait capability key's `argumentN` is excluded: struct equality/hash is
an implicit reader. Shared GPU interfaces and serialized/public metadata are
also excluded. Retiring private write-only state must not change comparisons,
resource ownership, preflight/admission, error order or operator transitions.
Run the existing owning planner/operator suites normally and with race detection,
plus incremental vet/lint; no redundant per-field tests are needed.

## Design review decision

Version 3 records approval of the unchanged version-2 retirement proposal after
rebase, and updates obsolete Elkan-specific comments to describe the retained
clustering resolver. It adds no algorithm or ownership change.

- **Phase:** current-proposal design re-admission after existing implementation
  and conflict-free rebase, before further implementation and final
  implementation review. Historical approval is not established; commit ordering
  alone is not approval.
- **Reviewer:** `gpt-6.1-sol` / `xhigh`.
- **Session:** `01a163ca-7d53-77e0-b947-5ff37ee4875d`, 2026-10-22.
- **Decision:** **GO — design only**, no blocking design findings.
- **Reviewed design:** version 2, Git blob
  `83663ed7c99db55ead45dce382ccb5cc0ef9b94d`, in proposal
  [`6776142db5b3d7601fe9a714d953d97733f9cd48`](https://github.com/XuPeng-SH/matrixone/blob/6776142db5b3d7601fe9a714d953d97733f9cd48/docs/design/retire-obsolete-compute-kernels.md).
- **Reviewed scope:** the complete four-commit proposal: three kernel retirements
  and twelve private bookkeeping fields across the planner and operators.
- **Frozen main base:** `3ad9adbb4ce6c9f8e421917233cc6f6d40ab5f25`.
- **Original remote head:** `6ae3b27230ade978e8c429a47f8d50b8ec5a8151`.
- **Accepted decision:** direct removal within the documented boundaries;
  preserve live consumers, ownership, admission, serialization and independent
  behavioral oracles. Provenance-only corrections need no substantive design
  re-review; material deviations require a revised design and another gate.
- **Implementation review:** separate and required; this design decision does
  not approve the implementation.
