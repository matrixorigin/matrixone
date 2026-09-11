# Prepared percentiles and sortable discrete percentiles

**Status:** Proposed for design approval

## Scope and review boundary

This design covers the prepared-percentile and extended-`PERCENTILE_DISC`
follow-up discussed in issue
[#25110](https://github.com/matrixorigin/matrixone/issues/25110) and implemented
by PR [#28540](https://github.com/matrixorigin/matrixone/pull/28540). It extends,
but does not replace, the original
[`APPROX_PERCENTILE ... WITHIN GROUP` design](approx_percentile_within_group.md)
implemented by PR
[#27864](https://github.com/matrixorigin/matrixone/pull/27864).

The extension crosses the SQL binder, prepared-statement compile caches,
aggregate construction and retained state, distributed partial-state exchange,
and the configured MORPC compatibility boundary. The exact revision of this
document must receive traceable design approval on PR #28540 before the
implementation is accepted. Any later implementation change that alters the
public type set, rank relation, cache transition, retained payload, or protocol
gate requires design re-review.

## Problem and goals

The existing percentile implementation has two independent limitations:

1. `p` must be known while binding. A prepared marker cannot supply a value
   that is fixed for one `EXECUTE`, even though it is not row-dependent.
2. `PERCENTILE_DISC` accepts only the numeric family used by
   `PERCENTILE_CONT`, although it selects an input value and performs no
   interpolation. Values that already have an engine ordering, such as
   `VARCHAR`, `DATE`, `UUID`, and `DECIMAL256`, do not require numeric
   arithmetic.

The goals are to admit a prepared marker for `p` in `APPROX_PERCENTILE`,
`PERCENTILE_CONT`, and `PERCENTILE_DISC`; evaluate that marker exactly once per
execution; prevent any physical compile from retaining a previous execution's
value; and make `PERCENTILE_DISC` accept every scalar/vector type supported by
the shared MatrixOne sort implementation while preserving the selected value's
type and string provenance.

The following remain non-goals:

- a percentile expression that depends on an input row or group;
- multiple `WITHIN GROUP ORDER BY` expressions;
- `DISTINCT` ordered percentiles;
- `PERCENTILE_CONT` interpolation for nonnumeric values or `DECIMAL256`;
- ordered-set percentile window forms using `OVER (...)`;
- a durable aggregate-state or catalog migration; and
- automatic discovery of each worker binary's capability independent of the
  configured cluster protocol version.

## SQL and result contract

The percentile argument is a non-NULL numeric constant or one prepared marker
whose value is finite and in the inclusive range `[0, 1]`. A prepared marker is
configuration for one `EXECUTE`, not an aggregate input: every row and every
partial aggregate in that execution uses the same value. A column reference or
other row-dependent expression remains an error. An invalid execution does not
invalidate the prepared statement; a later execution with a valid value must
build clean configuration and succeed.

`PERCENTILE_CONT` and `PERCENTILE_DISC` retain their existing rank definitions.
After NULL order values are removed and the remaining `N` values are sorted in
the requested direction, `PERCENTILE_DISC(p)` selects the zero-based rank

```text
max(ceil(N * p), 1) - 1
```

so `p = 0` selects the first value and `p = 1` selects the last value. `ASC` is
the default; `DESC` applies the same rank to the descending order. Empty and
all-NULL groups return NULL. Equal sort keys are peers and no stable input-order
tie break is promised; the returned value is nevertheless one original peer,
not a converted or interpolated value.

`PERCENTILE_DISC` accepts a value type exactly when `sort.IsSupportedType`
accepts its resolved type. The supported families and their order are:

- booleans and bits; signed and unsigned integers; floating point using the
  engine SQL total order, including its defined NaN placement;
- `DECIMAL64`, `DECIMAL128`, and `DECIMAL256` by exact decimal comparison;
- `DATE`, `DATETIME`, `TIME`, `TIMESTAMP`, `YEAR`, and `ENUM` by their native
  encoded value order;
- `UUID`, transaction timestamps, row IDs, and block IDs by their native
  engine comparison;
- character, text, binary, blob, and datalink values by the same bytewise
  relation used by the shared physical `ORDER BY` sorter;
- JSON by `bytejson.CompareByteJson`; and
- supported array/vector types by the shared element-wise array comparison.

The result has the complete input `types.Type`, including width, scale, and
charset. For MySQL string values, it also carries the runtime string domain and
`StringSource` of the selected row. This matters when a downstream expression
distinguishes explicit binary data or prepared/COM_STMT provenance. Static
binary types derive their semantics from their result type. No collation or
type coercion unique to the aggregate is introduced.

`PERCENTILE_CONT` remains restricted to numeric types supported by its exact
interpolation implementation. `DECIMAL256` and maximum-width decimal inputs
remain rejected there. Existing numeric `PERCENTILE_DISC` continues to use its
typed implementation and existing external-run spill behavior.

## Binding and physical configuration

The ordered-set binder lowers an accepted call to plan arguments `[value, p]`.
A bare prepared marker is wrapped in the binder's numeric `FLOAT64` cast because
the marker has a text transport type at prepare time. Literal types continue
through the existing overload rules. The binder accepts only a constant or a
direct marker for `p`; the compiler repeats that validation before constructing
the aggregate.

At physical construction, the compiler evaluates `p` to one singleton vector,
checks NULL, finiteness, and range, and removes it from the row argument list.
A direct marker is preflighted before compiling the aggregate's child scopes,
so an invalid execution value follows the ordinary user-error path without a
panic stack or partially constructed scopes. For exact ordered percentiles,
immutable extra configuration is encoded as:

```text
byte 0       ordered-percentile config version (1)
byte 1       direction (0 = ASC, 1 = DESC)
bytes 2..N   canonical numeric text for p
```

The executor parses the numeric text into `big.Rat`. Merge is legal only when
aggregate ID, input/result type, percentile value, and direction match.
`APPROX_PERCENTILE` keeps its established textual configuration and sketch
state. In its ordered-set `DESC` form, compile time computes the exact rational
complement and supplies that text to the existing ascending executor.

## Prepared-statement cache state machine

The logical prepared plan remains reusable; a physical compile containing a
percentile marker is value-sensitive and is not reusable. The required state
transitions are:

1. `PREPARE` records the marker in the logical plan. A tree walk recognizes the
   direct marker, including the binder-inserted cast, and marks the plan
   ineligible for the ordinary prepare-time physical compile cache.
2. Before each `EXECUTE`, parameter decoding creates the current process-owned
   parameter vector. The compiler preflights the current `p` before child-scope
   construction, then uses it while constructing that execution's aggregate
   configuration.
3. A percentile-marker plan is also ineligible for runtime-specialization cache
   lookup and installation. If an earlier plan generation left a runtime
   specialization entry, it is cleared before execution. This rule also applies
   when the statement contains another parameter that would normally trigger
   numeric/domain specialization.
4. Successful execution releases the execution-specific compile by the normal
   statement path. A repeated execution, whether `p` is equal or different,
   constructs a fresh physical aggregate from the current vector.
5. NULL, conversion failure, NaN/infinity, or an out-of-range `p` fails before
   child scopes are compiled and installs no physical or runtime-specialization
   cache entry. The original user error is returned without panic detail. A
   subsequent valid execution returns to step 2; it cannot observe the failed
   value or a prior successful value.
6. Schema/metadata rebuild replaces the logical prepared-plan generation and
   clears the same runtime cache before the new generation is considered.

Caching the logical plan keeps parameter metadata stable without making the
immutable aggregate configuration shared mutable state. The cache is bounded
exactly as before because this feature only removes entries from two physical
cache paths.

## Extended discrete executor, ownership, and bounds

Nonnumeric `PERCENTILE_DISC` uses `orderedPercentileDiscreteExec`. It embeds the
generic `aggExec` with one saved argument, NULL-on-empty behavior, and the input
type as its return type. Every non-NULL input is copied into the aggregate's
allocation-accounted argument arena; no borrowed input-vector bytes survive a
fill call. Merge copies saved arguments into the destination's account and does
not transfer ownership of the source arena.

The resident key is the generic non-distinct saved-argument key:

```text
group uint16 (big endian) || ordinal uint32 (big endian) || retained payload
```

The per-group count is a `uint32`; overflow and oversized arena/key allocation
are reported as controlled errors. Group state is chunked by the existing
`AggBatchSize`. Arena, scratch, restored vectors, metadata sidecars, and selector
arrays are allocated through the aggregate MPool/allocation account. A failed
flush releases partial result vectors immediately; failed unmarshal frees the
receiver state; and the owning operator calls `Free` for retained executor state
on success or error.

In a production `Group`, the account controls resident growth and the existing
group spill controller externalizes saved-argument state when required. The
extended executor does not add a second private spill file. During flush it
restores and sorts one group at a time, requiring `O(N)` retained values plus an
`O(N)` selector array for that group; both are charged, and allocation failure
is returned instead of bypassing the bound. Cancellation is checked between
groups and periodically while restoring values. Numeric percentiles retain the
separate bounded external-run implementation (64 KiB minimum, 8 MiB maximum
run target, fan-in 64) that predates this extension.

## Saved-state encoding and merge compatibility

The outer aggregate intermediate/spill envelope is unchanged. Integer envelope
fields use the existing little-endian `types.Write*` encoding. A normal
intermediate state is the existing start magic, `int32` chunk count, each
chunk's `int32` row count, the selected row states below, and the end magic. A
private group-spill record substitutes the existing `GRPSPILL` start/end magic
and contains one `int32` row count. Each row state is encoded as:

```text
uint32 argument count
repeated fixed argument:    ordinal uint32 (big endian) || fixed value bytes
repeated variable argument: int32 byte length ||
                            ordinal uint32 (big endian) || retained payload
```

The group `uint16` is an arena key prefix and is not transmitted; the reader
reconstructs it from the destination row. It validates row/count/length bounds
and copies every argument into the receiving account. Configuration (`p` and
direction) is carried by the pipeline's aggregate expression, not duplicated
inside this saved state, and the merge compatibility check requires it to match.

The extension adds a versioned inner payload to every retained MySQL string
row, including rows whose runtime metadata has the default values:

```text
f1 53 4d                 metadata magic
01                       metadata version
domain byte              RuntimeStringDomain
source byte              StringSource
raw value bytes
```

There is no untagged string variant. Inherited domain and expression source are
encoded explicitly as their enum values, so user data beginning with the magic
prefix occurs only after this envelope and cannot be mistaken for framing. The
v63 decoder rejects a missing envelope, unknown metadata version, or invalid
enum and restores the metadata before sorting. Merge and intermediate/spill
serialization copy this inner payload byte-for-byte. Result publication uses
`Vector.UnionOne`, so the chosen row's raw value, domain, and source reach the
result together.

This representation is not a promise that pre-v63 workers can execute extended
types: those workers know the aggregate ID from v17 but do not know the new
executor or metadata payload. There is no lossy wire downgrade. Correctness is
provided by fail-closed remote admission at v63, not by asking an old worker to
ignore the header. Numeric exact-percentile state and
`APPROX_PERCENTILE` sketch state retain their previous representation and
remain admitted at their existing v17 boundary.

## Distributed rollout, downgrade, and rollback

`MORPCVersion62` is already assigned to VARCHAR `OCT` overload identities.
This feature therefore uses `MORPCVersion63`. When serializing a remote `Group`
or `MergeGroup`, the coordinator identifies a `PERCENTILE_DISC` input outside
the historical numeric family. If the configured protocol version is below 63,
serialization fails before dispatch with a not-supported error. Numeric
ordered-set percentiles continue to require only version 17.

The operational rollout is:

1. deploy binaries containing the v63 executor and decoder to every
   participating CN while the configured protocol remains at most 62;
2. run mixed-version qualification; extended local work may be tested, but
   distributed extended-percentile work must continue to fail closed;
3. after every participating worker is capable, raise the configured protocol
   to 63 and enable distributed use; and
4. monitor protocol rejection, allocation/spill errors, and result regressions.

To roll back, first lower the configured protocol to 62 so no new extended
remote pipeline is dispatched, drain in-flight queries and prepared sessions,
then roll back binaries. Aggregate state and private spill files are ephemeral
and query-owned; no catalog rows, user tables, durable object format, backups,
or restore procedures are changed. A process crash follows existing temporary
spill cleanup. Downgrading a live query or injecting a v63 partial state into an
older worker is unsupported and must fail at admission rather than fall back to
numeric or text coercion.

The configured version is a cluster operator assertion; it is not per-peer
binary discovery. Raising it before all workers are upgraded violates the
rollout precondition.

## Alternatives and decision

- Putting the percentile value in the runtime-specialization cache key would
  preserve compile reuse, but creates a value-cardinality cache concern and a
  second semantic key for what is immutable executor configuration. Rebuilding
  the physical compile is simpler and makes invalid-then-valid cleanup explicit.
- Mutating a cached aggregate's percentile before each run would share mutable
  configuration with reset, retry, and parallel pipeline lifecycles. It is
  rejected as an ownership and stale-state risk.
- Keeping `PERCENTILE_DISC` numeric-only avoids a new executor but rejects types
  that require selection, not interpolation. Coercing them to numeric or text
  would change ordering and result type, so it is rejected.
- Adding one typed executor per sortable type would duplicate sort semantics
  and omit future supported types. Reusing the shared vector sorter keeps one
  ordering boundary and one generic retained representation.
- Silently running extended types only on the coordinator would avoid a wire
  version, but changes placement and memory behavior and can be defeated by
  later optimizer changes. An explicit fail-closed v63 contract is selected.
- Dropping string runtime metadata keeps the old raw saved-argument payload but
  can change downstream binary/text semantics. The versioned inner metadata
  envelope is selected; static binary types remain represented by their type.

## Acceptance and validation map

The following evidence maps each contract to tests. Passing source-level tests
does not substitute for traceable approval of this design revision.

| Contract | Evidence |
|---|---|
| constant/direct-marker binding and row-dependent rejection | `pkg/sql/plan/prepared_aggregate_params_test.go`, `pkg/sql/plan/base_binder_approx_percentile_test.go` |
| per-execution evaluation, preflighted user errors, and exact physical config | `pkg/sql/compile/operator_test.go` (`TestConstructAggregateConfigPreparedPercentile`, `TestPreflightOrderedPercentileConfigsReturnsPreparedValueError`) |
| no ordinary or runtime-specialization compile reuse | `pkg/frontend/prepared_percentile_cache_test.go` |
| repeated values and invalid-then-valid prepared execution | `test/distributed/cases/function/func_aggr_ordered_set.test` |
| rank, direction, NULL, groups, native numeric order, exact decimal and NaN behavior | `pkg/sql/colexec/aggexec/ordered_percentile_test.go` |
| `VARCHAR`, `DATE`, and `DECIMAL256` execution and unchanged result type | executor tests above and the ordered-set public BVT |
| retained-state ownership/accounting and merge | `TestOrderedPercentileDiscreteSortableTypes`, `TestOrderedPercentileDiscreteVarcharMergeAndWireRoundTrip` |
| row-exact string domain/source and unambiguous magic-prefix values through selection and wire merge | `TestOrderedPercentileDiscreteVarcharMergeAndWireRoundTrip`, `TestOrderedPercentileDiscreteVarcharSelectedRuntimeDomain`, `TestOrderedPercentileDiscreteRawMagicPrefixRoundTrip` |
| v62 rejection, v63 admission, and unchanged v17 numeric boundary | `pkg/sql/compile/remote_expr_test.go` |
| build and changed-package regression | `make build`; targeted `go test` commands recorded on PR #28540 |

Before enabling protocol 63 in a release, deployment qualification must also
exercise a real mixed-version cluster, upgrade then downgrade admission, query
cancellation during a large extended percentile, allocation failure/spill and
reload, and process termination cleanup. Those are release/operational tests;
the protocol UT proves the fail-closed serialization decision but does not
claim a live mixed-version deployment was run.

## Decision record

The design owner accepts per-`EXECUTE` physical compilation for percentile
markers, the shared sorter as the ordering authority, the `O(N)` exact
`PERCENTILE_DISC` state and per-group flush work, the versioned metadata-bearing
saved payload, and protocol 63 as the no-downgrade remote boundary. Window
percentiles, row/group-dependent `p`, continuous nonnumeric interpolation, and
per-peer capability discovery require separate designs.
