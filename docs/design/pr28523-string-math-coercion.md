# PR #28523: String-Math Numeric Coercion

- Status: APPROVED
- Design revision: 18
- Issue: [#28487](https://github.com/matrixorigin/matrixone/issues/28487)
- Pull request: [#28523](https://github.com/matrixorigin/matrixone/pull/28523)

Revision 18 defines the contract for consistent string-math conversion,
prepared-parameter ownership, and binary-literal provenance.

## Problem and scope

String values passed to `ABS`, `SIGN`, `CEIL`/`CEILING`, `FLOOR`, `MOD`,
`ROUND`, and `TRUNCATE` must not take different numeric paths merely because
they originated as literals, columns, or prepared parameters. The contract
covers conversion mode and warnings, binary-literal provenance, control versus
value roles, plan specialization/reuse, and remote execution compatibility.

The change does not make MySQL and MatrixOne native behavior identical, widen
general string-to-`INT64` conversion, change explicit `CAST` semantics, add a
new overload identity, or promise that older peers implement newer semantics.

## Frozen semantic invariants

### Sources, roles, and casts

- Equivalent character sources use the same conversion rules for the same
  compatibility mode and argument role. Native integer, DECIMAL, and floating
  sources retain their native overload and precision behavior.
- Prepared-plan specialization works on a deep copy. The cached base plan keeps
  `ParamRef` provenance; each execution binds its current value and source type,
  and a failed execution cannot publish a partial rewrite into the next one.
- Only value occurrences owned by eligible string-math functions receive the
  string numeric source. `ROUND`/`TRUNCATE` precision remains an `INT64` control
  input; `MOD` owns both operands. Numeric return type alone does not transfer
  ownership through string-domain or unknown functions.
- Implicit casts transparent to the same string value role preserve source
  provenance. SQL-authored explicit `CAST` is a semantic boundary.
- `NULL` remains `NULL`; masked and unevaluated rows do not cause conversion
  warnings.

### Compatibility modes and warnings

| Effective mode | Incomplete numeric strings |
| --- | --- |
| Default, empty, unset, or legacy/nil process state | Strict: reject incomplete, non-numeric, empty, whitespace-only, and malformed-exponent tokens; no MySQL truncation warning |
| `MYSQL_NUMERIC_COMPATIBILITY` | Opt in to the existing MySQL numeric-prefix parser and its existing warning behavior |
| `MATRIXONE_NATIVE` | Strict; wins if both mode tokens are present |

`MYSQL_NUMERIC_COMPATIBILITY` is a positive SQL mode, not the similarly named
account/database `MYSQL_COMPATIBILITY_MODE` setting. The mode is explicit on
the process wire state and absent legacy fields do not imply MySQL behavior.
Append the new mode without shifting existing SQL-mode values; preserve
protobuf field numbers 18 and 19 for the compatibility mode and sender contract
marker.
Direct string executors, casts, prepared parameters, and historical CEIL/FLOOR
string overloads use the same effective-mode decision.

### HEX/BIT provenance and transient row state

- HEX/BIT literals retain their byte-numeric interpretation (`X'31'` and
  `B'00110001'` produce 49); an ordinary `BINARY`/`VARBINARY` string is still
  text-numeric and `'1'` produces 1. Runtime binary-string domain is not numeric
  literal provenance.
- `CASE`, `IF`, and `COALESCE` can select different source kinds per row. A
  separate execution-only row-level `IsBin` marker follows selected HEX/BIT
  values. The implicit string-to-`DOUBLE` cast consults this marker (or the
  source's uniform scalar marker), never the runtime string-domain sidecar.
- The marker survives implicit binder casts, selection, vector/batch transfer,
  and remote pipeline transport. It is not stored in stable vector bytes or
  ordinary materialized table columns. The versioned batch metadata trailer is
  the transport owner; the ordinary runtime-domain sidecar remains separate.
- Provenance-sensitive flow-control output is fenced at MORPC v94 in every SQL
  mode when it can select a non-NULL HEX/BIT value. This includes mixed rows,
  uniformly marked results, and marked-plus-NULL results: older flow-control
  executors can drop the marker even when no mixed-row bitmap is needed.
  Explicit casts remain a boundary.

### Remote compatibility

MORPC v93 belongs to the existing `LAST_INSERT_ID` connection-migration
contract. Changed string-numeric and flow-control provenance behavior uses the
separate v94 boundary. Placement falls back to local execution when a worker
is too old or has unknown capability; sender preflight rejects a destination
downgrade; and receivers fail closed on pre-v94 or legacy session contracts.

## Ownership and correctness decisions

The execution path is:

```text
prepare role/source classification
  -> execute-time source identification
  -> value/control binding
  -> existing overload and cast execution
  -> cache restoration or remote transport
```

- The binder owns provisional types. `ResetParamRefRule` owns each execution's
  source expression and rebind state.
- The shared expression-role logic is the authority for which value
  occurrences may be rebound. Control arguments and non-owning string
  functions do not inherit a parent's numeric role.
- `Vector` owns transient scalar/per-row provenance and its allocation,
  selection, append, remap, reset, and cleanup lifecycle. `Batch` owns the
  versioned trailer encoding; the receiver validates before publishing decoded
  metadata. Stable storage/materialization drops execution-only provenance.
- Existing warning-aware casts own conversion warnings. No new goroutine,
  background resource, external I/O, or retry loop is introduced.
- String-to-number conversion is not monotonic under string ordering. String
  paths therefore do not advertise function-wide zonemap pruning; correctness
  takes priority over that optimization.

## Performance decision

Prepared source discovery remains a bounded per-parameter expression scan with
`O(P*N)` worst-case work for `P` parameters and `N` expression nodes; it avoids
rescanning descendants at every function edge. The recorded role-discovery
benchmark on Darwin/arm64 reported zero allocations, approximately 2.7–2.9 μs
for common single-parameter ownership cases, 50.8 μs for a deep no-match
`P=8` case, and 23.4–23.8 μs for a mixed-role `P=5` case. Those measurements
support retaining the bounded scan, not a zero-cost claim. A shared role table
or cross-execution cache is deferred until cache invalidation and ownership
equivalence are specified and measured.

The row-provenance bitmap uses bulk population/normalization and a uniform
append fast path; it must not rescan or renormalize the existing prefix on
each append. The vector regression/benchmark covers this hot path. No
function-wide zonemap optimization is included.

## Alternatives

| Alternative | Decision |
| --- | --- |
| Keep separate literal, column, and prepared conversion paths | Rejected; source-dependent results and warnings remain inconsistent |
| Add string-specific overload IDs | Rejected; expands serialized identities and duplicates warning/binary logic |
| Convert every argument to `DOUBLE` | Rejected; breaks precision controls and native exact/DECIMAL semantics |
| Reuse existing overloads with role-aware source binding | Selected; keeps serialized identities stable and centralizes mature cast behavior |
| Add a cached occurrence-role table | Deferred; needs explicit invalidation rules, ownership proof, and measurements |
| Restore string-derived zonemap pruning | Deferred; requires an overload-specific equivalence proof and endpoint-trap tests |

## Validation matrix

| Contract | Focused evidence |
| --- | --- |
| Source and mode behavior | Literal/column/prepared tests; strict default, explicit MySQL mode, native-wins, warning count/code, and NULL/masked-row controls |
| Roles and boundaries | Value/control argument tests; precision remains `INT64`; nested ownership; explicit CAST stops provenance; unknown/non-owning function controls |
| HEX/BIT row provenance | CASE/IF/COALESCE tests for mixed, uniform, marked-plus-NULL, text, ordinary BINARY, explicit casts, and nested/implicit casts; selected-row/vector/batch lifecycle tests |
| Wire lifecycle | Batch v1/v2/v3 round trips, malformed/truncated rejection, legacy sender, stale/reused vector reset, and remote trailer tests |
| Mixed-version admission | v93 local fallback plus destination and receiver rejection; v94 placement/send/receive acceptance; default/MySQL/native modes; legacy-session rejection |
| Prepared-plan reuse | Type changes, error-to-success, NULL-to-success, and restoration of the cached base plan |
| Numeric correctness and pruning | Integer/DECIMAL/FLOAT source controls; zonemap endpoint traps must not prune matching rows |
| Resource/performance | Allocation-failure atomicity, vector reset/cleanup/accounting, focused race checks where shared-state risk applies, and plan/bitmap performance tests |
