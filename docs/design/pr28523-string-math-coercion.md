# PR #28523: String-Math Numeric Coercion

- Issue: [#28487](https://github.com/matrixorigin/matrixone/issues/28487)
- Pull request: [#28523](https://github.com/matrixorigin/matrixone/pull/28523)

This note records the behavior contract, ownership boundaries, compatibility
mechanisms, validation evidence, and known limits for the change.

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
- For prepared `ROUND`/`TRUNCATE`, a complete ordinary text value binding may
  select its exact numeric domain at the value argument. This applies to SQL
  `EXECUTE` and COM_STMT text sources; binary bytes, binary static/runtime
  domains, and unknown sources retain their existing coercion path. Complete
  numeric spellings use this exact-domain path in both strict/default and
  compatibility modes; completeness does not authorize numeric-prefix parsing
  of malformed text. Incomplete or unparseable text remains on the existing
  mode-selected string-math conversion path. The precision argument remains
  independent, and the cached expression keeps its original parameter
  reference.
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
Append the new mode without shifting existing SQL-mode values. `SessionInfo`
field 24 carries the opt-in and field 25 carries the sender contract marker.
Fields 22 and 23 are not part of this numeric contract and are not interpreted
as its mode or sender marker.
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
  the transport owner and remains trailer v3, independently numbered from
  MORPC; stable vector/batch bytes and the ordinary runtime-domain sidecar
  remain unchanged.
- Provenance-sensitive flow-control output is fenced at MORPC v107 in every SQL
  mode when it can select a non-NULL HEX/BIT value. This includes mixed rows,
  uniformly marked results, and marked-plus-NULL results: older flow-control
  executors can drop the marker even when no mixed-row bitmap is needed.
  Explicit casts remain a boundary.

### Vector allocation ownership

The row-level numeric-literal bitmap follows the vector's immutable allocation
selection. Account changes are allowed only before vector backing is created;
reset clears row marks but retains external capacity under its original
account, and `Free` releases that capacity. Live storage is not moved between
accounts.

### Remote compatibility

MORPC v101 retains the upstream JSON/YearBit contract. This change's
string-numeric and flow-control provenance behavior uses MORPC v107. For
expressions requiring the new numeric contract, placement falls back to local
execution when a worker is below v107 or has unknown capability; sender
preflight rejects a destination downgrade; and receivers reject the changed
feature when its protocol or session marker is legacy. Unchanged expressions
retain their existing compatibility gates.

## Deployment and rollback boundary

The new numeric contract is guaranteed for new SQL coordinators and admitted
new workers. During a mixed-version rollout, route SQL that relies on the new
numeric behavior through new frontends. An old frontend can still execute its
local query with the older default behavior and may reject the new SQL-mode
token, so arbitrary frontend routing does not promise identical results.

Worker placement and the final send-time version probe are separate operations.
The new coordinator falls back to local execution for an old or unknown worker
and rejects a downgrade observed by the final probe before it marshals or sends
the remote scope. A new receiver validates the expression and legacy sender
marker before constructing operators. The final probe is not an atomic lease on
the worker process: replacement of the same address after the probe is outside
the guarantee.

Before replacing or downgrading a worker, withdraw its endpoint from SQL
routing and worker placement, stop new admissions, and drain or cancel
in-flight executions and batch streams. Reuse the address only after that
quiescent boundary. Reconnect clients and recreate prepared sessions before
traffic resumes. On rollback, stop relying on the new mode token and results
before routing to old frontends. A full-cluster quiescent cutover is also a
valid rollout. This change does not introduce a stable vector format migration
or claim general historical catalog/data rollback compatibility.

## Ownership and correctness decisions

The execution path is:

```text
prepare role/source classification
  -> execute-time source identification
  -> value/control binding
  -> existing overload and cast execution
  -> cache restoration or remote transport
```

- The binder owns provisional types. Prepared execution keeps original
  `ParamValue` and source-type provenance in its binding state, and the existing
  AST precision consumer binds that source at the current prepared execution
  boundary. The cache key includes binary-protocol identity and source-type
  provenance. DDL/SET paths keep their existing reset-based specialization owner.
- Prepared `ROUND`/`TRUNCATE` execution also resolves the deferred value
  occurrence from the original text-source binding when its complete spelling
  has an exact numeric type. This local value-argument decision uses the
  existing runtime binder and retains parameter provenance; fixed DECIMAL
  sources and explicit casts remain authoritative, while incomplete text and
  binary or unknown sources stay on their existing path. It does not refine
  the precision occurrence or unrelated string-math functions.
- The shared expression-role logic is the authority for which value
  occurrences may be rebound. Control arguments and non-owning string
  functions do not inherit a parent's numeric role.
- `Vector` owns transient scalar/per-row provenance and its allocation,
  selection, append, remap, reset, and cleanup lifecycle. `Batch` owns the
  versioned trailer encoding; the receiver validates before publishing decoded
  metadata. Stable storage/materialization drops execution-only provenance.
- Existing warning-aware casts own conversion warnings. No new goroutine,
  background resource, external I/O, or retry loop is introduced.
- Native SQL DOUBLE precision `2.5` retains the private ties-to-even conversion
  to `2`; native DECIMAL precision `2.5` remains `3`. Binary or unproven DOUBLE
  precision retains ordinary INT64 conversion to `3`. The source distinction
  comes from original binding provenance, with binary protocol taking
  precedence; it is not inferred from a numeric payload or binding type. Value
  DECIMAL domains remain separate from precision controls, and explicit SQL
  casts retain their own boundary. Failed specialization cannot replace the
  cached valid plan.
- String-to-number conversion is not monotonic under string ordering. String
  paths therefore do not advertise function-wide zonemap pruning; correctness
  takes priority over that optimization.

## Performance decision

The recorded role-discovery benchmark on Darwin/arm64 reported zero
allocations, approximately 2.7–2.9 μs for common single-parameter ownership
cases, 50.8 μs for a deep no-match `P=8` case, and 23.4–23.8 μs for a
mixed-role `P=5` case. These measurements apply to the reset-based role scan,
not the public prepared-QUERY execution path, and are not end-to-end query
latencies. The current prepared-QUERY binding path preserves per-execution
source provenance at the existing AST consumer; it has not been measured by
that benchmark. A shared role table or cross-execution cache is deferred until
cache invalidation and ownership equivalence are specified and measured.

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
| Source and mode behavior | Literal/column/prepared tests; strict default, explicit MySQL mode, native-wins, warning count/code, and NULL/masked-row controls; complete-text prepared ROUND/TRUNCATE domains in strict and compatibility modes |
| Roles and boundaries | Value/control argument tests; precision remains `INT64`; nested ownership; explicit CAST stops provenance; unknown/non-owning function controls |
| HEX/BIT row provenance | CASE/IF/COALESCE tests for mixed, uniform, marked-plus-NULL, text, ordinary BINARY, explicit casts, and nested/implicit casts; selected-row/vector/batch lifecycle tests |
| Wire lifecycle | Batch v1/v2/v3 round trips, malformed/truncated rejection, legacy sender, stale/reused vector reset, and remote trailer tests |
| Mixed-version admission | v106 local fallback plus destination and receiver rejection; v107 placement/send/receive acceptance; upstream JSON/YearBit v101 controls; default/MySQL/native modes; legacy-session rejection |
| SessionInfo wire allocation | Unrelated varints at fields 22/23 are not interpreted as numeric mode/marker; fields 24/25 round trip the explicit mode and sender marker; forwarding preserves a zero legacy marker |
| Prepared-plan reuse | Type changes, error-to-success, NULL-to-success, and restoration of the cached base plan |
| Numeric correctness and pruning | Integer/DECIMAL/FLOAT source controls; zonemap endpoint traps must not prune matching rows |
| Resource/performance | Allocation-failure atomicity, vector reset/cleanup/accounting, focused race checks where shared-state risk applies, and plan/bitmap performance tests |

Public prepared-query validation includes scalar precision results and metadata,
the complete-text ROUND/TRUNCATE value-domain path, the binary-protocol numeric
overload/error-reuse case, prepared DECIMAL extrema, and the existing
string-math fixture. Mixed-binary validation records the exact source revisions
and binaries; mocked protocol integers and UTs alone are not evidence of
executable interoperability. A same-address replacement after the final worker
probe remains outside the contract and requires the quiescent replacement
procedure above.
