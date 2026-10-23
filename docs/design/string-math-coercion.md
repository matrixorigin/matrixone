# String-Math Numeric Coercion

- Issue: [#28487](https://github.com/matrixorigin/matrixone/issues/28487)

This note records the behavior contract, ownership boundaries, compatibility
mechanisms, validation evidence, and known limits for the change.

## Problem and scope

String values passed to `ABS`, `SIGN`, `CEIL`/`CEILING`, `FLOOR`, `MOD`,
`ROUND`, and `TRUNCATE` must not take different numeric paths merely because
they originated as literals, columns, or prepared parameters. The contract
covers conversion mode and warnings, binary-literal provenance, control versus
value roles, plan specialization/reuse, and remote execution compatibility.

The change does not make MySQL and MatrixOne native behavior identical, widen
general string-to-`INT64` conversion, transfer a string-math source role across
an explicit SQL `CAST`, or add user-selectable SQL overloads. It uses private
plan instruction IDs for paired execution outputs and does not promise that
older peers implement newer semantics. String-to-floating `CAST` still follows
the selected strict/default or `MYSQL_NUMERIC_COMPATIBILITY` policy.

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

### HEX/BIT provenance and paired expression outputs

- HEX/BIT literals retain their byte-numeric interpretation (`X'31'` and
  `B'00110001'` produce 49); an ordinary `BINARY`/`VARBINARY` string is still
  text-numeric and `'1'` produces 1. Runtime binary-string domain is not numeric
  literal provenance.
- `CASE`, `IF`, and `COALESCE` can select different source kinds per row. The
  plan carries the selected kind as an ordinary BOOL output beside the value
  program; both outputs come from one evaluation of the original selector and
  use the same row mask. `IFF` carries its condition kind as a separate operand
  so truth conversion happens before its lazy value arm is selected.
- Private plan functions `SCALAR_PROGRAM`/`SCALAR_PROGRAM_REF` own the lexical
  value and kind relationship. They are not SQL-callable functions and do not
  add semantic flags to vectors or batches. The implicit numeric cast receives
  the explicit BOOL input; explicit SQL casts remain semantic boundaries.
- A query, dependent scalar result, or SELECT INTO result that must expose a
  kind passenger declares its physical layout separately from its visible
  columns. Compilers validate that layout and keep the BOOL beside its value
  through projection, joins, set operations, recursive feedback, and remote
  execution. Equality keys and user-visible headings continue to describe only
  the SQL columns.
- Table materialization ends the execution-only kind. CTAS, defaults,
  INSERT/INSERT SELECT, UPDATE, and rollback/retry paths store ordinary values;
  a later read does not inherit the original HEX/BIT source marker.

### Ordinary vector and batch ownership

Paired BOOL outputs use the existing vector allocation selection and normal
vector lifecycle. A scalar program owns synthesized rows and invocation
snapshots; a BOOL column borrowed from an input remains owned by that input.
`EvalWithoutResultReusing` returns an owned copy, using the requested
allocation account when one is selected. No numeric-kind bitmap or numeric
metadata trailer is added to vector or batch formats.

### Remote compatibility

MORPC v101 retains its upstream JSON/YearBit contract, signed-INT
WEEK/DAYOFYEAR results retain their independent MORPC v112 floor, and
collation-aware LEAST/GREATEST, transactional database defaults/table charset,
and coordinator-owned parameterized subquery programs retain their independent
v117, v118, and v119 floors. Upstream exact fractional TIMESTAMPADD SECOND
intervals retain their independent MORPC v124 floor. This change's numeric
value/kind programs, private function IDs 605/606, SELECT INTO kind layouts,
dependent scalar result layouts, and strict/default numeric semantics use
MORPC v130. Upstream JSON string-consumer contracts use v129, LOAD IGNORE
remains v125, VIEW metadata remains v126, index/vector-scan features remain
v127, and mixed JSON comparisons remain v128. Numeric support does not relabel
an upstream feature as v130. Function IDs 603/604 remain the upstream view metadata functions. The separate upstream
Unicode-collation metadata contract uses v109. For expressions whose features
require numeric programs, placement falls back to local execution when a
worker is below v130 or has unknown capability, sender preflight rejects a
destination downgrade, and receivers reject private IDs or layout descriptors
they cannot interpret. Unchanged expressions retain their existing
compatibility gates.

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
or claim general historical catalog/data rollback compatibility. Actual
old/new binary interoperability and physical rolling-upgrade/rollback tests
have not been run; the mocked protocol tests do not establish those properties.

## Ownership and correctness decisions

The execution path is:

```text
prepare role/source classification
  -> execute-time source identification
  -> lower value and selected-kind outputs together
  -> consume kind at the owning numeric conversion
  -> validate and transport declared physical layouts
  -> drop execution-only kind at table materialization
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
- Private program IDs 605/606 are validated lexical plan owners. ID 605 defines
  a value program and ID 606 references its value or BOOL output; IDs 603/604
  remain upstream view metadata functions. The numeric feature classifier
  recognizes the complete program and its explicit layout descriptors at
  MORPC v130.
- Cross-operator kind passengers are ordinary BOOL columns declared by the
  lowering pass. DISTINCT, GROUP BY, and set operations retain their original
  visible equality keys and carry passengers only with the chosen payload row.
  Recursive UNION DISTINCT hashes its original SQL key columns, not the BOOL
  passenger. No vector/batch semantic sidecar or trailer-version change is
  involved.
- SELECT INTO stores a plan-level map from each visible output to its optional
  physical BOOL column. Correlated scalar results use a descriptor that marks
  the second physical column as the BOOL kind. Consumers validate width and
  type, then copy the value/kind pair before releasing the producer's buffers.
  Cached plans and descriptor copies retain their declared layouts.
- Materialized table values deliberately lose execution-only kind provenance;
  ordinary table storage does not preserve the source's HEX/BIT identity.
- Existing warning-aware casts own conversion warnings. No new goroutine,
  background resource, external I/O, or retry loop is introduced.
- The `CEIL`/`FLOOR` precision argument retains its source-aware conversion:
  native SQL DOUBLE `2.5` uses the private ties-to-even conversion to `2`, while
  native DECIMAL `2.5` remains `3`; binary or unproven DOUBLE precision uses
  ordinary `INT64` conversion to `3`. The source distinction comes from
  original binding provenance, with binary protocol taking precedence; it is
  not inferred from a numeric payload or binding type. Value DECIMAL domains
  remain separate from precision controls, and explicit SQL casts retain their
  own boundary. Failed specialization cannot replace the
  cached valid plan.
- String-to-number conversion is not monotonic under string ordering. String
  paths therefore do not advertise function-wide zonemap pruning; correctness
  takes priority over that optimization.

## Performance decision

The prepared-role summary now collects all value and precision roles in one
plan traversal. Its Darwin/arm64 microbenchmark reports 48 markers at 7.397
μs/op, 6,792 B/op, and 11 allocations; 192 markers at 26.474 μs/op, 27,240
B/op, and 15 allocations; and 768 markers at 102.021 μs/op, 110,296 B/op, and
22 allocations. The benchmark constructs `ABS(?)` and `CEIL(?, ?)` pairs and
measures only role summarization, not execution or end-to-end query latency.
It does not compare against the old repeated plan walk. Roles are summarized
per execution; no cross-execution role cache is used.

A separate Darwin/arm64 expression microbenchmark evaluates one prebuilt
128-row batch per operation. Native numeric `ABS` measured 553 ns/op, 8 B/op,
and 1 allocation; ordinary text-column `ABS` measured 6.888 μs/op, 56 B/op,
and 2 allocations. For a matched control, direct ordinary-text CASE through
CAST0 measured 12.592 μs/op, 408 B/op, and 6 allocations; the same CASE values
and row selector through a paired program and CAST9 measured 15.532 μs/op,
600 B/op, and 9 allocations. This current-code comparison measured 3 extra
allocations and 192 B/op for the paired path (about 23% more time for this
expression). It is a focused executor microbenchmark, not a historical
before/after comparison or an end-to-end query result. Mixed HEX/text CASE
through the paired program measured 32.751 μs/op, 720 B/op, and 14 allocations;
that case returns 49 for the HEX-selected row and 1 for the text-selected row,
so it is not the matched control. A native integer ROUND/FLOOR/CEIL fixture
also changed from one two-row block to two one-row blocks after removing
function-wide zonemap eligibility. That observed pruning cost is recorded
rather than described as a string-only change. Any optimization must preserve
native exactness, selected-branch behavior, and one evaluation of volatile
expressions.

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
| HEX/BIT row provenance | CASE/IF/COALESCE tests for mixed, uniform, marked-plus-NULL, text, ordinary BINARY, explicit casts, and nested selectors; paired-output masks, allocation, reuse, and cleanup |
| Physical layouts | Query SELECT INTO visible-to-kind mapping, correlated scalar value/BOOL descriptors, deepcopy/retry validation, recursive feedback schemas, and unchanged equality keys |
| Mixed-version admission | Numeric program IDs and nonempty numeric layout descriptors require v130: local fallback below/unknown, v128/v129 rejection, and v130 acceptance. Independent upstream v101 JSON/YearBit, v109 Unicode collation, v112 date results, v117 LEAST/GREATEST, v118 database defaults/table charset, v119 dependent-program controls, v120 current-session KILL authorization/proxy cleanup, v121 exact unsigned DECIMAL DIV, v122 temporal result contracts, v123 capability-gated nonblocking row-lock admission, v124 exact fractional TIMESTAMPADD SECOND intervals, v125 LOAD IGNORE FK admission/diagnostics, v126 VIEW metadata, v127 index/vector scan, v128 mixed JSON, and v129 JSON string consumers; legacy-session rejection |
| SessionInfo wire allocation | Unknown varints at fields 23/26 are not interpreted as numeric mode/marker; field 22 remains `lc_time_names`; fields 24/25 round trip the explicit mode and sender marker; forwarding preserves a zero legacy marker |
| Prepared-plan reuse | Type changes, error-to-success, NULL-to-success, and restoration of the cached base plan |
| Numeric correctness and pruning | Integer/DECIMAL/FLOAT source controls; zonemap endpoint traps must not prune matching rows |
| Resource/performance | Allocation-failure cleanup, copied-result ownership and selected-account accounting, program cache reset/reuse/free, focused race checks, and the measured role-summary and prebuilt 128-row expression microbenchmarks above; neither is an end-to-end query benchmark |

Public prepared-query validation includes scalar precision results and metadata,
the complete-text ROUND/TRUNCATE value-domain path, the binary-protocol numeric
overload/error-reuse case, prepared DECIMAL extrema, and the existing
string-math fixture. Mixed-binary validation records the exact source revisions
and binaries; mocked protocol integers and UTs alone are not evidence of
executable interoperability. A same-address replacement after the final worker
probe remains outside the contract and requires the quiescent replacement
procedure above.
