# Prepared JSON Comparison Semantics

- Status: proposed for PR #27540
- Tracking issue: #27529
- Wire capabilities: numeric-prefix casts use MORPC v30; prepared JSON comparison uses MORPC v36

## Problem

Prepared parameters are stored as TEXT so one cached plan can accept different
runtime bindings. JSON equality cannot use that transport type as the SQL type:
a Boolean `true`, the string `"true"`, FLOAT `16777216`, and BIGINT
`9223372036854775807` need different coercion and error behavior.

The equivalent typed literal is the semantic oracle. This must hold for `=`,
`<>`, `!=`, `<=>`, `IN`, and `NOT IN`, in either operand orientation.

## Invariants

1. Runtime SQL type and string-conversion provenance are different facts.
2. Only the binder-inserted JSON comparison adapter may enter the prepared-only
   comparison path. Ordinary JSON vectors remain on the native JSON fast path,
   even if vector materialization carries parameter provenance.
3. A cached plan is value-independent. Runtime values remain owned by Process.
4. Exact integer width/signedness, FLOAT32 rounding, and DECIMAL value are
   preserved when they affect typed-literal parity. Exact DECIMAL and numeric
   string spellings never pass through FLOAT64; only actual FLOAT/DOUBLE and
   JSON FLOAT64 values use floating-point semantics.
5. Missing, malformed, incompatible, or version-ineligible metadata fails
   closed. It must not silently change comparison semantics.
6. Reset, retry, selected-row evaluation, remote dispatch, and plan rebuild
   cannot leak metadata from an earlier execution generation.
7. Comparison category and explicit cast behavior are independent. A JSON
   string compared with a SQL BOOL is a definite category mismatch, not SQL
   NULL; an explicit `CAST(JSON AS BOOL)` continues to parse supported string
   spellings according to the public cast contract.
8. Ordinary `=`/`!=` JSON/BOOL results remain planner-nullable even when both
   vector operands are declared NOT NULL, because a non-NULL JSON container can
   hold the JSON null scalar. `<=>` remains non-NULL by contract.

## Design

### Plan ownership

The binder wraps only a direct dynamic parameter paired with JSON equality in
`__mo_json_comparison_param`. `PrepareStmt` scans the plan once per generation
and caches the affected parameter positions. Schema/protocol rebuild recomputes
that bounded position list.

### Runtime metadata ownership

The frontend derives parameter category from the protocol/user-variable value.
For affected positions only, it also records concrete int8-int64,
uint8-uint64, or FLOAT32 type. Process owns the packed per-execution metadata;
its reusable buffer avoids steady-state allocation. Remote Process transport
uses the existing category sections plus eight exact-type bits.

For SQL `EXECUTE ... USING`, the concrete type comes from the binder's
assignment-time user-variable type. Re-inferring it from the decoded Go value
is not equivalent: that compatibility inference intentionally widens all
signed integers to BIGINT and all unsigned integers to BIGINT UNSIGNED. Callers
without binder metadata retain that conservative fallback, while an exact
type/category mismatch fails closed.

Exact types are intentionally sparse. BOOL has its own category; FLOAT64 and
DECIMAL select their source comparison domains. Adding exact
metadata to unrelated parameters would increase wire size and rolling-upgrade
surface without changing semantics.

### Expression identity

The adapter emits JSON data plus two orthogonal metadata axes:

- value provenance/category and optional concrete SQL type;
- an adapter-output identity bit.

The identity bit is set only by the adapter, cleared on vector reset, and
preserved only while the same adapter result is scattered from a selected-row
evaluation. It is not inferred from `PrepareParamKind`, merged across ordinary
vector operations, or set by ParamExpressionExecutor.

The JSON equality functions inspect only the identity bit before selecting the
prepared comparator. Exactly one operand must have it. Consequently ordinary
JSON column joins and unrelated functions cannot enter the prepared path.

### Comparison

The adapter encodes a constant parameter once per batch and shares the varlena
payload. The comparator decodes constant operands once, honors the selection
mask before conversion, and uses native ByteJSON scalar accessors. It performs
no text marshal/unmarshal. Fixed-width and BOOL paths do not allocate per row;
exact DECIMAL normalization is bounded by the numeric spelling.

Concrete integer and FLOAT32 metadata applies the same range checks, errors,
and rounding as the equivalent typed cast. Integer and DECIMAL category
fallbacks share ByteJSON's exact normalized coefficient/exponent model:
integer conversion truncates toward zero before range checking, and DECIMAL
comparison uses exact cross-numeric comparison. FLOAT64 remains deliberately
floating-point; BOOL and string retain their category behavior.

Direct JSON/BOOL equality also preserves the JSON scalar category instead of
inserting the generic JSON-to-BOOL cast. JSON booleans and numbers retain the
established boolean comparison coercion, JSON strings compare unequal to BOOL,
JSON null produces SQL UNKNOWN, and object/array inputs retain the established
cast error. Prepared BOOL comparison uses the same evaluator, so `=`, `!=`,
`<>`, `<=>`, `IN`, and `NOT IN` have one truth table across both execution
protocols and operand orientations.

## Compatibility and unhappy paths

MORPC v30 contains only the pre-existing numeric-prefix cast capability.
MORPC v36 independently introduces the hidden prepared-JSON adapter, exact
typed parameter metadata, and direct mixed JSON/BOOL equality execution. The
last capability has no new protobuf field, but pre-v36 workers dispatch the
BOOL operand through a varlena comparison overload and therefore cannot safely
execute the plan. The owner scanner identifies the final physical equality
expressions (`=`, `!=`/`<>`, and `<=>`); mixed `IN`/`NOT IN` elements are
covered after their binder lowering to equality.

Protocol versions are monotonic rollout capabilities and must never be reused
for a later feature: a worker advertising the old capability would otherwise
be mistaken for an implementation of the new one. Both sender and receiver
therefore reject numeric-prefix pipelines below v30 and prepared-JSON or mixed
JSON/BOOL pipelines below v36. Adapter plans are rejected even for BOOL and
string parameters that do not carry exact metadata. Typed metadata is also
validated independently at both codec boundaries and requires v36.

Invalid metadata length, unsupported type IDs, category/type mismatches,
truncated JSON, invalid literal codes, overflow, and unsupported object/array
casts return bounded errors. They do not panic, evaluate masked rows, or fall
back to a semantically different comparison.

Reset clears category, concrete type, and adapter identity together. Process
replacement/Free releases owned parameter vectors once; cached PrepareStmt
buffers contain metadata only and are bounded by parameter count.

## Performance contract

- Ordinary JSON equality adds one predictable false identity check per operand
  and otherwise executes the pre-existing native comparison.
- Ordinary prepared statements do not scan their plan or add typed metadata at
  EXECUTE time.
- Affected positions are computed once per plan generation.
- Metadata storage is O(parameter count), reused across executions, and sent in
  the twelve-section form only when at least one exact type is present.
- Constant adapter encoding, constant operand decoding, and constant DECIMAL
  normalization occur once per batch. Integer/FLOAT64/BOOL row paths remain
  allocation-free. Per-row DECIMAL work is proportional to the row's numeric
  spelling and never materializes exponent-sized powers of ten.

## Validation

Required gates:

- direct/prepared parity for BOOL, string, signed/unsigned integer boundaries,
  values around 2^53, FLOAT32 rounding, DOUBLE, DECIMAL, NULL, object, and array;
- both orientations across `=`, `!=`, `<>`, `<=>`, `IN`, and `NOT IN`, including
  the distinction between JSON-string/BOOL mismatch and actual SQL UNKNOWN;
- explicit JSON-to-BOOL string casts as an independent non-regression control;
- ordinary JSON joins and unrelated feature-registry calls;
- `COUNT(NULL)` and `COUNT(CAST(NULL AS SIGNED))` remain zero;
- all-selected, partially selected, and all-masked execution;
- local and remote Process codec at v35/v36, including malformed metadata;
- sender and receiver v29 rejection/v30 acceptance for numeric-prefix casts;
- sender and receiver v35 rejection/v36 acceptance for prepared JSON and direct
  JSON/BOOL `=`, `!=`/`<>`, and `<=>` in both orientations, plus lowered
  `IN`/`NOT IN` plans;
- reset/reuse and plan-rebuild generations;
- focused race tests and allocation benchmark for the JSON comparison row loop.
