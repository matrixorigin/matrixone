# Prepared JSON Comparison Semantics

- Status: proposed for PR #27540
- Tracking issue: #27529
- Wire capability: MORPC v30

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
4. Exact integer width/signedness and FLOAT32 rounding are preserved when they
   affect typed-literal parity. Common BOOL, DOUBLE, DECIMAL, string, and NULL
   cases keep the existing coarse category path.
5. Missing, malformed, incompatible, or version-ineligible metadata fails
   closed. It must not silently change comparison semantics.
6. Reset, retry, selected-row evaluation, remote dispatch, and plan rebuild
   cannot leak metadata from an earlier execution generation.

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
DECIMAL already select their established comparison domains. Adding exact
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
no text marshal/unmarshal and no per-row allocation.

Concrete integer and FLOAT32 metadata applies the same range checks, errors,
and rounding as the equivalent typed cast. The category fallback retains the
established BOOL/integer/FLOAT64/DECIMAL/string behavior.

## Compatibility and unhappy paths

The hidden function and exact metadata require MORPC v30. A new coordinator
must reject remote dispatch below v30 for every adapter plan, including BOOL
and string parameters that do not carry exact metadata. Typed metadata is also
validated independently at codec boundaries.

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
- Constant adapter encoding and constant operand decoding occur once per batch;
  the row loop remains allocation-free.

## Validation

Required gates:

- direct/prepared parity for BOOL, string, signed/unsigned integer boundaries,
  values around 2^53, FLOAT32 rounding, DOUBLE, DECIMAL, NULL, object, and array;
- both orientations across `=`, `<=>`, `IN`, and `NOT IN`;
- ordinary JSON joins and unrelated feature-registry calls;
- `COUNT(NULL)` and `COUNT(CAST(NULL AS SIGNED))` remain zero;
- all-selected, partially selected, and all-masked execution;
- local and remote Process codec at v29/v30, including malformed metadata;
- reset/reuse and plan-rebuild generations;
- focused race tests and allocation benchmark for the JSON comparison row loop.
