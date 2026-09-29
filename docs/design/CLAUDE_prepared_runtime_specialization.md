# Prepared Statement Runtime Specialization Contract

Status: versioned implementation contract for prepared-plan runtime specialization.

## Goal

A direct statement, its first prepared execution, and every reused prepared execution must expose the same SQL value and wire-visible result metadata for the same logical inputs. PREPARE-time placeholder types are provisional and must not become observable semantics.

## Ownership model

Each runtime parameter has three independent domains:

1. **Transport domain**: the cached marker representation used by COM_STMT or SQL `EXECUTE USING`.
2. **Source domain**: assignment-time `SourceType` for SQL user variables, or binary-protocol `RuntimeType`.
3. **Consumer domain**: the type required by the occurrence's SQL role.

Source-domain provenance belongs to the parameter occurrence that consumes it. A parameter position is not sufficient because one marker can appear in both comparison and result roles after rewrites such as `NULLIF` to `CASE`.

Binder-generated casts and peers carry sparse `PreparedNumericMetadata`. User-authored casts never carry provisional metadata.

## Canonical result consumers

Result-polymorphic classification is centralized in `preparedNumericResultPolymorphicFunction` and uses canonical function identity. Aliases are canonicalized before classification; currently `iff` canonicalizes to `if`.

Value-role ownership is centralized in `preparedSQLExecuteNumericResultValueArg`:

- conditional/common-value functions propagate only result/value arguments;
- type-preserving unary aggregates and window functions propagate their value argument;
- `LAG`/`LEAD` offsets, `NTH_VALUE` index arguments, predicates, conditions, and ordering keys are control arguments and do not own the result domain;
- `MAX_BY` variants propagate the returned value argument, not the key argument.

Adding an alias or result-polymorphic function requires updating the canonical identity/contract and the validation matrix, not adding a separate execution-time allowlist.

## Hard boundaries

- A user-authored explicit `CAST` is authoritative and source discovery must not cross it.
- Binder-generated marker casts marked provisional may be removed and rebound at execution.
- Comparison occurrences retain MySQL numeric-prefix behavior for string-backed sources.
- A shared comparison/result occurrence, such as rewritten `NULLIF`, uses numeric-prefix semantics only in the comparison; the result occurrence preserves the original string-family source type, including binary charset and `VARBINARY` wire metadata.
- Assignment casts and DML write roots retain their target-domain contract.
- `LIMIT`, `OFFSET`, and `LAG`/`LEAD` offset markers have fixed unsigned-integer contracts and are excluded from source-domain result specialization.

## Cache equivalence and invalidation

A runtime specialization cache key represents semantic parameter categories, not concrete values. Plans containing retained parameter references restore current values before execution. A cache hit is legal only when overload choice, result domain, numeric-prefix category, and direct-result positions are equivalent.

Runtime text-comparison detection is skipped when no parameter is string-backed. When needed, its first result is passed to specialization; an uncached execution must not repeat the plan walk.

## LockRows ABI

Runtime specialization may rebuild expressions but must preserve DML write-root shape and `LockRows` positional/physical-key contracts. Lock expressions are normalized only through the dedicated DML-preserving specialization path; source-domain propagation must not change lock target types or column positions.

## Protobuf compatibility

Prepared provenance is sparse optional metadata. New fields are additive protobuf fields:

- older CNs ignore unknown metadata and retain legacy behavior;
- newer CNs treat absent fields as non-provisional;
- explicit-cast authority never depends on an unknown field defaulting to true;
- semantic provenance is not encoded in executor memo IDs.

Mixed-version rollout must therefore remain safe, although exact direct/prepared parity is guaranteed only after all executing CNs understand the corresponding provenance field.

## Hot-path budget

Ordinary executions without runtime-sensitive consumers must retain the cached plan/compile path and perform no unconditional full-plan scan. Runtime work is bounded by:

- prepare-time cached consumer/position discovery;
- one text-comparison scan only when a string-backed parameter exists;
- one copied-plan rewrite only when specialization is required.

## Validation matrix

Every contract change must cover relevant rows below:

| Dimension | Required evidence |
| --- | --- |
| Execution lifecycle | direct, first prepared execution, and at least one reused execution |
| Observation | value and `DatabaseTypeName`; charset/binary type when applicable |
| Numeric precision | exact DECIMAL value above 2^53 |
| Canonical names | canonical function and every registered alias, including `IF`/`IFF` |
| Consumer families | CASE/common-value, GREATEST/LEAST, SUM/AVG, MIN/MAX/ANY_VALUE, value-preserving windows, MAX_BY variants |
| Boundaries | explicit CAST, condition/control arguments, assignment/DML roots, LIMIT/OFFSET, LAG/LEAD offsets |
| Occurrence roles | rewritten NULLIF comparison and result occurrences using the same position |
| Source families | exact numeric, text numeric-prefix with trailing text, CHAR/VARCHAR/TEXT, BINARY/VARBINARY/BLOB |
| Cache | fresh and reused values from different exact numeric values/categories |
| Performance | no string-backed parameter means no text-comparison plan walk; no duplicate scan |
| Compatibility | protobuf round-trip/deep-copy and absent-field behavior |

Owning-package tests and focused embedded SQL parity tests are required before merge. CI failures in prepared/vector pagination must additionally validate that fixed integer contracts remain `UINT64` despite assignment-time numeric SourceType.
