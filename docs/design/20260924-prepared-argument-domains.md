# Prepared argument domains through execution and CTAS

- Status: Proposed for design review
- Design revision: 4
- Owning issues: [#29306](https://github.com/matrixorigin/matrixone/issues/29306), [#29307](https://github.com/matrixorigin/matrixone/issues/29307), [#29311](https://github.com/matrixorigin/matrixone/issues/29311), [#29313](https://github.com/matrixorigin/matrixone/issues/29313), [#29314](https://github.com/matrixorigin/matrixone/issues/29314)
- Implementation PR: [#29369](https://github.com/matrixorigin/matrixone/pull/29369)
- Last updated: 2026-09-24

## Problem and evidence

SQL `PREPARE` carries unknown parameters as text. That transport type is not the
SQL source type of a `USING` variable or the type advertised by COM_STMT_EXECUTE.
The five issues show four first violations: `generate_series` binds numeric
markers as temporal endpoints; `UNNEST` rejects valid JSON text before parsing;
HLL/bitmap state consumers bind opaque bytes as TEXT; and prepared numeric
aggregates lose ENUM/SET provenance. A fifth violation occurs when the private
integer precision conversion for CEIL/FLOOR turns a scalar parameter into a
flat vector. Existing direct queries and explicit-cast controls establish the
intended function contracts. PR #29369 also exposed downstream failures in
UNION ordering, SUM/AVG, and prepared CTAS after the direct call was repaired.

This design covers the complete prepared execution path, including SQL `EXECUTE`,
COM_STMT_EXECUTE, cached-plan reuse, CTAS's internal INSERT, and retry. The
existing PR code is an implementation candidate, not the authority for this
contract.

## Protocol precedent and scope

[MySQL 8.4 SQL EXECUTE](https://dev.mysql.com/doc/refman/8.4/en/execute.html)
requires one `USING` variable per marker and permits a prepared statement to
run repeatedly with different variables. The
[COM_STMT_EXECUTE protocol](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html)
carries each parameter's type and unsigned flag along with its value. These
are interoperability inputs for count, position, reuse and binary type
provenance. MatrixOne's table functions, opaque aggregate states and CTAS
internal reparse are its own implementation contracts; MySQL does not define
their behavior. No new wire field or client obligation is proposed.

## Invariants and decisions

1. **Template isolation and cache identity.** PREPARE owns an immutable template.
   Each EXECUTE owns its values and visible result metadata. Logical and
   physical generations may be reused only when the cache key covers every
   semantic choice that changes an overload, datetime scale, string/binary
   domain or output schema, and every value-dependent expression remains a
   runtime parameter reference. A materialized literal or value-dependent
   operator setting makes that generation ineligible for reuse. Same-domain
   executions with different values, and a failed execution followed by a
   successful one, must observe the current values and metadata. Reuse cannot
   mutate the template or leak one execution's values into another.
2. **Domain provenance.** The frontend records assignment-time `SourceType` for
   SQL `EXECUTE USING` and protocol `RuntimeType` for COM_STMT_EXECUTE. The
   transport vector carries bytes and NULLs. A known explicit SQL cast defines
   its own boundary and takes precedence. No generic conversion infers numeric
   type from a string's characters: `'1'` remains a string source unless the
   consuming function's established SQL contract converts it.
3. **Consumer coherence.** A function scan's argument vectors, declared output
   column, projection and result metadata, and the typed expressions of its
   consumers describe the same execution generation. Rebinding must cover
   aggregate, predicate, window, sort, set operation and DML write paths that
   depend on a changed type. If an owner cannot safely rebind a consumer, fail
   planning rather than execute with inconsistent types.
4. **CTAS coherence.** Inferred target columns use the execution generation's
   SELECT output schema, including type and dependent default/nullability
   metadata. Explicit target column definitions remain authoritative. Mapping
   from SELECT output positions to target columns must account for target-only
   columns and explicit overrides; duplicate or ambiguous headings must fail
   through the existing CTAS validation, not choose the first name match.
5. **Internal parameter identity.** CTAS's generated INSERT has exactly the
   original prepared marker count and order before it receives semantic
   parameter values. A mismatch when semantic values are present is an error;
   silently dropping source types would create a different INSERT contract.
   The internal executor uses the same current-generation source domains after
   every plan build, including retry. It must not re-resolve session variables.
6. **Scalar identity.** A parameter-derived precision stays scalar through the
   private integer conversion, including a CAST chain and selected-row
   evaluation. A flat column whose values happen to be equal is not scalar.
   NULL, conversion errors, masks and executor reuse retain their ordinary
   error and cleanup behavior.

### Function-specific boundaries

- `generate_series` chooses its integer or temporal endpoint path from the
  first endpoint's declared or execution source domain. Integer endpoints and
  step are converted to INT64 with range checking. Temporal endpoints use
  DATETIME with scale derived from their actual values; the interval step is
  VARCHAR. A string source follows the existing temporal-string contract. The
  operator's argument vectors and output type are fixed before execution.
- `UNNEST` accepts lossless JSON TEXT or JSON for its first argument, VARCHAR
  path, and BOOL outer flag. Invalid JSON and invalid path still return errors.
  Parameter bytes beyond 65,535 bytes cannot be narrowed to VARCHAR.
- Direct untyped markers to HLL_CARDINALITY, HLL_MERGE_AGG, and BITMAP_OR_AGG
  take VARBINARY, preserving embedded NUL bytes. Explicit casts retain their
  declared domain. Invalid opaque state remains an error and does not poison
  the prepared template.
- Prepared SUM/AVG of stored ENUM/SET values uses the same ordinal/bitmap
  numeric provenance as ordinary SQL, even if the statement has no marker or
  has an unrelated marker. Ordinary VARCHAR aggregate rejection is unchanged.
- CEIL/FLOOR precision applies the existing integer-argument conversion to
  DECIMAL, DOUBLE and text source values and publishes a scalar only when the
  source is scalar. Existing NULL precision error semantics remain.

## Ownership and execution flow

```text
frontend source/protocol type + bytes/NULL
    -> immutable PREPARE template
    -> EXECUTE deep copy + parameter/consumer specialization
    -> public result metadata and colexec vectors
    -> CTAS inferred schema + internally reparsed INSERT (when applicable)
    -> retry rebuild of the same execution generation
    -> release of plan, vectors and semantic sidecar
```

The frontend owns the original parameter values for one statement execution.
The planner owns type changes on an execution copy and validates that references
and consumers agree before physical execution. A physical compile may be cached
only under the cache-identity rule above. The compile object carries a semantic
reference slice only for a prepared CTAS that needs internal reparse; ordinary
prepared queries must not retain it. The internal executor receives that
metadata alongside its existing byte/NULL transport and applies it to its own
plan. Retry inherits the current execution's values before terminal cleanup.
Every success, error, cancellation and failed retry clears the CTAS sidecar
before a compile is cached or pooled; Reset cannot expose an earlier sidecar.
No process-global type cache or durable state is added. Transaction rollback
follows existing statement ownership: no partially specialized plan is
published to PREPARE, and CTAS creation/population follows the existing
transaction path.

For `P` markers and `N` plan nodes, the additional work is bounded by O(P + N)
per specialized execution, plus the existing plan copy and conversion work.
The CTAS-only sidecar copies slice headers and small type descriptors; it does
not copy large parameter payloads a second time or retain them past execution
termination, even when a physical compile remains cached.
No new background work, persistent storage, queue or retry loop is introduced.

## Alternatives considered

| Approach | Correctness and cost | Decision |
| --- | --- | --- |
| Keep TEXT on the cached plan and add only operator exceptions | Leaves overloaded function choice, consumers and CTAS metadata inconsistent. | Reject. |
| Guess domains from the parameter's text bytes | Conflates numeric strings with numbers and risks corrupting opaque binary state. | Reject. |
| Reparse and fully optimize the entire user SQL on every EXECUTE | Can choose types, but discards prepared-plan reuse, adds planning cost and may observe changed session/catalog state. | Reject for this repair. |
| Preserve source domains in a generation-scoped sidecar and specialize a copied plan | Keeps template reuse and explicit-cast boundaries; requires complete downstream rebinding and CTAS sidecar lifetime checks. | Select, subject to validation below. |

## Compatibility, rollout and observability

The external SQL and MySQL binary protocols are unchanged; type provenance is
already available at their frontend boundaries. No catalog or storage format is
changed. The CTAS sidecar stays on the coordinator CN, but a function-scan or
precision expression can execute on a remote CN. In particular a new planner's
UNNEST TEXT argument would be rejected by an old remote operator. This is a
real mixed-version boundary, even though no new wire field is sent.

Use `MORPCVersion95` for this capability. Version 94 is the latest in both
the reviewed base `c701ffa4ef` and remote main `ac46f1a09b` at design review.
Deployment's `MOProtocolVersion` is the oldest live service version. Until it reaches the new capability, the planner must not
publish any newly shaped prepared function/precision plan that relies on an
upgraded executor; affected calls keep the pre-upgrade behavior or an explicitly
proved old-operator-compatible plan. When the cluster floor reaches the new
capability, prepared-plan protocol-version invalidation rebuilds old templates
and physical caches before new plans execute. If the floor falls during
rollback, invalidate again and stop publishing new shapes. Immediately before remote dispatch, the coordinator must read the current
cluster-floor capability and fail closed if it is below 95 and the plan needs
new executor behavior. An already running plan cannot send an incompatible
operator to an older CN. The version gate must be checked at both planning and
remote dispatch, not inferred from the coordinator's binary version alone. If the implementation selects a JSON cast for UNNEST to keep an
old-compatible plan, its >65,535-byte and error semantics need direct proof;
this does not waive the gate for CEIL/FLOOR executor behavior.

Existing SQL diagnostics for invalid values and states remain the user-visible
signal; logs must not include raw parameter payloads. Rollback is a source
revert of this PR and lowering the cluster capability floor; there is no
migration or persisted state to undo. Release requires SQL, binary-protocol
and mixed-CN validation and monitoring of prepared execution errors during
normal rollout.

## Validation and acceptance

| Contract | Cheapest required evidence |
| --- | --- |
| Template isolation and domain changes | Unit plan-copy assertions; same-statement numeric → temporal/text → numeric and same-domain different-value executions after success and failure; cache-key/eligibility checks. |
| Scan and consumer coherence | Planner assertions plus public SQL for SUM/AVG, predicate, window, ORDER BY and numeric/temporal UNION results and result metadata. |
| CTAS schema and reparse | SQL/COM_STMT inferred, explicit and aliased columns; target-only columns and defaults; marker-count mismatch; retry rebuild, cached-compile terminal cleanup and pool reuse, including a large payload on an ordinary prepared query. |
| Lossless UNNEST and opaque states | More than 65,535-byte JSON, invalid JSON, embedded-NUL HLL/bitmap, invalid state and recovery. |
| Integer precision scalar | DECIMAL/DOUBLE/text and explicit CAST, partial/empty masks, flat equal-valued control, NULL/error/reuse. |
| Mixed-version delivery | New planner with old remote CN for lateral/child UNNEST and remote CEIL/FLOOR, capability below/above threshold, in-flight downgrade fencing, and prepared-cache invalidation on upgrade/rollback. |
| Operational evidence | Exact-head CGo package UT, relevant embedded SQL and BVT fixture comparison, incremental vet/SCA, build, plus CI failure classification. |

The implementation review must verify the complete change map against these
invariants. A test that only inspects a plan's immediate function arguments
cannot substitute for public-result and CTAS population checks. CI failures
before SQL begins are classified separately; they cannot count as BVT pass.
