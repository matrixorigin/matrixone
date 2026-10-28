- Status: draft
- Revision: 1 (2026-10-25)
- Start Date: 2026-10-25
- Authors: PR 31088 contributors
- Implementation PR: https://github.com/matrixorigin/matrixone/pull/31088
- Issue for this RFC: https://github.com/matrixorigin/matrixone/issues/31036
- Implementation described: `f182916ca22c62b56046373303b8cf1fdcd1bab9`
- Design approval: pending independent repository review; no approval is asserted.

# Integer arithmetic execution width and logical precision

## Summary and review boundary

Use checked 64-bit integer execution for binary integer `+` and `*`, while
preserving the source expression's logical precision independently for result
presentation and schema inference. A query-owned, optional precision recipe graph
survives folding and query boundaries without changing executable vector types.

This revision documents the implementation already present in PR 31088. It
responds to [review 5643764396](https://github.com/matrixorigin/matrixone/pull/31088#pullrequestreview-5643764396).
It is a distinct design submission, not a claim that design-first approval
occurred before the existing code. Implementation acceptance remains subject to
independent design approval and subsequent implementation review. Future design
changes must increment this revision and identify affected code and acceptance
cases; publishing this document does not mark the RFC in progress or completed.

## Motivation and acceptance boundary

Issue 31036 records comparison against MySQL 8.0.46: a TINYINT column containing
100 must yield 10000 for `b*b`, and INT multiplication must compute beyond
int32. The previous resolver selected the wider operand width and the otherwise
correct checked kernel rejected these values prematurely. Merely returning
BIGINT everywhere fixes values but loses the issue's CTAS and prepared metadata
requirements: for example `TINYINT*TINYINT` infers INT for CTAS, whereas
`INT*INT` infers BIGINT. The issue supplies the compatibility baseline and
reproduction; logical precision is not a proof of runtime value bounds.

The invariants are:

1. Executable arithmetic uses signed int64 for signed inputs, uint64 for unsigned
   inputs, and the existing checked mixed-integer overload for mixed signs.
   Negative unsigned results and actual 64-bit overflow still fail; NULL remains
   NULL. Existing mixed-overload support is a deployment prerequisite.
2. Metadata never narrows an executable vector or chooses an arithmetic kernel.
   Logical precision may change CTAS/view column inference and MySQL display
   length, but cannot make an overflowing integer computation succeed.
3. Missing or unknown logical sources preserve physical typing rather than
   inventing precision. Malformed present graphs are rejected at admission.
4. A prepared statement retains effective source domains, not prior values.
   NULL uses the previous admitted domain (or the inferred initial domain).
   Packet/plan cache identity must reflect that effective domain.

Non-goals are changing subtraction, division, decimal/string arithmetic,
historical table/view migration, a general expression provenance replacement,
a new cache framework, or a new mixed-integer protocol capability. Other numeric
domains keep their existing owners. Compatibility here is the stated integer
contract, not a claim of complete MySQL equivalence for every expression.

## Technical design

### Ownership and data flow

| Owner | State and responsibility | Consumer / lifetime |
| --- | --- | --- |
| Function resolver | Physical input adapters, overload and 64-bit result type | Existing executor; independent of the graph |
| `QueryBuilder.integerPrecision` | Source recipes, expression-to-ID and import maps | Bind-time query lifetime; not shared between sessions |
| `OutputColumnProvenance.IntegerPrecisionID` | Reference into that builder's graph | Carries identity across projection/rewrite boundaries; not another type system |
| `Query.IntegerPrecision` (protobuf field 20) | Optional canonical recipes and one output record per heading | Bound plan, deep copies, RPC admission and metadata evaluation |
| `ColDef.IntegerResultPresentation` (field 24) | Optional evaluated display length | Central frontend column conversion; not execution or storage typing |
| `PrepareStmt.integerArithmeticBindings` | Last admitted source binding per relevant parameter slot | Statement lifetime; released on CLOSE |

Binding captures syntax before physical adapters or folding erase it. Explicit
SQL casts are sealed sources; binder-generated physical casts are transparent.
The existing provenance structure still owns column origin, flags and its other
metadata. Integer IDs only add a reference to precision recipes; they do not
replace `Expr.Typ`, alter source-column privileges, or duplicate executable plans.

Ordinary derived tables, views, CTEs and set outputs establish SQL-reference
classification. Internal grouping/window rewrites preserve original expression
identity instead of pretending to be user-written references. Imports copy and
remap IDs into the destination query and preserve source budgets; the import map
avoids copying the same source graph repeatedly within that builder. Query and
column deep-copy paths own independent graph/presentation copies. Capture attaches
the graph only to eligible SELECT/RETURNING output when arithmetic provenance is
present. Bind completion validates it before consumers evaluate recipes.

### Canonical recipes and precision rules

Nodes use one-based IDs; zero is a valid unknown source, never an array index.
Every nonzero input refers to an earlier node. Derived nodes serialize a recipe,
not a cached numeric answer. Outputs record a root class separately from the
node's arithmetic provenance.

| Node | Precision / interpretation |
| --- | --- |
| DECLARED | Integer declaration digits: TINYINT 3, SMALLINT 5, MEDIUMINT 8, INT 10, BIGINT signed 19 / unsigned 20 |
| SQL_LITERAL | Signless integer token length, including leading zeros; unary signs/parentheses preserve the lexical source |
| EXPLICIT_CAST, PARAMETER | SIGNED raw precision 20; UNSIGNED 21; explicit casts stop source-history traversal |
| ADD | `max(offered(left), offered(right)) + 1` |
| MUL | `min(offered(left) + offered(right), 65)` |
| REFERENCE | Preserve the referenced precision with SQL-reference root classification |
| COMMON_INTEGER | Combine integer branch display envelopes; an unknown required branch makes the result unknown |

`offered` uses raw precision for a direct SQL literal and clamps other sources to
1..65. ADD/MUL result unsignedness is the OR of input signs. Display length adds
one sign character for signed precision. A pure untyped NULL is neutral in CASE;
a typed NULL has a real declared/cast envelope. A decimal common domain stays
with decimal typing and receives no integer presentation override. Long lexical
tokens can therefore have display precision above 65 without changing values or
allowing uint64 arithmetic to wrap. Conversion of an integer literal into string,
binary or JSON data still uses its numeric value; lexical zeros are metadata,
not preserved assignment text.

`GetResultColumnsFromPlan` evaluates admitted recipes once per metadata request.
It attaches presentation only when the result is int64/uint64, the source is
arithmetic-derived, known, and agrees with the physical signedness. Frontend
`colDef2MysqlColumn` keeps LONGLONG and its flags, applies display length, and sets
decimals to zero. Ordinary source columns and noninteger outputs keep the existing
conversion path.

`integerSchemaType` selects INT/INT UNSIGNED when display length is at most 11
for a direct CTAS expression, or at most 9 for a SQL-reference root or view
descriptor; otherwise it selects BIGINT/BIGINT UNSIGNED. It applies only to known
arithmetic-derived integer outputs with matching signedness. These are schema
inference rules, not runtime narrowing. Prepared CTAS refresh applies the same
rules to privately rebound source columns, subject to the existing inferred-column,
column-identity and type-dependent-default guards. Explicitly declared target
columns retain their declared type, and assignment still checks whether computed
values fit that type; precision is not a guarantee that any chosen INT schema
can store every possible runtime value. Existing stored schemas are unchanged;
recreating a view refreshes its descriptor. Selecting a view rebinds its body and
does not silently migrate its catalog descriptor.

### Admission, costs and trust boundaries

`Query.ValidateIntegerPrecision`, including generated `Query.Unmarshal`'s final
admission call, checks output arity, nil nodes, topological edges, legal kinds and
root classes, leaf/derived arity, source precision, parameter positions and
derived arithmetic provenance. A present invalid graph returns MatrixOne invalid
input before metadata indexing. A nil graph is accepted for older plans.

`SourceBudget` starts at 65 and covers the largest admitted integer token or
parameter-count envelope, including imported view bodies. Admission requires
`65 <= SourceBudget <= MaxUint32-2` and `ParameterCount <= SourceBudget`.
It bounds precision arithmetic and display conversion; **it is not a graph-node,
serialized-byte or memory quota**. Derived recipes cannot contain forged cached
precision. Unknown edges propagate unknown results and prohibit narrowing.

For N nodes, E input edges, O outputs and P parameter slots, graph admission and
evaluation are O(N+E+O); evaluation allocates O(N) scratch, and the retained domain
slice is O(P). Bind maps and copied/imported graphs use O(N+E) per owned graph;
imports can include nodes unused by the outer projection. No graph lookup or
allocation is added per data row. Existing casts from narrow to wide physical
vectors still have execution cost; this document makes no throughput claim.
No background worker, goroutine, timer, cross-session cache or unbounded execution
history is introduced. A statement replaces its current binding slice/domain
records, rather than appending one record per execution.

There is no new independent hard N/E quota in this revision. SQL/plan input size
and surrounding service admission remain the outer resource constraints; graph
validation runs after protobuf allocation and is not a standalone hostile-message
memory defense. This is an explicit limitation for design review, not a claimed
capacity guarantee. A deployment requiring a stricter graph-specific quota needs
a separate bound and rejection-policy decision before that rollout. Parameter
values, secrets and row data are not serialized in this metadata graph; existing
SQL authorization and service RPC trust boundaries are unchanged.

### Prepared domains, errors and cache publication

Only parameters reachable from arithmetic output recipes participate in retained
history; sealed casts and unrelated markers do not acquire this state. The
frontend first parses/adopts the complete payload, performs existing parameter
checks and produces typed bindings. `rememberIntegerArithmeticBindings` then
updates the effective domains **before** specialization and SQL evaluation.

| Event | Domain history and subsequent behavior |
| --- | --- |
| First NULL | Use the template's inferred int64/uint64 domain |
| Admitted non-NULL | Replace the relevant slot with the full current binding, including decimal/string transitions |
| Later NULL | Reuse the last admitted domain; do not resurrect stale integer metadata after a decimal/string transition |
| Malformed payload / binding admission error | Do not publish a domain from that invalid payload |
| Error, cancellation or failed specialization after domain admission | The admitted domain is retained; it is not a transactionally rolled-back value |
| COM_STMT_RESET / parameter-buffer cleanup | Clear execution buffers using existing ownership; retain domain history |
| CLOSE / statement destruction | Clear history along with existing statement-owned plan/cache resources |
| New prepare or reconnect/restart | New statement and fresh inferred history; no domain history is persisted |

Effective domains feed the existing framed binding key before the existing field
revision is appended. Sign/category changes therefore cannot reuse the previous
packet/plan identity accidentally. Existing diagnostic, value-dependence and
reprepare guards still govern cache eligibility; history does not waive them.
This PR adds no concurrent statement execution model: the existing session-owned
execution contract remains in force. Retries use the admitted generation rather
than accumulating a new global history. On legacy parameter-replacement/DDL paths,
a private plan copy refreshes parameter recipes; noninteger domains invalidate
their integer edges instead of guessing digits. Template graphs stay immutable.

### Compatibility and deployment

The added protobuf fields are optional metadata. New readers accept absent
graphs and retain physical types without presentation overrides; old readers can
ignore unknown fields. Neither serialized executable expressions nor overload IDs
depend on the graph. Existing executors still need the existing mixed-integer
overload 2 to execute signatures using it. This design allocates no new MORPC
version or capability and does not promise execution on revisions lacking that
overload.

Ignoring graph/presentation fields preserves executable arithmetic, but does not
provide the new metadata contract on an old planning/frontend owner. Mixed-version
rollout must not claim uniform new CTAS/view/display behavior until those owners
are upgraded. There is no cross-binary rolling-upgrade proof in the current test
matrix. Local tests that remove metadata or serialize an executable expression
prove separation of concerns, not such a deployment test.

No existing table or historical view descriptor is rewritten on upgrade, restart,
backup or restore. Normal catalog schemas remain their authority. Rollback of
binaries restores their previous inference behavior; tables created under the
new inference retain their stored schemas. Prepared handles are session-local
and must be recreated across reconnect/restart. There is no persisted graph
migration or backfill job and no new configuration switch. Use existing query
errors, packet metadata and catalog schema inspection for diagnosis. If behavior
must be withdrawn, revert the coherent resolver/metadata change and recreate
affected views deliberately; do not retain widened kernels with arbitrary stale
inference assumptions or rewrite customer schemas automatically.

## Verification and acceptance cases

The following executable tests already exist in this PR. This document changes
no production code or test oracle; execution evidence belongs in the PR body and
CI artifacts, not committed validation receipts.

| Invariant / nearest control | Existing deterministic oracle |
| --- | --- |
| Narrow signed/unsigned operands compute in 64-bit; true overflow and NULL remain correct | `pkg/sql/plan/function/integer_add_mul_test.go`, `TestIntegerArithmeticWidth` vectors/aggregate and prepared recovery cases |
| Lexical digits, explicit casts, unknown/imported recipes, deep-copy isolation | `TestIntegerPrecisionLiteralPlan`, `TestIntegerPrecisionImportAndUnknown` |
| Malformed graph fails before metadata access; nil metadata accepted | `pkg/pb/plan: TestIntegerPrecisionAdmission` (direct and Query.Unmarshal) |
| Graph removal cannot change executable nodes; serialized wide signature executes | `TestIntegerPrecisionOptionalMetadataDoesNotExecute`, `TestIntegerPrecisionOldConsumerExecutesWidenedSignature` (same-binary compatibility model) |
| Prepared wire type/sign/length, NULL history, cache identity, malformed/reset/close | `pkg/frontend: TestIntegerArithmeticPresentationPackets`, `TestIntegerArithmeticDomainHistoryAndCacheKey`, `TestIntegerArithmeticDomainMalformedResetAndClose` |
| Decimal/common owner and legacy rebinding, cast ownership stop | `TestIntegerArithmeticMixedCommonUsesDecimalOwner`, `TestIntegerPrecisionLegacyParameterRefresh`, `TestIntegerPrecisionParameterOwnershipStopsAtCast` |
| Direct versus reference schema rules, set outputs and internal grouping/window boundaries | `TestIntegerPrecisionSetOperationOutputs`, `TestIntegerPrecisionCollapsedUnionIsReference`, `TestIntegerPrecisionGroupingSchemaRoots`, `TestIntegerPrecisionWindowGroupingActualReferences`; `TestIntegerArithmeticWidth` CTAS/view/CTE and 48 grouping/window combinations |
| Public comparison and neighboring assignment/mixed-sign behavior | `test/distributed/cases/expression/mixed_unsigned_arithmetic.sql`, dtype binary/boundary and insert-duplicate cases; affine SUM prepared rebind control |

Acceptance requires correct independent value, error, packet and stored-schema
oracles, including successful execution after an overflow and same-instance
cleanup/reuse. Existing single-CN fixtures suffice for the metadata state machine;
CI's multi-CN lanes supply broader consumer regression coverage. New topology,
benchmarks or repeated rounds are warranted only for a concrete changed risk,
not by adding this document. Cross-binary mixed-version metadata equivalence and
a dedicated adversarial graph-capacity budget are not claimed as validated.

## Rationale / alternatives

1. **Keep narrow physical types.** Smallest code surface but violates the issue's
   ordinary arithmetic values. Relaxing overflow checks would lose correctness.
2. **Widen physical results and infer all metadata from BIGINT.** Fixes values
   with little extra state, but loses token/declaration precision after folding
   and fails CTAS/prepared metadata acceptance. This cannot meet the whole issue.
3. **Put presentation width directly in every executable expression type.**
   Conflates schema/presentation with vectors and adapters, propagates through
   optimizer copies and overload selection, and cannot express direct-expression
   versus SQL-reference schema classification cleanly.
4. **Selected: optional query-owned recipes plus provenance IDs.** Extra graph
   and admission complexity, but preserves executable types and source identity
   across folding/copy/import boundaries, centralizes schema and packet consumers,
   and has an explicit unknown fallback. It reuses existing statement/cache owners
   instead of introducing a separate retained expression cache.
5. **Infer precision later from optimized expressions or cache it on each
   expression.** Late reconstruction loses token lengths and original SQL boundary
   identity after folding. Per-expression cached answers require invalidation
   across parameter rebinding and copying, and an independent distinction between
   trusted answers and received metadata. Canonical source recipes retain those
   inputs and recompute the answer at the metadata owner instead.

## Drawbacks and open decisions

The graph adds bind/metadata memory and protobuf size, requires generated
admission/deep-copy maintenance, and encodes context-sensitive schema rules that
must stay aligned across result, view and CTAS consumers. Preserving domain
history across RESET and post-admission failures is intentional but must remain
visible to reviewers and covered by independent packet/value oracles.

The repository design approvers must decide whether this architecture and its
documented resource/mixed-version limits are acceptable before implementation
acceptance. This RFC does not choose approvers, grant itself approval, or treat a
passing CI run as approval. No additional product-scope decision is invented by
this documentation change. Any requested stronger capacity or compatibility
guarantee must identify its relation to the issue or an introduced regression,
and its validation and implementation scope, before changing this contract.
