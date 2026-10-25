# ENUM/SET column assignment and storage identity

Owning issue: [#29923](https://github.com/matrixorigin/matrixone/issues/29923).
Implementation: [#29924](https://github.com/matrixorigin/matrixone/pull/29924).
Design revision: D1, 2026-10-25. This document describes the complete feature,
including the repair of transparent derived JOIN assignments. The design review
record must identify the versioned revision of this document before the repair
implementation proceeds.

## Problem and scope

A qualified `UPDATE dst d JOIN src s ... SET d.v=s.v` must bind `s.v` in the
UPDATE's real name-resolution scope. Issue #29923 records valid ENUM/SET updates
being rejected with ambiguous-column error 20301 on build `652c64366642`;
the corresponding VARCHAR control succeeds. A separate default binder must not
rebind the RHS without the join's aliases.

Binding alone is insufficient. ENUM ordinal 0 and a declared empty member both
display as an empty string. SET bitmap 0 and a bitmap containing an empty member
can also have the same display. Parsing display text cannot reconstruct storage
identity. At original PR head `3b8ab42ba066685e5bded7558943b9004b3364d2`,
[review 5644508460](https://github.com/matrixorigin/matrixone/pull/29924#pullrequestreview-5644508460)
records this supported derived JOIN updating ordinal 0 into ordinal 1:

```sql
-- src.v and dst.v are ENUM('','red'); src contains stored ordinal 0.
UPDATE dst d JOIN
  (SELECT a.id,a.v FROM src a JOIN peer p ON a.id=p.id) s
  ON d.id=s.id SET d.v=s.v;
```

That review reports the same failure with ORDER BY/LIMIT, while direct JOIN,
unary derived projection and scalar-subquery controls retain zero. Its raw
artifacts are not present in this workspace; this is attributed historical
execution evidence, not a new local reproduction. The causal source is visible:
assignment recovery proves only unary relational inputs, rejects the derived
JOIN, then falls back to text conversion. Fresh proof is required for the repair.

This feature covers qualified assignments, transparent ordinary joins and
projection/filter/sort/LIMIT/OFFSET chains, source/target domain conversion,
OLD/NEW DML consumers, and admission of the appended typed conversion through
local, remote and persistent-expression boundaries. It preserves the existing
strict, permissive and IGNORE diagnostic policy. It does not redesign general
SQL expression types, storage encoding, the optimizer, or join execution.

## Contracts and first owners

1. The real UPDATE binder owns RHS name resolution. Each explicit RHS is bound
   once in its correct row-image scope. Single-table sequential assignment reads
   preceding NEW values; simultaneous joined assignment reads the original row.
2. A transparent special-type source is `(physical value, member definition,
   effective NULLability, actual relational output path)`. Equal display text
   alone is never proof of equal physical values.
3. Every surviving row on a supported transparent path retains its ENUM ordinal
   or SET bitmap, including zero, a declared empty member, and NULL. Join
   duplication may copy values but must not reinterpret them. Null extension
   yields NULL, not the source column's default or ordinal 0.
4. A semantic expression or identity-destroying relational boundary owns a new
   value. Recovery must not reach below it to fetch a hidden input value. An
   explicit VARCHAR CAST or ordinary SQL IF retains display semantics.
5. The UPDATE normalization owner publishes each explicit NEW value once before
   generated and ON UPDATE consumers. OLD values used for equality checks,
   indexes, locks and unchanged target branches retain the stored domain.
   Internal target-selection IF chooses normalized storage values; inactive
   branches must not evaluate conversions or emit diagnostics.
6. Recovery is read-only until the complete proof succeeds. Failed proof leaves
   projections, types and caches unchanged. Successful materialization appends
   at most one hidden physical slot per display-producing projection position,
   regardless of numeric/assignment consumer order or repeated requests.
7. The scalar conversion owner preserves selected-row error and warning policy,
   evaluates a typed source expression once, and does not decode a definition
   for masked, NULL or zero rows unnecessarily.
8. Function identity is append-only. `CAST_VALUE_TO_INDEX` overload 0 retains its
   released signature and semantics; overload 1 requires MORPC 132. All readers
   and admission owners must discover that requirement before side effects or
   unsupported execution.

Negations are independently observable: error 20301 for a fully qualified RHS;
`CAST(v AS UNSIGNED)=1` after copying a stored ordinal 0; an unmatched outer row
becoming 0; hidden values changing DISTINCT/group cardinality; multiple NEW
side effects; or unsupported workers accepting overload 1.

## Representation and conversion

| Source and destination | Assignment rule |
| --- | --- |
| Same ENUM definition | Carry the physical ordinal and NULL unchanged. |
| Different ENUM definitions | Overload 1 receives target definition, source definition and native ordinal. Preserve source zero; decode positive ordinals to labels, then apply the existing target conversion policy. |
| Same SET definition | Carry the physical bitmap and NULL unchanged. |
| Different SET definitions | Convert through labels using the established SET assignment semantics; bitmap positions belong to their definition. An empty display does not grant a cross-domain bitmap identity. |
| ENUM versus SET, or a semantic string expression | Use the existing display conversion and target assignment policy. |
| Numeric scalar or parameter | Retain existing numeric/string distinction and SQL-mode policy. |

Planner ENUM expressions retain `T_enum` plus `Enumvalues`; SET expressions use
`T_uint64` plus member metadata. Numeric SET consumers may require the same
physical slot without member metadata. Return an appropriately typed column
reference to that slot rather than mutating the shared producer's type or
allocating a second slot. Re-derive assignment metadata from the proven source
when a numeric consumer populated the cache first.

Normal SQL IF/iff binds display-valued branches. Internal normalized IF supports
`T_enum` and retains identical branch-domain metadata. Generated-column
volatility checks recognize only the known deterministic display adapters, and
still inspect their children for volatile functions.

## Source-specific relational proof

The proof and materializer belong to `QueryBuilder`, adjacent to the existing
transparent ENUM/SET and projected SET helpers. Use planner-local tags and the
existing cache, not a new global provenance registry or long-lived cache.
`OutputColumnProvenance` supplies domain lineage for schema consumers; its
single-source metadata is not by itself proof that the value remains visible
through the executable relational path.

The helper operates on the pre-optimizer, pre-remap logical plan built during
binding. It is not valid on physical `RelPos` references. In particular,
`IsRightJoin` is selected before `swapJoinChildren` changes physical children;
assignment-time traversal must use the logical child order, not guess the phase
from that flag. Supporting a later phase requires an explicit phase contract.

For each reference to a candidate project output, carry its actual input root
as a local proof argument. The entry point must receive that root where it is
available. A directly bound adapter at the entry without a project root may
rely only on the real binder's already validated scope; recursive projected
recovery must always validate against the selected project's actual child.
Finding an arbitrary tag in `tag2NodeID` is never a scope proof.


1. Validate tag lookup, node, column position and recursion state. Reject missing,
   nil, out-of-range or cyclic inputs. Reject the selected projection's DISTINCT,
   grouping or aggregate context.
2. Read only that output expression. It must be a transparent reference to
   another project output, or a recognized special-type display adapter with a
   well-typed underlying physical expression. Arbitrary display functions, CASE,
   user-written CAST, mixed domains and SQL IF do not acquire source identity.
   The adapter's physical argument is not necessarily one column: retain an
   internal native IF/expression as a whole and validate every column leaf in
   the same input scope. A native literal has no visibility edge to validate.
   Do not move evaluation below a join, change the IF mask, or independently
   evaluate an already materialized NEW producer. Unproven raw expression
   shapes fail recovery rather than creating a generic expression framework.
3. Prove that the selected expression's referenced owner is an actual visible
   input of this projection. Traverse its real child path through FILTER/SORT
   and the allowed JOIN edges below. At a tag-owning PROJECT, stop this
   visibility traversal and recursively prove its specified output. A different
   PROJECT/schema owner is a boundary: do not search beneath it by tag alone.
   Check every column of the underlying adapter input in the same scope;
   unwrapping a display function must not introduce an invisible scan-column
   reference. A reference to a boundary's already typed published output can be
   consumed as that output, but gives no license to recover one of its hidden
   inputs or append a physical grouping/equality key below the boundary.
4. Reject attempts to recover pre-boundary storage through AGG, DISTINCT,
   set-operation, window, recursive/shared materialization or unknown nodes on
   the selected source path. Existing canonical and set-operation mechanisms
   retain their own semantics; this recovery must not append a raw grouping/
   equality key or bypass their output schema. This does not forbid an ordinary
   predicate or native expression from referencing a separately published typed
   aggregate output. It forbids treating that output as its pre-aggregate input.
5. Do not require unrelated join branches to be transparent. A join between raw
   `a.v` and a grouped peer may still forward `a.v` unchanged. Conversely, an
   aggregate in `a.v`'s own branch must stop recovery even if the peer is a scan.
6. Preserve the existing LIMIT/OFFSET, ordering, predicates and join conditions
   in place. Add hidden values to the same selected rows, never below a moved
   selection. Perform all checks before any projection/cache/type mutation.

Use a small recursion guard and local memoized `(relational node, requested
source owner)` results, or an equivalent traversal that visits each such pair
once per proof. Neither an incomplete proof nor a cache hit
may bypass the current visibility and domain checks. After proof succeeds,
materialization follows the proven project chain, unwraps the display adapter,
reuses or appends hidden slots, and returns a fresh reference with the effective
outer expression's NULLability and proven member domain. Relax any affected
leaf/output NotNullable flag across null-extending edges, preserving the raw
expression's evaluation. Conservatively nullable metadata is safe when exact
function inference is unavailable; a nullable leaf must not make a COALESCE or
IF result NULL at runtime. Literal-only expressions retain their own contract.
The remapper retains that referenced value through the actual join outputs.

The supported JOIN edge matrix is explicit:

| Logical JOIN | Visible source child | NULL behavior and restrictions |
| --- | --- | --- |
| INNER / CROSS | Either | A surviving pair contains each child's original value. No uniqueness assumption is needed. |
| LEFT | Either | Right-side source is nullable on unmatched rows; left retains its input NULLability. |
| RIGHT | Either | Left-side source is nullable on unmatched rows. |
| FULL / OUTER | Either | Either side can be null-extended. |
| SEMI / ANTI | Preserved logical left only | Matching-side columns do not exist in output; never recover them. |
| MARK | Logical left data only | The computed boolean marker is not a storage source; right-side columns are invisible. |
| SINGLE | Either visible data side | Scalar-side logical right is nullable; preserve its existing multiple-row error and selection semantics. |
| ASOF / ASOF_LEFT | Either | A selected predecessor forwards its actual input value; ASOF_LEFT null-extends the right source. Keep temporal selection and tie rules unchanged. |
| INDEX | Logical left only | The index-join output constructor permits only left result columns; the matching input is invisible. |
| L2, DEDUP, APPLY or unknown specialized node | No new recovery in this repair | These have separate output/materialization contracts. Do not infer safety from two children. Existing direct typed expressions retain their established behavior; this design makes no new identity guarantee for these derived specialized nodes. |

This is a bounded explicit join capability, not a claim that every node named
JOIN is transparent. Expansion to other specialized joins needs its own output proof
and the corresponding small public witness. No change to join algorithms is
needed for the supported matrix.

Counterexamples that the proof must reject include a hidden scan tag behind a
PROJECT that no longer outputs it; an untagged AGG/DISTINCT between the current
projection and a referenced tag; a SEMI/ANTI matching-side source; a MARK
right-side source; a mixed SET/VARCHAR set operation; and a computed expression
with a misleading source member type. Outer-join NULL extension is an accepted
case with a different NULL contract, not a reason to reject the entire join.

## DML and scalar ownership

The existing UPDATE binder owns the row-image and target-selection rules. Keep
conversion at `normalizeUpdateAssignment` and the shared stored OLD-value
normalization owner. Preserve OLD before sequential projections copy the row;
normalize explicit NEW before generated/ON UPDATE projections. Consumers refer
to a materialized NEW slot instead of copying a potentially volatile producer.
Index insert/delete and change detection read those same normalized values.

Overload 1 is implemented by the existing vector conversion owner. Selection
admission precedes decoding and diagnostics. NULL propagates. Ordinal zero
appends zero directly; positive ordinals decode against the source definition
and then enter the existing target lookup/error/warning path. Constant source
member splitting is lazy and batch-local; dynamic definitions retain the
established checked parser. Malformed positive ordinals retain the prior error
class. No new session diagnostic owner or persistent cache is introduced.

## Protocol, persistence and delivery

The frozen rebase target is `bcefda15b8260bba4c71618e16fdf5715aaec003`;
its latest MORPC version is 131. Version 132 therefore remains free for this
feature. The rebased pre-repair feature head is
`a78049cae10ccb3673a139fae5c12b591c3e4ed5`. Do not renumber released overloads
or independently assign the same protocol number to a different contract.

The feature detector identifies function 306 / overload 1 in every executable
and dependent expression owner. Compose requirements by maximum, including
other expression features and vector stream requirements. Required owners are:

- fresh local compilation, early DDL admission, cached-plan Run and re-execution;
- worker placement and the actual remote destination, checked again when a
  destination/version changes or capability lookup is unknown/cancelled;
- outgoing serialized scope, execution-stream requirement and incoming scope
  validation before allocating operator owners;
- persisted expression authoring after assignment casts, persisted reads after
  decode/restart, and CTAS's deferred population query before CREATE effects.

Persisted protobuf shape and ENUM/SET row encoding are unchanged. New expression
content may contain the appended function overload. Readers below 132 must fail
closed; authoring must meet both local execution and authoring floors. Reading
existing metadata uses the read floor independently of the authoring floor.
Legacy overload 0 and expressions without the new feature keep their previous
requirements. Constant folding must not erase a new requirement before admission.

Deployment for this PR is an offline upgrade. Synthetic capability tests prove
rejection/admission, not a certified live mixed-version rollout. Metadata using
overload 1 cannot be handed to or rolled back onto readers below 132; preserve a
compatible snapshot or remove/re-author such definitions before downgrade.
Backup/restore retains the same expression bytes and must apply the same read
admission on restore/open. No row-data migration is required. Existing rows,
including ordinal zero, are copied without format changes. No feature flag or
new operational service is necessary; existing error diagnostics identify an
unsupported protocol. Do not add per-row logs or unbounded metric labels.

## Resources, failures, cancellation and security

`QueryBuilder` exclusively owns proof maps, hidden slots and the existing
position cache for one build. The proof publishes nothing. Materialization is
synchronous after successful proof; ordinary planner failure discards that
query-owned plan. No goroutine, lock, channel, retry, I/O or background owner is
introduced. Cancellation/timeout continues through existing planner/executor and
RPC contexts; this change must neither replace a context nor swallow its error.
A local finite proof does not need a new cancellation worker.

The scalar function borrows input vectors and appends to the caller-owned result.
Its temporary source member slice/index die at the end of the batch call.
Append/parser/conversion failure returns the existing error; expression and
pipeline owners free partial results on error/cancel. Strict failure must leave
base rows and index state unchanged; permissive/IGNORE must use the existing
warning cap and record the expected warning count. Reuse starts with a fresh
builder or existing reset owner; do not retain query pointers in service state.

Remote capability request/reply release, receiver rejection before allocation,
CTAS pre-side-effect rejection, executor cleanup and SQL test connection/rows
cleanup retain their existing owners. A capability failure must not leak a
response, allocate a rejected pipeline or trigger CREATE. There is no new
wait-for edge, generation, retry or recovery state to model.

Authorization and tenant resolution remain in the real UPDATE/catalog owners.
Hidden slots are planner execution fields, not extra user-visible headings or
new privilege access. Proof must stay within the current reachable relational
inputs, which also prevents recovering an unrelated binding from the global tag
map. All referenced metadata remains query-local; no cross-tenant cache exists.

## Cost model and alternatives

Let N be input plan nodes, P the transparent projection boundaries requested by
one proof, U the unique recovered display-output positions, B batch rows, and M
the source member-definition length. A source visibility search is O(N), bounded
by the existing finite plan, with a local recursion guard/memo. Across P source
owners the conservative bound is O(P*N); no exponential traversal or global scan
per output row is allowed. Storage materialization adds O(U) expressions/cache
entries and at most one slot per unique projection output. Repeated requests
must leave both counts unchanged.

A surviving ENUM column requires its existing 2-byte ordinal plus NULL bits; SET
requires its 8-byte bitmap plus NULL bits. Carrying hidden values can widen join
and sort batches and retained hash rows, but no value-size-dependent strings or
new row multiplicity are added. Existing operator memory admission/spill owns
that width. The scalar decoder remains O(B+M) for a constant definition plus
existing target lookup, and uses no process-wide cache. Supported local protocol
132 execution takes the existing constant-time capability fast path without an
extra expression walk. No measured throughput improvement is claimed. A
benchmark is required only if implementation introduces a new per-row scan,
unbounded cache or material additional copying; the repair must avoid those.

| Alternative | Correctness and total cost |
| --- | --- |
| Reparse display strings | Small code change but cannot distinguish stored zero/empty; rejected. |
| Make every bound ENUM/SET value native throughout SQL | Would change string functions, IF, set/group semantics and client metadata across many consumers; larger compatibility/migration/test surface than this task. Rejected. |
| Accept all JOIN nodes, or require every branch to be unary | Unconditional acceptance exposes invisible branches and crosses identity-destroying boundaries. Requiring all branches rejects an unchanged source solely because an unrelated peer aggregates. Rejected. |
| Selected source visibility plus existing hidden slots | Closes the observed ordinary-join boundary, preserves established owners, bounds state to one query, and admits precise counterexample tests. Selected. |

## Complete feature map and validation

The original 35-file delta has 382 changed non-test production lines. It crosses
planner, scalar-function, plan/protocol, compile and persistent-expression
owners; G-FEATURE-DESIGN is mandatory by complexity triggers and at least five
owning components, regardless of whether it is labelled a bug fix.

| Closure and files | Risk / proof |
| --- | --- |
| `plan/bind_update.go`, `bind_delete.go`, `build_constraint_util.go`, `make.go` | R2 SQL and OLD/NEW/index contract; focused transparency/nullability/no-mutation/cache UT plus real SQL storage/error/rollback oracles. |
| `plan/base_binder.go`, `mysql_special_types.go`, `query_builder.go` | R2 ordinary display, numeric, IF and LIMIT boundaries; retain existing typed and public controls. |
| `function/func_mo.go`, `list_builtIn.go`, `operatorSet.go` | R2 scalar overload/mask/diagnostic/domain contract; legacy/typed overload UT and execution consumers. |
| `defines/const.go`, `pb/plan/string_literal_form.go`, `compile/{compile2.go,expression_protocol.go,expression_destination_protocol.go,remoterun.go}` | R3 wire/capability contract; detector, local/cached, placement/destination, stream, receiver, cancellation and release tests at 131/132/unknown. No changed operator lifecycle. |
| `plan/{build_ddl.go,build_util.go,persisted_ip.go}` | R3 persisted/DDL admission contract; final-cast feature discovery, serialized read/authoring floors, legacy control and CTAS rejection before effects. |
| Plan tests: `delete_scope_test.go`, `enum_source_assignment_protocol_test.go`, `mysql_special_order_provenance_test.go`, `mysql_special_types_test.go`, `timestamp_result_protocol_test.go`; function tests: `enum_set_assignment_test.go`, `func_mo_test.go`, `operatorSet_test.go`; pb test: `enum_source_assignment_protocol_test.go`; compile tests: `enum_source_assignment_protocol_test.go`, `lifecycle_admission_test.go` | Test changes retain distinct semantic oracles, restore runtime globals, and free acquired executors/operators immediately after successful acquisition. |
| `tests/sqlintegration/special_type_update_assignment_test.go`; dtype BVT `issue_29923_update_enum_set_column_rhs.sql/.result`, `mysql_compat_enum_set_numeric.result`, `set.result` | R2 public SQL/metadata/teardown contract; reuse the existing shared cluster/session fixture and extend the coherent issue case. Every expected-result change needs an independent semantic explanation. |

Required new cells use minimum data and reuse the existing fixtures:

| Invariant | Cheapest new proof and independent public oracle |
| --- | --- |
| Derived INNER/CROSS source identity | Typed proof chooses the actual child; real UPDATE of ordinal 0, declared empty, ordinary member and NULL, plus an unmatched target. Assert both label and `CAST(v AS UNSIGNED)` with deterministic IDs. |
| Selection remains in place | Derived JOIN ORDER BY/LIMIT/OFFSET preserves selected IDs and ordinals. Reuse existing LIMIT boundary data rather than duplicating all limits. |
| Both source sides and nested joins | Parameterized typed UT varies source child and join depth; one real source-right query prevents assuming every source is the left child. |
| Outer NULL extension | Typed LEFT/RIGHT/FULL cases use a NOT NULL base source and require nullable recovered output; small public LEFT/RIGHT/FULL queries assert unmatched NULL independently of source zero and declared empty. |
| Preserved versus invisible output | Typed SEMI/ANTI/MARK/INDEX cases accept only preserved data; retain public EXISTS/scalar controls where the planner emits these shapes. SINGLE retains NULL/multiple-row behavior. |
| ASOF selected row | Small typed and public ASOF/ASOF_LEFT source-right tests assert the chosen predecessor's physical value and unmatched NULL; do not retest temporal ranking permutations owned by existing ASOF suites. |
| Native raw expression | Literal and internal native IF raw adapter arguments prove that all column leaves are visible, preserve selection and evaluate materialized NEW values once; reject an invisible leaf without plan mutation. |
| Aggregate isolation | Accept a raw source joined to a grouped peer; reject recovery across AGG/DISTINCT/set/window in the selected source path. For rejects, compare project/cache/type snapshots before and after. |
| Cache/domain order | Extend `TestSpecialAssignmentStorageCachePreservesDomain`: numeric-first versus assignment-first through a JOIN, repeated requests, identical slot count, correct domain and NULLability. |
| SET independent representation | Same-definition empty-member bitmap survives derived ordinary JOIN; retain reordered-domain label conversion as the nearest negative identity control. |

Reuse existing oracles rather than rewriting them: `TestSpecialTypeUpdateAssignments`
already covers qualification, unary derived LIMIT, same/cross definitions,
prepared assignment, indexes, duplicates, OLD zero, sequential evaluation,
generated consumers, normal IF display, strict rollback, warnings and IGNORE.
Function tests already cover zero/NULL/malformed ordinal, lazy decode, masks and
legacy overload identity. `TestEnumSourceAssignment*` suites already cover
feature detection, dependent expressions, local/cached admission, destinations,
receiver rejection, streams, vector composition, persistent floors and CTAS.
The previous review's IF/LIMIT/generated/CTAS concerns are not reopened without a
new contradictory input.

Validation order: establish the unfixed public failure and nearest control;
run selected helper tests with non-empty selection; run owning planner tests;
run the unchanged or modified feature owner suites where evidence is stale;
run the public SQL integration suite; run the changed dtype BVT case in normal
comparison mode twice on one clean ready test-owned instance and assert teardown.
Run the owning dtype group only if fixture/harness/conventions change. Use the
repository CGo wrapper and a verified native build for CGo-transitive packages.
Incremental formatting/vet/lint covers the final changed Go package closure and
consumers. Race, multi-CN and restart evidence is required only where the final
implementation or explicit session requirements trigger that dimension; the
local proof introduces no shared state or operator lifecycle change. Existing
serialized read/receiver tests are sufficient for unchanged format/admission
contracts; they do not certify a live rolling upgrade.

Exact original-head CI 40212132532 supplies executed UT/SCA/BVT evidence.
Rebase or repair invalidates only affected semantics/dependencies/modes, but an
unchanged filename alone is insufficient: reconcile relevant inputs before
reuse. Historical author claims without receipts are not passes. Record new
commands, selected tests, revision, mode, terminal status and environment
failures separately. Pending compilation, skipped jobs and zero tests are not
evidence. Do not rerun the full repository merely to compensate for a missing
focused oracle.

## Acceptance and decision log

The repair is ready only when the versioned design revision is approved, the
observed derived ordinary-JOIN assignment retains all four storage/NULL states,
the allowed join edges and rejected boundaries obey this contract, no failed
proof mutates the plan, hidden-slot growth is idempotent, SQL diagnostics and
index rollback stay correct, and protocol/persistent admission still fences
132 while preserving overload 0. Final implementation review checks alignment
with this design after those results exist.

Accepted tradeoffs: keep special-type display adapters and existing hidden
slots; use logical source-specific proof; retain cross-domain SET label
semantics; exclude other specialized join/materialization expansions; deploy with an
offline upgrade and fence new persisted content on downgrade. These choices may
be revisited only on new evidence or an explicit capability expansion.

There are no deferred correctness decisions. Required execution evidence is an
implementation/delivery gate, not a design assumption. The implementation owner
must link this exact reviewed document revision from PR #29924; material changes
to allowed paths, conversion policy, protocol or persistence require affected
design decisions to be reviewed again.

## External compatibility references

The interoperability target is MySQL 8.4, not SQL-standard ENUM semantics:
[ENUM](https://dev.mysql.com/doc/refman/8.4/en/enum.html) documents indexed members,
a distinct error member at index zero and numeric-context retrieval;
[SET](https://dev.mysql.com/doc/refman/8.4/en/set.html) documents bitmap-based
membership and numeric contexts. Their dual display/storage representations
motivate carrying typed identity rather than guessing it from labels.
[UPDATE](https://dev.mysql.com/doc/refman/8.4/en/update.html) distinguishes
single-table left-to-right assignments from multi-table assignment ordering.
[JOIN](https://dev.mysql.com/doc/refman/8.4/en/join.html) defines NULL extension
for unmatched outer input. MatrixOne's existing binder, vector and protocol
owners implement these rules; no MySQL internal execution design is assumed.
