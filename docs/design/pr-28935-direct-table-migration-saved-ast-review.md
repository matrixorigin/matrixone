# Independent technical decision: D2-DTS-1.1

APPROVED FOR BOUNDED IMPLEMENTATION, not accepted for publication or CR closure.

The root read the complete proposed amendment, current classifier and real
planner mutation at query_builder.go:8988 and bind_context.go:761-762. The saved
AST shares the original planner pointer: unqualified `id` becomes `t.id`.
Requiring NumParts=1 for saved projections rejects a supported raw SELECT.
This is representation normalization, not a reason to enlarge the raw language.

Approve a separate saved-name classifier allowing only NumParts=1 or exactly
NumParts=2 with table Origin equal byte-for-byte to the single admitted unaliased
base-table name. Require lowercase admitted identifiers, !Star and no extra
components/fields. Apply separately to projection and ORDER, then compare the
canonical descriptor with the strictly unqualified raw parse. Never mutate the
saved AST or remove catalog/object/schema/policy/generation/capability guards.

Reject raw t.id, foreign saved qualifier, db.t.id, aliases, star, expression,
dead CASE/CRC, parameter, views and generated columns. Add deterministic controls
and extend the real binder counterexample to prove accepted saved normalization.
The first-owner immutable-origin alternative has broader retained-state and
reprepare lifetime costs and is not selected for this bounded repair.

Both original issue29148 SQL/procedure migration tests must actually reach and
pass unchanged successful transfer, [1,2], later row-rule [1], old-handle
NeedReprepare, explicit reprepare [1] and cleanup assertions. Later preflight,
actual bind/publication, auth/policy/DDL/cancel/disposal/retry/nonwrite evidence
is NOT established by the earlier source-export failure or isolated unit PASS.
Any new semantic blocker must be traced and repaired within the approved domain,
not silenced by weakening those assertions or admitting a broader SQL domain.

No candidate publication yet: current original migration tests FAIL. Race
compilation was interrupted for disk safety; no race PASS or BVT exists. Disk
currently has about3.6GiB available. Reuse valid normal-test/native evidence;
do not repeat a large race build without demonstrably adequate scratch/cache
capacity, and do not delete unrelated or shared data. Resource blockage remains
separate from product failure. Candidate protocol124 is provisional until root's
publication-time actual-main prefix review.

Scope remains local implementation and validation in the exclusive existing
worktree. No commit/push/GitHub/public-state writes by worker. Preserve original
findings and exact evidence, hand back and stop once decisive results exist.
