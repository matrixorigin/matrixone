# ALTER TABLE CHECK management (#25387)

## Scope and contract

Issue: https://github.com/matrixorigin/matrixone/issues/25387
Implementation: independent branch `fix/alter-table-check`, based on main
`7af631bdf7`; the implementation PR will link this design revision.

ADD CHECK already parses, but the INPLACE planner rejects it. DROP CHECK does
not parse. DROP CONSTRAINT is incorrectly parsed as DROP FOREIGN KEY, so it
cannot remove a stored CHECK. CREATE TABLE CHECK, DML enforcement, schema-extra
persistence, SHOW CREATE, and transactional COPY replacement already exist.

An accepted ADD must validate all existing rows and enforce future INSERT and
UPDATE, with SQL UNKNOWN accepted and FALSE rejected. Failure must leave the
source definition, rows, indexes, and foreign-key relationships intact. DROP
must remove precisely the named constraint and preserve all other constraints.
Constraint name resolution and duplicates must use the existing catalog naming
policy, and planning must not mutate the resolved source definition or AST.

This expands the DDL compatibility contract across parser, planner, and COPY
execution ownership boundaries, so the design-first gate applies. No new
catalog/protobuf format, DML operator, worker, or distributed protocol is needed.

## Standards and alternatives

MySQL 8.0 supports ADD [CONSTRAINT name] CHECK, DROP CHECK, and generic DROP
CONSTRAINT. Generic drops are rejected if different constraint types share the
name. Existing rows must satisfy an added enforced CHECK.
Sources: https://dev.mysql.com/doc/refman/8.0/en/alter-table.html and
https://dev.mysql.com/doc/refman/8.0/en/create-table-check-constraints.html.

1. Keep rejecting ALTER: preserves current behavior but blocks migrations.
2. Add INPLACE check actions: efficient drops, but needs new plan actions and
   schema-extra mutation plus a correctly locked validation scan. This adds
   execution and compatibility contracts unnecessarily for the initial closure.
3. Reuse transactional COPY (selected): add/drop CHECK in the planner-owned copy
   definition, generate the existing CREATE and INSERT SELECT, and replace the
   source only after data copy validates every constraint. Existing index clone,
   FK reconciliation, cancellation, rollback, and lifecycle admission remain
   owned by COPY execution.

## Data and control flow

Parser retains distinct DROP CHECK and DROP CONSTRAINT intent. A planner-only
ordered name view resolves generic drops to CHECK, FK, PRIMARY, or UNIQUE and
rejects missing/ambiguous targets before execution. Ordinary indexes are not
constraints. Explicit DROP FOREIGN KEY must never remove a CHECK.

Any CHECK mutation requires COPY; explicit INPLACE/INSTANT or LOCK=NONE must
fail through the existing algorithm/lock admission checks. CHECK actions are
applied in order to an evolving list of names and pending expressions. Newly
added expressions bind against the final column definition so a CHECK can refer
to a column added by the same statement. Anonymous names skip occupied names.
NOT ENFORCED remains explicitly unsupported, as for CREATE TABLE.

Existing expression trees and origin SQL are cloned, not rebound under a new
session mode. Legacy metadata recovery, when needed, uses the existing
ambiguity-rejecting recovery helper. Internal COPY reconstruction uses the
existing expression replay context. The final definition uses the existing
CheckDef schema-extra representation and protocol admission.

Commit/publication remains the existing transaction's COPY replacement point.
Validation errors, cancellation, retry, and partial target creation follow COPY
rollback; no source drop is published before successful validation. No new
long-lived resource, state generation, or wait is introduced.

## Compatibility, cost, and limits

Old CREATE CHECK metadata remains readable. New DDL writes the same metadata as
CREATE CHECK; existing check-protocol version gates remain mandatory. Restart,
backup/restore, and mixed-version behavior inherit CREATE/COPY contracts. A
restart probe will verify successful ADD/DROP persistence and failed ADD
cleanup. No migration or feature flag is introduced.

COPY costs a full row scan and replacement relation; unchanged indexes use the
existing clone path. DROP is deliberately conservative and may cost more than
an eventual INPLACE implementation. No DML hot-path cost is added. Existing
ALTER privilege, external-table, subscription, and temporary-table admission
remain enforced. Unsupported mixed COPY actions must fail before execution.

## Verification and design decision

Parser UT: distinguish DROP CHECK/CONSTRAINT/FK and formatting round trips.
Planner UT: CHECK COPY routing and hints; final-column binding; ordered add/drop,
name collision, missing/ambiguous target; source immutability; legacy recovery;
protocol and NOT ENFORCED rejection; generic FK/UNIQUE routing controls.
BVT: empty/populated ADD, FALSE rejection and NULL acceptance; INSERT/UPDATE,
failed ADD data/metadata atomicity, selective DROP, replacement, generated
column, indexes/FK preservation, and MLflow status-IN migration expression.
Use existing planner fixtures and a small test-owned standalone service; no
new permanent harness. Review expected rows independently, compare twice, and
verify cleanup. Owning parser/tree/planner tests, vet, and incremental lint are
required. A restart probe verifies persisted state; distributed topology/race
and benchmarks add no distinct claim because their contracts are unchanged.

Design review (before implementation): parser intent cannot alias FK; COPY
holds validation and publication in one transaction; persisted format and
expression replay remain owned by existing components; full-scan cost is
explicit. These decisions close the compatibility, atomicity, ownership, and
resource questions. Decision: PASS for this design; no unresolved design
blocker. Implementation deviations must be recorded before delivery.
