# DELETE target name ownership

Revision 1. Refs #29249. Reviewed baseline: `b4cd00ee34f1`.

## Problem and contract

READ ONLY admission and the modern/legacy DELETE planners independently collect
FROM aliases and substitute target names. An alias can shadow a temporary table:
`DELETE db.x FROM p AS x` writes `p`, even when a temporary `x` exists. The three
consumers must identify the same base schema/object before applying their policies.
This is maintenance consolidation; no new runtime failure or speedup is claimed.

## Owner and integration

`pkg/sql/plan/dml_table_name.go` owns two pure operations:

- `CollectDeleteSourceAliases(tableRefs)` collects explicit aliases into the existing
  `map[string][2]string` representation. Values contain raw schema/object names.
- `ResolveDMLTargetName(db, table, alias, aliases)` substitutes both names on an alias
  hit and keeps the original target object as its binding alias. A miss preserves
  all inputs, including an explicit schema and any existing binding alias.

The frontend already depends on planner. It consumes these name facts without
catalog access. Both planners use the same collector and substitution. Remove
`getAliasToName`, `deleteSourceAlias`, `resolveDeleteTarget`, and the two inline
planner substitution blocks. Keep `sourceTableName`, which UPDATE still uses.

Collection visits aliased tables and joins left-to-right, retaining last-entry-wins
behavior. Unaliased tables and derived SELECTs contribute no aliases. The MySQL
parser flattens parentheses around base-table references; its `ParenTableExpr`
source wraps a derived SELECT. Do not add synthetic-AST compatibility modes.

CTE exclusion remains before substitution in each planner. An omitted source schema
remains empty until the existing default-database fallback immediately before
catalog Resolve. The statement default is stable across this pure operation;
prepared/session/remap resolution stays with the existing compiler/frontend owners.
Frontend classification copies the target value locally and never mutates the
parsed/prepared AST. Unsupported target shapes retain its conservative verdict.

## Preserved boundaries and cost

READ ONLY admission still precedes `TxnHandler.Create` and DDL implicit commit.
DELETE introduces no deferred check or catalog read. Temporary-table exemptions,
transaction survival after rejection, user-origin procedure checks, sequence guards,
unknown-table errors, and modern/legacy route selection keep their existing owners.

For S source nodes, A aliases and T targets, work remains O(S + T), storage O(A).
Each phase owns one short-lived map; no cache, shared state, worker, I/O, AST clone,
new traversal, or additional catalog lookup is introduced. There is no persisted,
wire, configuration or external API change, so no migration or fallback is needed.

## Alternatives and scope

Reusing a full binder in admission would require catalog state and change error or
transaction ordering. A general DML visitor would add policy modes: UPDATE's modern,
legacy and pre-transaction paths differ on hidden/generated columns and incomplete
ownership facts. Leave UPDATE and default-database remapping for separate designs.
Sharing only collection would leave substitution rules duplicated; consolidate both.

## Validation

- Parser-based literal expectations cover alias precedence, qualified source/target,
  default database, joins, derived sources and unchanged ASTs.
- Existing planner fixtures verify actual victim identity through both modern and
  legacy consumers; retain unknown/unsupported/CTE and DML error-routing controls.
- Existing frontend temporary-table and transaction tests retain their independent
  verdicts. Classifying a reused AST must observe live temporary bindings.
- Reuse `set/transaction_read_only.sql` for alias shadowing, qualified targets,
  READ WRITE execution controls and transaction/session behavior; add no duplicate
  BVT setup. A bounded alias-substitution mutation checks oracle sensitivity.
- Run affected owning-package normal tests, focused race tests and configured
  incremental SCA. Do not claim latency gains from code reduction alone.

A real producer with a different source AST, a changing statement default during
planning, or a changed public error invalidates the corresponding preservation claim
and requires revisiting this design.
