# Table dump expression binding (PR #29241, issue #28594)

Status: revision 10 passed independent design review.
Revision: 10, 2026-09-25.

## Problem and evidence

`div_precision_increment` is resolved while a persistent expression is bound.
Its bound plan, including the decimal division result type, survives restart and
`CREATE TABLE ... LIKE/CLONE`. A table can contain expressions authored at
different increments. At PR head `cb7025e3`, a DEFAULT bound at 10 and a
GENERATED column bound at 4 return `0.333333333333` and `0.333333000000` for
the same `(1,3)` inputs. `SHOW CREATE` and table dump v1 carry the expression
text only. A target created from that text at increment 4 has the same dump
schema hash; LOAD accepts it, and an old row and new row disagree.

Ordinary snapshot/PITR restores use `CREATE TABLE ... CLONE`, not their fetched
`SHOW CREATE` text. The clone planner's `WithPersistedDDLReplay` copies bound
expressions. This is a control to test, not a diagnosed failure. Independently
copying `SHOW CREATE` SQL into a new session does lose mixed bindings, but making
one CREATE statement express separate per-expression increments is a new SQL
feature. It needs its own SQL compatibility design; this change must not claim
that standalone SQL copy is lossless.

## Invariant and scope

For DUMP/LOAD, an accepted load of a table with stored expressions must
preserve each expression's bound semantics for both existing and future rows,
or reject before any catalog or object mutation. The target declaration and
every copied expression must describe the same SQL object. Error, cancellation,
retry, and concurrent ALTER/INSERT must not expose a partly installed binding.

The owner of binding is the SQL planner; the catalog owns the bound expression;
the dump manifest transports it; LOAD validates it and publishes it in the
target catalog. This change covers ordinary persisted DEFAULT, GENERATED, and
CHECK expressions. ON UPDATE is handled defensively for historical catalog
entries; the current SQL grammar only accepts datetime functions there, so a
division expression cannot be authored. It does not change query-time arithmetic,
the `SHOW CREATE` output contract, or snapshot/PITR's clone protocol.

## Alternatives and decision

1. Keep v1 text hash and current session binding: fails the demonstrated mixed
   session case. Setting one session value around CREATE also cannot represent
   mixed bindings. An outer CAST is insufficient for nested division because
   rounding already occurred inside the expression.
2. Store only serialized `Plan.Expr` in v2 and transplant it after checking a
   SHA256 digest: rejected. The digest is not an authentication mechanism. Even
   an account administrator can edit the manifest and recompute it, bypassing
   normal SQL binding restrictions. Matching only column names and origin text
   does not validate the executable expression tree.
3. **Selected:** v2 carries a bounded copy of the source bound declarations as
   evidence, but LOAD accepts each one only if the normal SQL planner produces
   exactly that expression when rebinding its current catalog `OriginString`
   against the destination schema under one of the 31 legal increments.
   Candidate matching is independent per expression,
   so mixed authoring values are representable. After validation, copy only
   those matching bindings to a private target definition. No new SQL syntax or
   query execution path is introduced.

Normal SQL binding remains the security authority. A successful match means
the executable tree is reachable from that SQL with legal settings; the source
payload alone cannot introduce an extra function, constant, reference, or cast.
If a legacy or unusual expression cannot be reproduced by a candidate, LOAD
fails closed with a binding mismatch. The first implementation must prove that
ordinary source and target plans compare equal after mapping their column
references and removing physical catalog identity. No permissive
"similar-enough" tree comparison is allowed.

## Format and flow

The v2 manifest keeps v1 fields and adds `bound_expressions` plus its SHA256.
The digest detects accidental corruption; planner matching supplies semantic
validation. The payload contains the column/check skeleton and expression
declarations with stable column/check identities, type and original SQL. It excludes physical
table, database, relation and column IDs. Existing 64 MiB manifest and relation
limits apply; payload receives a smaller explicit cap. Parsing rejects
duplicate JSON fields, duplicate identities, unknown expression kinds, missing
payload/digest and unsupported versions.
The v2 protobuf decoder must receive only a wire-preflighted payload. An
iterative walk of known `TableDef` → `ColDef`/`CheckDef` → expression message
fields rejects malformed wire types, more than 16,384 columns, more than
16,384 checks, more than 100,000 aggregate fields, or 64 levels of expression
nesting before `Unmarshal` can allocate or recurse. Reject unexpected top-level
fields and unsupported recursive expression kinds. Test many tiny repeated
fields and deeply nested function arguments. DUMP applies the same preflight
to its serialized payload before writing any fixture, so it cannot emit a v2
dump that its own LOAD rejects.
Classification uses each *current*
catalog declaration, including columns added by ALTER. Parse its `OriginString`
and walk the AST for exact division (`/`); if parsing or classification fails,
reject. Before any candidate binding, both DUMP and LOAD enforce at most
1 MiB of aggregate expression text, 16,384 AST nodes, and one million total
candidate-node visits per table. An over-budget table fails before DUMP copies
an object or LOAD changes its target. For each expression containing division,
bind all legal increments
0..30 using its actual DDL binder. If every candidate bound tree is identical,
the expression is insensitive. Otherwise it is sensitive and DUMP emits v2;
the source catalog tree must match one candidate or DUMP rejects. This also
detects division folded to a literal in the stored plan. An expression without
an exact division AST node is insensitive to this variable. A no-sensitive-
expression table keeps v1 to avoid needless compatibility cost.

At LOAD, first validate the manifest's text schema identity and relation
topology. Parse each current catalog `OriginString` as an expression. The
formatters are not uniformly mode-independent: DEFAULT and GENERATED request
single-quoted strings, ON UPDATE uses the ordinary formatter, and CHECK uses
mode-independent string literals. Historical author `sql_mode` is not stored.
Try a bounded set of parser modes made from the relevant syntax flags
`NO_BACKSLASH_ESCAPES`, `ANSI_QUOTES`, and `PIPES_AS_CONCAT`; accept only a
candidate whose fully bound tree equals the catalog tree. There are at most
eight profiles, applied lazily, and no guessed profile may alter a mismatched
expression. Other mode-sensitive declarations may fail closed even when
currently valid: DUMP must report the unsupported expression before emitting
a fixture, and LOAD must not mutate the destination. This availability limit
is part of v2 acceptance and needs a focused test with backslashes and `||`.
The v2 manifest does not claim to recover a missing historical SQL mode. Then
rebind it with the same DEFAULT, ON UPDATE, GENERATED or CHECK binder used by
ordinary DDL against the complete destination schema, for increments 0..30.
The rebind operation must apply the same assignment cast, constant folding,
CHECK boolean conversion and persisted-format normalization as authoring DDL.
Compare each source expression to the corresponding candidate expression. A
source expression may match a different candidate from its neighbor. Bind
candidates only on this administrative path; stop once every expression has a
match. This avoids reconstructing the current schema from `def.Createsql`,
which may be a historical CREATE that omits later ALTER additions. The target's
declarations, visible column order, value types, generated storage mode, check
names, and original SQL must match. Match declarations by stable name and
require rebound local references to have the same type and resolved position
as the source, including hidden-column layouts. A different physical order
that changes these positions fails closed. If the candidate planner cannot
reproduce an expression, reject it.

Before catalog mutation, validate all auto-increment state, object stats and
names, file availability, duplicate relations, total block bounds, target
emptiness and capabilities. Hold target table locks. Prepare a private cloned
definition and check the persisted-expression protocol. Apply one guarded
`ReplaceDef` only after pure validation, then install objects in the same SQL
transaction. Transaction rollback must discard both catalog replacement and
object references; existing object GC owns copied but unreferenced files.
Preserve creator/owner/create-time and verify target version in the guard.
Refresh any relation handle whose cached definition is used after replacement.
No in-place mutation of `Relation.GetTableDef` is permitted.

For v1, LOAD retains existing behavior when the *current destination catalog*
has no precision-dependent persistent expression under the classification
above. If any current declaration can bind to different persistent results
across increments, source binding is unknowable from v1, so reject before
mutation and explain that a v2 dump is required. The original CREATE SQL is
never used to infer the current expression set.
Readers older than this change reject v2 as an unsupported version. A mixed
version deployment must upgrade LOAD readers before producing v2 dumps; v1
remains available for unaffected tables. Downgrade readers cannot load v2 and
must retain the original catalog or export a compatible v1 table. No online
catalog migration is required.

## Cost, safety and operations

There is no change in row evaluation or query-plan hot paths. Both DUMP and
LOAD perform bounded candidate rebinding only for expressions containing
exact division. Either operation can bind at most 248
candidates (31 increments × eight parser modes) per persisted expression, with
memory bounded by the manifest cap and one candidate expression at a time.
Most expressions use the first parser mode, so measure both common and worst
case. The shared 1 MiB text, 16,384 AST-node and one-million candidate-node
visit limits above prevent planning cost from being hidden behind the 64 MiB
manifest output limit. No goroutine,
background work, cache or new file lifetime is introduced.

`frontend/txn.go:164-205,1090-1135` rolls back an unsuccessful statement;
an explicit multi-statement transaction calls the workspace's
`RollbackLastStatement`. `disttae/types.go:1179-1254` discards the statement's
writes, restores table definitions through `restoreTxnTableFunc`, and clears
load-file protections; `disttae/txn_table.go:1810-1855` registers the
ReplaceDef restore callback. Thus the linearization point is transaction
commit. Validate this path with an injected failure after ReplaceDef followed
by a successful explicit COMMIT and catalog/readback check; if the rollback
does not hold for LOAD, block delivery rather than rely on this assumption.

LOAD already requires an account or system administrator. The new input remains
untrusted despite that privilege: validate length before allocating, validate
SQL-derived semantics before catalog write, scope all catalog lookups to the
target tenant and locked table ID, and give deterministic errors without
printing executable payloads or secrets. Existing statement/transaction
logging provides diagnosis; a v1 precision rejection must identify the remedy.
Rollback is to stop emitting v2 and use the old reader only for unaffected
tables. Keep v2 fixtures for later upgraded readers.

## Verification and delivery gate

- White-box planner: candidate exact matching for 0, 4, 10 and 30; two
  expressions with different increments in both directions; nested divisions;
  a folded constant; generated/default/check; parser proof that ON UPDATE
  division is not authorable; parser-mode profiles with backslashes and `||`;
  altered executable tree rejected; missing and duplicate metadata rejected.
- Frontend: v1 unaffected table succeeds; v1 ambiguous expression rejects;
  v2 mixed bindings succeeds; wrong schema, nonempty target, changed version,
  tampered payload, invalid object and auto-increment metadata reject before
  catalog mutation; explicit transaction rollback restores old definition.
- Public SQL/BVT: dump under mixed authoring values, create destination under
  a third value, load, compare an old and future row and an update; repeat with
  reversed values. Exercise clone and snapshot/PITR as controls, plus a service
  restart of the loaded table.
- Static analysis and owning package tests; strict BVT twice on one owned
  service; scoped race evidence where shared mutable state changed. Record
  exact revision, commands, counts and logs. Check manifest size and load
  planning time against the same fixture before and after this change.

Design review must approve this exact revision before implementation review or
delivery. Standalone `SHOW CREATE` replay remains a separately tracked product
contract decision; the PR description must state that limit explicitly.
