# Table dump expression binding (PR #29241, issue #28594)

Status: revision 17; temporal-nullability rebase compatibility design approved by independent gpt-6-astra/medium review.
Revision: 17, 2026-10-20. Revision 16 passed independent zero-temporal review; validation is recorded in the PR.

## Current extension: migration-era binding and format 3

The precision-only admission was incomplete for direct fractional TIMESTAMPADD
SECOND expressions. Historical main3fc00709 binds DECIMAL1.50 to rounded2
seconds; newly authored SQL uses fractional1.5 seconds, even without division.
DUMP/LOAD must preserve verified stored bindings for future writes as well as
copied rows. The frozen direct baseline and public roundtrip controls record
this boundary; a compatibility-mode seed alone is not historical provenance.

The existing planner enumerates DEFAULT, ON UPDATE, generated and CHECK
expressions without lexical slash filters. It verifies each complete supplied
binding through current binding and applicable complete historical eras, and
separately classifies whether candidates differ. A current match cannot skip
legacy comparison. Only an AST containing division enumerates precision0..30.
Existing budgets, cancellation, declaration matching, private replacement and
single guarded catalog publication remain the owners; no parallel codec,
validator, execution path or expression cache is added.

Binding-dependent dumps now emit format3 with the existing payload layout.
New readers accept v1/v2/v3; v2/v3 share validation/restoration. Older v2 readers
explicitly reject v3 instead of accepting and ignoring non-division bindings.
An ambiguous v1 restore requests a new dump before object/catalog mutation.
Whole declaration origins, positions, nullability, storage and types are checked;
only expression fields are transplanted, preserving destination identity.
Fresh SQL retains current normalization. Direct TIMESTAMPADD functional indexes
remain excluded by their eligibility owner; ordinary indexes over generated
results remain supported. Additional classification cost is confined to DDL,
DUMP and LOAD; per-row normalization is unchanged.

Current frozen-base allocations: main charset118, coordinator subquery119, session
authorization120, unsigned DECIMAL DIV121 and functions599/600 stay published;
unary temporal122 and nonblocking row-lock123 stay published; fractional TIMESTAMPADD uses capability124, normalizer601 and end602. The allocation follows official main
c7bd2e7c1d9ca53daf80d640b3b307902d54161c; rollback coverage cycles123→124→123.
No protobuf schema change or provisional-function-ID guessing is introduced.
Historical revision10 evidence below retains its original input identity.

## Revision 17: temporal nullability and CHECK namespace rebase

Main 8dd623e7 made string DATE and DATETIME expression conversions nullable.
Origin verification must reproduce the old inference metadata without erasing
NotNullable during comparison. A private binding-only context selects the old
DATE overload 3/4 and string-to-DATETIME CAST overload 0/1 inference at their
shared owner. Ordinary inference, executor conversions and SQL mode retain main
semantics. This preserves provenance, not old errors on malformed future input.

The finite whole-origin eras include current binding, prior-PR fractional
binding with old temporal metadata, published pre-fractional binding with current
metadata, pre-fractional binding with old metadata, and the existing pre-DIV and
pre-special compositions with old metadata. Zero-literal and implicit-timestamp
owners remain disjoint. Each successful whole tree must compare exactly; no
node-wise migration mixing or metadata normalization is allowed. The descriptor
bound is 31×6; cancellation and the one-million candidate-node budget stay fixed.
Constant-folding can remove the affected identities, so eligibility does not rely
solely on the saved tree. Genuine incoming f9271d9780 definitions cover DATE,
explicit/implicit DATETIME casts and their fractional TIMESTAMPADD composition.

Main now allocates schema-unique CHECK names. Exact-name matching remains a LOAD
precondition. Public historical tests use identical logical table names across
three reused schemas so current declaration identities agree legally; frozen
expression trees are transplanted only under matching origins and verified
against exact old expressions. Current CHECK names remain unchanged. Goldens
retain their original bytes; expression-only UT declarations align to their old
identities. This adaptation does not weaken declaration verification or claim
that current generated names are the historic names.

## Revision 16: zero-temporal normalization rebase

Main b233ff1 normalizes integer-zero DATE/DATETIME/TIMESTAMP defaults before
assignment casting. Ordinary DDL retains its strict plus NO_ZERO_DATE gate for
all three types. Persisted verification skips only that final CREATE admission;
all binding, conversion, folding, protocol and whole-tree equality checks stay
active without changing reader SQL mode or diagnostics.

A private pre-normalization purpose reproduces the actual old TIMESTAMP DEFAULT
0 binding (1970-01-01 UTC rather than zero). It suppresses only the new integer
zero substitution. The candidate applies solely to temporal DEFAULT origins
whose unwrapped AST is an integer-zero literal, disjoint from the function and
DIV historical eras; no per-node era composition is introduced. Both eras still
participate in sensitivity classification after a current match. Older failed
DATE/DATETIME numeric casts remain failed candidates. The existing cancellation
and candidate-node bounds apply unchanged (at most one additional descriptor for
these literal-only defaults; other expressions retain their existing bound).

Frozen definitions for 0 and (0) were captured by genuine incoming 129d977,
independently of this compatibility branch. That old epoch-zero binding is below
the TIMESTAMP assignment minimum; the incoming binder rejects subsequent default
assignments with 1366 in both strict and permissive modes. Restoration preserves
this policy rather than silently replacing the stored binding with zero.
UT and the reused public SQL fixture
cover historical stored bindings and existing strict/permissive future-assignment
policy, strict cross-mode DUMP/LOAD for all three
types with numeric/canonical origins, ordinary strict DDL rejection, forged
value/type rejection and immutable session/catalog state. No manifest, function,
protocol, runtime cast, per-row path or resource ownership change is added.

## Revision 15: persisted zero-TIMESTAMP defaults

Origin verification selects an explicit private binding purpose. It skips only
CREATE-time strict zero-TIMESTAMP default admission: a persisted origin was
already admitted by its authoring session. Ordinary DDL retains this check.
Binding, assignment casts, folding, protocol admission and exact whole-tree
comparison remain active. The reader's SQL mode, conversion semantics and
warning sink are preserved; no blanket permissive mode or global state is used.
Cross-mode tests cover permissive authorship followed by strict DUMP/LOAD,
strict new-CREATE rejection and forged value/type rejection. Subsequent writes
retain their separate session policy.

## Revision 14: cheap candidates before session reconstruction

Each parsed expression first tries precision/era pairs in the backup session.
All cheap candidates participate in sensitivity classification even after a
current-era match. Next it tries all cheap locale-only precision/era witnesses. Only if none
matches does the verifier enumerate timezone and joint session witnesses. A bounded list of at most 31×6 small descriptors retains dependency
flags, never speculative expression graphs. All phases use the same cancellation
checks, whole-tree equality and shared 1,000,000 candidate-node budget. Unmatched
exhaustive reconstruction can still reach this bound; cheap valid precision
witnesses no longer compete with wrong-precision session cross-products.

Regressions cover precision 10 with one localized default, three such defaults
at precision 4, public backup/restore and future values after session changes.
No format, row-evaluation or ownership change accompanies the search reorder.

## Revision 13: DECIMAL DIV rebase compatibility

Official main v121 changed DECIMAL DIV result width and unsigned overloads.
The complete-origin verifier must also reproduce the pre-v121 physical casts,
signed overload zero and unannotated result width. A dump-only context selects
that historical checker and numeric resolver; ordinary SQL keeps main semantics.
The finite candidates follow published migration order: current, pre-fractional
with current DIV, pre-DIV with pre-fractional, and pre-special. Each candidate
must match the whole expression and uses the existing cancellation/node budget;
no per-node era mixing or semantic metadata normalization is accepted.

Frozen signed/unsigned generated-column and CHECK definitions were captured by
the actual binder at 0f9b9cc6c4d7d8afa19a24624f5c3aa019614eb1, before the DIV
change. Regression coverage verifies both old definitions, current-domain
controls, forged-expression rejection and public DUMP/LOAD followed by writes.
The transport remains format 3; no new runtime function or protocol capability
is added for compatibility reconstruction.

## Revision 12: locale-dependent defaults

The same pre-fold observation now records locale dependencies for MONTHNAME,
DAYNAME and DATE_FORMAT, including nested expressions, NULL results and bind
failures caused by a reading-locale assignment overflow. Folded dependencies
require capture even when CREATE and DUMP use the same locale. Literal controls
and unchanged executable bindings retain their classification.

Reconstruction obtains the finite supported locale inventory from the temporal
function owner (currently en_US, fr_FR, de_DE and ja_JP). The private child uses
that owner's effective reading locale, respecting variable-resolver precedence
over the session snapshot. Its resolver overrides only session lc_time_names;
all other lookups forward to the inherited resolver, without changing the parent.
Cheap locale-only witnesses precede joint locale/timezone reconstruction. Each
candidate must reproduce the complete expression under one configuration tuple;
independently matching subtrees or dimensions cannot authorize a payload.

All candidates still share cancellation and the one-million-node table budget.
The locale axis can multiply the finite timezone inventory by four, but cannot
increase the shared visit limit; expensive legitimate reconstructions may still
fail closed. Existing custom-zone/tzdata limitations remain. Formatted
FROM_UNIXTIME and STR_TO_DATE currently do not read lc_time_names and their
function semantics are unchanged. No format, protocol or row-evaluation change
is introduced. Acceptance includes four source locales, combined timezone/locale
recovery, resolver isolation, NULL/overflow, forged payloads, cancellation/budget,
and authenticated DUMP/LOAD, exact binding readback and future defaults.

## Revision 11: folded defaults and timezone witnesses

Default verification observes the ordinary DDL owner's resolved expression
before folding. TIMESTAMP conversions and session-dependent epoch/UUID
conversions can fold to DATETIME, VARCHAR, numeric or UUID values. Such folded
bindings require a format-3 witness even when CREATE and DUMP use the same
timezone; LOAD may use another one. Ordinary integer/string expressions,
timezone-independent DATETIME literals, and an unchanged executable
UNIX_TIMESTAMP default retain their existing classification. Literal NULL and
zero TIMESTAMP controls remain unchanged; a timezone-dependent expression
folding to either value still requires capture. Dependencies are observed before
assignment and folding, so a reading-zone overflow cannot suppress reconstruction
under a valid authoring zone. This is a conservative capture rule,
not a claim to recover the creation session from a folded value.

On mismatch, verification rebinds the complete origin through the same default
owner under UTC, the local location, 598 IANA names (including aliases, from
Go 1.27.1's tzdata 2026c inventory), and the 1,680 numeric minute offsets admitted
by SET time_zone. Named rules always come from time.LoadLocation, the frontend's
owner. This supports historical second offsets and a single expression spanning
different DST offsets; an arbitrary second-offset FixedZone is not a legal
witness. The existing checked TIMESTAMP delta remains only a minute-offset
fast path. No substring, output-delta or partial-tree equivalence is accepted.

Candidates are yielded one at a time without goroutines or a global cache.
Every bind uses the existing private process, cancellation and shared
one-million-node candidate-visit budget; matching or exhaustion stops the
search. The finite timezone search can add at most 2,280 witnesses per binding
era/precision, still constrained by that shared table budget. Invalid or
unavailable locations are skipped. The parent session, diagnostics and catalog
trees remain untouched. The change is confined to administrative binding
validation; no row-evaluation, manifest-layout or protocol version changes.

Catalogs do not record custom ZONEINFO names, old machine-local rules or the
creation tzdata version. Missing/changed historical rules can therefore still
fail closed; this revision does not claim universal environment recovery.
Retaining creation timezone provenance would require a separate catalog
contract. Cross-zone public roundtrips, exact binding readback, future writes,
type/value tamper rejection, cancellation and budget controls are the acceptance
boundary for this correction.

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
LOAD verify persisted expressions through the applicable binding eras. Only
division expressions enumerate all 31 precision increments; parser modes and
the bounded timezone witnesses above share the same table-wide candidate-node
budget. Memory is bounded by the manifest cap and one candidate expression at a time.
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
