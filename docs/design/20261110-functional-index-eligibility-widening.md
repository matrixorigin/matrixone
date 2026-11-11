# Functional index scalar expression and numeric JSON eligibility extension

Revision r7 (2026-11-10). Owning issue: #32437 (split from #31448).
Implementation PR: to be linked after design approval.

Revision r7 resolves the single P2 finding of the r6 review: the
render-once exemption was disproven — RENAME COLUMN and the renaming
half of CHANGE COLUMN rebuild the persisted functional-expression
SQL (`rewriteFunctionalColumnDependency`, functional_index.go:366-392,
called from build_alter_rename_column.go:96 and
build_alter_change_column.go:71) by re-parsing `OriginString`,
rewriting the renamed identifiers and re-formatting the rebuilt tree
with the widthless literal options, which reverts the
width-preserving X-form persisted spelling to
`cast(0x... as varchar)`; a later COPY replay — temporary-table SQL
built from the stored text (build_alter_table.go:608-616), executed
(compile/alter.go:1639-1646), hidden columns rebuilt from the text
because the replay loop skips their bound expressions
(ddl_expression_replay.go:122-125) — then re-binds the degraded text.
Probe-verified on the real rewrite code path
(TestFunctionalIndexIssue32437RenameRewriteLiteralPersistence,
probe-rename-replay-run.log, exit 0): the X-form text comes back as
the widthless spelling whose re-bind is TEXT. The width-preserving
serialization now contracts every producer of persisted
functional-expression SQL — the lowering render and the
identifier-rewrite render share one rendering entry point — a
round-trip assertion at the rewrite seam fails the RENAME/CHANGE
closed if the rewritten text stops re-binding the rewritten tree's
admitted key type (same OID, width and scale), and older binaries'
texts are stable through the rewrite (spelling and binding class
unchanged), so renaming tables authored before this feature keeps
behaving exactly as on base.

Revision r6 resolves the single P2 finding of the r5 review: the r5
tolerance for the widthless mode-independent literal spelling
(`cast(0x... as varchar)`) was unsound for width-deriving functions.
Probe-verified on the assigned base (TestFunctionalIndexIssue32437
TextLiteralReplay, exit 0): `concat(name, 'a\\b')` binds a
`varchar(43)` key but persists widthlessly as
`concat(\`name\`, cast(0x615c62 as varchar))`, and that text re-binds
to a TEXT result (40 + MaxVarcharLen exceeds the VARCHAR domain) — a
key the replayed table's text-result gate rejects; r5's guard would
have failed the DDL. The persistence boundary now renders
backslash-containing text literals through the width-preserving,
mode-independent hex-introducer spelling that view definitions
already persist (`_utf8mb4 X'615c62'`, tree/constant.go:195-199),
which re-binds the authored key type exactly (`varchar(43)` in the
probe; value preserved); the round-trip assertion's text-type
comparison thereby becomes exact — type OID, width and scale, no
width or domain tolerance — the lower-backslash positive control
stays positive with its persisted-spelling pin updated, and the
validation plan gains concat-with-backslash admission and
OriginString/ConstructCreateTableSQL/LIKE/COPY controls asserting the
same key domain, width and values.

Revision r5 resolves the two P2 findings of the r4 review: the CAST
target matrix is now derived from CAST's actual grammar —
`mo_cast_type`, whose first arm is `column_type` with the
`unsigned_opt` productions, not the CONVERT-only `mysql_cast_type`
grammar r4 audited — so the reachable width-named unsigned spellings
(uint8/uint16/uint32/uint64 targets) are admitted and covered
explicitly; and the persisted expression text now carries a
replay-losslessness guarantee: OriginString replay of an admitted
CAST head is probe-verified per spelling, lossy mix-spellings
(`SIGNED INTEGER`, `UNSIGNED INTEGER`, whose formatted SQL re-binds
to 32-bit targets) are persisted in a canonical lossless family
spelling, every admitted head is round-trip-asserted at lowering, and
the validation plan gains type- and value-preservation controls
beyond the signed/unsigned 32-bit limits across OriginString,
ConstructCreateTableSQL, LIKE and COPY replay.

Revision r4 resolved the single P2 finding of the r3 review:
the numeric JSON admission rule now keys on the two bound CAST forms the
binder actually produces for integer targets — the ordinary CAST
overload carrying explicit-syntax provenance for the plain integer
spellings, which includes the issue's exact `AS INT` form, and the
dedicated explicit-cast overload for the SIGNED-family spellings —
with a per-spelling bound-form matrix captured by a focused probe,
and paired INT/SIGNED admission, replay and equality-match controls
plus advertised-family coverage added to the validation plan.

Revision r3 resolved the single P2 finding of the r2 design review:
`md5` is the only one of the eight admitted functions whose CHAR
subject binds through the binder's implicit CHAR-to-VARCHAR argument
adapter, which the unchanged recursive walk rejected — so admission is
now given a concrete audit-backed rule (implicit NewCast node, source
`T_char`, target the fixed matcher's `MaxVarcharLen` envelope, charset
and collation version retained, pure widening), and a match-time
byte-stability rule now governs when the hidden-column physical
equality is added, with the full padding audit, MD5 CHAR/VARCHAR
admission and replay controls, and the reconciled contract recorded in
the decision log.

Revision r2 resolved the four P2 findings of the r1 design review:
the session-dependent diagnostic is reserved for conversions with
demonstrated session dependence, the integer-result admission rules
for `crc32`/`length` are stated separately from the text-result
rules, the unreachable MEDIUMINT CAST spelling is removed from the
advertised surface, and the position/column signedness boundary is
decided explicitly.

This increment widens only the DDL eligibility of the existing functional
index feature described in
`docs/design/20260929-functional-index-initial-support.md`. The
hidden generated column, generated-column DML machinery, secondary
index maintenance, protocol-107 representation, and equality-match
optimization all remain the mechanisms of that document and the bounded
JSON text follow-up.

## Problem and evidence

MatrixOne functional indexes deliberately admit a small expression
allowlist: text `lower`/`upper`, integer `+`, `-`, `*`, `abs`, identity
casts, same-signedness integer-widening casts, and the bounded
`cast(json_unquote(json_extract(json_col, constant_path)) as char|varchar(n))`
text form (`pkg/sql/plan/functional_index.go:104-182`,
`pkg/sql/plan/functional_json_index.go`).

Issue #32437 tracks three remaining components, all reproduced on the
assigned base (`origin/main` at `bf086489ba`, worktree base including
`b743219dd2`) by a focused planner probe
(`pkg/sql/plan/functional_index_issue32437_probe_test.go`,
`TestFunctionalIndexIssue32437Probe`; log:
`state/artifacts/issue-32437/probe-base-run.log`, exit 0):

1. Eight ordinary scalar functions still fail closed at
   `functionalExpressionEligible` when applied over otherwise eligible
   varchar/char columns: `substring(name,1,8)`, `left(name,4)`,
   `md5(name)`, `crc32(name)`, `hex(name)`, `reverse(name)`,
   `length(name)`, `concat(name,'-x')` — each rejected as
   `not supported: functional index expression must have session-independent
   semantics` (positive controls `lower(name)` and `id+1` admit).
2. `CAST(JSON_EXTRACT(doc,'$.a') AS INT)` and `... AS SIGNED` — the
   numeric JSON form — are rejected with the same message; only the
   unquote-plus-bounded-text form is admitted today.
3. Every eligibility miss reveals the same diagnostic: the deterministic
   functions above, the genuinely volatile `RAND()`/`NOW()` (whose
   accurate non-deterministic-function error is shadowed because
   `checkExprForVolatileFunc` runs after the eligibility gate in
   `lowerFunctionalIndex`), and the raw JSON keys of the existing JSON
   BVT pins all read
   `functional index expression must have session-independent
   semantics`. Only the genuinely zone-dependent temporal conversion
   (`CAST(ts AS CHAR(19))`) belongs in that category.

## Change classification and residents

Classification: feature extension that materially expands the
functional-index capability boundary. Design-first gate trigger:
user-visible DDL surface plus the persisted catalog generated-column
expression contract (old binaries fail closed on tables authored with
the widened class). Not an architectural handoff: no new persistent
state shape, protocol, global write path, or cross-subsystem
coordination is introduced; planner-local admission code and its tests are
the only production residents (`pkg/sql/plan/functional_index.go`,
`pkg/sql/plan/functional_json_index.go`, their tests, the DDL BVT
suites, and the pinned `functional_json_key.result` messages).

## Invariants

1. Session-independent persistence. A persisted functional expression
   must be a pure per-row function of its referenced column values and
   statement-embedded constants. Its value must never depend on
   session variables (time zone, sql_mode), statement context,
   environment, clock, randomness, or cluster/network state, because the
   hidden generated column is evaluated at write time and replayed
   (COPY ALTER, LIKE clone, checkpoint) in later sessions.
2. Fail-closed admission. Anything not positively admitted by the
   eligibility walk keeps failing closed: unknown functions, wrong
   arity, wrong argument domains, temporal conversions, raw JSON keys,
   dynamic paths, floats/decimals, binary/text/blob domains.
3. Key contract preserved. The persisted result must pass the existing
   gates unchanged: integer or CHAR/VARCHAR result, indexable BTREE key
   (`checkIndexColumnSupportability`), and ownership validation
   (`validateFunctionalTable`).
4. Match-time symmetry. The DDL admission walk and the planner's
   structural equality-match walk (`addFunctionalIndexFilters`) evaluate
   the same eligibility function, so exactly the DDL-admitted class can
   ever become a hidden-column equality; the base-row predicate always
   remains as a residual. A physical equality is then added only for
   byte-stable expressions (integer or VARCHAR hidden key, and no
   `T_char` column reference in the persisted expression, per the
   padding audit below), so force-index and ignore-index return the
   same rows in every session sql_mode.
5. No new persisted format or protocol version. Admission widens the
   set of expressions that may occupy the existing protocol-107 hidden
   generated-column metadata. No new `MORPCVersion` is minted
   (`.agents/skills/mo-dev/references/upgrade-compatibility.md`);
   older coordinators keep their conservative metadata rejection, and
   upgrade remains shutdown-only.
6. Replay losslessness. The SQL text persisted for a functional index
   expression (`OriginString`, re-emitted by `ConstructCreateTableSQL`
   for display, LIKE-clone construction and the COPY ALTER rewrite,
   and re-bound on every replay because the replay loop skips hidden
   columns, `ddl_expression_replay.go:122-125`) must re-admit under
   the eligibility walk and re-bind to the identical hidden key plan
   type — the same type OID, width and scale, with no width or domain
   tolerance. The width-preserving literal serialization, the
   canonicalization rule and the round-trip assertions below guarantee
   this at every producer of persisted functional-expression SQL — the
   authoring (lowering) site and the identifier-rewrite site that
   RENAME COLUMN and renaming CHANGE COLUMN rebuild the text through;
   a rendered text whose replay would silently change an admitted
   expression's key type or width can therefore never be persisted,
   and an identifier rewrite can never silently degrade the text every
   later replay rebuilds from — the DDL, respectively the ALTER, fails
   closed instead.

## Design

### 1. Scalar text-function admission (issue item 1)

Admission decisions are made on the bound plan expression — after
binding resolves overload identity — following the structural pattern
already used by `functionalJSONTextCastEligible`: function identity by
encoded overload ID (`fn.Func.Obj`) plus name, verified against the
registry like `functionalJSONFunction` does. Admission is not raw-SQL
name matching.

Functions to admit (all are positively-audited pure in-process value
transformations in `provenPureValueFunctions`,
`pkg/sql/plan/function/function.go:186-260`, and non-volatile, so
`checkExprForVolatileFunc` also passes):

| Function | Admitted shape | Persisted key type |
|---|---|---|
| `substring` (incl. `substr`/`mid` bindings of function id SUBSTRING) | text-source argument plus one or two integer position/length arguments; text result | derived text (`varchar`/`char`) |
| `left` | text-source argument plus one integer count argument; text result | derived text |
| `reverse` | exactly one text argument; text result | derived text |
| `md5` | exactly one text argument; a CHAR subject binds through the audited implicit CHAR-to-VARCHAR adapter below | `varchar(32)` |
| `crc32` | exactly one text argument | `uint64` |
| `hex` | exactly one text argument (the string overloads; the integer-formatted overloads stay out of this increment) | derived text |
| `length` | exactly one text argument | `int64` |
| `concat` | one or more arguments, every argument in the text domain (column references or string literals); text result | derived text |

Eligibility rule per function, with separate result predicates so the
walk admits all eight functions:

- Text-result functions (`substring`, `left`, `reverse`, `md5`, `hex`,
  `concat`): result and subject argument must be `functionalTextType`
  (varchar/char). `substring`/`left` position/count arguments must be
  integer plan types (see the signedness boundary below).
- Integer-result functions (`crc32`, `length`): the single text
  argument must be `functionalTextType`, and the result must be an
  integer plan type (the registrations return `uint64` for `crc32` and
  `int64` for `length`), so the existing integer-or-text result gate,
  index supportability, and the persisted key type agree.

Every argument must additionally pass the existing recursive
eligibility walk (constants, eligible ordinary columns, same-signedness
integer widening casts, and - through that walk - nested admitted
functions). Binaries, BLOB/TEXT source columns, JSON sources, and
binary-string results remain rejected because the result gate and
`functionalTextType` fail closed on them. `concat` requires every
argument to be already text-typed: an argument needing the binder's
formatted-scalar cast (numbers, temporal, JSON) introduces a conversion
whose source type fails the identity/cast tests of the walk, so
`concat(name, id)`-style mixed arguments stay rejected in this
increment. This is a deliberate, documented boundary rather than an
oversight; see Rejected alternatives.

Position/count boundary (`substring`, `left`): the binder binds
positional arguments to `int64` through the integer-argument machinery,
leaving an integer-argument cast wrapper around any column source.
Integer literals and signed integer columns are admitted: a literal
binds as one already-`int64` constant, and a signed `int8/int16/int32`
column widens through the already-admitted same-signedness
integer-widening cast rule. Unsigned position/count columns stay
unsupported in this increment: their binder wrapper is an
unsigned-to-signed conversion, which the unchanged recursive gate
fails closed (and which is not value-preserving for `uint64`
sources). No new conversion rule is minted for them; planning includes
signed/unsigned column controls so the boundary stays observable.

CHAR-subject adapter admission (`md5` only, today). Of the eight
functions, only `md5` has no CHAR overload — its registrations cover
varchar/text/blob (`pkg/sql/plan/function/list_builtIn.go:9518-9556`)
— so binding `md5(char_col)` delegates to the fixed matcher
(`json_string_consumers.go:72-88` -> `fixedTypeMatchSelection`,
`pkg/sql/plan/function/type_check.go:457-504`), whose cheapest
offered conversion for a non-matching string input is the
char-to-varchar implicit cast (`type_check.go:3270-3310`), and the
binder inserts that implicit NewCast argument adapter with the
explicit-cast flag clear (`base_binder.go`, `appendCastBeforeExpr`).
The other seven functions bind CHAR subjects natively (explicit
CHAR overloads for substring/left/reverse/length/hex; the string
domain matcher preserving the MySQL string input for lower/upper;
the varlen direct-bind for crc32 — `T_char` `FixedLength()==-24`, so
`IsVarlen()` covers it — and concat's own domain check), so this rule
exists for the one real gap. The recursive walk therefore admits
exactly this adapter node, and no other cast, as an argument of an
admitted function, when all of the following hold:

- the node is the implicit ordinary cast: the CAST function resolved
  to the NewCast implicit overload with `SyntaxExplicitCast` clear.
  User explicit casts such as `CAST(char_col AS VARCHAR(2))` stay
  fail-closed — their width can truncate and their plan shape is
  deliberately excluded;
- the source type is `T_char`. Binary/BLOB/TEXT domains never reach
  this rule (different OIDs; `functionalTextType` and the result
  gate already fail closed on them);
- the target is the matcher's fixed envelope: `T_varchar` with the
  registered width `MaxVarcharLen` and the source's charset and
  collation version retained (`types.go` `ToType`;
  `MaxCharLen=255`, `MaxVarcharLen=MaxStringSize` in
  `pkg/container/types/bytes.go`), so the adapter is a pure widening
  (target width >= any CHAR width, non-truncating) and a
  representation-preserving copy of the same bytes with no session
  reader on its execution path.

Like every admission decision here, this is a decision on the bound
plan (resolved identity + flag + types), not raw-SQL matching: the
binder inserts the identical adapter when binding the query-side
expression, so DDL and match-time shapes stay symmetric (invariant
4). Per-function subject-argument checks apply to each argument's
bound result type, which for `md5(char_col)` is the adapter's
`T_varchar` target; key-type accounting is unaffected (the derived
key of md5 is `varchar(32)` regardless of subject spelling). Beyond
the walk's pre-existing cast admissions (identity casts,
same-signedness integer widening, binary-literal text casts), this
rule and section 3's numeric JSON head are this increment's only two
new admission sites for CAST-family nodes, and they are disjoint:
the adapter is
a flag-clear overload-0 node in argument position with a `T_char`
source and `T_varchar` envelope, while the numeric JSON head admits
flag-set overload-0 or overload-1 nodes wrapping the json_extract
chain with an integer target node.

Determinism and safety accounting, per the selector's risk note:

- Conversions: the only conversions inside this class are text-byte
  transforms plus integer literal/column position arguments. Number
  formatting is never persisted because non-text `concat`/`hex`
  arguments are excluded by the walk.
- SQL modes: none of these kernels read session sql_mode; input widths,
  charsets and padspace of the persisted result come from the bound
  column types recorded in the metadata. The one sql_mode surface that
  does interact with this class — scan-level CHAR padding under
  `PAD_CHAR_TO_FULL_LENGTH` — is handled by the byte-stability rule
  in the next section, not by the kernels.
- Collations: source collation is the column's own schema metadata; the
  persisted expression carries the same collation/width provenance the
  existing `lower`/`upper` keys already carry, and the
  `sameFunctionalValueType`/`ValidateCollation` rules are untouched.
- NULLs: the functions are STRICT-class NULL-propagating; a NULL
  argument yields a NULL hidden value exactly like `lower(name)` today.
- Volatility/diagnostics: each function is registered STRICT without
  volatile or real-time flags, so `checkExprForVolatileFunc` admits
  them and no row-scoped diagnostics are invoked.

### 2. Match-time byte-stability of the physical equality

Admitting `md5(char_col)` exposed a second, deeper constraint: the
physical hidden-column equality is only sound when its bytes are
provably the same in every session that can run the query. Padding
audit, cited from the assigned base:

- The implicit CHAR-to-VARCHAR adapter is itself
  representation-preserving and session-independent: it executes the
  ordinary NewCast path, and `ResolvePadCharToFullLength` has exactly
  three non-test consultation sites on base — the scan pipeline
  (`table_scan.go:55`), the set-operation cast arm (`func_cast.go:10329`),
  and a remote-execution protocol gate (`compile/remoterun.go:3102`, a
  capability check that computes no cast value) — none of which the
  argument-adapter path consults; it copies the CHAR payload bytes
  unchanged.
- CHAR padding is materialized at scan time, per reader session:
  with `PAD_CHAR_TO_FULL_LENGTH` set in sql_mode
  (`pkg/vm/process/sql_mode.go:121-131`), the scan pipeline pads every
  `T_char` column's read bytes to the declared width
  (`table_scan.go:55` resolves the mode; `padCharColumnsToFullLength`
  at `table_scan.go:211` runs at the scan sites `table_scan.go:198`
  and `table_scan.go:303`); a hidden generated column stored as
  `T_char` is padded like any other CHAR column.
- The hidden key value is materialized at write time as a function of
  the stored (unpadded) row bytes — the backing column is
  user-invisible ("virtual" only in that sense in the initial-support
  design, whose Ownership and execution section records that each
  value is materialized by the existing generated-column DML
  machinery, retained in PRE_INSERT's physical input, with ordinary
  secondary-index maintenance owning the physical index) — while the
  base-row residual is evaluated over the scan-normalized — possibly
  padded — bytes. In a
  pad-mode session the two can disagree: a row whose residual
  predicate passes can be excluded by the added hidden equality, so
  force-index and ignore-index would return different rows. On the
  assigned base this divergence is latent for every already-admitted
  CHAR-subject class (`lower`/`upper` over CHAR columns), because
  `addFunctionalIndexFilters` (`functional_index.go:412-457`) adds
  the equality on a purely structural and typed match — the same
  expression shape both sides — with no byte-stability condition.
- The feature family already contains the correct precedent:
  CHAR-keyed JSON text indexes were pinned residual-only by the
  bounded JSON follow-up (a CHAR equality can match several padded
  physical values, so a raw key cannot narrow candidates), while
  VARCHAR-keyed JSON indexes keep the physical lookup.

Rule (match-time only; DDL admission is unchanged):
`addFunctionalIndexFilters` adds the hidden-column physical equality
only when both conditions hold:

1. the hidden key type is an integer family type or `T_varchar`
   (a `T_char` hidden key — the explicit `AS CHAR(n)` JSON text
   form, and any CHAR-domain function result — is padded at scan time
   and excluded);
2. the persisted expression contains no column reference whose plan
   type is `T_char`, recursively, including through the admitted
   adapter's source.

Every other DDL-admitted expression keeps its base-row residual and
matches through that residual alone — exactly the JSON CHAR-key
behavior, now as a stated rule instead of an incidental shape
mismatch. Consequences, all explicit:

- `md5(varchar_col)`, every varchar-subject text function, and every
  integer-key expression (including the numeric JSON case)
  keep the physical equality; VARCHAR bytes are not touched by
  `PAD_CHAR_TO_FULL_LENGTH`.
- CHAR-subject indexes — the new `md5`/`substring`/`left`/`reverse`/
  `hex`/`concat` forms over CHAR, `crc32`/`length` over CHAR, and the
  pre-existing `lower`/`upper` over CHAR — admit at DDL and are
  residual-only at match time. Existing lower/upper-over-CHAR
  indexes therefore lose a physically unsound equality optimization;
  result sets are unchanged because the residual owns correctness, and
  the change is observable only as fewer index-equality rewrites.
  md5 results carry no pad-space flags (md5 is not a value-selecting
  function; the pad-space provenance path applies only to
  case/coalesce-family select results), so the stored key is
  unpadded and byte-stable whenever its subject is.

### 3. Numeric JSON functional index (issue item 2)

A distinct numeric case, symmetric to the bounded text case:

```
CAST(JSON_EXTRACT(<ordinary JSON column>, <constant single-location path>) AS <integer type node>)
```

Gate requirements, reusing the existing text-case helpers:

- explicit user CAST, admitted in either of the two bound forms the
  binder actually produces for integer targets, with identity,
  arguments and target metadata all validated. The binder routes a
  CAST target through `useExplicitCastOverload` (base_binder.go:466-
  489): the dedicated explicit-cast function — CAST overload 1,
  `NewExplicitCast` (function/list_operator.go:2857-2874), the form
  the existing JSON text helper keys on (`functionalJSONFunction(...,
  function.CAST, 1)`, `functional_json_index.go`) — is selected for
  the float/decimal, character/binary families and the LONGLONG-based
  SIGNED-family spellings; every other target spelling binds as the
  ordinary CAST function, overload 0 (`NewCast`), with explicit-syntax
  provenance recorded on the node (`SyntaxExplicitCast` true;
  base_binder.go:336-339 -> `appendSyntaxExplicitCastBeforeExpr`,
  base_binder.go:12561-12577). The issue's exact `AS INT` form is in
  this second class — mysql_sql.y:15821-15831 parses INT to
  `MYSQL_TYPE_LONG`, which `useExplicitCastOverload` does not select —
  so an admission rule restricted to the explicit-cast overload would
  exclude the very spelling the issue names (the r3 wording did
  exactly that; this revision corrects it). The gate therefore admits
  the CAST head when the bound node resolves to function CAST with
  either (a) overload 0 and `SyntaxExplicitCast` set, or (b)
  overload 1 — and never as a flag-clear overload-0 node: that node
  shape is the binder-inserted implicit adapter excluded by section 1,
  and the flag is serialized plan metadata, already
  consulted by structural comparison elsewhere
  (base_binder.go:13761, bound_expression_compare.go:68), so the two
  rules are disjoint by flag value, argument shape and target domain.
  The target type node must be present in the plan (`args[1].GetT()`)
  and integer-family, with `sameFunctionalValueType` over head and
  result — decimal, float, bool and text targets of this JSON chain
  remain outside this case (the bounded text case owns text targets);
- the target surface is derived from CAST's actual grammar, which r4
  audited from the wrong nonterminal. `CAST(<expr> AS <type>)` parses
  `<type>` through `mo_cast_type` (mysql_sql.y:13429), whose first arm
  is `column_type` (mysql_sql.y:13928-13929) — `numeric_type
  unsigned_opt zero_fill_opt` (mysql_sql.y:15658-15663, with
  `unsigned_opt`/`zero_fill_opt` recorded on `InternalType`) —
  beside the `SIGNED [INTEGER]` and `UNSIGNED [INTEGER]` family
  productions (mysql_sql.y:13941-13971, both storing
  `MYSQL_TYPE_LONGLONG`, width 64, with the family string taken from
  the SIGNED/UNSIGNED token or, when written, from the trailing
  `INTEGER` token — the root of the replay defect fixed below — and
  `Unsigned` set for the latter). The `mysql_cast_type` grammar r4
  audited is the CONVERT-only grammar (mysql_sql.y:13448). The
  width-named unsigned spellings are therefore reachable through
  CAST — `TINYINT UNSIGNED`/`SMALLINT UNSIGNED`/`INT|INTEGER
  UNSIGNED`/`BIGINT UNSIGNED` bind uint8/uint16/uint32/uint64 plan
  targets and satisfy the two-form gate — so r4's
  "no width-specific unsigned spellings" claim was simply wrong and
  this revision admits and covers them. The numeric aliases are
  reachable the same way (`INT1`/`INT2`/`INT4`/`INT8` bind the
  tinyint/smallint/int/bigint widths), the `ZEROFILL` display suffix
  binds through the same column_type mapping (`INT ZEROFILL` keeps
  the `int32` LONG target, since the LONG arm does not branch on
  `Zerofill`, build_util.go:279-283), the `LONG` alias is reachable
  only through the CONVERT grammar (parse error through CAST,
  probe-confirmed), and the `MEDIUMINT`/`INT3` spellings keep the
  pre-existing upstream bind-time ErrNYI rejection (pinned by
  `TestBuildCastToMediumIntIsRejected`), which this change does not
  touch;

- the bound form and the persisted-spelling replay verdict per
  spelling, captured on the assigned base by the round-4 and round-5
  probe matrices (each mirroring the binder call of the lowering
  path, and in round 5 the `OriginString` rendering options plus the
  re-bind; logs `probe-cast-forms-run.log` and
  `probe-cast-replay-run.log`, exit 0, all spellings PASS):

  | SQL spelling | Bound cast form | Hidden key type | Rendered spelling replays |
  |---|---|---|---|
  | `AS INT` / `AS INTEGER` / `AS INT4` | overload 0, `SyntaxExplicitCast` set | `int32` | identically |
  | `AS TINYINT` / `AS INT1` | overload 0, `SyntaxExplicitCast` set | `int8` | identically |
  | `AS SMALLINT` / `AS INT2` | overload 0, `SyntaxExplicitCast` set | `int16` | identically |
  | `AS BIGINT` / `AS INT8` | overload 0, `SyntaxExplicitCast` set | `int64` | identically |
  | `AS TINYINT UNSIGNED` | overload 0, `SyntaxExplicitCast` set | `uint8` | identically |
  | `AS SMALLINT UNSIGNED` | overload 0, `SyntaxExplicitCast` set | `uint16` | identically |
  | `AS INT UNSIGNED` / `AS INTEGER UNSIGNED` | overload 0, `SyntaxExplicitCast` set | `uint32` | identically |
  | `AS BIGINT UNSIGNED` | overload 0, `SyntaxExplicitCast` set | `uint64` | identically |
  | `AS INT ZEROFILL` | overload 0, `SyntaxExplicitCast` set | `int32` | identically |
  | `AS SIGNED` | overload 1 (`NewExplicitCast`) | `int64` | identically |
  | `AS SIGNED INTEGER` | overload 1 (`NewExplicitCast`) | `int64` | loses the key: renders `AS INTEGER` (`int32` re-bind) — canonicalized to `AS SIGNED` |
  | `AS UNSIGNED` | overload 1 (`NewExplicitCast`) | `uint64` | identically |
  | `AS UNSIGNED INTEGER` | overload 1 (`NewExplicitCast`) | `uint64` | loses the key: renders `AS INTEGER UNSIGNED` (`uint32` re-bind) — canonicalized to `AS UNSIGNED` |
  | `AS MEDIUMINT` / `AS INT3` | bind-time ErrNYI (upstream) | — | not admitted |
  | `AS CHAR(n)` / `AS VARCHAR(n)` (text case, control) | overload 1 | char/varchar | length-bearing spellings render identically; the lengthless `AS CHAR` target is already normalized to the `varchar` family by the parser itself (mysql_sql.y:13934-13939) |

  The gate keys on the bound integer plan type, so every spelling is
  handled uniformly whichever form it binds to. Match-time symmetry
  holds because the query-side expression binds the same way
  (invariant 4); replay symmetry is the subject of the rule below;
- `json_extract` resolved on its (json, varchar) overload, taking an
  ordinary JSON column of the table — non-hidden, non-generated,
  non-auto-increment, type `T_json`, value-equal plan type — plus a
  non-NULL constant string literal path;
- the path must pass both the existing restricted single-location
  grammar (`functionalJSONPath`) and the ByteJSON parser, exactly as
  the text case does;
- the hidden key type is the integer target; `JSON` itself is never a
  physical key, and no `JSON_UNQUOTE` stage exists in this case — the
  existing `jsonToNumeric` cast reads the JSON value directly (JSON
  null literal -> NULL key; JSON integer/number or numeric string ->
  the integer value; out-of-range or non-numeric values -> the existing
  deterministic `jsonCastErr`).

Result-gate parity: result-type gate, index supportability, and
`validateFunctionalTable` are unchanged, so the case stays inside the
existing contract. DML-time failing rows for out-of-range or
non-numeric JSON payload values propagate the existing cast error
deterministically (no sql_mode branch in `jsonToNumeric`), consistent
with how generated columns surface expression errors.

#### Persistence and replay losslessness (r5)

The persisted expression is SQL text, not the bound plan:
`lowerFunctionalIndex` renders `OriginString` from the user's
expression tree with the lowering formatter options
(`tree.StringWithOpts(key.Expr, dialect.MYSQL, ...)`,
functional_index.go:260-262), `ConstructCreateTableSQL` re-emits that
text for show-create display, LIKE-clone construction and the COPY
ALTER rewrite (`build_show_util.go:459-465`), and replay re-builds the
hidden backing expression by re-binding that SQL, because the
plan-preservation loop skips hidden columns
(`ddl_expression_replay.go:122-125`). Whatever the formatted text
re-binds to is therefore what every replayed copy of the table has.
`CastExpr.Format` delegates the target to `InternalType.Format`
(tree/expr.go:1483-1489), which prints the lowered family string plus
the `unsigned`/`zerofill` suffixes (tree/types.go:184-186, 268-272),
and re-binding that text follows the `mo_cast_type` -> `column_type`
mapping (build_util.go:259-288). The round-5 probe measured every
spelling through that pipeline: the two family spellings whose
rendered family string comes from the trailing `INTEGER` token re-bind
narrower — `AS SIGNED INTEGER` renders `as integer` and re-binds an
`int32` target against the authored `int64`; `AS UNSIGNED INTEGER`
renders `as integer unsigned` and re-binds `uint32` against the
authored `uint64` — while every other spelling, including all
width-named unsigned spellings, the bare `SIGNED`/`UNSIGNED`,
`ZEROFILL` and the numeric aliases, renders its own target back. A
LIKE clone or COPY replay of an authentically spelled `AS SIGNED
INTEGER`/`AS UNSIGNED INTEGER` index would therefore carry a silently
narrowed hidden key: JSON values beyond the 32-bit domains
(2147483648, 4294967296) would fail the replayed table's writes with
the narrower casts' range errors (func_cast.go:3653-3659, 3677-3683)
where the authored table persists them. The same two spellings are
already admitted today by the pre-existing same-signedness
integer-widening case of the walk (functional_index.go:150-169), so a
widening index authored as `CAST(id AS SIGNED INTEGER)` carries the
same latent narrowing on base and is repaired by the same rule.

Rule (persistent-spelling canonicalization; the admitted bound plan
is unchanged — every admitted node, overload, flag and key type is
exactly what was admitted — only the rendered persistence text is
normalized):

- every persisted CAST-family head renders verbatim, except the two
  probe-proven lossy spellings: an `AS SIGNED INTEGER` head persists
  as `CAST(... AS SIGNED)` and an `AS UNSIGNED INTEGER` head persists
  as `CAST(... AS UNSIGNED)` — the bare family spellings of the same
  bound `int64`/`uint64` targets. `mo_cast_type` already establishes
  the precedent of storing each target faithfully to its bound
  semantics rather than to the spelled token display (a lengthless
  `AS CHAR` target is normalized to the `varchar` family by the
  parser itself, mysql_sql.y:13934-13939); this rule gives the
  LONGLONG-family mix-spellings the same treatment. The
  canonicalization lives at the persistence rendering site, so it
  covers every persisted CAST head — the JSON numeric case and the
  pre-existing integer identity/widening casts alike;
- `lowerFunctionalIndex` round-trip-asserts the persistence before
  accepting the DDL: it re-parses the rendered `OriginString` under
  the same lowering options, re-binds it with the same binder call,
  and requires the re-bound expression to re-admit under the
  eligibility walk and to carry the same hidden key plan type. The
  type comparison is defined precisely and applies uniformly, with
  no width or domain tolerance: the re-bound key type must equal the
  authored key type on OID, Width and Scale — width-deriving text
  functions included — because the width-preserving literal
  persistence below guarantees identical re-derivation (integer keys
  are width/scale-free, so this is full equality there as well). A
  mismatch rejects the DDL fail-closed, so no rendered text can be
  persisted with a replay that would silently change the table, and
  a future grammar, binder or formatter change that alters a
  rendering fails the authoring DDL instead of corrupting replays.

Stated consequences: `SHOW CREATE TABLE`, LIKE clones, COPY ALTER
rewrites and replay of an index authored with one of the two mixed
spellings display and carry the canonical bare spelling — the
identical bound target in a lossless rendering. Tables authored
before this feature cannot carry the new classes, and existing
persisted texts are not rewritten, so no migration is required and
pre-existing replay behavior is unchanged.

Text-literal persistence (r6). The r5 tolerance for widthless
binary-literal casts is removed; it was unsound for every function
whose result width derives from its arguments. Probe-verified on the
assigned base (`TestFunctionalIndexIssue32437TextLiteralReplay`, exit
0, probe-literal-replay-run.log): `concat(name, 'a\\b')` binds a
`varchar(43)` key, but today's literal rendering is widthless —
`concat(\`name\`, cast(0x615c62 as varchar))`
(tree/constant.go:201-203), whose cast target re-binds at
`MaxVarcharLen` (build_util.go:347-353; the probe's lower control
measured the drift at the base spelling's re-bind: authored width 3
re-binds width 65535) — and concat sums argument widths
(list_builtIn.go:151-154 with string_result_domain.go:444-452), so
40 + MaxVarcharLen crosses the VARCHAR domain and the re-bind
produces a TEXT result (string_result_domain.go:388-393): the
replayed key cannot pass the text-result gate, and the r5 guard
correctly fails what would otherwise be a broken DDL. The repair
sits at the persistence boundary and mirrors the treatment the
formatter already gives view definitions: a backslash-containing
string literal persists in the width-preserving, mode-independent
hex-introducer spelling `_utf8mb4 X'615c62'`
(tree/constant.go:195-199 — the branch exists precisely to keep a
text literal's value and inferred character width, without a CAST's
default width), so the persisted text re-binds with the authored
value and character width and the derived key re-derives exactly
(probe: the X-form re-bind of the concat case yields varchar, width
43; the lower control yields width 3 with the literal value
preserved in both spellings). Backslash-free literals keep their
plain quoted spelling and re-bind identically (probe control
`concat(name, 'abc')`). Older binaries' persisted texts keep the
widthless `cast(0x... as varchar)` spelling and their replay
behavior unchanged — the walk's binary-literal cast arm remains as
the old-text compatibility path, and new authoring never writes that
spelling. The implementation-turn admission tests pin the exact node
form the X-form binds to on re-bind (a plain literal argument, which
the walk admits as `Expr_Lit`, or the binary-literal cast shape it
already admits).

Persistence across identifier rewriting (r7). The render-once
assumption above is withdrawn: the r6 review disproved it. RENAME
COLUMN and the renaming half of CHANGE COLUMN rebuild the persisted
text — `renameFunctionalColumnDependencies`
(functional_index.go:336-363, called from
build_alter_rename_column.go:96 and build_alter_change_column.go:71)
re-parses `OriginString`, rewrites the renamed identifiers, re-binds
the rewritten tree and re-formats it
(`rewriteFunctionalColumnDependency`, functional_index.go:366-392)
with the same widthless literal options — the only other producer of
persisted functional-expression SQL besides the lowering render
(functional_index.go:262; every other surface,
`ConstructCreateTableSQL` and build_show_util.go:237,463, re-emits
the stored text verbatim and changes nothing). On the assigned base
this re-render reverts the X-form persisted spelling to
`cast(0x... as varchar)` (round-7 probe, case A, executed through the
real rewrite code path), while the bound expression stored beside the
text keeps the authored key — so the divergence is silent until the
next replay: the COPY temporary table is rebuilt from the stored text
(build_alter_table.go:608-616), the SQL is executed
(compile/alter.go:1639-1646), and the hidden column is rebuilt from
the degraded expression because the replay loop skips hidden
columns' bound expressions (ddl_expression_replay.go:122-125); the
degraded text re-binds a TEXT key the text-result gate rejects, so a
supported RENAME followed by any replay-capable ALTER breaks a table
whose first replay passed.

The rule is therefore placed on every producer: the lowering render
and the identifier-rewrite render serialize through one shared
implementation of the width-preserving literal rendering, so a
backslash literal written once keeps its spelling class through any
rebuild — rendered after parsing and identifier rewriting, the
rewritten tree persists `concat(\`renameto\`, _utf8mb4 X'615c62')`
and its re-bind keeps the authored key type exactly (round-7 probe,
case B: `varchar(43)`; the hex-introducer spelling survives the
identifier rewrite). A round-trip assertion mirrors the authoring one
at the rewrite seam: the ALTER re-binds the freshly rewritten text
with the same persisted-expression rules and fails closed unless it
re-admits and re-binds the rewritten tree's hidden key type — the
same OID, width and scale, no tolerance; a future formatter or
grammar change therefore fails the rename instead of silently
corrupting the persisted text every later replay rebuilds from.
Older binaries' texts need no migration through this path: the
widthless spelling parses to an explicit CAST node whose own
rendering the rewrite reproduces verbatim, so the old spelling and
its binding class are stable through the rewrite (round-7 probe,
case C), and renaming tables authored before this feature behaves
exactly as on base. Backslash-free literals rewrite exactly (round-7
probe, case D).

### 4. Diagnostic split (issue item 3)

The eligibility walk keeps failing closed, but reports why, in one of
three stable categories:

1. Genuinely non-deterministic functions (`rand`, `now`, ...): run
   `checkExprForVolatileFunc` before the eligibility gate so these
   report the existing, accurate
   `expression of generated column cannot refer to a non-deterministic function '<name>'`
   (InvalidInput) instead of the generic functional-index message.
2. Session-dependent semantics: a conversion is placed here only when
   the cast implementation demonstrably reads session state. The
   audited set, cited from `pkg/sql/plan/function/func_cast.go`, is:
   (a) any cast whose source or target plan type is TIMESTAMP — every
   such conversion path reads the session time zone
   (`proc.GetSessionInfo().TimeZone`, e.g. timestamp-to-string/int/
   date/datetime/time/year/decimal dispatch sites and the
   date/datetime/decimal/string-to-timestamp sites); 
   (b) any cast whose source is a string-family type and whose target
   is float32/float64 — it reads the session's native-compatibility
   mode (`CompatibilityModeFromProcess` /
   `proc.GetSessionInfo().MatrixOneNativeMode`, func_cast.go:7774);
   and (c) any cast whose source is a string-family type and whose
   target is DATETIME/DATE/TIME — the parsing paths read session
   state: the zero-date policy applies the session SQL mode
   (func_cast.go:9867-9879, mode-dependent controls at
   func_cast_zero_temporal_test.go:161-203) and `strToDatetime`
   reads `TIME_TRUNCATE_FRACTIONAL` (func_cast.go:10075) and
   applies it (func_cast.go:10109-10140). These reject with the
   retained message
   `not supported: functional index expression must have session-independent semantics`.
   Keeping the category conversion-based, not family-based, is what
   repairs the issue's diagnostic complaint: deterministic temporal
   conversions such as `CAST(date_col AS CHAR(10))` (dispatched to
   `dateToStr`, which formats the stored date with width checks and no
   session input) or `CAST(datetime_col AS CHAR(19))` are rejected as
   unsupported, not mislabeled as session-dependent.
3. Unsupported expression: every other fail-closed miss (unknown
   function, arity, argument-domain, result-gate, raw JSON keys, path
   violations, non-integer JSON target, deterministic-but-unadmitted
   temporal conversions like DATE-to-CHAR, unsigned position columns,
   ...) gets a new NotSupported error whose message is `functional
   index expression is not supported` decorated with the offending
   expression's SQL text, so the message states rejection-by-coverage
   instead of a false claim of session dependence. When a newly
   discovered conversion is proven to read session state, moving it
   into category 2 is a one-line gate change with paired tests.

Error surfaces: `lowerFunctionalIndex` produces the message; the
`validateFunctionalTable` metadata path remains fail-closed with its
existing invalid-metadata codes. The BVT pins moved from category 2 to
category 3 (`functional_json_key.result` for raw `json_extract`/`->>`
rejections) are updated in the same change; no UT asserts those words
today except through the dedicated diagnostic tests this change adds.

## Documented supported boundary (complete, after this change)

- Text functions over text columns (or satisfying literals):
  `lower`, `upper`, `substring`/`substr`/`mid`, `left`, `reverse`,
  `md5`, `crc32` (integer key), `hex`, `length` (integer key), `concat`.
- `md5` admits CHAR subjects through the audited implicit
  CHAR-to-VARCHAR binder adapter under the rule above (identical
  decision at DDL and match time). User-written casts are rejected
  exactly when they fall outside the retained admission rules: an
  explicitly written `CAST(char_col AS VARCHAR(2))` (width can
  truncate — the negative control) stays fail-closed, while a
  value-identical explicit cast such as
  `CAST(varchar40_col AS VARCHAR(40))` admits through the identity
  arm of the walk, exactly as on base.
- Physical hidden-column equality is added only for byte-stable
  expressions: integer or VARCHAR hidden key and no `T_char` column
  reference. CHAR-subject functional indexes (including `md5` over
  CHAR and the pre-existing `lower`/`upper` over CHAR) are DDL-admitted
  and match through the residual alone in every session sql_mode.
- Integer arithmetic: `+`, `-`, `*`, `abs` over integer operands with
  integer results.
- Casts: identity casts; same-signedness integer widening;
  mode-independent binary-literal text casts (the persisted-spelling
  compatibility form written by older binaries — backslash literals
  in newly authored expressions persist through the width-preserving
  hex-introducer spelling, never through a widthless cast); the
  bounded JSON text
  cast; the numeric JSON integer cast (either bound form; target
  spellings per the section-3 matrix: TINYINT/SMALLINT/INT/INTEGER/
  BIGINT and their `INT1`/`INT2`/`INT4`/`INT8` aliases for the
  signed widths int8/int16/int32/int64; TINYINT UNSIGNED/SMALLINT
  UNSIGNED/INT or INTEGER UNSIGNED/BIGINT UNSIGNED for the unsigned
  widths uint8/uint16/uint32/uint64; SIGNED/SIGNED INTEGER (int64)
  and UNSIGNED/UNSIGNED INTEGER (uint64); `ZEROFILL` display
  suffixes keep the underlying signed width). The MEDIUMINT (and
  INT3) spellings keep their pre-existing upstream rejection (ErrNYI
  before eligibility) and are not part of any admitted form. Of the
  mixed family spellings, `AS SIGNED INTEGER` persists and replays as
  `AS SIGNED` and `AS UNSIGNED INTEGER` as `AS UNSIGNED` — the same
  bound target with a lossless rendering, per the replay-losslessness
  rule in section 3; every other spelling persists verbatim.
- Identifier rewriting (RENAME COLUMN, renaming CHANGE COLUMN)
  preserves the persisted literal spelling class: new authoring keeps
  the width-preserving hex-introducer spelling and its exact hidden
  key semantics through the rewrite and every subsequent replay
  (SHOW, LIKE, COPY); texts persisted by older binaries keep their
  spelling and base behavior through the rewrite. A rewrite whose
  re-rendered text would stop re-binding the rewritten tree's
  admitted key type fails the ALTER (InvalidInput family) instead of
  persisting a degraded expression.
- Position/count arguments of `substring`/`left`: integer literals and
  signed integer columns; unsigned columns are rejected fail-closed in
  this increment.
- Everything else remains rejected, including deterministic temporal
  conversions (`CAST(date_col AS CHAR(n))`, `CAST(datetime_col AS
  VARCHAR(n))` — the DATE-to-CHAR control keeps reporting
  unsupported) using the unsupported
  diagnostic; conversions that demonstrably read session state
  (timestamp-involving casts, string-to-float casts, and
  string-to-temporal parsing casts, whose literals read the
  zero-date SQL-mode policy and `TIME_TRUNCATE_FRACTIONAL`) using
  the session-independence diagnostic; BLOB/TEXT/binary-string domains,
  floats/decimals/`bool` results, dynamic or wildcard JSON paths, raw
  JSON keys, session/environment readers, and any function not
  positively audited in `provenPureValueFunctions` and enumerated here
  (e.g. `sha1`, `locate`, `replace`, `instr` are out of this increment
  by scope).

## Standards, precedent and alternatives

MySQL permits functional indexes over deterministic built-in function
expressions and rejects non-deterministic ones (`Not allowed function`),
so a wider-but-audited deterministic set matches precedent. MySQL has
no general session-independence vocabulary; MatrixOne's persistence
contract is stricter because the expression is replayed across
sessions, so we admit only functions with a positive in-repo purity
audit.

- Alternative A — admit every non-volatile builtin: rejected. MO's
  volatile flag governs execution-time effects, not persistence safety;
  several non-volatile registrations read session/env state (e.g.
  `charset`, `collation`, temporal formatting) or are not warranted
  persistable; admission-by-default would invert the fail-closed
  invariant.
- Alternative B — match the eight function names as strings: rejected.
  Alias (`substr`/`mid`) and case handling make name matching fragile,
  and it silently grants every overload argument shape. Admission
  after binding by resolved identity plus explicit arity and
  argument-domain accounting is testable and keeps the walk's owner
  (the bound plan, not the SQL surface) as the single decision point.
- Alternative C — for the JSON numeric case, require an explicit
  `JSON_UNQUOTE` stage (mirror the text case): rejected. Unquoting is
  a text operation; the existing `jsonToNumeric` conversion already
  reads the JSON value and the MySQL-documented numeric JSON cast form
  is `CAST(JSON_EXTRACT(...) AS INTEGER)`; keeping the two JSON cases
  text-vs-numeric distinct is precisely the issue's requirement.
- Alternative D — broaden `concat` to formatted non-text arguments:
  deferred to a follow-up; formatting paths widen the persisted
  conversion surface (number/temporal-to-string) for a small
  convenience gain and are not requested by the issue.
- Alternative E — admit the implicit CHAR-to-VARCHAR adapter as a
  general identity cast everywhere in the walk: rejected. A general
  rewrite weakens the fail-closed invariant for every future
  overload matcher and is unnecessary — only `md5` needs the adapter
  today, so the rule pins the exact implicit node shape and envelope
  instead.
- Alternative F — keep the physical equality for CHAR subjects by
  padding the hidden key value to the column width at write time:
  rejected. It would couple the key to the declared width, still
  disagree with non-pad sessions and with trimmed query constants,
  and touch the generated-column write path that this increment
  promises to leave unchanged. The residual-only boundary is the
  existing JSON CHAR-key precedent, requires no write-path change,
  and keeps every session byte-stable.

## Compatibility, rollout and operations

- No schema, wire, or catalog changes; existing functional indexes and
  their metadata are untouched. New authored indexes store expressions
  that old coordinators reject conservatively on replay, identical to
  the previous two increments' handling, and per the shutdown-only
  upgrade policy no mixed-version window exists.
- Persisted-spelling note: newly authored indexes store
  replay-lossless expression SQL per the section-3 rule — the two
  mixed SIGNED-family spellings persist as their canonical bare
  spellings with the bound target unchanged, every other CAST
  spelling persists verbatim, and backslash-containing text literals
  persist through the width-preserving hex-introducer spelling
  (`_utf8mb4 X'...'`, the literal serialization view definitions
  already use) instead of the widthless binary-cast spelling — so
  `SHOW CREATE TABLE` output changes only for those classes.
  A rename is not a migration: RENAME COLUMN and renaming CHANGE
  COLUMN re-render the persisted expression (the second producer
  this design contracts); the shared width-preserving rendering
  keeps each text's own spelling class through the rewrite — new
  texts keep the width-preserving spelling across the rename and
  every later replay, old texts keep their widthless spelling and
  base behavior — whereas base's single-producer assumption lets a
  widened-class text degrade at the rename and break the next COPY
  replay. Existing indexes, metadata
  and persisted texts are not migrated or rewritten, so existing
  tables keep exactly their base behavior.
- Existing-optimization note: the byte-stability rule withholds the
  hidden-column equality rewrite from CHAR-subject indexes that
  already exist (lower/upper over CHAR). Query results are identical
  in every sql_mode because the base-row residual always owns
  correctness; the withheld rewrite is the object of a dedicated
  pad-mode force/ignore agreement control in the validation plan.
- No feature flag: admission is the allowlist itself; rollback on
  failure is reverting the admission switch, which cannot corrupt
  already-authored tables (old binaries reject DDL for them; the
  hidden-column machinery and existing indexes are unchanged).
- Width cap: derived text results whose bound exceeds the VARCHAR
  domain degrade to a TEXT result by the existing return-type helpers
  (`textStringResultType`); the existing result gate then rejects that
  DDL deterministically, so no new width rule is introduced.
- Performance: the gate is one O(expression size) walk at DDL and at
  bind/match time. Per-row DML cost is one additional scalar string/hash
  kernel bounded by the (already bounded) input column widths; derived
  key widths flow through the existing index-supportability limits, so
  over-wide keys keep failing at DDL with today's errors.
- Security/permissions unchanged: admission adds no privilege surface;
  the underlying DDL privilege checks own CREATE INDEX.

## Validation plan (cheapest discriminating evidence first)

1. Red/green focused UT in `pkg/sql/plan`:
   - base-rejected probe (already captured,
     `probe-base-run.log`) becomes formal admission tests: each of the
     eight functions plus the numeric JSON cast must build DDL, produce
     exactly one hidden owner column with the asserted key type
     (per-function: derived text for `substring`/`left`/`reverse`/
     `md5`/`hex`/`concat`, `uint64` for `crc32`, `int64` for `length`,
     the cast target for the numeric JSON case), pass
     `validateFunctionalTable` round-trip through the physical type
     codec and `ConstructCreateTableSQL` replay, and keep negative
     controls (temporal casts, binary/blob sources, over-width results,
     raw JSON keys, dynamic paths, non-integer JSON targets) rejected;
     the numeric JSON case is admitted per bound form — paired
     `AS INT` (overload-0, flag-set; hidden key `int32`) and
     `AS SIGNED` (overload-1; hidden key `int64`) submissions with full
     metadata replay of both, plus family coverage over the corrected
     section-3 matrix: the signed widths (`AS INTEGER`/`AS TINYINT`/
     `AS SMALLINT`/`AS BIGINT`, alias control `AS INT4`), the
     unsigned widths (`AS TINYINT UNSIGNED`/`AS SMALLINT UNSIGNED`/
     `AS INT UNSIGNED`/`AS INTEGER UNSIGNED`/`AS BIGINT UNSIGNED`,
     asserting hidden keys `uint8`/`uint16`/`uint32`/`uint64`) and
     the SIGNED/UNSIGNED family spellings, each with its expected
     bound form and hidden key; `AS MEDIUMINT`/`AS INT3` keep their
     bind-time ErrNYI boundary;
   - equality-match plan tests for each admitted function: the
     same-expression predicate must produce a hidden-column index
     equality plus a base-row residual, mirroring
     `TestFunctionalIndexDDL`'s indexScan/residual assertions. For md5
     these tests come in the paired subject spellings below; the
     numeric JSON case runs in both bound forms — an `AS INT` predicate
     (overload-0 form) and an `AS SIGNED` predicate (overload-1 form)
     each add the hidden-column equality (byte-stable: integer key, no
     `T_char` reference) alongside the base-row residual;
   - md5 adapter controls: admission of `md5(varchar_col)` and
     `md5(char_col)` with the hidden key type `varchar(32)` in both
     spellings and an exactly-one-hidden-owner-column count, plus
     `validateFunctionalTable` round-trip and
     `ConstructCreateTableSQL` replay of both forms; a user explicit
     `CAST(char_col AS VARCHAR(2))` argument stays rejected as the
     negative control, paired with the identity-VARCHAR positive
     control — a value-identical user-written
     `CAST(varchar40_col AS VARCHAR(40))` (inside `md5(...)` and as a
     standalone head) admits through the identity-cast arm, per the
     retained-P3 correction; the other seven functions' CHAR
     subjects must bind with no adapter node (their subjects keep
     exact plan equality with persisted shapes);
   - byte-stability plan-shape controls: an equality over
     `md5(varchar_col)` adds the hidden equality plus the residual,
     while over `md5(char_col)` and over the pre-existing
     `lower(char_col)` no hidden equality is added and the residual
     alone matches (mirroring the JSON CHAR-key residual-only pins);
   - diagnostic tests: three-way category assertions with exact
     messages, including `rand()`/`now()` no longer reported as
     session-dependent, the paired conversion controls requested by
     the r1 review — `CAST(date_col AS CHAR(10))` reporting
     unsupported and `CAST(ts_col AS CHAR(19))` reporting
     session-dependent — the retained-P3 string-to-temporal control
     (`CAST('2020-01-01' AS DATETIME)` and `CAST(date_str_col AS
     DATE)` reporting session-dependent, since the parsing paths
     read the zero-date policy and `TIME_TRUNCATE_FRACTIONAL`), and
     the position-column controls: a signed
     integer column position (e.g. `substring(name, pos_col, 2)`)
     admitted while an unsigned column position rejects as
     unsupported;
   - JSON numeric eligibility unit tests mirroring
     `TestFunctionalJSONTextEligibility`, keeping the numeric case
     separate from the text case and covering both bound cast forms
     (the flag-set overload-0 plain-integer spellings and the
     overload-1 SIGNED-family spellings, against the section-3
     matrix), plus corruption counterexamples
     (metadata tamper with the new expressions fails
     `validateFunctionalTable`);
   - persisted-spelling replay tests (the round-5 probe becomes the
     formal UT): for every spelling of the section-3 matrix, the
     lowered `OriginString` must re-bind to the authored hidden key
     type under the same binder call and re-admit under the
     eligibility walk, except that `AS SIGNED INTEGER` and
     `AS UNSIGNED INTEGER` persist canonically as `AS SIGNED`/
     `AS UNSIGNED` in the `OriginString` and
     `ConstructCreateTableSQL` output and re-bind `int64`/`uint64`;
     the round-trip assertion's own tests additionally pin the
     widening-class repair (`CAST(id AS SIGNED INTEGER)` authored
     over an int32 column persists canonically and re-binds as an
     `int64`-keyed widening) and the unchanged classes
     (`lower`/`upper`/integer arithmetic/`md5` adapter forms
     re-bind identically), so the fail-closed assertion cannot
     start rejecting expressions that were admitted before;
   - text-literal persistence tests (the round-6 probe becomes the
     formal UT): `concat(varchar_col, 'a\\b')` must admit with the
     derived `varchar(43)` hidden key (exactly one hidden owner
     column) and persist the width-preserving literal spelling —
     `concat(\`col\`, _utf8mb4 X'615c62')` in both `OriginString`
     and `ConstructCreateTableSQL` — which re-binds the authored key
     type exactly; the widthless spelling `cast(0x615c62 as varchar)`
     re-binds a TEXT result and must never be what new authoring
     persists (the counterexample regression control, verified
     pre-fix by the probe); the existing lower-backslash positive
     control (`TestFunctionalIndexOriginStringQuotesLiterals`,
     functional_index_test.go:374-415) stays admitted and
     replay-exact, with its persisted-string pin updated to the
     width-preserving spelling and the low-character exactness
     asserted against the base 65535 drift; backslash-free literal
     controls persist plain quoted spellings and re-bind exactly.
   - identifier-rewrite persistence tests (the round-7 probe becomes
     the formal UT): through the real
     `renameFunctionalColumnDependencies` /
     `rewriteFunctionalColumnDependency` path, an X-form persisted
     text must rewrite with only the identifiers changed and keep
     the width-preserving literal spelling, its replay re-binding
     the authored hidden key exactly; the regression control pins
     the base behavior the fix removes — the re-render reverting
     the X-form to the widthless spelling whose replay type is TEXT
     — and an old-form (widthless) text must rewrite with its
     spelling and binding class unchanged (the pre-rename and
     post-rewrite texts re-bind equal), a backslash-free literal
     rewrites exactly, and the rewritten tree's bound expression
     keeps the authored key type (the text, not the bound plan, is
     the artifact under protection); the same controls then run
     through the plan-level ALTER TABLE ... RENAME COLUMN and a
     renaming ALTER TABLE ... CHANGE COLUMN build, and each
     rewritten table replays once more (`ConstructCreateTableSQL`
     re-parse -> create -> `validateFunctionalTable`) with identical
     admission verdict, key OID, width, scale and literal values.
2. BVT (`test/distributed/cases/ddl/`): extend the functional-index
   cases and the JSON-key companion with the admitted expressions;
   verify DDL acceptance hidden-column counts, rejected DDL leaving
   zero hidden columns, insert/update/rollback index-versus-scan row
   agreement (`force index` vs `ignore index`), NULL and edge payload
   behavior for the numeric JSON case, and a session-scoped
   `PAD_CHAR_TO_FULL_LENGTH` force/ignore-index agreement control
   over `md5` with both a CHAR and a VARCHAR subject; then replay
   controls with values beyond the 32-bit limits — author
   `CAST(JSON_EXTRACT(doc,'$.a') AS SIGNED INTEGER)` and
   `... AS UNSIGNED INTEGER` indexes, insert 2147483648 and 4294967296,
   and assert the LIKE clones and COPY-replayed tables keep reading
   the same rows under both `force index` and `ignore index`, while
   an `AS INT` index and its replayed copy raise the identical range
   error for 2147483648; pin `SHOW CREATE TABLE` displaying the
   canonical `AS SIGNED`/`AS UNSIGNED` spelling for the mixed-spelling
   indexes and the verbatim spelling for every other admitted target;
   add a concat-with-backslash literal index (`concat(varchar_col,
   'a\\b')`, key `varchar(43)`) and assert its LIKE clones and
   COPY-replayed tables keep the same key domain, width and literal
   values — rows agree under both `force index` and `ignore index`,
   and `SHOW CREATE TABLE` pins the width-preserving literal
   spelling;
   add RENAME/CHANGE replay controls for that index: rename the
   source varchar column via `ALTER TABLE ... RENAME COLUMN` and via
   a renaming `ALTER TABLE ... CHANGE COLUMN`, then replay each
   renamed table (LIKE clone and a COPY-triggering ALTER) and assert
   the replayed tables keep the same key domain, width and literal
   values — rows agree under both `force index` and `ignore index`,
   and `SHOW CREATE TABLE` after the rename pins the preserved
   width-preserving literal spelling (the reviewer-requested
   rename-then-replay control);
   and update the `functional_json_key.result` message pins.

## Drawbacks and accepted tradeoffs

- The allowlist remains explicitly enumerated; customers wanting other
  approved-pure functions (`sha1`, `locate`, ...) still receive a
  rejection until a follow-up widens the set with the same audit bar.

## Open decisions

None blocking. Follow-up candidate (owner: this code path, decide when
requested): extend to more `provenPureValueFunctions` entries or mixed
`concat` domains, each with the same per-function safety accounting.

## Decision log

- r1 (2026-11-10): initial design.
- r2 (2026-11-10): resolved four P2 design-review findings.
  1. The session-dependent diagnostic category is now defined by
     demonstrated conversion semantics (timestamp-involving casts
     reading the session time zone; string-to-float casts reading the
     session native-compatibility mode) instead of the raw
     `IsDateRelate()` family, so deterministic temporal conversions
     such as DATE-to-CHAR report the unsupported message. Paired
     DATE-to-CHAR / TIMESTAMP-to-CHAR diagnostic tests are planned.
  2. Admission result predicates are stated per function:
     text results for the text functions, integer results for
     `crc32` (uint64 key) and `length` (int64 key), removing the
     contradiction that would have excluded two of the requested
     functions; hidden-key type assertions kept per function in the
     validation plan.
  3. The MEDIUMINT CAST spelling is removed from the advertised
     surface: it is rejected before eligibility by the pre-existing
     MySQL-parity boundary (ErrNYI in `base_binder.go`, pinned by
     `TestBuildCastToMediumIntIsRejected`); reachable integer targets
     are TINYINT/SMALLINT/INT/BIGINT and unsigned forms, and the
     upstream rejection is planned as a boundary test.
  4. Position/column boundary decided: integer literals and signed
     integer columns admitted (same-signedness widening), unsigned
     position/count columns rejected fail-closed with no new
     conversion rule; signed/unsigned column controls planned.
- r3 (2026-11-10): resolved the r2 review's MD5/CHAR finding
  (verified against base before revising; details in section 1 and
  the byte-stability section).
  1. The binder's implicit CHAR-to-VARCHAR adapter now has a concrete
     admission rule — implicit NewCast node with the explicit-cast
     flag clear, source `T_char`, target the fixed matcher's
     `T_varchar`/`MaxVarcharLen` envelope with the source's charset
     and collation version retained, a pure non-truncating widening —
     audited at `list_builtIn.go:9518-9556` (md5's varchar/text/blob
     overloads, no CHAR), `json_string_consumers.go:72-88` and
     `type_check.go:457-504, 3270-3310` (matcher and cast-cost
     selection), the binder insert site (`appendCastBeforeExpr`,
     `base_binder.go`), and `bytes.go` width constants
     (`MaxCharLen=255` < `MaxVarcharLen=MaxStringSize`). The other
     seven functions were re-verified to bind CHAR subjects natively,
     so md5 is the rule's only consumer today; user explicit casts
     stay fail-closed.
  2. A match-time byte-stability rule now gates the physical
     hidden-column equality: integer or VARCHAR hidden key and no
     `T_char` column reference in the persisted expression. The
     padding audit behind it (adapter representation-preserving;
     `PAD_CHAR_TO_FULL_LENGTH` pads at scan time per reader session
     against write-time-materialized keys) shows the added-equality
     divergence was latent for the existing lower/upper-over-CHAR
     class on base; that unsound rewrite is now withheld
     (result sets unchanged; residual owns correctness), and the
     JSON CHAR-key residual-only pin becomes a stated family rule
     rather than an incidental shape mismatch.
  3. Validation plan gains md5 CHAR/VARCHAR admission and replay
     controls, the explicit-cast negative control, residual-only and
     physical-equality plan-shape assertions, and a
     `PAD_CHAR_TO_FULL_LENGTH` force/ignore agreement BVT control;
     the public contract is reconciled with the adapter and
     byte-stability boundary.
- r4 (2026-11-10): resolved the r3 review's numeric JSON bound-form
  finding (verified against base code and a focused probe before
  revising; details in section 3).
  1. The r3 wording reserved the numeric JSON case for the
     dedicated explicit-cast overload, but the binder emits that form
     only for `useExplicitCastOverload`-selected targets
     (base_binder.go:466-489): the issue's exact `AS INT` spelling
     parses to `MYSQL_TYPE_LONG` (mysql_sql.y:15821-15831) and binds
     as the ordinary CAST overload 0 with `SyntaxExplicitCast` set
     (base_binder.go:336-339 -> appendSyntaxExplicitCastBeforeExpr,
     12561-12577), so r3 would have excluded the issue's spelling.
     The r4 gate admits both bound forms (flag-set overload 0 or
     overload 1) with identity, argument and target-metadata
     validation, and never a flag-clear overload-0 node (disjoint from
     section 1's implicit adapter; the flag is serialized plan metadata
     already consulted at base_binder.go:13761 and
     bound_expression_compare.go:68).
  2. Per-spelling bound-form matrix captured by the round-4 probe
     (probe-cast-forms-run.log, exit 0, all spellings PASS): INT/
     INTEGER/TINYINT/SMALLINT/BIGINT -> overload 0 + flag (keys
     int32/int32/int8/int16/int64); SIGNED/SIGNED INTEGER/UNSIGNED/
     UNSIGNED INTEGER -> overload 1 (keys int64/int64/uint64/uint64);
     MEDIUMINT -> bind-time ErrNYI reconfirmed; the text-case CHAR/
     VARCHAR controls bind overload 1 exactly as the existing helper
     requires. `mysql_cast_type` has no width-specific unsigned
     spellings, so the advertised unsigned target is only the uint64
     of the SIGNED-family spellings — the public contract is
     corrected accordingly.
  3. Validation plan gains the paired `AS INT`/`AS SIGNED` admission
     and replay controls, advertised-family coverage per bound form,
     and both-form equality-match plan tests; the correction stays
     local to functional-index eligibility.
- r5 (2026-11-10): resolved the two P2 findings of the r4 design
  review (both verified against base code and a focused replay probe
  before revising; details in section 3).
  1. Replay losslessness. The persisted `OriginString` is the sole
     source for show-create, LIKE-clone construction, COPY-replay and
     hidden-expression reconstruction (functional_index.go:260-262;
     build_show_util.go:459-465; ddl_expression_replay.go:122-125),
     and the LONGLONG-family mixed spellings render through
     `InternalType.Format` to family strings that re-bind 32-bit: the
     round-5 probe (probe-cast-replay-run.log, exit 0, all spellings
     PASS) proved `AS SIGNED INTEGER` renders `as integer` and
     re-binds `int32` against the authored `int64`, and
     `AS UNSIGNED INTEGER` renders `as integer unsigned` and re-binds
     `uint32` against the authored `uint64`, while every other
     spelling — including all width-named unsigned spellings —
     renders its own target back. r4's "rebinds the identical bound
     form per spelling" claim was therefore unsound. Resolution:
     persisted-spelling canonicalization (the two lossy spellings
     persist as `AS SIGNED`/`AS UNSIGNED` with the admitted bound
     plan unchanged) plus a lowering-time round-trip assertion
     (re-bind the rendered text; the same admission outcome and
     hidden key type, else the DDL fails closed), with type- and
     value-preservation controls beyond the 32-bit limits through
     OriginString, ConstructCreateTableSQL, LIKE and COPY replay
     added to the validation plan, and invariant 6 recording the
     replay-losslessness contract.
  2. CAST target matrix. CAST parses its target through `mo_cast_type`
     (mysql_sql.y:13429), whose `column_type` arm carries the
     `unsigned_opt`/`zero_fill_opt` productions (mysql_sql.y:13928-
     13929, 15658-15663), not the CONVERT-only `mysql_cast_type`
     grammar r4 audited (mysql_sql.y:13448). The width-named unsigned
     spellings (uint8/uint16/uint32/uint64 targets via
     build_util.go:259-288) and the numeric aliases are reachable,
     and `AS LONG` is not (parse error; a CONVERT-grammar alias). The
     section-3 matrix, the documented boundary and the public
     contract are re-derived from the actual grammar: the signed and
     unsigned width families and the SIGNED/UNSIGNED family spellings
     admit (the two mixed family spellings under the canonical
     persisted spelling), `MEDIUMINT`/`INT3` keep the upstream
     bind-time ErrNYI, and the family/alias coverage goes to the
     validation plan. The same canonicalization also repairs the
     pre-existing widening class, whose `CAST(col AS SIGNED
     INTEGER)` spellings are admitted today by the walk
     (functional_index.go:150-169) and carry the identical rendered
     narrowing the probe measured.
- r6 (2026-11-10): resolved the single P2 finding of the r5 design
  review (verified against base code and an executed focused probe
  before revising; details in the section-3 text-literal persistence
  paragraphs).
  1. The r5 tolerance for the widthless mode-independent literal
     spelling was unsound: the persisted `concat(\`name\`,
     cast(0x615c62 as varchar))` re-binds its cast target at
     `MaxVarcharLen` (tree/constant.go:201-203; build_util.go:347-
     353), concat sums argument widths (list_builtIn.go:151-154 with
     string_result_domain.go:444-452) and crosses the VARCHAR domain
     (string_result_domain.go:388-393), so the reviewed design's
     promoted `concat(name, 'a\\b')` example would replay as a
     TEXT-keyed hidden expression — neither admissible in replay nor
     acceptable to the r5 guard. The round-6 probe pinned the
     counterexample before repair (probe-literal-replay-run.log,
     exit 0: authored id varchar width 43; widthless re-bind id
     text; X-form re-bind id varchar width 43; lower control widths
     3 / 65535 / 3 with the literal value preserved; backslash-free
     control exact).
  2. Resolution: the functional-expression persistence boundary
     renders backslash-containing text literals in the
     width-preserving, mode-independent hex-introducer spelling
     (`_utf8mb4 X'...'`) — the branch tree/constant.go:195-199
     already provides for view definitions, keeping a text literal's
     value and inferred character width with no CAST default width —
     and the round-trip assertion's key comparison is defined
     exactly (OID, width, scale; no tolerance). The widthless
     binary-cast spelling remains only as the old-text compatibility
     form, the lower-backslash positive control
     (functional_index_test.go:374-415) stays positive with its
     persisted-string pin updated, and the validation plan gains
     the concat-with-backslash admission, OriginString/
     ConstructCreateTableSQL, and LIKE/COPY replay-value controls
     asserting the same key domain, width and values.
- r7 (2026-11-10): resolved the single P2 finding of the r6 design
  review (verified against base code and an executed focused probe on
  the real rewrite code path before revising; details in the
  section-3 persistence-across-identifier-rewriting paragraphs).
  1. The r6 render-once exemption was false: RENAME COLUMN and the
     renaming half of CHANGE COLUMN rebuild the persisted text —
     `renameFunctionalColumnDependencies`
     (functional_index.go:336-363, from build_alter_rename_column.go:96
     and build_alter_change_column.go:71) re-parses `OriginString`,
     rewrites identifiers and re-formats the rebuilt tree through
     `rewriteFunctionalColumnDependency` (functional_index.go:366-392)
     with the widthless literal options; the only other code path
     that renders persisted functional-expression SQL besides the
     lowering render (functional_index.go:262) — all other surfaces
     re-emit the stored text. The round-7 probe
     (probe-rename-replay-run.log, exit 0) pinned the counterexample
     through the real rewrite function: the X-form text
     `concat(\`name\`, _utf8mb4 X'615c62')` comes back as
     `concat(\`renameto\`, cast(0x615c62 as varchar))` while the
     bound expression stored beside it keeps the authored
     `varchar(43)` key — the divergence is silent until the next
     replay, when the COPY temporary table (built from the stored
     text at build_alter_table.go:608-616, executed at
     compile/alter.go:1639-1646, hidden columns rebuilt from the
     text per ddl_expression_replay.go:122-125) re-binds the
     degraded expression as a TEXT key the text-result gate rejects.
  2. Resolution: the width-preserving literal serialization is
     contracted to every producer — the lowering render and the
     identifier-rewrite render share one rendering entry point, so
     the X-form persists its spelling class through parsing and
     identifier rewriting and re-binds the authored key type exactly
     (probe case B), a round-trip assertion at the rewrite seam
     fails the ALTER closed unless the rewritten text re-admits and
     re-binds the rewritten tree's admitted key type (same OID,
     width and scale), older binaries' texts keep their spelling and
     binding class through the rewrite (probe case C) so supported
     renames of pre-feature tables behave exactly as on base, and
     the validation plan gains the identifier-rewrite UT bullet and
     the reviewer-requested rename-then-replay BVT controls over
     RENAME COLUMN and renaming CHANGE COLUMN (replay after rename
     asserting unchanged literal values and key OID, width and
     scale).
- r7 errata (2026-11-10, implementation turn): the r7 design review
  approved this revision and retained two P3 findings whose corrections
  are executed here before implementation; they correct wording — the
  admitted code behavior is unchanged from the reviewed contract.
  1. P3 temporal parsing classification: string-to-temporal parsing
     conversions read session state (the zero-date SQL-mode policy at
     func_cast.go:9867-9879, mode-dependent controls at
     func_cast_zero_temporal_test.go:161-203; `strToDatetime` reads
     `TIME_TRUNCATE_FRACTIONAL` at func_cast.go:10075 and applies it
     at func_cast.go:10109-10140), so they are moved into the
     session-dependent diagnostic category (audit item (c) above),
     while DATE-to-CHAR remains the deterministic-unsupported control.
     The boundary and validation-plan sections carry the correction.
  2. P3 explicit-cast exclusion wording: "user explicit varchar casts
     stay rejected" was too broad. The walk's identity arm
     (functional_index.go:165-179 on base) admits value-identical
     casts regardless of explicit syntax, and no later guard imposes a
     blanket explicit-VARCHAR exclusion. The exclusion is narrowed to
     casts outside the retained admission rules (explicit
     CHAR-to-VARCHAR stays the negative control), the identity-VARCHAR
     positive control is added to the validation plan, and the
     documented boundary carries the corrected wording.
  3. Registry re-verification return-width premise (implementation
     turn discovery, probe-verified before the correction): the
     scalar-verification text stating the bound node must re-resolve
     "to the same encoded overload ID and return type it was bound
     with" is wrong for the two value-derived-width functions. The
     registry re-resolves `left(varchar(40), bigint)` to VARCHAR(40)
     while the bind-time derivation reads the constant values and
     produces VARCHAR(4) for `left(name,4)` (and VARCHAR(8)/VARCHAR(2)
     for the three-argument substring spellings) — measured on the
     assigned base with the exact authoring binder
     (`bindPersistedExpr`+`GetFunctionByNameWithOverload`: encoded
     overload and implicit-cast expectations re-resolve exactly; only
     the width is refined). The verifier for the eight scalar
     functions therefore keeps exact same-OID, same-scale return
     comparison but permits a bound width that narrows the
     registry's type-level envelope; the refined width is a pure
     function of the persisted text, re-derives identically at every
     replay and is pinned exactly by the authoring and
     rewrite-round-trip assertions, so admission safety is unchanged.
     Every other verification caller (JSON text/numeric casts,
     json_extract/json_unquote) keeps exact identity. This keeps the
     reviewed admission set — `substring`/`left` with integer
     literals are required admissions — executable as designed.

  4. Match-seam candidate comparison-widening (implementation turn
     discovery, probe-verified before the correction): equality against a
     literal binds the literal INTEGER-family constant at BIGINT, so a
     query `cast(json_extract(doc,'$.a') as int) = 1` compares on the
     BIGINT common type — the equality binder wraps the (already bound)
     INT32 cast in a second, flag-clear overload-0 CAST to BIGINT, and the
     structural match of the byte-stability seam then sees the wrapper
     instead of the persisted INT32 expression (dumped on the assigned
     base; the INT64 `AS SIGNED` and pre-existing int64 arithmetic keys
     never widen and match unchanged). The wrapper is value-exact —
     every source value injects into the wider comparison type — so the
     candidate unwrapping that already strips the CHAR-to-VARCHAR
     comparison wrapper (functionalJSONComparisonOperand) also strips
     the flag-clear integer widening: same-signedness widening and
     unsigned-into-wider-signed only, with a user-written explicit
     widening cast keeping its syntax flag and never matching the arm.
     The hidden-key domain never changes: the added equality binds the
     same persisted hidden column, the base-row predicate stays, and
     narrow-integer-keyed indexes behave like their INT64 siblings at
     match time as the byte-stability rule already classifies them.

  5. Compact diagnostic argument-list rendering (challenge-repair
     round 3): the r7 error model requires a category-3 message to
     name the authored expression, and the retained challenge oracle
     matches the exact authored spelling (case_rejections.py requires
     the raw token `CONCAT(v,1)` inside the client error text). The
     tree package's standard rendering joins expression-list items
     with ", ", so multi-argument calls were reported with a spelling
     that differs at presentation whitespace only (`CONCAT(v, 1)`;
     json_extract lists and cast-of-multi-argument-call spellings
     likewise). The correction adds an opt-in format option that
     joins expression-list items with "," and applies it only at the
     two diagnostic producers (functionalIndexErrorExprText, the
     bind-stage mapping and the eligibility-walk site). Persisted
     SQL rendering is unchanged and keeps ", " because that output is
     re-parsed and compared canonically; default tree formatting is
     unchanged for every other consumer. No rejection category,
     admission rule or metadata behavior changes - any expression
     category-rejected the r7 way is still rejected the same way,
     and the category-2 session-independence and generated-column
     volatility messages never carried expression text and are
     untouched.

  6. mo-bot implementation-review repairs (PR #32510 review at merge
     head 3e8c72bb; each correction is probe-verified against the
     merged binder before implementation and completes a contract the
     reviewed document already states):

     a. Signed-literal position/count admission. The admission table
        requires `substring`/`left` integer-literal position/count
        arguments, but the persisted bind renders an authored negative
        constant as `unary_minus(<literal>)` (query-side constant
        folding does not apply to persisted SQL), so
        `substring(v,-2,2)` and `left(v,-1)` failed the recursive
        walk while equivalent signed-column positions admitted. The
        walk now admits exactly that constant shape — one non-null
        signed-integer literal argument under the identical
        signed-integer result type — and the structural comparison
        copy folds the same spelling into the query-side folded
        literal so suffix-position indexes match their own lookups.
        Boundary retained: negated columns (signed or unsigned),
        non-integer negations and every other unary-minus form keep
        failing closed, so the unsigned-position-column boundary and
        the persistence round-trip assertions are unchanged.

     b. Query-only prepared-numeric provenance. The early-runtime-filter-
        domain machinery merged into this base annotates query-bound
        `substring`/`left` results with
        `PreparedNumericMetadata.StringDomainSource`; the DDL-side
        bind never carries the witness and no executor evaluates it,
        yet the structural `proto.Equal` compared the pointer, so
        ordinary literal-equality lookups lost the physical plan for
        admitted `substring(name,1,8)`/`left(name,4)` indexes.
        `normalizeFunctionalExpression` now clears the provenance
        pointer on both comparison copies exactly as it already zeroes
        AuxId/Ndv/Selectivity; value-bearing tree shape and type
        metadata stay pinned by the eligibility walk and the pending
        equality, and the base-row residual is untouched.

     c. Integer-key DECIMAL comparison promotions. Equality against a
        decimal-literal common type wraps an integer key in the same
        flag-clear overload-0 CAST the integer-widening rule sees
        (probe: `crc32(name)=123` and
        `cast(json_extract(doc,'$.a') as unsigned)=4294967296` wrap
        in `CAST(... AS DECIMAL128(38,0))`, and `... as int)=1.5`
        in `DECIMAL128(11,1)`), so uint64-keyed indexes stayed
        unmatched for ordinary numeric-literal lookups. The comparison
        operand unwrap strips this promotion only when every source
        value injects exactly — the decimal's integer-digit capacity
        (width minus scale) covers the source type's widest value —
        mirroring the value-exactness rule of erratum 4. An
        insufficient-capacity adapter stays unstripped, a user-explicit
        cast keeps its syntax flag and never matches, and the added
        hidden-key equality re-binds into the same promotion, so the
        base-row residual still owns correctness.

These errata do not change any admission rule, inode gate,
persistence producer or error surface beyond the reviewed contract;
the third and fourth correct verifier wording so the reviewed
admission set binds and matches as required, the fifth aligns
category-3 diagnostic rendering with the authored-spelling clause
of the same reviewed error model without touching persistence
rendering, and the sixth keeps the reviewed admission and matching
set executable on the merged base: it admits the already-required
literal spellings, restores the stated purely-structural match
against planner-only provenance, and extends the match-seam
value-exact wrapper strip of erratum 4 to the integer-to-DECIMAL
promotion with the same fail-closed guards.
