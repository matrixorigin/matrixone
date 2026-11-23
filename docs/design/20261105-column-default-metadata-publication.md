# Column default metadata: typed COLUMN_DEFAULT rendering and the DEFAULT_GENERATED attribute

Status: revision 6 (2026-11-06) - revised after design-review
REQUEST_CHANGES on revision 5; resubmitted for design-first review.
Revision 6 dispositions one verified blocker (see the revision history):
bare variable-reference defaults (`DEFAULT @@time_zone`) escaped
expression classification while the parenthesized spelling of the same
executable default marked.
Owning issue: matrixorigin/matrixone#31161 (carrying #31159).
Publication, recognition and upgrade machinery:
[20261026-declared-catalog-metadata-publication-and-repair.md](20261026-declared-catalog-metadata-publication-and-repair.md)
(the reviewed publication model, section 5.1/5.5/6) and
[20261028-catalog-upgrade-407.md](20261028-catalog-upgrade-407.md) (the
unreleased consolidated 4.0.7 fold-in target) govern every persisted-identity
aspect of this change; this document adds only the default-metadata decisions
those documents do not cover.

## 1. Classification and gate record

The complete change crosses complexity triggers of
`.agents/skills/mo-dev/references/feature-design-review.md`:

1. It affects upgrade, migration, restart, backup and restore of persisted
   state: the per-account persisted information_schema.COLUMNS current
   identity changes (a new EXTRA arm), so the pre-change current becomes a
   historical published generation that must keep binding in snapshot readers
   while live definitions converge through the consolidated 4.0.7 COLUMNS
   entry. The shutdown-only contract of
   `.agents/skills/mo-dev/references/upgrade-compatibility.md` governs that
   surface.
2. It changes a client-visible SQL semantics family
   (information_schema.COLUMNS.COLUMN_DEFAULT/EXTRA and SHOW COLUMNS
   Default/Extra, per MySQL 8.3).

The default size trigger is not met (three owning production packages:
`pkg/sql/plan/function`, `pkg/util/sysview`, `pkg/sql/plan`; well under 500
non-generated production lines). The change is not high-risk architecture: it
introduces no persistent state, no distributed protocol, no global or shared
write path and no coordinated production change across subsystem ownership
boundaries; it rides the existing builtin, persisted-view publication and
upgrade-entry machinery only.

## 2. Problem, evidence, and invariants

### 2.1 Observed problem (recorded evidence)

Two public metadata defects on an otherwise correctly executed column
default, reproduced on the assigned base (`692c8e17a6`, fast-forwarded
origin/main; the reporting base `5fe08a3307e` shows identical code on both
owner paths):

- Defect A (typed COLUMN_DEFAULT). `DECIMAL(10,4) DEFAULT 1.2` is reported
  as `1.2` and `BIT(8) DEFAULT b'00110011'` as `0b00110011` by
  information_schema.COLUMNS.COLUMN_DEFAULT and SHOW COLUMNS. MySQL 8.3
  reports `1.2000` and `b'110011'` (issue evidence, 3/3 fresh databases).
  The inserted values are already correct (`1.2000`, `51`): the defect is
  rendering, not execution. Root cause:
  `columnDefaultValue` (pkg/sql/plan/function/func_builtin.go) receives only
  `plan.Default.OriginString` and returns every non-string default as its raw
  origin text; the declared type and folded value in `plan.Default.Expr` are
  never consulted.
- Defect B (expression-default EXTRA). `TIMESTAMP(6) NULL DEFAULT
  CURRENT_TIMESTAMP(6)` reports `EXTRA=''` in both surfaces; MySQL 8.3
  reports `DEFAULT_GENERATED` (issue #31159 evidence, 3/3). Root cause: the
  EXTRA case in the persisted current COLUMNS template
  (`cast(case when mc.att_is_auto_increment = 1 ... else '' end as
  varchar(24))`) and the four SHOW COLUMNS templates in
  pkg/sql/plan/build_show.go inspect auto-increment and generated columns
  only; the stored default expression is never classified.

Recorded reproduction of this turn (commands and outputs in the handler
report's artifact log): both defects reproduced live on the assigned base
through a fresh isolated LOG/TN/CN cluster; literal controls
(`INT PRIMARY KEY`, `VARCHAR(10) DEFAULT 'x'`, `INT DEFAULT NULL`,
`MEDIUMINT DEFAULT 7`, `ENUM('a','b') DEFAULT 'b'`, `DATE DEFAULT
'2024-01-01'`) show empty EXTRA and unchanged literal defaults; the unhappy
path (invalid DECIMAL insert) is rejected with the row count and existing
values preserved.

The persisted shape table behind both fixes (probe
`TestProbeIssue31161DefaultShapes`, run on the assigned base, log retained
with the handler evidence): every default persists as `plan.Default{Expr,
OriginString}` where Expr is the constant-folded, assignment-cast expression
and OriginString the formatted original spelling. Relevant shapes:

| DDL default | OriginString | folded Expr root |
|---|---|---|
| `decimal(10,4) default 1.2` / `1.200` / `'1.2'` | `1.2` / `1.200` / `'1.2'` | LIT decimal64(10,4) = 12000 |
| `decimal(10,0) default 1.6` | `1.6` | LIT decimal64(10,0) = 2 (rounded at DDL) |
| `decimal(20,6) default -12.34` | `(-12.34)` | LIT decimal128(20,6) = -12.340000 |
| `bit(8) default b'00110011'` / `51` | `0b00110011` / `51` | LIT bit(8) = 51 |
| `bit(1) default b'0'`, `bit(64) default b'1'` | `0b0` / `0b1` | LIT bit = 0 / 1 |
| `timestamp(6) default current_timestamp(6)` | `current_timestamp(6)` | cast_strict(current_timestamp(6)) |
| `timestamp default current_timestamp on update ...` | `current_timestamp()` | current_timestamp() |
| `char(36) default (uuid())` | `(uuid())` | cast_strict(uuid()) |
| `json default (json_array())` | `(json_array())` | json_array() |
| `int default (1+2)` | `(1 + 2)` | LIT int32 = 3 (folded) |
| `mediumint default 7` | `7` | cast_assign(LIT 7) |
| `enum('a','b') default 'b'` | `'b'` | cast_value_to_index(LIT, LIT) |
| `int default '7'` / `7` / `0x1F` / `null` | `'7'` / `7` / `0x1f` / `null` | LIT int32 (null: isnull LIT) |
| `varchar(10) default 'x'` / `('x')` | `'x'` / `('x')` | LIT varchar |
| `date default '2024-01-01'`, `float/double default 1.50` | quoted / `1.50` | LIT date / LIT float|double |

Revision 2 extended the same probe (same base, log retained with the
handler evidence) over the review-disputed shapes:

| DDL default | OriginString | folded Expr root |
|---|---|---|
| `decimal(10,4) unsigned default 1.2` | `1.2` | cast_assign(10,4)\[unsigned_decimal_admission\[cast(LIT varchar `1.2` -> decimal64(2,1)), CARRIER, LIT 'd'\]\] |
| `decimal(10,4) unsigned default '1.2'` | `'1.2'` | cast_assign(10,4)\[unsigned_decimal_admission\[LIT varchar `1.2`, CARRIER, LIT 'd'\]\] |
| `decimal(10,4) unsigned zerofill default 1.2` and `decimal(10,4) zerofill default 1.2` | `1.2` | identical composition (authoring normalization sets the UNSIGNED bit for any nonzero attribute mask) |
| `decimal(10,0) unsigned default 1.6` | `1.6` | same composition; the source keeps `1.6` unrounded because the volatile admission blocks folding (execution rounds to 2) |
| `decimal(10,4) unsigned default (1+2)` | `(1 + 2)` | cast_assign(10,4)\[unsigned_decimal_admission\[F `+`(LIT 1, LIT 2), CARRIER, LIT 'd'\]\] (the volatile boundary also keeps `+` unfolded) |
| `decimal(10,4) unsigned default null` | `null` | isnull LIT decimal64(10,4); no admission (NULL-by-construction sources take the plain cast wrapper, byte-identical to unattributed) |
| `decimal(20,6) unsigned default -12.34` | - | DDL build error `Invalid default value for 'd'` (existing rejection, unchanged) |
| `decimal(40,4) default 1.2` / `decimal(40,0) default 1.6` | `1.2` / `1.6` | cast(LIT varchar spelling -> decimal256(40,s)); stays executable because Literal has no decimal256 scalar and the implicit assignment cast is not admitted for LiteralVec materialization |
| `decimal(40,4) unsigned default 1.2` | `1.2` | the admission composition with a decimal256(40,4) outer/carrier |
| `decimal(10,4) default (1+2)` | `(1 + 2)` | LIT decimal64(10,4) = 30000 (folded at the declared scale) |
| `decimal(40,4) default (1+2)` | `(1 + 2)` | cast(LIT int64 3 -> decimal256(40,4)) (`+` folded, cast executable) |

(`CARRIER` is the `Expr_T` target-type carrier whose type equals the outer
node's attributed type exactly; the two-argument admission form predating
the column-name argument also persists on older rows.)

Revision 3 extended the same probe a second time (same base, log retained
with the handler evidence) over the round-3 review-disputed shapes, plus a
cast-semantics probe pinning the execution conversion numbers:

| DDL default | OriginString | folded Expr root |
|---|---|---|
| `decimal(10,0) unsigned default '1.4999999999999999999'` | `'1.4999999999999999999'` | cast_assign(10,0)\[unsigned_decimal_admission\[LIT varchar `1.4999999999999999999`, CARRIER, LIT 'd'\]\] (quoted source retained unconverted) |
| `decimal(10,0) unsigned default 1.4999999999999999999` | `1.4999999999999999999` | cast_assign(10,0)\[unsigned_decimal_admission\[cast(LIT varchar -> decimal128(20,19)), CARRIER, LIT 'd'\]\] (19-digit literal binds to its literal shape decimal128(20,19)) |
| `decimal(20,6) unsigned default '1.4999999999999999999'` | `'1.4999999999999999999'` | same bare-varchar admission composition with a decimal128(20,6) outer/carrier |
| `decimal(10,4) unsigned default 1.2e0` | `1.2e0` | cast_assign(10,4)\[unsigned_decimal_admission\[LIT float64 1.2, CARRIER, LIT 'd'\]\] (scientific spelling binds a float64 literal: `isPlainDecimalLiteral` rejects `e`) |
| `decimal(10,4) unsigned default true` / `false` | `true` / `false` | the admission composition over LIT bool true / false |
| `decimal(40,4) unsigned default 1.2e0` | `1.2e0` | the float64-source composition with a decimal256(40,4) outer/carrier |
| `decimal(10,4) default 1.2e0` / `default true` | `1.2e0` / `true` | LIT decimal64(10,4) = 12000 / 10000 (unattributed defaults fold; the typed literal is already at the declared scale) |
| `decimal(10,4) unsigned default +1.2` | `(+1.2)` | cast_assign(10,4)\[unsigned_decimal_admission\[unary_plus\[cast(LIT varchar `1.2` -> decimal64(2,1))\], CARRIER, LIT 'd'\]\] (the builder-less DefaultBinder keeps `unary_plus`, and the volatile admission boundary blocks later folding) |
| `decimal(10,4) unsigned default -1.2` | - | DDL build error `Invalid default value for 'd'` (negative on UNSIGNED; existing rejection, unchanged) |
| `decimal(10,4) default +1.2` | `(+1.2)` | LIT decimal64(10,4) = 12000 (unattributed signed literals fold) |
| `decimal(10,4) unsigned default +1.2e0` | `(+1.2e0)` | the admission composition over unary_plus\[LIT float64 1.2\] |

Cast-semantics probe (`TestProbeIssue31161CastSemantics`, same base):
`parseDecimal64CastString('1.4999999999999999999', 10, 0)` = `1` while
`types.ParseDecimal64('1.4999999999999999999', 10, 0)` = `2`, both
succeeding: the assignment-cast string conversion quantizes the validated
token once at the destination scale (func_cast.go:9103-9132, "rounds a
validated token ONCE"), while `types.ParseDecimal64` pre-rounds at 18
significant digits (decimal.go:2143-2155) and rounds again when rescaling
(decimal.go:2217-2218) - a double rounding. The decimal128 pair agrees on
the same input (`parseDecimal128CastString(..., 20, 6)` =
`types.ParseDecimal128(..., 20, 6)` = `1.500000`) because 19 digits need no
pre-rounding at 38-digit precision, but only the cast helper is guaranteed
to match execution in general. Float conversion:
`types.Decimal64FromFloat64(1.2, 10, 4)` = `1.2000`,
`(1, 10, 4)` = `1.0000` (the TRUE case), `(1.25, 10, 1)` = `1.3` - the
executor's `floatToDecimal*` path (func_cast.go:4798-4821, :4852-4867).

Revision 4 extended the same probe a third time (same base, log retained
with the handler evidence) over the round-4 review-disputed shapes:

| DDL default | OriginString | folded Expr root |
|---|---|---|
| `decimal(10,4) unsigned default cast(1.6 as decimal(10,0))` | `cast(1.6 as decimal(10))` | cast_assign(10,4)\[unsigned_decimal_admission\[cast(cast(LIT varchar `1.6` -> decimal64(2,1)), CAST overload 1 -> (10,0)), CARRIER, LIT 'd'\]\] - the user cast SURVIVES under the volatile admission; `SyntaxExplicitCast` is NOT set because DECIMAL targets route through `appendTemporalIntegerCast` (base_binder.go:305-309, temporal_integer_cast.go:76-79), so the flag at base_binder.go:12254-12258 never fires for this family |
| `decimal(10,4) default cast(1.6 as decimal(10,0))` | `cast(1.6 as decimal(10))` | LIT decimal64(10,4) = 20000 (the user cast and assignment cast fold; 2.0000 is execution's own result) |
| `decimal(10,4) unsigned default (cast(1.6 as decimal(10,0)))` | `(cast(1.6 as decimal(10)))` | the same surviving admission composition as the unparenthesized spelling |
| `decimal(10,4) default abs(1.2)` | `abs(1.2)` | LIT decimal64(10,4) = 12000 (deterministic ABS constant-folds; the function node is erased) |
| `decimal(10,4) unsigned default abs(1.2)` | `abs(1.2)` | cast_assign(10,4)\[unsigned_decimal_admission\[abs(cast(LIT varchar `1.2` -> decimal64(2,1))), CARRIER, LIT 'd'\]\] - `abs` SURVIVES under the admission |
| `decimal(10,4) default (abs(1.2))` | `(abs(1.2))` | LIT decimal64(10,4) = 12000 (folded; parenthesized spelling) |
| `decimal(10,4) default round(1.25, 1)` / attributed | `round(1.25, 1)` | LIT decimal64(10,4) = 13000 / the admission composition over round(cast(...), LIT 1) (same fold/survive split as `abs`) |
| `varchar(40) default 'cast(1.6 as decimal(10,0))'` / `default 'abs(1.2)'` | `'cast(1.6 as decimal(10,0))'` / `'abs(1.2)'` | LIT varchar (quoted call-spelling strings are plain literals) |

Round-4 structural facts behind the fix: the persisted OriginString is
formatted from the user's tree expression (`originExpr := expr`,
build_util.go:980; `fmtCtx.PrintExpr(originExpr, ...)` at :1124-1129 with
`WithSingleQuoteString`), so user-written call syntax always appears there
- `cast(` (tree/expr.go:1483-1486), `abs(` (FuncExpr.Format,
tree/expr.go:983-1030) - while binder-generated casts never appear in the
origin at all. Constant folding erases function nodes from the persisted
Expr (utils.go:2155-2158) but cannot touch the origin. Plan-level cast
provenance is NOT a uniform discriminator: `SyntaxExplicitCast`
(plan.proto Function field 5) is set only for explicit casts whose target
lies outside the `useExplicitCastOverload` families - FLOAT/DOUBLE/
DECIMAL/VARCHAR/TEXT/BLOB/LONGLONG targets take the
`appendTemporalIntegerCast` path instead, which leaves the flag unset
(base_binder.go:305-309/:435-458/:12254-12258,
temporal_integer_cast.go:76-79) - and CAST overload 1 also serves internal
rewrites (integer_arguments.go:411). The origin scan in 4.3 is therefore
the single user-cast/folded-function discriminator; provenance fields
corroborate where present but are never required.

Revision 5 extended the same probe a fourth time (same base, log retained
with the handler evidence) over the round-5 review-disputed shapes,
including a multi-column harness mirroring the DDL pre-scan
(build_ddl.go:3778-3804) and per-column bind (build_ddl.go:4006):

| DDL default | OriginString | folded Expr root |
|---|---|---|
| `decimal(10,4) unsigned default b'00110011'` | `0b00110011` | cast_assign(10,4)\[unsigned_decimal_admission\[LIT varchar `3` with IsBin=true, LiteralForm=BIT, CARRIER, LIT 'd'\]\] - the decoded binary payload (one byte 0x33, displayed as `3`) SURVIVES unconverted under the volatile admission |
| `decimal(10,4) default b'00110011'` | `0b00110011` | LIT decimal64(10,4) = 51 (unattributed spellings fold with execution's own binary conversion embedded: 51 at scale 4 is `0.0051`) |
| `decimal(10,0) unsigned default b'00110011'` | `0b00110011` | the same IsBin admission composition at scale 0 (execution stores `51`) |
| `decimal(40,4) unsigned default b'00110011'` | `0b00110011` | the same IsBin composition with a decimal256(40,4) outer/carrier |
| `decimal(10,4) unsigned default 0x33` / `x'33'` | `0x33` / `0x33` | the same composition over LIT varchar `3` with IsBin=true, LiteralForm=HEX (the `x'33'` introducer spelling normalizes to the `0x33` origin and the identical HEX shape) |
| `(a int default 7, b int default a)` | b: `a` | b: COL{0.0} - a bare `Expr_Col`, no cast (same-typed reference retained by make.go:1040-1041); dependency validation accepts |
| `(a int default 7, b int default (a))` | b: `(a)` | b: COL{0.0} (identical shape; only the origin carries the parentheses) |
| `(a int default 7, b int default a + 1)` | b: `(a + 1)` | cast\[+(cast\[COL{0.0}\], LIT 1)\] (arithmetic over a reference) |
| `(a decimal(10,4) default 1.2, b decimal(10,4) default a)` | b: `a` | b: COL{0.0} (same-typed decimal reference stays bare) |
| `(a decimal(10,4) unsigned default 1.2, b decimal(10,4) unsigned default a)` | b: `a` | b: cast_assign(10,4)\[unsigned_decimal_admission\[COL{0.0}, CARRIER, LIT 'b'\]\] - the reference persists BENEATH the admission composition |
| `(a int default 7, b decimal(10,4) unsigned default a)` / `default (a)` | b: `a` / `(a)` | b: the same admission-over-COL composition (type-changing reference; the admission converts int to decimal at execution) |
| `(a varchar(10) default 'x', b varchar(10) default a)` | b: `a` | b: COL{0.0} |

Round-5 structural facts behind the fix: bit and hex literal spellings bind
through `returnRawLiteralExpr` (base_binder.go:12070-12087,
:11892-11901) to varchar constants carrying the DECODED byte payload with
`IsBin=true` and `LiteralForm` HEX/BIT. With the attributed-DECIMAL bind
type suppressed (build_util.go:1012-1016) that IsBin payload persists
unconverted inside the admission composition, and at execution the
admission dispatches it through `unsignedDecimalAdmissionBinary` ->
`renderUnsignedDecimalBinarySource` -> `types.ParseDecimal64/128/256FromByte`
(func_cast_unsigned_decimal.go:588-594, :397-418; decimal.go:2242-2259):
the admission's carrier is the declared column type, which never carries
the private charset-255 marker, so `mysqlPrefix` is false and every IsBin
payload takes the binary branch. The decoder reads the payload as a
big-endian base-256 COEFFICIENT at the destination scale (0x33 -> 51 ->
`0.0051` at scale 4, `51` at scale 0) - the binary-decimal contract, not a
text parse of the payload's display form (a text parser reads `3` and
produces `3.0000`; both conversions succeed, so no fallback fires).
Column-reference defaults: the DDL path binds every default against the
complete row schema (build_ddl.go:4006), `default_binder.go:64-90` resolves
an unqualified name to `Expr_Col`, same-typed references stay bare
(make.go:1040-1041) while type-changing references keep COL beneath the
assignment/admission wrappers; `exprHasLocalColumnRef` gates authoring on
the expression-default protocol (build_util.go:1031-1035, :1136-1146) and
`validateDefaultColumnDependencies` permits references to preceding
ordinary columns (build_util.go:1479-1544). The planner's OWN
classification already treats reference-bearing defaults as expression
defaults: `isExpressionDefault` returns true when
`collectRefColPos(def.Expr)` is non-empty (build_util.go:1554-1573) - the
revision-4 classifier's three rules contradict it for the bare-spelling
rows above (origin `a`, no function node, no parentheses, no call syntax).

Revision 6 extended the same probe a fifth time (same base, log retained
with the handler evidence) over the round-6 review-disputed shapes:

| DDL default | OriginString | folded Expr root |
|---|---|---|
| `varchar(64) default @@time_zone` | `@@time_zone` | cast_strict\[VAR{name=time_zone sys=true}, CARRIER\] - the variable SURVIVES unconverted beneath the transparent cast; no fold is possible |
| `varchar(64) default (@@time_zone)` | `(@@time_zone)` | the identical cast_strict-over-VAR tree; only the origin carries the parentheses |
| `varchar(64) default @@global.time_zone` | `@@global.time_zone` | the same tree with VAR{global=true} |
| `varchar(64) default @probe_user_var` / `default (@probe_user_var)` | `@probe_user_var` / `(@probe_user_var)` | the same cast_strict-over-VAR tree with VAR{sys=false} (user variable) |
| `int default @@sort_buffer_size` | `@@sort_buffer_size` | cast\[VAR{name=sort_buffer_size sys=true}, CARRIER\] |
| `decimal(10,4) unsigned default @@time_zone` / `default (@@time_zone)` | `@@time_zone` / `(@@time_zone)` | cast_assign(10,4)\[unsigned_decimal_admission\[VAR{name=time_zone sys=true}, CARRIER, LIT 'd'\]\] - the variable persists BENEATH the admission composition |
| `varchar(64) default '@@time_zone'` | `'@@time_zone'` | LIT varchar (a quoted variable spelling is a plain string literal) |

Round-6 structural facts behind the fix: the grammar accepts a variable
as a DEFAULT source (`DEFAULT bit_expr`, mysql_sql.y:12785;
`simple_expr: variable`, mysql_sql.y:13200; `variable:
system_variable | user_variable` at mysql_sql.y:2309-2317, both parsing
to `tree.VarExpr`). `baseBindVar` (base_binder.go:523-604) retains the
variable as an `Expr_V` node carrying name/system/global, and the default
fold runs with `varAndParamIsConst=false` (build_util.go:1066), so
`IsConstant` rejects it (`case *plan.Expr_P, *plan.Expr_V: return
allowParameters`, function/function.go:433-434) and the variable always
survives unconverted - beneath `cast_strict` for string destinations,
beneath `cast` for the integer destination, and beneath the
cast_assign-plus-admission composition for the attributed-DECIMAL
destination (all inside the 4.3 transparent wrapper set). The origin
formatter writes a system variable without parentheses or quotes
(`@@time_zone`, `@@global.time_zone`; tree/expr.go:1923-1936) and a user
variable as `@name`, so the bare spellings carry no function node, no
parentheses and no call syntax - the revision-5 rules (a)-(d) classify
them as literals while the parenthesized spellings mark via rule (b),
despite both persisting the same executable default. The planner's OWN
classification already treats both spellings as expression defaults: a
variable never folds to a literal root, so the third arm of
`isExpressionDefault` (any non-`Expr_Lit` root, build_util.go:1563-1571)
fires for every row above. At execution the VarRef resolves the session
or global value at insert time (long-standing expression capability), so
no protocol gate applies - `exprHasLocalColumnRef`
(build_util.go:2282-2303) matches only `Expr_Col` - and dependency
validation sees no column references (build_util.go:1479-1544).

### 2.2 Invariants

- I1 (typed literal rendering): for DECIMAL columns whose default
  classifies as a literal (I3's classifier runs first), COLUMN_DEFAULT/SHOW
  COLUMNS Default renders the value formatted at the declared scale
  (`1.2`/`1.200`/`'1.2'` all render `1.2000` for DECIMAL(10,4)), on every
  DECIMAL carrier (decimal64/128/256; declared precision 1..65), under
  every numeric-attribute composition (plain, UNSIGNED, ZEROFILL, UNSIGNED
  ZEROFILL), and from every literal source kind the binder retains inside
  the attributed admission composition (unquoted/quoted decimal spellings,
  scientific-notation float64 spellings such as `1.2e0`, boolean spellings
  `true`/`false`, bit/hex literal spellings such as `b'00110011'` /
  `0x33` / `x'33'`, and canonical signed literals such as `+1.2`). The
  rendered value is produced with the executor's own assignment-conversion
  semantics - one rounding at the destination scale, and the binary
  big-endian coefficient decode for IsBin payloads - so it always names
  the value an omitted-column insert actually stores (`1.6` under
  DECIMAL(10,0) renders the rounded `2`; the 19-digit string
  `'1.4999999999999999999'` under DECIMAL(10,0) renders `1`, the inserted
  value, never the double-rounded `2` a general-purpose parser produces;
  `1.2e0` renders `1.2000`; `true` renders `1.0000`; `b'00110011'` under
  DECIMAL(10,4) renders `0.0051` - coefficient 51 at scale 4 - never the
  `3.0000` a text parse of the payload's display form produces, and `51`
  under DECIMAL(10,0)). For BIT columns it
  renders the MySQL bit-literal form `b'<bits>'` with leading zeros
  stripped (`b'110011'`, zero renders `b'0'`), independent of the original
  SQL spelling. Expression defaults are excluded from typed rendering
  (I3a).
- I2 (string/NULL preservation): string defaults keep the existing
  scanner-decoded rendering (`'it''s'` -> `it's`), an explicit or implicit
  NULL default renders SQL NULL, and a column without a default renders SQL
  NULL. No control column's metadata changes.
- I3 (expression attribute): EXTRA/Extra shows `DEFAULT_GENERATED` exactly
  for expression defaults: function-bearing defaults
  (`CURRENT_TIMESTAMP(6)`, `(UUID())`, `(JSON_ARRAY())`, the implicit
  current-timestamp form), user-written explicit casts in any spelling
  (`CAST(1.6 AS DECIMAL(10,0))`, plain, attributed or parenthesized),
  deterministic function defaults whose nodes constant-fold away
  (`ABS(1.2)`, `ROUND(1.25, 1)` - the persisted origin keeps the
  unparenthesized function spelling), column-reference defaults in every
  accepted spelling (`DEFAULT a`, `DEFAULT (a)`, `DEFAULT a + 1`,
  including references persisted beneath the attributed-DECIMAL admission
  composition, matching the planner's own `isExpressionDefault`),
  variable-reference defaults in every accepted spelling
  (`DEFAULT @@time_zone`, `DEFAULT (@@time_zone)`, `DEFAULT @user_var`,
  including variables persisted beneath the attributed-DECIMAL admission
  composition, matching the planner's own `isExpressionDefault`),
  and parenthesized-origin defaults
  whose inner text is not a signed numeric literal (`(1+2)`, `('x')`).
  Literal defaults (numeric, string, NULL, BIT, DATE, ENUM, MEDIUMINT,
  negative decimals written without parentheses, canonical signed literals
  whose retained `unary_plus`/`unary_minus` nodes wrap only literal/cast
  machinery, string literals that merely spell a call (`'cast(...)'`,
  `'abs(1.2)'`), and attributed-DECIMAL literals whose persisted admission
  composition wraps only literal/cast/sign machinery) and default-less
  columns stay empty.
  The planner-owned `unsigned_decimal_admission*` entries and retained
  canonical-sign nodes are internal machinery, not user expressions: they
  never trigger the mark by themselves. Genuine arithmetic kept under the
  volatile admission boundary (`(1+2)` persists an unfolded `+`) still
  marks. Auto-increment and generated columns keep their existing arms and
  precedence.
- I3a (expression-text precedence): a default classified as an expression
  renders its persisted expression text in COLUMN_DEFAULT/Default
  (`(1 + 2)`, `current_timestamp(6)`), never a typed-normalized value; the
  I1 typed rendering applies only to literal-classified defaults. This
  matches the MySQL 8.3 rendering order (expression text is produced
  before the typed-value branch) and keeps `DECIMAL(10,4) DEFAULT (1+2)`
  from publishing `3.0000` next to `DEFAULT_GENERATED`.
- I4 (surface agreement): information_schema.COLUMNS and SHOW COLUMNS/DESC
  report the same Default and Extra for the same column, after CREATE,
  supported ALTER, clone and reconnect.
- I5 (execution untouched): inserted default values, invalid-INSERT
  rejection and existing-row preservation are byte-identical to base; this
  change reads catalog blobs and never writes row data.
- I6 (publication discipline): the pre-change current identity keeps binding
  byte-stably as a frozen historical generation at every recognition seam;
  live per-account definitions converge to the corrected identity through
  the existing consolidated 4.0.7 COLUMNS entry; fresh accounts publish the
  corrected identity. Historical snapshots keep their recorded text.

### 2.3 Scope boundary (non-goals)

- FLOAT/DOUBLE-column and hex-integer default rendering (`1.50`, `0x1f`):
  separate mismatches not reported in #31161; the typed-rendering hook
  generalizes but this change does not widen its oracle. (Float64/boolean
  literals carried inside DECIMAL defaults - scientific-notation and
  `true`/`false` spellings on a DECIMAL column - are DECIMAL defaults and
  are in scope; see 4.3/5.1. Bit/hex literal spellings carried inside
  DECIMAL defaults - `b'00110011'`, `0x33`, `x'33'` on a DECIMAL column -
  are likewise DECIMAL defaults and are in scope; the non-goal is only
  the rendering of such spellings on FLOAT/DOUBLE/integer columns.)
- `ON UPDATE CURRENT_TIMESTAMP` composition in EXTRA (`DEFAULT_GENERATED on
  update ...`): MO renders no on-update attribute in EXTRA today; unchanged.
- Generated-column semantics and binary charset metadata (#31160, delivered
  under the parent design).
- Explicitly parenthesized signed numeric literals (`DEFAULT (-12.34)`):
  MySQL treats the explicit-parentheses spelling as an expression default,
  but the persisted OriginString of an unparenthesized negative literal is
  already `(-12.34)` (the formatter wraps unary minus), so the two spellings
  are indistinguishable from catalog state. This change classifies
  parenthesized signed numerics as literals (section 4.3), matching the
  common unparenthesized form; the explicit-parentheses form keeps empty
  EXTRA (misclassification direction chosen by I3's "no false marks on
  literal defaults" priority).

## 3. Standards, precedent, and MatrixOne constraints

MySQL 8.3 is the reference oracle, as recorded in the issue:
`DECIMAL(10,4)` renders `1.2000`, `BIT(8)` renders `b'110011'`,
`CURRENT_TIMESTAMP(6)` carries `DEFAULT_GENERATED`, and plain
`INT PRIMARY KEY` / `VARCHAR DEFAULT 'x'` controls carry empty EXTRA. MySQL's
documented rules agree: a default is converted to the declared column type,
expression defaults are marked `DEFAULT_GENERATED` in
information_schema.COLUMNS.EXTRA and SHOW COLUMNS, any parenthesized default
is an expression default, and BLOB/TEXT defaults are only allowed in
expression form. The MySQL 8.3 server source confirms the rendering order
this change adopts (design-review citation): `dd_table.cc` produces the
expression text for expression defaults before its typed-value branch, and
`columns.cc` projects that result into information_schema.COLUMNS, so a
folded expression default such as `DEFAULT (1+2)` is published as
expression text with `DEFAULT_GENERATED`, never as a typed-normalized
value.

MatrixOne constraints: the folded `plan.Default` blob is the authoritative
persisted default record for both old and new rows (no data migration is
possible or needed); the persisted information_schema.COLUMNS definition is a
byte-versioned published identity whose historical generations must stay
recognized (parent design 5.5); catalog 4.0.7 is unreleased (latest release
tag v4.2.6 carries 4.0.6/5), so the supported delivery is folding the
corrected identity into the existing 4.0.7 COLUMNS entry
(20261028-catalog-upgrade-407.md: "Before release, fold the later COLUMNS ...
refreshes into the existing 4.0.7 entries").

## 4. Alternatives (decision log)

### 4.1 OriginString-only rendering and classification (rejected)

Rendering typed values from the origin text re-parses SQL spelling the
planner already resolved (signed/quoted/parenthesized variants) and cannot
produce the declared scale or bit form without re-deriving the column type;
classification from origin text alone cannot see folded functions reliably
and mistakes the formatter's unary-minus parentheses (`(-12.34)`) for
expression parentheses. The folded Expr is persisted precisely to avoid
this.

### 4.2 Persisted classification flag on plan.Default (rejected)

Adding an `is_expression` field changes the catalog blob format for a
metadata-rendering defect, forces format-version reasoning for rows written
by older binaries, and duplicates information derivable from the persisted
Expr and OriginString. The derivation is deterministic and total over
existing blobs, so no new persisted state is justified.

### 4.3 Classification and rendering from the folded plan.Default (selected)

Decode `plan.Default` once per rendered row (modes 1 and 4 already decode
it). The pipeline runs classification first and lets the classification
decide the rendering branch (I3a), matching the MySQL 8.3 order
(expression text before the typed-value branch).

Transparent wrapper set. Both phases recognize the same set of internal
wrappers that carry no user-expression semantics:

- the value-cast family by builtin name: `cast`, `cast_assign`,
  `cast_strict`, `cast_ignore`, `cast_value_to_index`. Every cast node
  this set peels is binder-generated (implicit, assignment or
  literal-shape): a user-written `CAST(...)` always carries its `cast(`
  spelling in the persisted origin, so classification rule (c) below marks
  it as an expression default before any peel runs (I3a), and the explicit
  cast's own conversion is never bypassed;
- the attributed-DECIMAL admission composition by typed shape, mirroring
  `matchUnsignedDecimalAssignmentComposition`
  (pkg/sql/plan/build_constraint_util.go): a cast-family outer node whose
  arg0 is an `unsigned_decimal_admission[_strict|_ignore]` node whose arg1
  is the `Expr_T` carrier for the identical attributed type (id, width,
  scale, numeric attributes all equal). The admission entries are volatile
  plan-owned domain-enforcement machinery persisted around attributed
  DECIMAL defaults (make.go's assignment-cast builder); treating them as
  user functions would falsely mark every attributed-DECIMAL literal
  default `DEFAULT_GENERATED` (review finding 1). Name-only transparency
  is not sufficient: the full composition shape is required so a stray or
  user-spelled admission-named node elsewhere still counts as a function.
- the canonical-sign chain by builtin name and shape: a `unary_plus` or
  `unary_minus` node whose own child chain contains only transparent
  wrappers around a core literal. The builder-less DefaultBinder returns
  `DEFAULT +1.2`'s `unary_plus` node unfolded (base_binder.go:1188-1206)
  and the volatile admission boundary blocks any later fold
  (utils.go:2026-2027), so attributed signed literals persist with the
  sign node retained (round-3 review finding 3; 2.1 probe rows
  `decimal(10,4) unsigned default +1.2` / `+1.2e0`). Sign transparency
  applies only when the whole child chain resolves to a literal: a sign
  over any genuine function or arithmetic node (`+(1+2)`, `-uuid()`)
  keeps its chain non-transparent and the default classifies as an
  expression. Extraction records and applies the sign.

Classification (D2): expression default iff (a) the Expr tree contains a
function node outside the transparent wrapper set, (b) the OriginString
is parenthesized and its inner text is not a signed numeric literal,
(c) the OriginString carries user-written call syntax: an identifier
immediately followed by `(`, scanned outside quoted spans, (d) the
Expr tree contains an `Expr_Col` row-reference node anywhere, including
beneath transparent wrappers, or (e) the Expr tree contains an `Expr_V`
variable-reference node anywhere, including beneath transparent
wrappers. Rule
(a) catches `current_timestamp`, `uuid`, `json_array` in every spelling
and the unfolded `+` persisted under the volatile admission boundary for
attributed `(1+2)`; rule (b) catches folded constant expressions (`(1+2)`,
plain or decimal256-carried) and parenthesized strings (`('x')`, the only
BLOB/TEXT default form); the signed-numeric exclusion (leading `+` or `-`)
keeps formatter-generated `(-12.34)` and retained-sign `(+1.2)` literal.
Rule (c) is the explicit-cast and folded-function net (round-4 review
findings 1 and 2): the persisted OriginString is formatted from the user's
tree expression (build_util.go:980, :1124-1129), so a user-written
`CAST(...)` or `ABS(...)` spelling always appears there - even when
constant folding erased the function node from the persisted Expr
(unattributed `DEFAULT ABS(1.2)` folds to a bare scale-4 literal;
unattributed `DEFAULT CAST(1.6 AS DECIMAL(10,0))` folds to the literal
2.0000; 2.1 round-4 probe rows) - while binder-generated casts never
appear in origin text at all. The scan skips single-quoted spans with
`''` doubling (the origin formatter's `WithSingleQuoteString` form) and,
defensively, double-quoted and backtick spans, so string literals that
merely spell a call (`'cast(1.6 as decimal(10,0))'`, `'abs(1.2)'`) stay
literals. Rule (c) marks the same defaults rules (a)/(b) already catch
(surviving `abs` under the admission; parenthesized `(cast(...))`), so
each spelling keeps exactly one classification. MO accepts unparenthesized
function and cast defaults that MySQL 8.3 rejects (MySQL requires
expression defaults to be parenthesized, except special-cased temporal
functions); MO classifies every accepted user-written call spelling as an
expression default, consistent with base behavior that always kept its
origin text and with MySQL's parenthesized-expression semantics.
Rule (d) is the column-reference net (round-5 review finding 2): the DDL
path binds defaults against the complete row schema and persists accepted
references as `Expr_Col` nodes - bare for same-typed references
(make.go:1040-1041), beneath the assignment/admission wrappers for
type-changing or attributed references (2.1 round-5 probe rows) - while
the origin of the bare spelling (`a`) carries no function, parentheses or
call syntax, so rules (a)-(c) all miss it. The planner's own
`isExpressionDefault` already classifies reference-bearing defaults as
expression defaults via `collectRefColPos` (build_util.go:1554-1573);
rule (d) mirrors that walk exactly (a pure tree traversal, no origin
dependence), so the metadata classifier cannot contradict the planner.
References inside genuine arithmetic (`a + 1`) were already marked by
rules (a)/(b); rule (d) marks the same defaults exactly once. Execution,
the authoring protocol gate (build_util.go:1031-1035) and the dependency
rules (build_util.go:1479-1544) are unchanged.
Rule (e) is the variable-reference net (round-6 review finding): the
grammar accepts a variable as a DEFAULT source (`DEFAULT bit_expr`,
mysql_sql.y:12785; `simple_expr: variable`, mysql_sql.y:13200; both
`@@system` and `@user` spellings parse to `tree.VarExpr`), the binder
retains it as an `Expr_V` node (base_binder.go:523-604), and constant
folding runs with `varAndParamIsConst=false` (build_util.go:1066;
`IsConstant` returns false for `Expr_V` at function/function.go:434), so
the variable survives unconverted - bare for same-typed destinations or
beneath the assignment/admission wrappers otherwise (2.1 round-6 probe
rows). The bare spelling's origin (`@@time_zone`, formatted without
parentheses or quotes, tree/expr.go:1923-1936) carries no function node,
no parentheses and no call syntax, so rules (a)-(d) all miss it while
`DEFAULT (@@time_zone)` already marks via rule (b) - despite both
spellings persisting the same executable default. The planner's own
`isExpressionDefault` already classifies both spellings as expression
defaults: a variable never folds to a literal root, so its third arm
(any non-`Expr_Lit` root, build_util.go:1563-1571) fires. Rule (e)
mirrors that classification with the same pure tree walk as rule (d)
(the two walks share one traversal in the builtin package; the import
ban stands), so metadata cannot contradict the planner. Rule (e) adds
no new acceptance: variable-reference defaults already bind and execute
at base (the VarRef resolves the session/global value at insert), the
authoring protocol gate and dependency validation inspect only column
references and are unchanged. MO's acceptance of variable-reference
defaults extends MO's accepts-more-than-MySQL contract: MySQL 8.3
prohibits variable references in default expressions even when
parenthesized (the default-value rules at
dev.mysql.com/doc/refman/8.0/en/data-type-defaults.html), so both the
bare and the parenthesized variable spellings are MO-only acceptance.
A variable spelling inside genuine arithmetic (`@@sort_buffer_size + 1`)
was already marked by rule (a); rule (e) marks the same defaults exactly
once.
`mediumint` (cast_assign-wrapped literal), `enum`
(cast_value_to_index-wrapped literals), canonical signed literals
(sign chain over literal/cast machinery) and attributed-DECIMAL literals
(admission composition over literal/cast/sign machinery) classify as
literals, matching MySQL.

Rendering (D1): NULL/empty origin returns SQL NULL first (existing order).
Expression-classified defaults keep the existing rendering of the
persisted OriginString (string-decode for strings, raw origin otherwise) -
base behavior, unchanged. Literal-classified defaults render typed: extract
the core literal by peeling the transparent wrapper set (recording the
canonical-sign chain's sign), then
- DECIMAL columns: reproduce the executor's assignment conversion of the
  extracted source to the declared attributed type (id, width W, scale S)
  and render `Format(S)`. The conversion dispatch mirrors the admission's
  own per-carrier routing (func_cast_unsigned_decimal.go:105-142) using
  the same pure helpers the assignment cast uses - all in the builtin
  package or `types`, no expression evaluation:
  - string/varchar source: reproduce the executor's binary-versus-text
    dispatch on the literal's IsBin provenance BEFORE converting
    (func_cast_unsigned_decimal.go:588-594): the admission's carrier is
    the declared column type, which never carries the private
    charset-255 marker, so an IsBin payload always converts through the
    binary branch at execution and the renderer reproduces exactly that
    branch. An IsBin payload (bit/hex literal spellings:
    `b'00110011'`, `0x33`, `x'33'` - persisted as the decoded byte
    string with IsBin=true and LiteralForm BIT/HEX, 2.1 round-5 rows)
    decodes through `types.ParseDecimal64/128/256FromByte`
    (decimal.go:2242-2259 and siblings) at (W, S) - the exact helper of
    `renderUnsignedDecimalBinarySource`
    (func_cast_unsigned_decimal.go:397-418) - reading the payload as a
    big-endian base-256 coefficient at the destination scale
    (`b'00110011'` renders `0.0051` at scale 4, the inserted value; a
    text parse of the payload's display form would produce `3.0000`,
    and both conversions succeed, so the failure fallback cannot
    protect a text-only renderer - round-5 review finding 1). A
    non-IsBin string source keeps the text conversion
    `parseDecimal64CastString` /
    `parseDecimal128CastString` / `parseDecimal256CastString`
    (func_cast.go:9103/:9270/:9303) at (W, S) - the single-quantize
    conversion the execution path runs. `types.ParseDecimal64/128/256` is
    deliberately NOT used for retained sources: it pre-rounds at 18
    significant digits and rounds again when rescaling, so it
    double-rounds tokens the assignment cast rounds once (round-3 review
    finding 1; probe: `'1.4999999999999999999'` at (10,0) converts to `1`
    by the cast helper - the inserted value - but to `2` by
    `types.ParseDecimal64`);
  - decimal64/128/256 source at its literal shape (w0, s0) (the inner
    literal-shaped cast under the admission, or a folded bare literal):
    the typed value is exact by construction at (w0, s0); rescale to
    (W, S) through the executor's decimal-to-decimal conversion
    (decimal128ToDecimal64 and siblings, func_cast.go:6981+), which rounds
    once at the destination. A folded literal already at (W, S) rescales
    idempotently;
  - float64/float32 source (scientific-notation spellings):
    `types.Decimal64FromFloat64` / `Decimal128FromFloat64` /
    `Decimal256FromFloat64` (decimal.go:1761/:1865/:1898) at (W, S) - the
    exact calls of `floatToDecimal64/128/256` (func_cast.go:4798-4821,
    :4852-4867);
  - bool source (`true`/`false` spellings): the `boolToDecimal` semantics
    (func_cast.go:4319-4345): `1` resp. `0` scaled to S with the same
    width check (`true` under DECIMAL(10,4) renders `1.0000`);
  - integer/uint/bit source: the width-checked integer conversion
    (`checkIntDecimal64/128/256Width`, func_cast.go:4561 and siblings):
    the value scaled by 10^S with the same overflow check;
  - decimal256 `LiteralVec{Len:1}` const source: decode the const vector
    (a pure decode, never expression evaluation) and treat as the
    decimal256 source above.
  The canonical sign recorded during extraction negates the converted
  value before formatting. Every conversion failure (overflow, invalid
  token) falls back to the existing rendering;
- BIT columns: render `b'` + base-2 digits + `'` from the uint64 literal;
- every other shape keeps the existing string-decode/raw-origin behavior,
  including the nil-Expr fallback pinned by existing tests. Any extraction
  or conversion failure falls back to the existing rendering, so
  unrecognized future shapes degrade to base behavior rather than failing
  metadata queries.

## 5. Selected design details

### 5.1 Typed literal rendering (D1)

`columnDefaultValue` takes the decoded `*plan.Default` instead of the bare
origin string. NULL/empty origin returns SQL NULL first (existing order).
Classification per 4.3 runs next: expression-classified defaults keep the
existing OriginString rendering (I3a). Literal-classified defaults extract
the core literal by peeling the transparent wrapper set (cast-family nodes
peel to their source argument; a root matching the admission-composition
shape peels to the admission's arg0; canonical-sign nodes peel to their
child while recording the sign) and render. Every cast the peel removes
is binder-generated: user-written casts classified as expressions by rule
(c) never reach this path, so the renderer cannot bypass an explicit
cast's conversion (the round-4 review's `CAST(1.6 AS DECIMAL(10,0))`
counterexample, which must publish its expression text next to the
inserted 2.0000, never 1.6000):

- DECIMAL (declared type id decimal64/128/256, width W, scale S): convert
  the extracted source with the executor's own pure assignment-conversion
  helpers per 4.3's dispatch - IsBin string payloads through
  `types.ParseDecimal64/128/256FromByte` (the
  `renderUnsignedDecimalBinarySource` helper), non-IsBin string sources
  through `parseDecimal64/128/256CastString`, decimal-to-decimal rescale
  for decimal sources,
  `types.Decimal64/128/256FromFloat64` for float sources, `boolToDecimal`
  semantics for bool sources, width-checked integer scaling for int/uint
  sources, pure `LiteralVec{Len:1}` decode for decimal256 const sources -
  then apply the recorded sign and render `Format(S)`. All conversions
  round once at the destination scale, exactly as the execution-time
  assignment cast does, and IsBin payloads decode as big-endian
  coefficients exactly as the admission's binary branch does, so the
  rendered text names the inserted value on
  every retained source shape the 2.1 probes pin: unattributed spellings
  fold to a bare literal already at (W, S) (conversion is idempotent);
  attributed spellings persist inside the volatile admission composition
  with their original carrier (unconverted string, IsBin bit/hex payload,
  literal-shaped
  decimal128, float64, bool) and render the destination-rounded value an
  insert stores (`1.6` under `decimal(10,0) unsigned` renders `2`;
  `'1.4999999999999999999'` under `decimal(10,0) unsigned` renders `1`;
  `1.2e0` renders `1.2000`; `true` renders `1.0000`; `+1.2` renders
  `1.2000`; `b'00110011'` under `decimal(10,4) unsigned` renders `0.0051`
  and under `decimal(10,0) unsigned` renders `51`; `0x33`/`x'33'` under
  `decimal(10,4) unsigned` render `0.0051`); decimal256 spellings persist
  as an executable
  `cast(LIT ...)` or a `LiteralVec` const.
- BIT: `b'` + base-2 digits + `'` from the uint64 literal.
- Everything else (and any extraction/conversion failure): the existing
  string-decode/raw-origin behavior, including the nil-Expr fallback pinned
  by existing tests.

Attributed-DECIMAL metadata renders the plain typed value (`1.2000`):
ZEROFILL is a row-retrieval display attribute and never pads metadata, so
the rendered text matches the value consumers read back from inserts. The
renderer never evaluates the volatile admission entry or any other
function; it only decodes persisted literals. Modes 1 (information_schema)
and 4 (SHOW COLUMNS) share the function, so both surfaces stay equal (I4).

### 5.2 Expression-default classification: new builtin mode 7 (D2)

`mo_show_visible_bin(att_default, 7)` decodes `plan.Default` and returns
`DEFAULT_GENERATED` or the empty string per 4.3; the second-argument guard
becomes `[0, 7]`. One classification implementation serves both surfaces.
The builtin package (pkg/sql/plan/function) cannot import pkg/sql/plan
(plan imports function), so the admission-composition recognition is a
local typed-shape mirror of `matchUnsignedDecimalAssignmentComposition` -
same name set (case-folded, covering the two-argument legacy form), same
carrier-type equality - pinned against the plan package's own tests and the
2.1 probe table. The canonical-sign chain (4.3) is recognized by builtin
name (`unary_plus`/`unary_minus`) over a transparent-literal child chain,
so attributed signed literals classify as literals while genuine
arithmetic still marks. Rule (c) is a bounded quote-aware scan of the
persisted OriginString: identifier characters immediately followed by `(`,
outside single-quoted spans (with `''` doubling) and defensive
double-quoted/backtick spans - pure text scanning, no parser, no
evaluation - pinned by the 2.1 round-4 quoted-string control rows.
Rule (d) is a pure tree walk over the decoded Expr mirroring the plan
package's own `collectRefColPos` (build_util.go:2583-2608): any
`Expr_Col` node, including one persisted beneath the transparent
admission/assignment wrappers (2.1 round-5 rows), marks the default as an
expression default - the same classification the planner itself records
for column-reference defaults, so the two cannot diverge.
Rule (e) shares that single traversal with an added leaf predicate: any
`Expr_V` variable-reference node (system or user variable), including one
persisted beneath the transparent admission/assignment wrappers (2.1
round-6 rows), marks the default as an expression default - the same
classification the planner's own `isExpressionDefault` reaches through
its non-literal-root arm (build_util.go:1563-1571), since a variable
never constant-folds (function/function.go:434 with
`varAndParamIsConst=false` at build_util.go:1066). No
catalog column is added; the arm composes after
the generated-column arm in both templates, so auto-increment and
generated columns keep precedence (I3).

### 5.3 Current-template layer and frozen pre-change identity (D3)

The current COLUMNS identity gains one EXTRA arm, layered as a named
constructor over the existing chain, exactly like the delivered
unbounded-generated-expression and unsigned-decimal layers:

`... when mc.attr_has_generated = 1 then ifnull(mo_show_visible_bin(mc.attr_generated, 6), '')`
-> append `when length(mo_show_visible_bin(mc.att_default, 7)) > 0 then 'DEFAULT_GENERATED'`

(the `varchar(24)` EXTRA cast still bounds the result; `DEFAULT_GENERATED`
is 17 characters). The pre-change current text
(`informationSchemaColumnsWithUnsignedDecimalDDL()` at the base revision) is
re-published as a frozen identity constructor built only from frozen
templates and the fixed decoration/layer functions
(`InformationSchemaColumnsPreDefaultGeneratedDDL()`), never from the mutable
current constructor, with a byte-pinning test mirroring
`TestInformationSchemaColumnsDDL_UnsignedDecimalAncientConstructs`.

### 5.4 SHOW COLUMNS templates (D4)

The four `buildShowColumns` templates (system/other account x full/normal)
gain the same arm before `ELSE ''`:
`WHEN length(mo_show_visible_bin(col.att_default, 7)) > 0 THEN 'DEFAULT_GENERATED'`,
preserving the existing `tbl.relkind = 'r' and` auto-increment guard variants.
SHOW COLUMNS text is computed at query time and persists nowhere, so this leg
needs no migration.

### 5.5 Recognition seams (D5, parent design 5.5 pattern)

The frozen pre-change identity joins every seam that already carries the
frozen 4.0.18 identity: `AdaptLegacyInformationSchemaColumnsDDL` (byte-stable
pass-through; it already carries current column authority and projections),
`ViewMetadataColumnsDDL`'s known set, and
`IsInformationSchemaColumnsMigrationDDL`'s gate set.
`IsCurrentInformationSchemaColumnsDDL` keeps excluding it (no replay-drift
tolerance: the pre-change identity is a recognized generation, so recorded
text routes to the adapter, and live text converges by replacement - the
4018 precedent). Recognition stays exact-template at every seam.

### 5.6 Delivery and upgrade (D6)

- Fresh clusters/accounts: bootstrap init and CREATE ACCOUNT publish the
  corrected current identity (base behavior, parent design 5.1).
- Existing per-account definitions: the consolidated 4.0.7 COLUMNS entry
  (`InformationSchemaColumnsUpgradeEntry()` over the current symbol)
  check-fails on the pre-change text and issues its idempotent
  `CREATE OR REPLACE VIEW`; 4.0.7 is unreleased, so folding this refresh
  into the existing entry is the supported model and no new version handle
  or entry is registered (20261028 design). Startup and recurring
  maintenance keep their existing owners; nothing runs per row.
- Historical snapshots: recorded pre-change text binds through the D5
  recognition family unchanged (I6).

### 5.7 Ownership, bounds, hot paths, security

No runtime state, background pass, goroutine, queue, cursor, config flag,
metric or operator surface is added. Rendering cost is one proto decode per
rendered default (already paid by modes 1/4 today) plus a bounded tree walk;
metadata queries only. The template arms are fixed repository string
constants inside existing system transactions; no user input is handled.
Tenant isolation is unchanged: the corrected identity publishes through the
same per-account machinery with the same authorization predicates.

## 6. Compatibility and upgrade model (shutdown-only)

| Surface | Older binary wrote | This binary's behavior |
|---|---|---|
| mo_columns.att_default blobs | unchanged format | reads unchanged; rendering derives from the same bytes |
| Persisted COLUMNS identities | pre-change current (unsigned-decimal decorated identity) and older generations | pre-change current binds as the D5 frozen generation; live rows converge through the existing 4.0.7 COLUMNS entry; older generations unchanged |
| Fresh publication | - | publishes the corrected identity unconditionally |
| Historical snapshots | pre-change current text recorded as a live definition | binds byte-stably through the D5 family |
| SHOW COLUMNS | computed at query time | corrected text on first use; nothing persists |
| Default execution | inserted values, invalid-insert rejection | byte-identical (I5); no write path touched |

Downgrade remains unsupported. Backup/restore carries the persisted identity
with the account; a pre-change identity restores and converges exactly as an
upgraded account does. No protocol gate is added: the new builtin mode is
referenced only by definitions this binary publishes, and shutdown-only
upgrade guarantees every peer runs it.

## 7. Verification plan (cheapest deterministic validation per invariant)

- UT, rendering (I1/I2/I3a, red on base / green on fix):
  `pkg/sql/plan/function/func_builtin_column_default_test.go` gains
  `plan.Default{Expr, OriginString}` cases built by the real planner shapes
  (the 2.1 tables):
  decimal64 scale-4 spellings (`1.2`, `1.200`, `'1.2'`) -> `1.2000`;
  decimal128 negative -> `-12.340000`; decimal scale-0 rounded -> `2`; BIT
  widths (`b'00110011'`, `51`, `b'0'`, 64-bit) -> `b'110011'`, `b'0'`,
  `b'1'`; string/NULL/no-default controls unchanged; nil-Expr fallback
  unchanged (existing tests pin it).
  Revision-2 additions: attributed-DECIMAL literal cases
  (`decimal(10,4) unsigned/zerofill/unsigned zerofill default 1.2` and
  `default '1.2'`) -> `1.2000`; attributed scale-0 (`decimal(10,0) unsigned
  default 1.6`) -> the rounded `2`; decimal256 literal cases
  (`decimal(40,4) default 1.2`, `decimal(40,0) default 1.6`,
  `decimal(40,4) unsigned default 1.2`) -> `1.2000` / `2`; expression cases
  (`decimal(10,4) default (1+2)`, `decimal(40,4) default (1+2)`,
  `decimal(10,4) unsigned default (1+2)`) assert BOTH that Default keeps
  the expression text `(1 + 2)` AND that the classification marks them
  (I3a counterexamples against publishing `3.0000`); admission-composition
  shapes assert the value renders from the admission's source, never by
  evaluating the volatile entry.
  Revision-3 additions: the rounding counterexample
  (`decimal(10,0) unsigned default '1.4999999999999999999'`) -> `1` (the
  inserted value; a `types.ParseDecimal64`-based renderer would produce
  `2`, so this case is red against any double-rounding implementation),
  the unquoted 19-digit variant (`decimal(10,0) unsigned default
  1.4999999999999999999`, decimal128(20,19) carrier) -> `1`, and
  `decimal(20,6) unsigned default '1.4999999999999999999'` -> `1.500000`;
  float64 carriers (`decimal(10,4) unsigned default 1.2e0`,
  `decimal(40,4) unsigned default 1.2e0`) -> `1.2000`; boolean carriers
  (`decimal(10,4) unsigned default true` / `false`) -> `1.0000` /
  `0.0000`; canonical signed literals (`decimal(10,4) unsigned default
  +1.2`, `decimal(10,4) unsigned default +1.2e0`) -> `1.2000`;
  unattributed folded variants (`decimal(10,4) default 1.2e0` / `true` /
  `+1.2`) -> `1.2000` / `1.0000` / `1.2000`.
  Revision-4 additions (expression-text precedence over the new
  expression categories, I3a): explicit-cast cases over both persisted
  shapes - attributed `decimal(10,4) unsigned default cast(1.6 as
  decimal(10,0))` (user cast surviving under the admission) and
  unattributed `decimal(10,4) default cast(1.6 as decimal(10,0))` (folded
  literal 2.0000) - assert Default keeps the persisted expression text
  `cast(1.6 as decimal(10))`, never a typed `1.6000` or `2.0000`; folded
  deterministic function cases (`decimal(10,4) default abs(1.2)`,
  `decimal(10,4) default round(1.25, 1)`, and the attributed
  surviving-function shapes) assert the persisted text `abs(1.2)` /
  `round(1.25, 1)`; quoted call-spelling string controls
  (`varchar(40) default 'cast(1.6 as decimal(10,0))'` / `'abs(1.2)'`)
  keep the decoded string rendering.
  Revision-5 additions (IsBin binary-provenance rendering, I1):
  `decimal(10,4) unsigned default b'00110011'` -> `0.0051` (the inserted
  value; a text-parse renderer produces `3.0000` and both conversions
  succeed, so this case is red against any text-only implementation);
  `decimal(10,0) unsigned default b'00110011'` -> `51`; the hex spellings
  `decimal(10,4) unsigned default 0x33` / `x'33'` -> `0.0051`; the
  decimal256 carrier `decimal(40,4) unsigned default b'00110011'` ->
  `0.0051`; the unattributed folded variant `decimal(10,4) default
  b'00110011'` -> `0.0051` with empty EXTRA (literal classification
  unchanged; the BIT-column rendering of the same spelling still renders
  `b'110011'`).
- UT, classification (I3, red/green): mode 7 over every shape in the 2.1
  tables: function defaults and `(1+2)`, `('x')` -> `DEFAULT_GENERATED`;
  literals, `mediumint`, `enum`, `(-12.34)`, NULL, empty -> empty.
  Revision-2 additions: attributed-DECIMAL literal compositions
  (unsigned/zerofill/unsigned-zerofill, quoted and unquoted, decimal64 and
  decimal256 carriers, two-argument legacy admission form) -> empty;
  attributed `(1+2)` (unfolded `+` under the admission) and attributed
  function defaults -> `DEFAULT_GENERATED`; a stray admission-named node
  not in composition shape still marks (shape-mirror negative test);
  attributed NULL composition-free shape -> empty.
  Revision-3 additions: retained canonical signs
  (`decimal(10,4) unsigned default +1.2` with `unary_plus` over a
  literal/cast chain, `decimal(10,4) unsigned default +1.2e0` with
  `unary_plus` over a float64 literal) -> empty; a sign over a genuine
  expression (`unary_plus` over the unfolded `+` of `(1+2)`, or over a
  function) still marks (sign-chain negative test).
  Revision-4 additions: explicit casts over both persisted shapes
  (attributed surviving-cast, unattributed folded-literal,
  parenthesized-spelling) -> `DEFAULT_GENERATED`; folded deterministic
  functions (`abs(1.2)`, `round(1.25, 1)` plain, and the attributed
  surviving `abs` shape) -> `DEFAULT_GENERATED`; quote-aware scan negative
  tests (`varchar(40) default 'cast(1.6 as decimal(10,0))'`,
  `default 'abs(1.2)'`, and a string with a doubled quote before call
  text) -> empty; every literal row of the earlier tables re-asserted
  empty under the extended classifier (no rule-(c) false marks on
  numeric/BIT/NULL/date spellings).
  Revision-5 additions (column-reference recognition, rule (d)): every
  round-5 reference shape -> `DEFAULT_GENERATED`: the bare same-typed
  reference (`b int default a`, bare COL node), the parenthesized
  reference (`b int default (a)`), arithmetic over a reference
  (`b int default a + 1`), the decimal-to-decimal reference, and the
  attributed compositions with COL beneath the admission
  (`b decimal(10,4) unsigned default a` / `default (a)`, same-type and
  int-to-decimal); literal controls in the same multi-column tables
  (`a int default 7`, `a decimal(10,4) unsigned default 1.2`,
  `a varchar(10) default 'x'`) re-assert empty, and the round-5 IsBin
  admission shapes assert empty (a bit/hex literal spelling is a literal,
  never a reference).
  Revision-6 additions (variable-reference recognition, rule (e)): every
  round-6 variable shape -> `DEFAULT_GENERATED`: the bare system-variable
  spelling (`v varchar(64) default @@time_zone`), the parenthesized
  spelling (`default (@@time_zone)`), the global-qualified spelling
  (`default @@global.time_zone`), the user-variable spellings
  (`default @probe_user_var` / `default (@probe_user_var)`), the typed
  integer destination (`i int default @@sort_buffer_size`), and the
  attributed compositions with VAR beneath the admission
  (`d decimal(10,4) unsigned default @@time_zone` / `default
  (@@time_zone)`); the quoted-literal control (`v varchar(64) default
  '@@time_zone'`) re-asserts empty (a string literal that merely spells a
  variable stays a literal), and every literal row of the earlier tables
  re-asserts empty under the extended classifier.
- UT, identity and seams (I6): byte-pinning of the frozen pre-change
  identity and its independence from the mutable current; recognition at
  `AdaptLegacy`/`ViewMetadataColumnsDDL`/migration gate; `IsCurrent`
  exclusion; current-template arm presence and `varchar(24)` bound; existing
  `predefined_test.go` identity tests re-run.
- SQL oracle (I1-I5, red on base / green on fix), `pkg/tests/issues`:
  the 2.1 repro tables asserting information_schema.COLUMNS and SHOW COLUMNS
  Default/Extra together with the inserted values (`1.2000`, `51`,
  populated timestamps), ALTER ... ADD COLUMN variants, clone
  (CREATE TABLE ... LIKE / CTAS metadata paths where supported) and
  reconnect; invalid-INSERT rejection with row preservation.
  Revision-2 additions: UNSIGNED/ZEROFILL literal and expression variants
  and decimal(40,s) variants through both surfaces with inserted-value
  equality (`d = 1.2000`, scale-0 rounding `2`), and the DDL-time rejection
  of a negative literal default on an attributed DECIMAL
  (`Invalid default value`) staying intact.
  Revision-3 additions: the rounding counterexample
  (`decimal(10,0) unsigned default '1.4999999999999999999'`) asserting
  metadata `1`, inserted value `1` and empty EXTRA together;
  scientific-notation and boolean defaults across plain/attributed/
  decimal256 variants (`1.2e0` -> `1.2000`, `true` -> `1.0000`, `false`
  -> `0.0000`) with inserted-value equality and empty EXTRA; canonical
  signed literal `decimal(10,4) unsigned default +1.2` asserting Default
  `1.2000` and empty EXTRA on both surfaces; the DDL-time rejection of
  `decimal(10,4) unsigned default -1.2` staying intact.
  Revision-4 additions, asserting metadata and inserted values together:
  the explicit-cast rounding example `decimal(10,4) unsigned default
  cast(1.6 as decimal(10,0))` asserting Default `cast(1.6 as
  decimal(10))`, EXTRA `DEFAULT_GENERATED` AND inserted `2.0000` on both
  surfaces (a renderer bypassing the user cast would publish `1.6000` with
  empty EXTRA); the unattributed spelling asserting the same metadata with
  inserted `2.0000`; `decimal(10,4) default abs(1.2)` plain and attributed
  asserting Default `abs(1.2)`, EXTRA `DEFAULT_GENERATED` AND inserted
  `1.2000`; a literal control (`default 1.2` -> `1.2000`, empty EXTRA) in
  the same table run.
  Revision-5 additions, asserting metadata and inserted values together:
  attributed bit/hex-literal defaults (`decimal(10,4) unsigned default
  b'00110011'`, `decimal(10,0) unsigned default b'00110011'`,
  `decimal(10,4) unsigned default 0x33` / `x'33'`) asserting Default
  `0.0051` / `51`, empty EXTRA AND inserted `0.0051` / `51` on both
  surfaces (a text-only renderer publishes `3.0000` next to the inserted
  `0.0051`), with the unattributed `decimal(10,4) default b'00110011'`
  variant asserting the same typed value; a multi-column
  column-reference table (`a int default 7`, `b int default a`,
  `c int default (a)`, plus an attributed reference
  `d decimal(10,4) unsigned default a`) asserting Default text `a` /
  `(a)`, EXTRA `DEFAULT_GENERATED` AND the inserted reference values
  (omitted-column insert stores `7` / `7.0000`) on both surfaces, with
  the referenced literal columns keeping their own literal metadata
  (`7` -> empty EXTRA, `1.2` -> `1.2000` empty EXTRA).
  Revision-6 additions, asserting metadata and inserted values together:
  a variable-default table (`v varchar(64) default @@time_zone`,
  `w varchar(64) default (@@time_zone)`) asserting Default text
  `@@time_zone` / `(@@time_zone)`, EXTRA `DEFAULT_GENERATED` on both
  surfaces AND an omitted-column insert storing the session value
  (`SET time_zone = '+08:00'` first; the stored value equals `@@time_zone`
  selected in the same session); a user-variable attributed case
  (`SET @probe_d = 1.2; d decimal(10,4) unsigned default @probe_d`)
  asserting Default text `@probe_d`, EXTRA `DEFAULT_GENERATED` AND the
  inserted `1.2000` (the admission's own text conversion of the variable
  value, equal to `CAST(@probe_d AS DECIMAL(10,4))` in the same session);
  a quoted-literal control (`v varchar(64) default '@@time_zone'`)
  asserting Default `@@time_zone` (the decoded string) with empty EXTRA
  and the inserted literal `@@time_zone`.
- BVT (I1/I3/I4 user-visible): extend
  `test/distributed/cases/ddl/show_columns_default_quote.sql` (and result)
  or add a sibling case covering DECIMAL/BIT typed rendering,
  `CURRENT_TIMESTAMP(6)` DEFAULT_GENERATED, literal controls and `(1+2)`,
  one attributed-DECIMAL literal, one decimal(40,s) literal, one
  scientific-notation DECIMAL literal (`1.2e0`), one canonical signed
  DECIMAL literal (`+1.2`), one explicit-cast DECIMAL default
  (`cast(1.6 as decimal(10,0))`) and one folded-function DECIMAL default
  (`abs(1.2)`), one bit-literal attributed-DECIMAL default
  (`decimal(10,4) unsigned default b'00110011'` -> `0.0051`) and one
  column-reference default pair (`a int default 7, b int default a`),
  plus one variable-reference default pair (`v varchar(64) default
  @@time_zone`, `w varchar(64) default (@@time_zone)`) under a fixed
  `SET time_zone` so the inserted value is deterministic, reviewed line
  by line against I1-I3a.
- Broader suites left to CI: full `pkg/util/sysview`, `pkg/sql/plan`
  function suites and the distributed DDL BVT set (recorded in the handler
  report with exact focused coverage).

## 8. Open decisions and risks

- The explicit-parentheses signed-numeric edge (`DEFAULT (-12.34)`)
  classifies as a literal (2.3); blocked by persisted form, documented for
  consumers.
- The `(1+2)` folded-expression classification depends on OriginString
  parenthesization surviving planner formatting; the 2.1 probe pins the
  current spellings (plain, attributed and decimal256-carried) and the UT
  suite pins the classification table.
- Rule (c) relies on origin formatting never quoting user call syntax and
  always quoting string literals: the origin formatter emits user text
  with `WithSingleQuoteString` (build_util.go:1124-1129), and the scan
  skips `''`-doubled single-quoted spans plus defensive double-quoted and
  backtick spans. The 2.1 round-4 rows pin the spellings (`cast(`,
  `abs(`, `round(` unquoted; quoted controls quoted) and the section 7
  negative tests pin the quote handling. A formatter regression that
  quoted or rewrote call syntax would degrade classification toward base
  behavior (origin text kept, mark lost), never toward wrong values.
- MO accepts unparenthesized function and cast defaults that MySQL 8.3
  rejects at parse; classifying them as expression defaults follows the
  issue's own parenthesized-expression contract and base behavior (the
  origin text was always preserved), and is documented for consumers here
  and in the handler's public-contract projection.
- Plan-level cast provenance is deliberately not the discriminator
  (`SyntaxExplicitCast` unset for DECIMAL-target explicit casts; CAST
  overload 1 shared with internal rewrites, 2.1 round-4 facts). The origin
  scan is total over existing blobs, so no recognition gap opens for rows
  written by older binaries.
- Revision 3 removes the parser-equivalence assumption: the renderer calls
  the executor's own assignment-conversion helpers (4.3), so rounding
  cannot diverge from execution by construction. The SQL-oracle
  inserted-value equality checks (metadata `2`/`1` versus stored `2`/`1`,
  including the 19-digit counterexample) pin this end to end.
- The float-carrier render inherits `types.Decimal*FromFloat64`'s
  strconv-based text conversion; that is exactly the executor's
  `floatToDecimal*` path, and the SQL oracle pins `1.2e0`-family metadata
  against inserted values rather than assuming float formatting details.
- The sign-chain and admission-composition recognition in the builtin
  package mirrors plan-owned machinery by typed shape (the import ban
  stands); drift between the mirror and the plan-owned builders is a
  maintenance risk pinned by the shape tests over every 2.1 row, including
  the sign-chain negative tests.
- The IsBin binary dispatch relies on the bit/hex payload retaining its
  IsBin provenance inside the admission composition (2.1 round-5 rows pin
  the persisted shapes, including the `x'33'` -> `0x33` origin
  normalization to the identical HEX shape). The renderer reuses the
  executor's own `types.ParseDecimal64/128/256FromByte` decode, so the
  conversion cannot diverge from execution by construction; the section 7
  red case (`0.0051` vs the text-parse `3.0000`) pins the dispatch, and
  the SQL oracle pins metadata against inserted values. Execution
  semantics are untouched: the metadata now matches the values inserts
  already store.
- Rule (d) adds no new acceptance: column-reference defaults already
  bind, gate on the expression-default protocol and pass dependency
  validation at base, and the planner's own `isExpressionDefault` already
  classifies them as expression defaults (build_util.go:1554-1573). The
  classifier mirrors that walk in the builtin package (the import ban
  stands), so metadata cannot contradict the planner; execution,
  protocol gating and dependency rules are unchanged.
- Rule (e) adds no new acceptance either: variable-reference defaults
  already bind (base_binder.go:523-604) and execute at base (the VarRef
  resolves the session or global value at insert time), no protocol gate
  applies (the expression-default protocol gate inspects only column
  references, build_util.go:1031-1035 and exprHasLocalColumnRef at
  :2282-2303) and dependency validation sees no column references
  (build_util.go:1479-1544). The planner's own `isExpressionDefault`
  already classifies them through its non-literal-root arm
  (build_util.go:1563-1571), because a variable never folds to a literal
  root (function/function.go:434 with `varAndParamIsConst=false` at
  build_util.go:1066). The classifier shares rule (d)'s traversal with an
  added `Expr_V` leaf predicate in the builtin package (the import ban
  stands), so metadata cannot contradict the planner; execution,
  protocol gating and dependency rules are unchanged. MO's acceptance of
  variable-reference defaults extends MO's accepts-more-than-MySQL
  contract: MySQL 8.3 prohibits variable references in default
  expressions even when parenthesized (the default-value rules at
  dev.mysql.com/doc/refman/8.0/en/data-type-defaults.html), while it
  permits constrained column references (the referenced column must be
  declared earlier in the table definition). The acceptance is
  documented for consumers here and in the handler's public-contract
  projection.
- No live MySQL 8.3 reference was reachable on the validation host during
  design; the issue's recorded 3/3 MySQL evidence is the oracle for the
  reported cases, and inserted-value equality (`b+0 = 51`, `d = 1.2000`,
  attributed and decimal256 variants) is the internal consistency oracle
  for the generalized edges (scale-0 rounding, numeric BIT spelling,
  attributed compositions, precision above 38).

## Standing decision record

- D1 (selected): typed COLUMN_DEFAULT rendering for literal-classified
  DECIMAL/BIT defaults from the folded `plan.Default`, over every DECIMAL
  carrier (decimal64/128/256), the attributed admission composition, and
  every retained literal source kind (decimal, string with the executor's
  IsBin binary-versus-text dispatch - bit/hex payloads decode through
  `types.ParseDecimal64/128/256FromByte`, text spellings through
  `parseDecimal64/128/256CastString` - float64, bool,
  integer/bit, decimal256 LiteralVec const), via the executor's own pure
  assignment-conversion helpers at the declared (width, scale) - one
  rounding at the destination, never a general-purpose re-parse; recorded
  canonical signs applied after conversion; expression defaults and all
  other shapes keep existing rendering (4.3, 5.1).
- D2 (selected): expression-default classification per 4.3 published as
  builtin mode 7, with the transparent wrapper set covering the
  binder-generated cast family, the typed-shape admission composition and
  the canonical-sign chain over literal/cast machinery, with the
  quote-aware origin call-syntax scan (rule (c)) classifying user-written
  explicit casts and constant-folded function defaults as expressions,
  and with the shared reference walk (rules (d) and (e)) classifying
  column-reference and variable-reference defaults as expressions exactly
  as the planner's own `isExpressionDefault` does
  (expression text kept, never typed rendering); one implementation for
  both surfaces (5.2).
- D3 (selected): DEFAULT_GENERATED arm layered into the current COLUMNS
  identity; pre-change current re-published as a frozen identity (5.3).
- D4 (selected): identical arm in the four SHOW COLUMNS templates (5.4).
- D5 (selected): frozen identity recognized at exactly the 4018-identity
  seams; `IsCurrent` exclusion preserved (5.5).
- D6 (selected): delivery folded into the existing consolidated 4.0.7
  COLUMNS entry; no new version handle, entry, protocol gate or runtime
  state (5.6).
- Rejected: OriginString-only derivation (4.1); persisted classification
  flag (4.2).

## Revision history

- Revision 1 (2026-11-05): initial submission for design-first review.
- Revision 2 (2026-11-05): dispositions the design-review REQUEST_CHANGES
  on revision 1 (sha256
  be80e7cc864af574586eec1ca13b688a8a83b2358e5bb9aa2f43beaa69298ada). All
  three findings verified against the base and the extended 2.1 probe
  before editing:
  1. Attributed-DECIMAL admission composition (valid): UNSIGNED/ZEROFILL
     literal defaults persist as
     `cast_assign(unsigned_decimal_admission(source, carrier[, name]))`
     around an unfolded source, so revision 1's classifier would falsely
     mark them `DEFAULT_GENERATED` and miss typed rendering. Fixed by the
     transparent wrapper set (4.3): the composition is recognized by typed
     shape (mirroring `matchUnsignedDecimalAssignmentComposition`) for both
     classification and rendering; UNSIGNED/ZEROFILL literal and expression
     cases join the 2.1 table, the invariants (I1/I3) and the section 7
     plan.
  2. Expression-text precedence (valid): folded expression defaults
     (`DECIMAL(10,4) DEFAULT (1+2)` persists a bare scale-4 literal with
     origin `(1 + 2)`) would have rendered `3.0000` next to
     `DEFAULT_GENERATED`; MySQL 8.3 renders expression text before its
     typed-value branch. Fixed by classify-first ordering and I3a:
     typed rendering applies only to literal-classified defaults;
     expression defaults keep their persisted expression text.
  3. Decimal256 carriers (valid): declared precision above 38 maps to
     T_decimal256, whose constants persist as executable cast trees (or
     `LiteralVec` consts) rather than `Literal` scalars, leaving
     `DECIMAL(40,4) DEFAULT 1.2` outside revision 1's renderer. Fixed in
     4.3/5.1: cast-wrapped literal extraction plus canonical-text re-parse
     through `types.ParseDecimal256`, and `LiteralVec{Len:1}` decode
     without evaluation; precision-above-38 cases join the 2.1 table, I1
     and the section 7 plan.
- Revision 3 (2026-11-05): dispositions the design-review REQUEST_CHANGES
  on revision 2 (sha256
  6a10725edb5c76856349f7a7532a743b30f7e3596c7087947b7c17350f72e56b). All
  three findings verified against the base, the third 2.1 probe extension
  and a new cast-semantics probe before editing:
  1. Assignment-conversion rounding semantics (valid): revision 2's
     renderer re-parsed canonical text with `types.ParseDecimal64/128/256`,
     which pre-rounds at 18 significant digits and rounds again when
     rescaling. The assignment cast's string conversion
     (`parseDecimal64CastString`, func_cast.go:9103-9132) quantizes a
     validated token once at the destination scale, so the two diverge:
     probe-pinned on `decimal(10,0) unsigned default
     '1.4999999999999999999'`, whose retained string source converts to
     `1` at execution but to `2` under `types.ParseDecimal64` (both
     conversions succeed, so no fallback fires). Fixed in 4.3/5.1 (D1):
     the renderer reproduces the executor's assignment conversion per
     source-carrier kind - `parseDecimal64/128/256CastString` for string
     sources, the executor's decimal-to-decimal rescale for decimal
     sources (exact at their literal shape, one rounding at the
     destination), `types.Decimal64/128/256FromFloat64` for float sources,
     `boolToDecimal` semantics for bool sources, width-checked integer
     scaling for int/uint sources, pure `LiteralVec{Len:1}` decode for
     decimal256 consts - mirroring the admission's own per-carrier
     dispatch (func_cast_unsigned_decimal.go:105-142); the general-purpose
     parser is never used for retained sources. The rounding
     counterexample joins the 2.1 table, I1 and the section 7 plan
     (metadata `1`, inserted `1`, empty EXTRA).
  2. Float64/boolean literal carriers (valid): with the target type
     suppressed, attributed-DECIMAL bindings retain a float64 literal for
     scientific spellings (`1.2e0`; `isPlainDecimalLiteral` rejects `e`)
     and a boolean literal for `true`/`false`, and the admission accepts
     and converts both (func_cast_unsigned_decimal.go:113-142 through
     `floatToDecimal*`/`boolToDecimal`). Revision 2's renderer kept the
     origin text (`1.2e0`, `true`) instead of the typed value. Fixed by
     the D1 per-carrier dispatch above (`1.2e0` -> `1.2000`; `true` ->
     `1.0000`; `false` -> `0.0000`, plain/attributed/decimal256 variants);
     the carrier rows join the 2.1 table, I1 and the section 7 plan. The
     FLOAT/DOUBLE-column non-goal is unchanged (2.3): these are DECIMAL
     column defaults.
  3. Retained unary signs on literal defaults (valid): `DEFAULT +1.2`
     binds a `unary_plus` node the builder-less DefaultBinder does not
     fold, and the volatile admission boundary blocks later folding, so
     the sign node persists (probe: `decimal(10,4) unsigned default +1.2`,
     origin `(+1.2)`). Revision 2's classifier rule (a) falsely marked it
     `DEFAULT_GENERATED`. Fixed in 4.3 (D2) and 5.2: the transparent
     wrapper set gains the canonical-sign chain - `unary_plus`/
     `unary_minus` over a child chain of transparent wrappers around a
     core literal - with the sign recorded and applied during extraction;
     a sign over genuine arithmetic or functions stays an expression.
     `+1.2`/`+1.2e0` rows join the 2.1 table, I3 and the section 7
     classifier/renderer/SQL checks (Default `1.2000`, empty EXTRA).
- Revision 4 (2026-11-05): dispositions the design-review REQUEST_CHANGES
  on revision 3 (sha256
  142901c2419a7ea3718d7b2c3ed9546d2a7e99f7a8dd384f11a2ae3fa9ace681). Both
  findings verified against the base and the fourth 2.1 probe extension
  before editing:
  1. Explicit user casts treated as transparent (valid): `DECIMAL(10,4)
     UNSIGNED DEFAULT CAST(1.6 AS DECIMAL(10,0))` binds the user cast on
     the explicit path (base_binder.go:305-309); the volatile admission
     preserves it (probe: the cast node survives under
     `unsigned_decimal_admission`, CAST overload 1), so revision 3's
     by-name cast transparency peeled it and rendered the pre-cast value
     `1.6000` while execution stores `2.0000` - and both conversions
     succeed, so no fallback fires. The unattributed spelling folds to a
     bare literal (probe: LIT decimal64(10,4) = 20000), which revision 3
     rendered as `2.0000` with empty EXTRA, missing the expression
     attribute. Plan-level provenance cannot be the uniform
     discriminator: `SyntaxExplicitCast` is set only via
     `appendSyntaxExplicitCastBeforeExpr` (base_binder.go:12244-12260),
     which DECIMAL-target casts never take
     (`useExplicitCastOverload` routes them to
     `appendTemporalIntegerCast`), and CAST overload 1 also serves
     internal rewrites (integer_arguments.go:411). Fixed by
     classification rule (c) (4.3, 5.2; D2): the persisted OriginString
     is formatted from the user's tree expression
     (build_util.go:980, :1124-1129), so user-written call syntax always
     appears there while binder-generated casts never do; a quote-aware
     identifier-`(` scan classifies explicit-cast defaults as expression
     defaults (expression text kept, `DEFAULT_GENERATED`), and the
     transparent cast family now peels only binder-generated casts by
     construction (4.3/5.1). Explicit-cast rows and the quoted-string
     controls join the 2.1 table, I3 and the section 7
     classifier/renderer/SQL checks asserting metadata expression text
     AND inserted `2.0000` together.
  2. Function defaults whose nodes fold away (valid): `DECIMAL(10,4)
     DEFAULT ABS(1.2)` is accepted unparenthesized (mysql_sql.y:12785
     `DEFAULT bit_expr`), passes the uuid-only rejection
     (build_util.go:1037-1041), and constant-folds
     (build_util.go:1066; utils.go:2155-2158 replaces the tree with a
     literal) to a bare scale-4 literal with origin `abs(1.2)`
     (FuncExpr.Format writes no outer parentheses,
     tree/expr.go:983-1030) - probe-pinned. Revision 3's rules (a)/(b)
     saw no function node and no parentheses, so it published `1.2000`
     with empty EXTRA where base kept the expression text. Fixed by the
     same rule (c): the folded function's spelling survives in the
     origin, so the scan marks it; the attributed spelling (probe: `abs`
     survives under the volatile admission) keeps its rule-(a)
     classification, and quoted call-spelling strings stay literals.
     Folded-function rows (`abs(1.2)`, `round(1.25, 1)`,
     plain/attributed/parenthesized) join the 2.1 table, I3 and the
     section 7 checks asserting expression text, `DEFAULT_GENERATED` and
     unchanged inserted values (`1.2000`).
- Revision 5 (2026-11-06): dispositions the design-review REQUEST_CHANGES
  on revision 4 (sha256
  1c44c11e0681954ff36af63e311cd6749c645a1656edf1b7363f8b798a990dbf). Both
  findings verified against the base and the fifth 2.1 probe extension
  (including a multi-column harness mirroring the DDL pre-scan and bind)
  before editing:
  1. Binary-literal provenance in DECIMAL rendering (valid):
     `DECIMAL(10,4) UNSIGNED DEFAULT b'00110011'` binds the bit literal
     through `returnRawLiteralExpr` (base_binder.go:12084-12087) to a
     varchar constant carrying the DECODED byte payload (0x33, display
     form `3`) with IsBin=true, LiteralForm=BIT; the suppressed bind type
     (build_util.go:1012-1016) lets it persist unconverted under the
     volatile admission (probe-pinned). At execution the admission's
     carrier never carries the private charset-255 marker, so
     `mysqlPrefix` is false and the payload converts through
     `unsignedDecimalAdmissionBinary` ->
     `renderUnsignedDecimalBinarySource` ->
     `types.ParseDecimal64FromByte`
     (func_cast_unsigned_decimal.go:588-594, :397-418;
     decimal.go:2242-2259): a big-endian base-256 coefficient decode at
     the destination scale (51 -> `0.0051`). Revision 4's string arm
     text-parses the payload's display form and renders `3.0000`; both
     conversions succeed, so no fallback fires and metadata disagrees
     with the inserted `0.0051`. Fixed in 4.3/5.1 (D1): the string arm
     reproduces the executor's binary-versus-text dispatch on the
     literal's IsBin provenance before converting, reusing the same pure
     `types.ParseDecimal64/128/256FromByte` helper the admission's binary
     branch calls; execution semantics are unchanged (inserts already
     store `0.0051`). The hex spellings (`0x33`, `x'33'` - the latter
     normalizes to the `0x33` origin with the identical HEX shape) and
     scale-0/decimal256 carriers take the same arm; unattributed
     spellings fold with execution's conversion embedded (probe: LIT
     decimal64(10,4) = 51) and were already correct. The IsBin rows join
     the 2.1 table, I1 and the section 7 renderer/SQL checks asserting
     metadata `0.0051`/`51`, empty EXTRA and inserted values together.
  2. Bare column-reference defaults (valid): the base accepts
     `CREATE TABLE t(a INT DEFAULT 7, b INT DEFAULT a)` - the DDL path
     binds every default against the complete row schema
     (build_ddl.go:3778-3804, :4006), `default_binder.go:64-90` resolves
     the reference to `Expr_Col`, same-typed references persist bare
     (make.go:1040-1041), the expression-default protocol gate
     (build_util.go:1031-1035) and dependency validation
     (build_util.go:1479-1544) accept a preceding ordinary column - with
     origin `a` carrying no function node, parentheses or call syntax, so
     revision 4's rules (a)-(c) classify it as a literal while
     `DEFAULT (a)` marks. The planner's own `isExpressionDefault` already
     treats reference-bearing defaults as expression defaults
     (`collectRefColPos`, build_util.go:1554-1573), so revision 4
     contradicts the planner. Fixed by classification rule (d) (4.3, 5.2;
     D2): an `Expr_Col` node anywhere in the persisted tree - including
     beneath the transparent admission/assignment wrappers (probe:
     `decimal(10,4) unsigned default a` persists COL under
     `unsigned_decimal_admission`) - marks the default as an expression
     default, mirroring the planner's own walk. Execution, protocol
     gating and dependency rules are unchanged. Bare/parenthesized
     reference rows, the arithmetic and attributed-wrapper variants and
     the literal controls join the 2.1 table, I3 and the section 7
     classifier/SQL checks asserting Default text, `DEFAULT_GENERATED`
     and inserted reference values together.
- Revision 6 (2026-11-06): dispositions the design-review REQUEST_CHANGES
  on revision 5 (sha256
  af78616d2049ac02227095a961f561eee525ff3a2712db78d884077b66d18359). The
  finding verified against the base and the fifth 2.1 probe extension
  before editing:
  1. Bare variable-reference defaults (valid): the grammar accepts a
     variable as a DEFAULT source (`DEFAULT bit_expr`, mysql_sql.y:12785;
     `simple_expr: variable`, mysql_sql.y:13200 - both `@@system` and
     `@user` spellings parse to `tree.VarExpr`,
     mysql_sql.y:2309-2317), `baseBindVar` (base_binder.go:523-604)
     retains it as an `Expr_V` node, and the default fold runs with
     `varAndParamIsConst=false` (build_util.go:1066), so `IsConstant`
     rejects it (function/function.go:433-434) and the variable always
     survives unconverted - probe-pinned: `varchar(64) default
     @@time_zone` persists cast_strict over VAR, `int default
     @@sort_buffer_size` persists cast over VAR, and `decimal(10,4)
     unsigned default @@time_zone` persists VAR beneath the
     unsigned_decimal_admission composition. The bare spelling's origin
     formats without parentheses or quotes (`@@time_zone`,
     tree/expr.go:1923-1936), so revision 5's rules (a)-(d) classify it
     as a literal while `DEFAULT (@@time_zone)` marks - despite both
     spellings persisting the same executable default. The planner's own
     `isExpressionDefault` already classifies both spellings as
     expression defaults through its non-literal-root arm
     (build_util.go:1563-1571), so revision 5 contradicts the planner.
     Fixed by classification rule (e) (4.3, 5.2; D2): an `Expr_V` node
     anywhere in the persisted tree - including beneath the transparent
     admission/assignment wrappers - marks the default as an expression
     default, sharing rule (d)'s traversal with an added leaf predicate.
     Execution (the VarRef resolves the session or global value at
     insert), the column-reference-only protocol gate
     (build_util.go:1031-1035, :2282-2303) and dependency validation
     (build_util.go:1479-1544) are unchanged. Bare/parenthesized system
     and user variable rows, the global-qualified spelling, the
     typed-destination and attributed variants and the quoted-literal
     control join the 2.1 table, I3 and the section 7 classifier/SQL
     checks asserting Default text, `DEFAULT_GENERATED` and inserted
     values together.
- Post-approval reference-note correction (2026-11-06, implements the
  independent design review's non-blocking P3 finding on revision 6):
  the rule (e) discussion and the section 8 rule (e) risk previously
  attributed MySQL 8.3's rejection of variable-reference defaults to
  missing parentheses. Corrected per the reviewer-cited default-value
  rules (dev.mysql.com/doc/refman/8.0/en/data-type-defaults.html):
  MySQL prohibits variable references in default expressions even when
  parenthesized, and it permits constrained column references (the
  referenced column must be declared earlier in the table definition).
  Reference notes only; every proposed MatrixOne acceptance, metadata
  and inserted-value expectation is unchanged from approved revision 6
  (sha256 f664862ca860beda942be507a60a6560080dc3d0f52d280258b38ab9d287b064).
