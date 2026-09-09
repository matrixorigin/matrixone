# `STATEMENT_DIGEST_TEXT` compatibility design

- Status: proposed; implementation review blocked pending independent design approval
- Design revision: v5 (2026-09-09; SQL-mode-aware compatibility rewrites)
- Owning issue: [matrixorigin/matrixone#23025](https://github.com/matrixorigin/matrixone/issues/23025)
- Implementation PR: [matrixorigin/matrixone#27990](https://github.com/matrixorigin/matrixone/pull/27990)
- Compatibility target: MySQL 8.0.42 behavior, with the MySQL 8.4 documented SQL surface

## 1. Design-review classification

This change adds a public SQL function and approximately 1,350 non-generated
production lines across the parser, function registry, execution, and error
boundaries. It therefore triggers mandatory design review for both reasons:

- the production-line count exceeds the 500-line default threshold; and
- it establishes a public SQL compatibility and diagnostic-disclosure contract.

This document is the stable design artifact for that gate. It was added after
implementation review identified the missing artifact. The implementation must
remain blocked until an independent reviewer approves an exact revision of this
document. A material implementation change must update the design revision and
repeat review of the affected decisions.

## 2. Problem and evidence

MatrixOne currently reports `statement_digest_text` as unsupported. MySQL
clients use this function to turn one SQL statement into the normalized token
text used for statement grouping and digest computation. Compatibility requires
more than replacing literals: statement admission, token aliases, list
reduction, SQL mode, truncation, input provenance, and error confidentiality all
change the observable result.

The reference behavior is defined by:

- [MySQL 8.0 encryption and compression functions](https://dev.mysql.com/doc/refman/8.0/en/encryption-functions.html),
  which defines `STATEMENT_DIGEST_TEXT(statement)`, NULL behavior, and the
  `max_digest_length` dependency;
- [MySQL statement digest concepts](https://dev.mysql.com/doc/refman/8.0/en/performance-schema-statement-digests.html),
  which defines token normalization and bounded collection; and
- MySQL 8.0.42 `sql/item_strfunc.cc`, `sql/sql_digest.cc`, and
  `mysql-test/include/func_digest_errors.inc`, used as the source and test
  comparison points for diagnostic and token edge cases.

The implementation is accepted only when focused MatrixOne tests and the public
SQL path agree with deterministic MySQL 8.0.42 differential cases. A normal-path
example such as `SELECT 1` is necessary but insufficient evidence.

## 3. Goals and non-goals

### Goals

- Add the one-argument `STATEMENT_DIGEST_TEXT` SQL function.
- Normalize one parser-approved SQL statement to MySQL-compatible digest text.
- Preserve MySQL-compatible literal replacement, list reduction, token aliases,
  optimizer hints, SQL-mode behavior, delimiter handling, and token-budget
  truncation.
- Preserve error confidentiality based on the digest argument's origin.
- Read `sql_mode` and `max_digest_length` at execution, including prepared-plan
  reuse, and preserve the statement-scoped setting across distributed fragments.
- Keep memory and work bounded by input length and `max_digest_length` without
  retained per-row state.

### Non-goals

- Implement `STATEMENT_DIGEST`, Performance Schema digest aggregation, sampling,
  or a digest cache.
- Accept every MySQL statement that MatrixOne cannot parse or execute. Only the
  narrow validation rewrites listed below are compatibility exceptions.
- Reuse MatrixOne AST formatting as the digest representation.
- Persist digest values, add catalog state, or introduce a new RPC service.
- Guarantee compatibility with undocumented token changes in future MySQL
  releases without new differential evidence.

## 4. Invariants

1. **Single-statement admission:** normalization succeeds only after the input is
   validated as one statement under the current SQL mode. Validation-only
   compatibility rewrites use the same quoting rules, including
   `NO_BACKSLASH_ESCAPES`, so they cannot change statement boundaries while
   scanning source. Empty/comment-only input, multiple top-level statements,
   malformed SQL, and parameter markers in the normalized statement fail.
2. **Value equivalence:** statements differing only in replaceable literal
   values normalize identically until the configured token budget is exhausted.
3. **Identifier distinction:** identifiers, including ANSI-quoted user-variable
   identifiers, remain distinguishable and are not collapsed into value
   markers.
4. **Delimiter distinction:** one terminal client semicolon is excluded from the
   digest and budget; semicolons inside parser-approved compound statements are
   retained and counted.
5. **Origin-based confidentiality:** detailed parser text is exposed only when
   every evaluated non-NULL digest argument is a direct SQL literal and none has
   binary provenance. Vector constness and unrelated prepared parameters do not
   change this decision.
6. **Runtime settings:** a cached/prepared plan observes the `sql_mode` and
   `max_digest_length` values in effect for each execution. All local child and
   remote processes of one statement observe one validated snapshot; the valid
   value zero is distinct from an absent legacy field. The function is not
   foldable and cannot appear in stored generated expressions.
7. **Bounded collection:** after the next complete token would exceed the token
   budget, no later token is collected. No partial identifier or token is
   emitted.
8. **No retained state:** all parser, token, and output storage is scoped to one
   function invocation and becomes unreachable when it returns.

The negations exercised by tests include folded expressions being mistaken for
literals, valid binary SQL being rejected, malformed binary bytes disclosing
parser text, internal semicolons being dropped, and a prepared plan retaining a
setting from an earlier execution.

## 5. Public SQL contract

### 5.1 Signature, inputs, NULL, and result metadata

`STATEMENT_DIGEST_TEXT(statement)` accepts exactly one argument.

- `CHAR`, `VARCHAR`, `TEXT`, `BINARY`, `VARBINARY`, and `BLOB` have direct
  overloads.
- Other scalar values follow the normal MatrixOne implicit string-cast path.
- `GEOMETRY` and `GEOMETRY32` have deterministic overload selection but are
  rejected at execution with the undisclosed digest parse error.
- The function is strict: a NULL argument produces NULL without parsing.
- A valid binary-domain argument is parsed from its bytes and can succeed.
  Binary provenance is not itself a rejection condition.
- The MatrixOne result OID is always `TEXT`; source charset metadata is retained
  for collation propagation, including the binary charset domain.

The result is the normalized token text. This design does not add a hash result;
that remains the responsibility of a future `STATEMENT_DIGEST` design.

### 5.2 Statement validation

The original input is first parsed with MatrixOne's MySQL dialect and current
SQL mode. A narrow rewritten copy may be used only to establish syntactic
admission for constructs whose token behavior is required for compatibility:

- MySQL `NCHAR` type spelling;
- multi-element `ROW(...)` constructors;
- `SOUNDS LIKE`; and
- quoted user-variable names unsupported by the ordinary MatrixOne grammar.

The original SQL, never the rewritten copy, is scanned for normalization. The
rewriter skips quoted text and comments so a keyword-shaped substring cannot
change admission. An incomplete quote/comment or a one-element `ROW(...)`
remains an error.

### 5.3 Token normalization

The scanner runs with the current SQL-mode flags. Complete tokens are collected
in order and rendered with one separating space.

- String, numeric, hexadecimal, bit, and value-position NULL tokens become `?`.
- Consecutive scalar values become `?, ...`.
- Homogeneous row lists reduce to `(?) /* , ... */` or
  `(...) /* , ... */`; `IN` value lists reduce to `IN (...)`.
- Unary signs on replaceable values are removed only in unary expression
  positions; binary `+` and `-` remain operators.
- Identifiers are backtick-normalized and stay distinct.
- MySQL aliases are canonicalized, including integer/float/string type aliases,
  schema/field statement aliases, interval units, `CURRENT_TIMESTAMP`,
  `CURRENT_DATE`, `CURRENT_TIME`, `SESSION_USER()`, `USER()`, `STD()` and
  `VARIANCE()`.
- `NULL` remains a keyword in `IS NULL`, column nullability, referential
  `SET NULL`, and MatrixOne `FILL(NULL)` positions.
- Character-set introducers normalize to `(_charset)` while the following value
  becomes `?`.
- Only the first syntactically eligible optimizer-hint comment immediately after
  `SELECT`, `INSERT`, `UPDATE`, `DELETE`, or `REPLACE` participates in the
  digest. Values and identifiers inside that hint follow hint-specific token
  rules.
- A semicolon is omitted only when the remaining source contains whitespace or
  comments and no token. Internal compound-statement semicolons remain ordinary
  digest tokens.

Every reduction is local to the token sequence and does not change statement
admission. Unknown optimizer-hint names may be rendered as identifiers, but
ordinary unsupported scanner tokens fail rather than disappear silently.

### 5.4 SQL mode

`sql_mode` is read from the session resolver for each invocation, with session
metadata as the fallback, through the shared process-level resolver. A nonempty
resolved string takes precedence. An empty frontend value explicitly clears the
mode; an empty value from a retained non-frontend resolver preserves a captured
nonempty snapshot. Missing resolvers, resolver errors, and non-string values
also preserve the snapshot. The explicit-empty transport sentinel is retained
when forwarding and translated to an empty string only at the digest scanner
boundary. This keeps local background and remote execution consistent without
preventing a frontend session from clearing its mode. Relevant modes include:

- `ANSI_QUOTES`: double quotes delimit identifiers, including quoted user
  variables, rather than string values;
- `PIPES_AS_CONCAT`: `||` follows the scanner's mode-specific token meaning; and
- `NO_BACKSLASH_ESCAPES`: string admission follows the active scanner mode.

Mode changes between executions of the same prepared plan must affect the next
result and must not mutate the cached plan.

### 5.5 Token budget and truncation

`max_digest_length` is read as a global system variable once for each statement
generation. The accepted range is 0 through 1 MiB and the fallback is 1024 when
the initiating process resolver is absent, fails, returns an unsupported type,
or yields an out-of-range value. The validated result is cached in the shared
base process, so local child pipelines cannot re-resolve a different value.
Frontend multi-statement execution clears the presence bit at each statement
boundary, allowing a later execution of a cached/prepared plan to observe a
new setting.

The initiating CN serializes both `max_digest_length` and
`max_digest_length_set` in the remote `SessionInfo`. The explicit presence bit
is required because zero means an empty digest and is not an unset sentinel. A
remote CN has no session resolver and consumes the carried snapshot. If it
forwards the pipeline again, it serializes the same value and presence bit.
Absent legacy fields and malformed carried values use the bounded default 1024.

The limit applies to MySQL-compatible stored token size, not directly to the
rendered UTF-8 byte length. Collection stops before the first complete token
that would exceed the budget. Parsing and admission still cover the complete
input, so an invalid marker or second statement after the collection boundary
cannot be hidden by truncation. A terminal client delimiter consumes no budget;
an internal delimiter does.

### 5.6 Errors and confidentiality

Two distinct internal MatrixOne errors map to MySQL's digest errors:

| Condition | MySQL code | Observable text policy |
|---|---:|---|
| parser failure for a direct text SQL literal with valid UTF-8 | 3676 | include the parser diagnostic |
| parser failure for an expression, prepared marker, column/variable value, binary-origin value, or geometry | 3677 | generic message only |
| malformed UTF-8 in any evaluated non-NULL argument, including a direct text literal | 3677 | generic message only, before parser diagnostics |

The execution vector carries `StringSource` provenance. `StringSourceLiteral`
is the only origin that can disclose details. A constant vector can also come
from folding, a cast, a scalar subquery, a nested function, or a prepared
parameter, so `Vector.IsConst()` is never used as the disclosure oracle.

For a mixed batch, one selected non-NULL nonliteral or binary-origin row makes
the single batch error undisclosed. Masked rows and NULL rows do not participate
in the decision. This conservative rule prevents one row from exposing details
through another row's batch failure.

## 6. Architecture and ownership

The end-to-end flow is:

1. The function registry resolves the overload, assigns function ID 579, marks
   the overload volatile/runtime-related, and declares a `TEXT` result.
2. Existing binder/vector machinery carries string-source and binary provenance
   from the argument expression to execution.
3. The executor snapshots `sql_mode` and `max_digest_length` for the statement,
   shares the snapshot with local child processes, transports it to remote
   processes, classifies disclosure for selected rows, and rejects geometry.
4. `NormalizeStatementDigest` validates the complete statement, scans the
   original source, collects/reduces bounded tokens, and renders the result.
5. The executor writes the result through the standard unary bytes-to-bytes
   vector wrapper, which owns NULL propagation and selected-row handling.
6. `moerr` maps internal error ownership to stable MySQL codes and messages.

The parser invocation owns its AST and returns it with `Free`. The scanner is
borrowed from the existing pool and returned with `PutScanner`. Token and output
slices are invocation-local. The base process owns a small statement-setting
mutex and value/presence pair; child processes already share that owner. There
is no new global mutable state, goroutine, channel, retry, file, network request,
catalog object, or cleanup protocol.

## 7. Performance and resource bounds

For input size `n` and token budget `m`:

- statement validation and scanning are `O(n)` apart from the existing parser's
  grammar costs;
- compatibility rewrite and optimizer-hint scanning are bounded by source
  length and do not recurse without an input-derived limit;
- collected token storage is `O(min(n, m))` in token representation, with
  rendering bounded by the collected identifiers and normalized tokens; and
- there is no retained per-session digest buffer, cache, or metric cardinality
  introduced by this function.

The code is reached only when the SQL function is explicitly evaluated. The
owning benchmark records allocations for a representative statement; BVT must
not use wall-clock thresholds as a correctness oracle.

## 8. Compatibility, rollout, and rollback

This is an additive SQL capability. It adds one function ID, two internal error
codes mapped to existing MySQL codes 3676 and 3677, and two additive protobuf
fields in pipeline `SessionInfo`. It changes no catalog, disk, backup, or
replication format.

MORPC version 59 is the capability boundary for remotely executing function ID
579 with its setting snapshot. Both the sender and decoded-owner boundary walk
every expression owner. A pipeline containing this function is rejected with
`ErrNotSupported` before remote execution when the oldest-live service protocol
is absent or below v59. Ordinary pipelines remain wire-compatible because the
new fields are additive. This explicit rejection is the routing contract; the
implementation does not silently execute with an older worker or promise an
automatic coordinator retry.

No persisted generated expression can contain the function because it is
registered as non-deterministic. Rolling back below v59 makes new senders stop
remote execution through the same protocol fence. Rolling back all nodes
restores the prior `function not supported` behavior and requires no data
migration or cleanup. Values already present in an in-flight protobuf are
statement-local and have no durable cleanup.

Failure containment is per expression evaluation: invalid input returns a
normal SQL error and allocates no durable state. There is no feature flag because
rollback is the ordinary binary rollback and the function has no background or
persistent effects.

## 9. Security and diagnostic exposure

Parser diagnostics can echo fragments derived from the statement argument.
Only a direct SQL literal is considered intentionally disclosed by the query
author. Runtime values may contain tenant or application data and therefore
receive the generic 3677 error. Binary or malformed encodings also receive 3677
because decoding failures must not expose byte-derived parser context.

The function neither executes the supplied SQL nor performs privilege checks on
objects named inside it. Parsing must not be mistaken for authorization or
existence validation. Input work is bounded by the normal SQL expression value
and the digest token budget; no new cross-tenant state is retained.

## 10. Alternatives

### Format the MatrixOne AST

Rejected. AST formatting loses the original token distinctions needed for
MySQL aliases, optimizer hints, value-list reductions, and SQL-mode-sensitive
lexing. It would also make unsupported-but-admitted compatibility constructs
depend on unrelated formatter behavior.

### Normalize with regular expressions or text substitution

Rejected. Text substitution cannot reliably distinguish literals from comments,
identifiers, operators, nested rows, or compound-statement delimiters. It also
cannot enforce single-statement admission or preserve the confidentiality
boundary for parser errors.

### Import or fork MySQL's parser/digest implementation

Rejected. It would introduce a second parser, a substantial native dependency,
license/update coupling, and a grammar that does not match MatrixOne execution.
The selected design reuses MatrixOne admission and scanner ownership while
pinning observed token semantics with differential tests.

### Mark the function deterministic and snapshot settings at bind time

Rejected. It would make prepared-plan reuse and stored generated values depend
on stale session/global settings. Runtime reads plus non-foldable registration
preserve the observable contract with less state.

### Resolve `max_digest_length` independently on every CN

Rejected. Remote processes have no frontend variable resolver, so they would
fall back to 1024 while the initiating CN could use zero or a custom value.
Even if every CN queried global state, propagation timing could make fragments
of one statement disagree. A statement-scoped transported snapshot is the
smallest deterministic ownership model.

### Treat zero as an absent protobuf value

Rejected. Zero is a valid MySQL setting whose result is an empty digest. An
explicit presence bit is required for legacy-field detection and repeated
forwarding.

## 11. Validation matrix

| Contract | Focused oracle | Public-path oracle / counterexample |
|---|---|---|
| basic token normalization | parser table tests | BVT `SELECT 1`, identifiers, values, lists |
| parser admission | empty, comments, malformed, multiple statements, marker after truncation | BVT disclosed literal errors |
| literal/expression confidentiality | literal, folded expression, nested function, mixed-source vector, masked row | prepared marker returns 3677; nested call returns 3677 |
| encoding confidentiality | malformed UTF-8 in text literals and binary inputs always returns 3677; valid UTF-8 controls | malformed bytes never disclose parser context |
| binary/geometry boundary | valid and malformed BINARY/VARBINARY/BLOB; GEOMETRY/GEOMETRY32; binary provenance on text OID | `_binary` and `CAST AS BINARY` valid controls |
| runtime registration | `CannotFold` and `IsRealTimeRelated` | generated-column rejection |
| runtime settings | statement generation cache; child sharing; resolver fallback/range/type table | SQL mode changed and restored; prepared plan reused |
| distributed setting snapshot | encode/decode/re-encode at 0/default/custom; absent/malformed controls; resolver-free evaluation | one-CN public result plus multi-CN CI topology |
| mixed-version fence | function ID 579 in plan and instruction owners; v58 rejects on encode and decode, v59 accepts; ordinary owner control | rolling-upgrade CI / service-version routing |
| SQL modes | ANSI quotes, pipes, hint quoting; retained empty resolver vs frontend clear; error/type/nil/sentinel fallback; repeated forwarding | quoted user-variable identifier under `ANSI_QUOTES` |
| alias compatibility | keyword/function aliases plus identifier controls | canonical function-alias BVT row |
| delimiter boundary | simple/compound, one/multiple internal statements, with/without terminal delimiter | simple and compound BVT results |
| length boundary | 0/1/exact token/identifier limits; internal vs terminal semicolon budget | differential long statement at configured limit |
| NULL and selection | constant/vector NULL and masked invalid row | table vector plus prepared NULL |
| type metadata | every direct string/binary/geometry overload and implicit scalar cast | client metadata on text/binary calls when available |
| error mapping | internal code, MySQL code, SQL state, exact message | text and binary protocol errors |
| regression breadth | deterministic MySQL 8.0.42 differential corpus | exact changed BVT case in normal comparison mode |

The minimum data is one statement per semantic cell and three table rows for the
vector/NULL path. Tests use no sleeps, retries, external timing assumptions, or
large fixtures. The distributed case owns one feature-specific database,
restores `sql_mode`, deallocates its prepared statement, and drops its table and
database so same-instance rerun is valid.

## 12. Acceptance criteria and review record

Implementation acceptance requires all of the following on the exact candidate
head:

- independent approval of this design revision;
- zero unresolved correctness findings against every invariant above;
- passing `moerr`, MySQL parser, planner generated-expression, and function
  owning-package tests, using the repository CGo wrapper where required;
- passing process snapshot/codec and sender/receiver MORPC v58/v59 capability
  tests, including zero and repeated-forward controls;
- passing exact distributed SQL case in normal comparison mode, including
  result-file review and same-instance cleanup/repeat evidence;
- passing relevant repository CI on the exact head; and
- a complete delivery diff with no generated, temporary, credential, container,
  or unrelated artifacts.

Design-review record for v4:

- Trigger: more than 500 production lines and a new public SQL compatibility
  contract.
- Status: proposed after the missing-design review and distributed-setting
  review on PR #27990.
- Independent reviewer: pending.
- Reviewed commit: pending.
- Blocking design questions: none known in the document; independent review may
  identify additional decisions.
- Implementation deviations: none recorded; approval must verify this claim
  against the exact implementation head.
