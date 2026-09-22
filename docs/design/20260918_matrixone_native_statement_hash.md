# MatrixOne-native statement hash

- Status: implementation-ready revision; a separate design-approval gate is not required for this PR under the agreed single-author workflow
- Implementation PR: [matrixorigin/matrixone#27988](https://github.com/matrixorigin/matrixone/pull/27988)
- Related issue: [matrixorigin/matrixone#23024](https://github.com/matrixorigin/matrixone/issues/23024) asks for MySQL-compatible `STATEMENT_DIGEST`; this proposal does not satisfy or close that issue
- Owner: SQL function / execution maintainers; PR #27988 is the tracking record for this function
- Design revision: `matrixone-native-statement-hash-2026-09-22-r6`
- Last updated: 2026-09-22

## 1. Decision proposed

Add a MatrixOne-specific SQL function, `MO_STATEMENT_HASH(sql)`, that hashes
the existing MatrixOne AST formatter's output for exactly one statement parsed
by MatrixOne's MySQL-dialect parser. The result is lowercase hexadecimal
SHA-256 of those formatted bytes. This is a hash of a canonical rendering, not
a semantic-plan hash and not MySQL's normalized-token digest.

This document records the native-hash direction and implementation contract for
PR #27988. The API, stability, resource, and remote-execution choices below
are the decisions implemented and validated by this PR.

### Decision record

The implementation uses the following choices as one complete contract rather
than as an unspecified future hash function:

| Decision area | Proposed decision | Acceptance boundary |
| --- | --- | --- |
| Public API | Add only `MO_STATEMENT_HASH(sql)` with the contract in section 3; do not alias MySQL `STATEMENT_DIGEST`. | The function name, arity, input overloads, NULL behavior, and 64-character lowercase result are accepted. |
| Stability | Promise equality only for the same executable, formatter, and effective SQL mode; do not promise a cross-upgrade or persistent identifier. | Consumers must treat the value as build-scoped syntax telemetry, not a durable key or wire format. |
| SQL-mode ownership | Resolve the effective mode at the coordinator, capture the snapshot, and forward it unchanged through every hop. | A worker never substitutes its local resolver/default for the captured snapshot; resolver failures follow section 5. |
| Remote isolation | Admit a remote scope only with MORPC v94+ and an exact matching 40-character source commit; reject on missing, invalid, or mismatching provenance. | No mixed-build hash, coordinator fallback, or per-row bypass is allowed. |
| Rollout and rollback | A heterogeneous cluster may continue plans that do not use this function; hash plans fail closed until an eligible worker is selected. No automatic digest migration is promised. | A single query never mixes different builds; hash values may change after an upgrade or rollback. |
| Resource and validation boundary | Enforce the inclusive limits in section 7 and the evidence matrix in section 10; the contract does not claim a total Go-heap ceiling. | Boundary, failure, cancellation, empty/masked, multi-hop, and exact-head SQL tests must pass before merge. |

These are the explicit implementation decisions and acceptance boundaries for
PR #27988.

## 2. Problem boundary and non-goals

The function provides a compact fingerprint of the AST rendering produced by
one MatrixOne build. Equal formatted bytes produce equal hashes, subject to
the usual theoretical possibility of a SHA-256 collision. Equal hashes are
not a proof of semantic equivalence: different SQL can format differently,
and SQL that formats identically can still behave differently under catalog,
data, privilege, or runtime state.

This function does not:

- implement or alias MySQL `STATEMENT_DIGEST` / `DIGEST_TEXT`;
- promise MySQL parser, tokenization, normalization, or error compatibility;
- hash a logical or physical plan, bind names to catalog objects, execute SQL,
  check privileges, or prove that a statement is executable;
- anonymize literals, identify equivalent queries, or provide a security,
  privacy, or tamper-proofing boundary;
- introduce a Merkle tree, a new AST identity scheme, a digest-specific lexer,
  cross-version persistence, or a stable external identifier.

Issue #23024 remains separate and unresolved by this change. A caller needing
MySQL digest compatibility must use a future implementation of that contract,
not `MO_STATEMENT_HASH`.

## 3. SQL API contract

- Name: `MO_STATEMENT_HASH` (MatrixOne-specific; no `STATEMENT_DIGEST` alias).
- Arity: exactly one SQL string argument. Current overloads are `VARCHAR`,
  `TEXT`, `BLOB`, `CHAR`, `BINARY`, and `VARBINARY`; other types follow normal
  MatrixOne overload resolution and casts.
- Result: `VARCHAR(64)`, exactly 64 lowercase ASCII hexadecimal characters
  for a successful non-NULL evaluation.
- NULL: strict unary-function semantics; NULL input returns NULL and does not
  parse SQL or resolve `sql_mode`.
- Evaluation: runtime/session-dependent; it must not be constant-folded using
  planner-process defaults.

For non-NULL input `s` and the effective SQL-mode snapshot `m`, the proposed
definition is:

```text
AST       = ParseWithSQLMode(MYSQL_DIALECT, s, m)
formatted = ExistingMatrixOneMySQLFormatter(AST,
              quote_identifiers=true,
              single_quote_strings=true,
              canonical_user_variable_names=true)
result    = lowercase_hex(SHA-256([]byte(formatted)))
```

No mode name, build ID, function version, or original SQL bytes are prefixed
to the SHA-256 input. The SQL mode affects parsing/formatting where applicable;
it is not an independent salt. If two modes produce identical formatted bytes,
they produce the same hash.

## 4. Input and canonicalization boundary

1. The input is parsed using MatrixOne's MySQL dialect and the session SQL-mode
   snapshot. “MySQL dialect” names MatrixOne's parser mode; it does not mean
   that every MySQL grammar production is supported.
   Input bytes are passed to the parser without digest-specific character-set
   transcoding or a separate encoding-validation step; the parser's existing
   acceptance and error behavior governs.
2. Parsing must yield exactly one non-empty statement. Empty or comment-only
   input and multiple statements return an error. A parser error, including
   syntax not implemented by MatrixOne, is returned as an error; no digest is
   produced for failed parsing. If MatrixOne is expected to accept such SQL,
   parser support is a separate fix before hashing it.
3. The function parses and formats only. It does not bind names, resolve
   objects, authorize, type-check expressions, or execute the statement.
4. The original source text is not hashed. Whitespace, comments, and other
   source distinctions disappear when the parser does not preserve them in the
   AST. AST constructs retained by the parser are included according to the
   existing formatter. In particular, this contract does not assert that all
   optimizer hints or comment-like syntax are ignored; their parser/formatter
   behavior must be pinned by tests.
5. Formatting uses the existing MySQL-dialect AST formatter with quoted
   identifiers, single-quoted strings, and canonical case-insensitive user
   variable names. No digest-specific rewrite is applied to literals or
   identifiers. Values and names therefore contribute as the AST formatter
   renders them; distinct values/names are not intentionally collapsed.
   User-variable values and other runtime/catalog values are not looked up;
   this function fingerprints parsed syntax, not values to which it may later
   bind or evaluate.
6. The contract is exact formatter bytes, not an independent list of SQL
   equivalences. Any additional equivalence must be demonstrated by the
   formatter and regression tests, not inferred from SQL semantics.

Consequently, the reviewer-reported case where MySQL accepts syntax that the
MatrixOne parser rejects is not a “successful digest with a parser error.” It
must return the parser error. Whether MatrixOne should accept that SQL is a
separate parser-compatibility question.

## 5. Errors, NULLs, and row evaluation

The following conditions return errors for an evaluated, non-NULL row:

| Condition | Required result |
| --- | --- |
| SQL-mode resolver fails or returns an invalid value | Propagate the error; do not hash using a guessed/default mode |
| Parse fails, input is empty/comment-only, or parse count is not one | Return the parser/statement-count error; no hash |
| AST formatting panics, produces empty output, or exceeds its output limit | Return an error; no fallback to raw SQL hashing |
| Input, token, nesting, or output limit is exceeded | Return a limit error; do not truncate or hash a prefix |
| Evaluation context is cancelled | Return cancellation; do not return a partial digest |

NULL propagation, a zero-row batch, or a row excluded by the expression mask
does not evaluate the function for that row and must not surface its parser,
limit, or SQL-mode resolver error. The SQL-mode snapshot is resolved at most
once per local function invocation, on the first active non-NULL row. Remote
process construction may capture a resolver error, but the error is deferred
until an active non-NULL row evaluates the function; it is not silently
discarded for an evaluated row.

## 6. Remote execution and rolling-upgrade boundary

Remote execution is supported only when both conditions hold:

1. The selected remote destination supports the complete statement-hash wire
   contract (`MORPCVersion94` or later). MORPC v88 remains reserved for the
   canonical vector `HLL_ADD_AGG` contract already present on main, and v91
   remains reserved for canonical CHAR/JSON `HLL_ADD_AGG`; v92 is reserved for
   canonical scalar FLOAT `HLL_ADD_AGG`; v94 is the first admission epoch for
   this complete native-hash contract.
2. Its full 40-character source commit ID exactly matches the coordinator
   source commit selected for this statement-hash scope.

The coordinator's original build identity and SQL-mode snapshot are immutable
provenance: intermediate remote hops must forward them unchanged, never
substitute their own local defaults. The destination is checked when selected
and the receiving process validates the build fence when the hash scope is
admitted, before operators can observe rows; the function entry point repeats
the check as a defensive local boundary. A missing/invalid identity,
unsupported protocol, failed capability probe, or build mismatch is an error.
This function-specific path does not silently fall back to coordinator
evaluation or compute a mixed-build hash. Local-only execution does not require
a remote build identity.

The destination capability/build check is a remote-scope dispatch gate, not a
per-row expression error. It can reject a remote scope containing this function
even when that scope later has zero rows or every function row is masked. By
contrast, SQL-mode resolver and parse errors are deferred to active, non-NULL
row evaluation as specified above.

Operational consequence: during a mixed-build rollout, a plan containing this
function can fail if its selected worker is not on the coordinator's exact
build. This contract chooses fail-closed behavior over potentially inconsistent
hashes; the behavior is part of the implementation boundary for this PR.

## 7. Resource and cancellation boundary

The reference implementation's per-input limits are inclusive maxima:

| Resource | Maximum | Above maximum |
| --- | ---: | --- |
| Input bytes, per active non-NULL row | 1 MiB | Reject before converting/copying for parse |
| Scanner tokens | 16,384 | Reject before parser invocation |
| Scanner-counted `()`, `[]`, `{}` nesting | 512 | Reject before parser invocation |
| Formatted statement bytes | 4 MiB | Reject; never truncate |
| Serialized remote SQL-mode error plus detail | 4 KiB | Replace oversized diagnostic with a bounded generic error |

The scanner uses the effective SQL-mode flags; lexical failures are left to the
parser so its source-position diagnostic is preserved. Nesting is the
pre-parser delimiter count, not a separately measured AST-depth or byte-memory
limit.

These limits cap source size, lexical complexity, recursive delimiter depth,
formatted output, and forwarded diagnostics. The current implementation does
not impose a byte-exact cap on total Go heap used by the parser AST or its
temporary allocations. Those allocations are structurally constrained by the
input/token/nesting limits and are per-row temporaries, but the 4 MiB formatter
limit is not a total-memory budget. There is also no cluster-wide aggregate
memory or concurrency budget in this function contract. If a hard heap-byte
ceiling is required, it needs separate measurement and implementation before
this design can claim it.

Cancellation is checked before work, periodically during the pre-scan, and
after parsing/formatting. Parsing receives the evaluation context. No separate
wall-clock SLA is promised beyond cancellation/error propagation and the
input-complexity limits.

## 8. Stability, security, and rollout

The source commit ID is a source-revision compatibility fence; it is not part
of the digest.
Identical formatted bytes always yield identical digests, regardless of build.
The function only guarantees that a fixed input under the same executable,
formatter, and effective SQL mode will keep producing the same result. A
formatter, parser, or SQL-mode behavior change may change the formatted bytes
and therefore the output in a later build. The current remote fence compares
source commit IDs only; it does not attest compiler flags, build tags, linked
dependencies, or artifact identity. If MatrixOne permits behaviorally
different binaries from one source commit, the fence must be strengthened
before those binaries can safely share this remote scope. Consumers must not
treat the value as a cross-upgrade persistent key, wire format, MySQL digest,
or security token.
This revision has no versioned digest prefix or migration guarantee.

The hash is not anonymized: literal values that survive formatting affect it.
It can reveal equality/repetition of statements and must not be used to protect
SQL secrets. Callers remain responsible for access control and retention.

For deployment, all remote workers selected for a statement-hash pipeline must
report the same full source commit. A heterogeneous cluster may continue
serving plans that do not use this function; this function's remote scopes fail
closed until an eligible worker is selected. No automatic fallback or digest
migration is part of this revision.

## 9. Alternatives considered

- Hash raw SQL bytes: simple, but treats whitespace/comments/formatting as
  identity and does not implement the chosen AST-rendering contract.
- Reproduce MySQL's normalized lexical digest: required for issue #23024, but
  it is a different compatibility feature and is intentionally not mixed into
  this native API.
- Hash a semantic or Merkle AST: would require a new, versioned node identity,
  child ordering, literal policy, and compatibility contract. It is not needed
  for hashing the existing formatter output and is out of scope.

## 10. Ownership and validation gates

PR #27988 is the implementation vehicle and tracking record. Issue #23024 is
related context, not the owner of this incompatible native behavior. The SQL
function/execution maintainers own the implementation through this PR; no
separate tracking issue is required by the agreed workflow.

The implementation contract is complete when the following decisions and
validation boundaries hold:

1. The function name/API and build-scoped (non-persistent) stability promise.
2. Fail-closed, no-fallback behavior for mixed-build remote workers.
3. The exact source commit ID is the compatibility fence for this remote
   contract; artifact/build configuration identity is outside this revision.
4. The stated structural resource limits and the absence of a byte-exact total
   Go-heap ceiling.
5. The SQL function/execution maintainers are the owner, and PR #27988 is the
   tracking record for the public SQL API.

Implementation evidence should cover:

- exact golden hash output and lowercase 64-byte encoding;
- whitespace/comments/formatter normalization, quoted identifiers, user
  variable canonicalization, and literal/name distinctions;
- parser-accepted and parser-rejected inputs, including MySQL syntax that
  MatrixOne does not support, proving that parser failure never yields a hash;
- SQL-mode snapshots, resolver errors, NULLs, masks, zero rows, and local and
  remote all-masked/empty-row paths;
- every limit at the exact maximum and one unit above, plus cancellation and
  formatter failure;
- remote protocol below/at the minimum, missing/invalid/matching/mismatching
  build IDs, capability-probe errors, and multi-hop identity preservation;
- a SQL-level test exercising the public function on the exact implementation
  head, plus the relevant focused Go tests and CI checks.

The document records the implementation contract; the listed evidence and CI
checks are the remaining validation gates for the PR.
