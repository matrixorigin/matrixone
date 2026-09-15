# MySQL `STATEMENT_DIGEST` / `STATEMENT_DIGEST_TEXT`

- Status: in-progress
- Start Date: 2026-09-09
- Authors: MatrixOne SQL automation
- Implementation PRs: [#27988](https://github.com/matrixorigin/matrixone/pull/27988),
  [#27990](https://github.com/matrixorigin/matrixone/pull/27990)
- Issue: [#23024](https://github.com/matrixorigin/matrixone/issues/23024)
- Design-review decision: pending independent approval

## Summary

Add the MySQL-compatible scalar functions `STATEMENT_DIGEST(sql)` and
`STATEMENT_DIGEST_TEXT(sql)`. Both validate one SQL statement under the
initiating session's SQL mode and share the bounded MySQL 8.4 token contract;
the former returns the SHA-256 digest as a 64-character string and the latter
returns MatrixOne's established rendered text projection. The feature is safe
for remote CN execution only when the receiving node advertises the shared
MORPC capability.

This document is the versioned design revision for #27988/#27990. It records
the consolidated feature contract before design approval; the approval record
must be made by an independent reviewer in the PR and is intentionally not
self-certified.

## Motivation and scope

Digesting the normalized token stream groups statements that differ only in
literal values while retaining syntax and mode-dependent token identities. It
is useful to clients that use MySQL's `STATEMENT_DIGEST` contract for query
analysis and compatibility testing.

The scope is deliberately limited to MySQL 8.4-compatible digest behavior.
This change does not introduce a query cache, a server-side digest registry,
new persistent metadata, a new SQL parser, or general parser compatibility
changes outside these functions.

`STATEMENT_DIGEST` (function ID 579) and `STATEMENT_DIGEST_TEXT` (function ID
580) are separate result projections over one contract. The hash path uses the
bounded token bytes from `mysql_digest`; the text path retains the existing
MatrixOne rendering contract, including omission of a terminal client
semicolon. They share parser admission, provenance/error handling, SQL-mode
resolution, setting snapshots, and the MORPC v73 fence. The text ID is 580
because the unreleased #27990 draft used 579 before #27988 reserved that ID for
the hash; no released plan ID is being reinterpreted.

### Why normalization is required

The result is SHA-256 of MySQL's normalized token bytes, not SHA-256 of the
input string. For example, `SELECT 1` and `SELECT 2` must share a digest while
raw-string hashing would separate them; comments, identifier quoting,
character-set introducers, and SQL-mode-dependent token identities likewise
need canonical treatment. The approximately 8,000 lines in the internal
package are primarily generated token/keyword data, the derived lexer, and
regression tests. A bounded linear scan plus the existing MatrixOne parser is
less coupled and more auditable for this MySQL-specific contract than
reusing the full general-purpose parser as a digest implementation.

## Compatibility contract

### SQL behavior

- The input is a single statement interpreted under the initiating session's
  `sql_mode`. The MatrixOne parser remains the authority for statement
  validity; a token lexer alone must not turn invalid SQL into a successful
  digest.
- A SQL `NULL` argument remains a SQL `NULL` result through the normal vector
  function contract. Empty or whitespace-only input is not treated as a
  statement. Ordinary-comment-only input retains MySQL's empty-token-stream
  digest only after the lexer has positively classified it as comment-only.
- Prepared-statement parameter markers in the argument are rejected, even if
  the general MatrixOne parser accepts them. Unterminated quotes and other
  lexical errors are returned as errors rather than being mistaken for an
  empty digest.
- The target digest token stream is MySQL 8.4. The compatibility boundary
  includes literal reduction, identifier quoting, comments and optimizer
  hints, executable-comment version guards, character-set introducers,
  `WITH ROLLUP`, and DDL `NULL` grammar roles. It includes the token-changing
  SQL modes `ANSI_QUOTES`, `NO_BACKSLASH_ESCAPES`, `PIPES_AS_CONCAT`,
  `HIGH_NOT_PRECEDENCE`, and `IGNORE_SPACE`; `ANSI` expands to its MySQL
  composite behavior, including `IGNORE_SPACE`.
- `max_digest_length` limits the recorded binary token stream. Its accepted
  range is `[0, 1048576]`; zero is an explicit valid setting, not a signal to
  skip SQL validation. The digest remains SHA-256 of the recorded token bytes
  and therefore has the MySQL-compatible fixed 64-character result shape.

### Ownership and execution path

`pkg/sql/plan/function/func_statement_digest.go` owns both SQL scalar functions:
it resolves the session snapshot, invokes the selected digest projection, and
uses the shared parser-admission helper for the complete input. The internal
`mysql_digest` package owns only deterministic lexical normalization and a
bounded token store; it has no session, catalog, goroutine, or network
ownership. The existing vector function framework owns NULL propagation,
batch iteration, allocation lifetime, and cancellation context.

The digest package is a reviewed internal copy of `mysql-digest`, with MatrixOne
compatibility deltas maintained in its README and regression suite. A future
MySQL target-version change must update both the version constant and the
differential test corpus; silently changing token semantics is not allowed.

### Session snapshot and forwarding contract

The initiating CN is authoritative for the variables that affect the digest:

| State | Owner and transport | Receiver rule |
|---|---|---|
| `sql_mode` | `process.SessionInfo.SqlMode`, serialized by the process codec | A remote CN honors the captured value. `EmptySqlModeSentinel` represents an explicitly empty coordinator setting and cannot be overwritten by a background resolver. |
| `max_digest_length` | `SessionInfo.MaxDigestLength` plus `MaxDigestLengthSet`, serialized in `pipeline.SessionInfo` fields 18/19 | A remote CN honors an explicit snapshot, including zero; an absent resolver uses the bounded default, while resolver/range/type errors fail closed. |
| MySQL executable-comment target | explicit digest option | The feature currently targets the declared MySQL 8.4 compatibility version; changing the target requires a design and oracle-test update. |

The codec copies these values at process serialization and reconstructs them
on receipt. A second remote forward preserves a decoded snapshot rather than
substituting a receiving node's compiled default. Resolver failures, invalid
variable types, out-of-range lengths, and malformed carried snapshots fail
through the evaluator or process-codec error path; they are not converted into
the default because that would make local and remote results diverge.

### Mixed-version safety and rollback

`STATEMENT_DIGEST` has plan function ID 579 and `STATEMENT_DIGEST_TEXT` has
plan function ID 580. Either may occur inside a remote scope. The coordinator
therefore rejects remote dispatch when the peer has no MORPC version, or
reports a version below `MORPCVersion73`. Version 73 is the shared minimum
fence because both functions depend on the same parser-admission, provenance,
and process-setting contract; the guard in remote planning is fail-closed and
names the required version in its error. The version check does not prove that
a transitional v73 worker has both function IDs, so rollout must also enforce
the capability-complete deployment rule described below.

The added protobuf fields are unknown-field compatible, but that alone is not
sufficient because an older CN also lacks the function ID. The protocol gate is
the wire minimum; the all-capability deployment rule is the authoritative
runtime rollout boundary. There is no stored data or migration: rollback
consists of draining statements that use these functions, restoring the prior
routing fence, and only then removing the combined build. A new coordinator
must not send a 580 plan to a transitional v73/hash-only CN; it waits for the
capability-complete deployment (or returns a deterministic not-supported error).

### Why the implementation is split across #27988 and #27990

The PRs are intentionally separate review units for two result projections,
not two competing normalization engines. #27988 owns `STATEMENT_DIGEST` and
reserves function ID 579 for the bounded MySQL token-byte/SHA-256 projection;
#27990 owns `STATEMENT_DIGEST_TEXT` with function ID 580 for MatrixOne's
client-facing rendered projection. They share parser admission,
provenance/error disclosure, statement-setting snapshots, protobuf fields, and
the MORPC v73 fence, but the output bytes are deliberately different and are
tested against separate contracts.

When the source PRs land independently, #27988 should land first so 579 is
stable, then #27990 rebases onto that exact head and integrates the shared
contract plus 580. That is a review/ownership order, not an independent runtime
deployment plan: with one v73 fence, a deployable release must contain both
implementations and must reach every candidate worker before a coordinator
sends a 580 plan. A squashed landing is valid only if it preserves both IDs and
one v73 fence. If the hash PR changes a shared helper, setting field,
target-version rule, or protocol allocation, the text PR must rebase and rerun
its design/test matrix; #27990 must not reserve or reinterpret 579. A worker
that has only 579 is not a valid target for 580; independent rollout would
require a separately approved capability/version fence. Rollback must drain
580 plans and restore the prior routing fence before removing the combined
build. This order and the shared validator/snapshot/fail-closed checks are the
mechanism that prevents version drift or result inconsistency during rolling
upgrades and rollback.

## Resource and failure contract

For an input of length `n`, lexical scanning and parser validation are linear
in `n` within the existing SQL parser input limits. Digest storage is bounded
by `max_digest_length` (at most 1 MiB); token collection stops recording when
that limit is reached but scanning and parser validation still consume the
complete input. The feature creates no cross-query cache, background task,
lock, channel, or retained session reference. Hash state and token storage are
per invocation and are released with the normal vector-function call.

Errors from variable resolution, range checks, parsing, or lexing are returned
to the caller. The sole parser-error exception is independently proven
ordinary-comment-only input. This separation is important for disabled digest
collection, malformed input, and no-emitted-token edge cases.

## Alternatives and trade-offs

1. **Use only the digest lexer.** Rejected: lexical success cannot establish
   SQL statement validity, and it previously admitted invalid SQL whenever no
   token bytes were recorded.
2. **Execute remotely without a capability fence.** Rejected: an older CN
   cannot resolve the new plan function ID. Unknown protobuf fields do not
   protect executor construction.
3. **Resolve session variables independently on every CN.** Rejected: SQL
   mode and explicit zero digest length could change after a remote hop and
   produce a different hash for one logical statement.
4. **Store unlimited normalized tokens.** Rejected: it weakens the existing
   `max_digest_length` resource contract. The selected bounded store preserves
   the MySQL setting while retaining full-input error checking.

## Validation and acceptance criteria

The implementation is accepted only when all applicable checks below pass on
the same PR head and an independent reviewer records design approval for this
revision.

| Contract | Required evidence |
|---|---|
| MySQL token compatibility | Versioned MySQL 8.4 oracle corpus covering literals, comments/hints, modes, DDL, invalid input, executable comments, and character-set introducers. |
| Parser/error authority | Pure-Go digest and function regressions for invalid SQL at `max_digest_length=0`, parameter markers, comment-only input, NCHAR escaping, and unterminated dollar quotes. |
| DDL context boundaries | CREATE/ALTER and table-level CHECK matrices covering DEFAULT/CHECK expressions, `NULL`/`NOT NULL`, JSON_TABLE response clauses, boolean/default forms, and state leakage between constraints and columns. |
| Session forwarding | Process-codec and function tests for explicit empty `sql_mode`, explicit zero/non-default digest length, malformed values, and second-CN forwarding. |
| Mixed-version behavior | Remote-planning tests proving v72 rejection and v73 acceptance for plans containing either digest function ID (579 or 580). |
| Concurrency and regressions | Focused race runs for digest/function state, repeated counterexample tests, `git diff --check`, and the required MatrixOne CI/BVT suites. |

The PR description records the concrete commands and CI evidence for the
current implementation. Any newly discovered MySQL grammar or mode mismatch
must add a focused regression and, where it changes this contract, a revision
to this document before approval.

## Rollout and review record

There is no feature flag or migration. The normal release process may expose
the functions once all nodes that can receive their plans satisfy MORPC v73.
Operational rollback is safe because no persistent state is created; planners
must avoid the function when targeting an older node.

Independent design approval is requested on #27988 and #27990 for this exact
consolidated document. The two PRs are not conflicting runtime implementations:
one supplies the hash projection and the other the text projection. They are
integrated here with distinct IDs and one compatibility fence so an old
receiver cannot execute either projection accidentally.
Future changes to the supported MySQL version, digest-length resource limit,
session snapshot fields, function ID, or MORPC allocation require a new
versioned design revision and re-review.
