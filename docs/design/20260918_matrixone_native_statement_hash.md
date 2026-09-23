# MatrixOne-native statement AST fingerprint

- Implementation: [matrixorigin/matrixone#27988](https://github.com/matrixorigin/matrixone/pull/27988)
- Related request: [matrixorigin/matrixone#23024](https://github.com/matrixorigin/matrixone/issues/23024) asks for MySQL-compatible `STATEMENT_DIGEST`; this design does not implement that contract.
- Owner: SQL frontend and statement-telemetry maintainers.
- Revision: `matrixone-native-statement-ast-fingerprint-2026-09-23-r7`

## Purpose and boundary

Record a MatrixOne-native fingerprint for the effective parsed statement AST in
the existing statement telemetry field `statement_fingerprint`. This is a
syntax/formatter fingerprint for observability; it is not a SQL function, a
semantic plan hash, a durable identifier, or a MySQL digest.

The implementation adds no public SQL API, telemetry schema, query-plan
protobuf, remote-execution protocol, or optimizer behavior. Statement hash is
computed by the frontend that owns the admitted AST and then copied to the
existing telemetry record. Remote execution of the query does not require a
remote hash evaluator or per-node hash agreement.

## Definition

For an admitted statement AST `A`, use MatrixOne's existing MySQL-dialect AST
formatter with quoted identifiers, single-quoted string literals, and
canonical case-insensitive user-variable spelling. The fingerprint is:

```text
lowercase_hex(SHA-256([]byte(format(A))))
```

Only the formatter output is hashed. Raw SQL, SQL mode text, build identity,
plan bytes, parameter values, catalog state, and runtime values are not added
to the hash. SQL mode still affects the normal parse result; the fingerprint
represents the resulting AST. Formatting behavior can change with parser or
formatter changes, so equal output bytes yield equal hashes but the hash is not
a cross-version or persistent key.

The AST formatter output is capped at 4 MiB. Empty output, a formatting panic,
output-limit overflow, or cancellation observed before or after formatting
produces an absent fingerprint. These telemetry-only conditions do not turn an
otherwise valid query into an error and never hash truncated bytes. The
formatter is synchronous; the cancellation checks do not promise interruption
while a formatter call is in progress, and the output cap is not a total
parser/formatter heap limit.

## Capture and ownership

1. The frontend parses the statement using the active SQL mode.
2. Existing frontend hint/remap rewrites are applied.
3. Before the first plan build or optimizer mutation, the frontend formats the
   effective AST once and retains only the resulting string and whether capture
   was attempted. The AST remains owned by its existing statement/wrapper/cache
   lifecycle and is not retained by fingerprinting.
4. `RecordStatement` copies the captured value to the existing telemetry field.

Each statement in a multi-statement request has its own capture. A genuine
reparse/replan captures the replacement AST again. A cached AST is never
formatted after planning merely to reconstruct a missing fingerprint. If a
cached plan was created while tracing was disabled and tracing is enabled
before reuse, the cache entry is evicted before a wrapper borrows its AST; the
normal parse/admission path then captures the fingerprint. A formatter failure
is a completed best-effort attempt with no value, so it does not trigger an
endless reparse loop.

For prepared statements, the stored value is captured from the parsed,
rewritten prepared-body template before its first plan build, and stored as a
string on the existing prepared-statement owner. Text and binary `EXECUTE`
records use the same template fingerprint; parameter bindings are excluded.
The PREPARE command itself may have its own fingerprint for its own telemetry
record. A failed EXECUTE lookup has no body fingerprint. Reprepare creates a
new template capture. The template is captured at PREPARE even if tracing is
currently disabled: tracing may be enabled before a later EXECUTE, by which
time planning may already have mutated the retained AST. This incurs at most
one bounded formatting pass per prepared template.

## Non-goals and compatibility

- Do not add or retain `MO_STATEMENT_HASH(sql)` or another runtime scalar.
- Do not parse raw SQL a second time for fingerprinting, add a digest lexer,
  build a Merkle tree, or hash a logical/physical plan.
- Do not change SQL parsing, planning, optimization, execution routing, or
  result semantics to produce a fingerprint.
- Do not add statement-hash fields to pipeline/query protobufs or gate remote
  execution by protocol version, build ID, or per-node hash capability.
- Do not claim MySQL `STATEMENT_DIGEST`/`DIGEST_TEXT` compatibility or claim
  that issue #23024 is resolved.

The implementation is tracked by PR #27988. Existing telemetry retention and
access-control rules apply; the fingerprint is not anonymization or a security
boundary, because formatted literals can contribute to it.

## Validation contract

Focused tests cover the exact formatted-byte hash, canonical formatter options,
AST non-mutation, output limit at and above the boundary, empty output,
formatter panic, cancellation, tracing-disabled admission followed by
trace-enabled cache lookup, cache ownership during eviction, multi-statement
alignment, and prepared template reuse across text/binary execution and
different parameter bindings. Existing statement telemetry tests cover the
unchanged field serialization and reporting path. SQL parse errors remain
ordinary frontend query errors and never produce a successful fingerprint.
