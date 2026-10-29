# Systemic SDK boundaries — revision 2.3

This revision extends the existing synchronous file/Arrow designs. Its scope is
shared query/type/DDL behavior, async analytics, capability/outcome observation,
and Stage catalog/URI correctness. Existing native interfaces remain the data
transport. There is no second query engine, automatic uncertain-write retry or
row-INSERT fallback for unavailable Arrow LOAD.

A full synchronous wrapper would retain buffering and block cancellation at the
wrong boundary; native async cursors plus bounded conversion preserve streaming.
Holding a DB slot throughout staging permits a pool/worker wait cycle, whereas
separate admission breaks that cycle. A second LOCAL engine would lose Session
transaction ownership; granting authority on the selected existing connection
preserves it. Rejecting all quoted Stage names would leave the original defect
unresolved; a shared path-form resolver represents logical names using ordinary
URI path encoding and the same catalog owner.

## Compilation and execution ownership

One query normalizer serves Client and Session variants. SQLAlchemy owns mapped
physical identity, explicit projection and bind/result processors; SDK text
binding uses the existing lexical owner. Results preserve ordinals even when
labels duplicate. Snapshot mutations fail before dispatch. Table shorthand and
index compilation have shared owners across sync/async entry points.

The supplied Session retains its connection and transaction. Client calls use
their existing execution path. Administrative statements unsupported in active
transactions reject explicitly instead of committing another owner's work.
Acknowledged CREATE is distinct from a durable table after transaction rollback.
Batch inserts preserve first-row column order, fill missing values with NULL and
reject extra columns before I/O.

## Async wait graph and lifetime

Each client retains two blocking-worker permits and two staging permits across
reconnects. Streams reserve a worker before a DB slot. Upload preparation holds
no DB slot, and an upload releases its CPU lease before waiting for the pool.
Thus two staged uploads cannot prevent the stream holding the sole DB slot from
converting rows and completing. A separate single-job unlink lane makes cleanup
independent of busy conversion workers.

Cancellation never releases a still-running worker's permit. Concurrent-future
callbacks retain accounting when an asyncio loop closes. Each cleanup attempt
has its own immutable completion Future; a retry cannot replace an older
waiter's notification. Failed unlink retains the file and staging permit.

Submission is not publication: CPython may enqueue before thread creation
fails. A per-job gate permits user work only after successful submission and
registration of its Future, callback and staging producer. A failed executor
generation is retired without running its rejected queued job. Explicit reconnect
or cleanup retry may replace that executor while preserving the same admission
accounting and still-running old leases. Queries are never retried automatically.

Client query_timeout bounds each public async operation; stream entry and each
fetch have separate budgets excluding caller idle time. network_timeout bounds
actual socket operations, excluding local validation and CPU admission. Database
finalizers are retained and bounded; blocked foreign Python readers/filesystem
calls cannot be forcibly interrupted. A process kill may leave recorded owned
artifacts for explicit reconciliation.

## Transaction outcome and observation

Uploads/imports exclusively own one physical checkout and verify actual server
autocommit is off before writes. UTC/strict/PK settings remain through COMMIT.
Restoration to autocommit=1 occurs only after acknowledged COMMIT or ROLLBACK;
otherwise the transport is aborted/inactivated before pool return. Cancellation
before COMMIT never schedules a later COMMIT. Its owned staging file may be
removed after producer exit and that local ownership transition, without claiming
remote rollback has already finished.

Errors retain existing catch classes and available original driver details.
committed=True means acknowledged COMMIT; False means pre-COMMIT failure; None
means unknown/not applicable. Cleanup cannot replace a primary error. Optional
immutable query events contain bounded phase durations/outcomes, no SQL, values,
credentials or identifiers. Callback failures cannot change query behavior.

Capability observations belong to their observed server/session, not every
future checkout from an engine. Explicit unsupported/disabled overrides may
reject before staging. Otherwise Arrow support is checked read-only on the
selected write checkout before CREATE/LOAD. A bounded staging file may already
exist and is cleaned on rejection. Successful EXPLAIN does not certify support;
only the qualified explicit Arrow-format rejection establishes unsupported.

## LOCAL file authority

Default SDK connections negotiate LOCAL capability with driver reads disabled.
Only from_local_file grants the exact filesystem path, once, on the connection
selected by the existing execution owner. The literal statement's finally scope
revokes authority even if later SQLAlchemy hooks, execution or cancellation fail.
The per-physical-connection result guard checks the first response packet, so a
NULL row is never mistaken for a LOCAL request. A mismatched/replayed request
aborts without sending protocol bytes. No global driver patch is installed.

aiomysql 0.2 lacks a file-open switch; default and explicit False connections are
therefore guarded before its network/setup sequence. The constructed connection
is adapted back into SQLAlchemy's existing pool. Explicit False rejects local
imports; explicit True retains the legacy unrestricted driver opt-in. Adopted
engines are not silently reconfigured. Minimum/current driver qualification is
required for this narrowly scoped use of driver internals.

## Stage catalog and URI identity

Stage catalog check/create/update/drop and the resolver's catalog read use the
existing common SQL literal owner. Global account-name validation is unchanged;
Stage identifiers use the catalog's nonempty UTF-8, no-NUL, 64-character bounds.
The existing comma-separated credential storage format remains. Commas in
keys/values, equals signs in keys, malformed pairs and case-insensitive duplicate
keys reject before the first catalog mutation; equals signs in values remain
representable. Normal quoted values round-trip instead of failing after CREATE.

Combined Stage ALTER applies all specified options in the existing catalog
transaction. Validation precedes writes, and any later update failure rolls back
the entire alteration. Session-bound Stage ALTER rejects before dispatch because
this administrative operation does not belong to the caller transaction.

Stage names have their own case-insensitive namespace: the SQL grammar always
uses ident.Compare(), independently of lower_case_table_names. URI decoding uses
that same canonical identity for cache and catalog lookup; SDK metadata uses the
server's LOWER on its existing checkout. No table-policy state or extra query is
introduced.

Existing stage://name/path remains supported. Logical identifiers unsuitable as
URL hosts use stage:///percent-encoded-name/path. The resolver splits the first
escaped path segment before decoding once; encoded slash/percent remain part of
the name. Dot-only names use the existing host form. The SDK's shared Stage URI
builder and file/staging consumers preserve that identity. Nested resolution
uses the same owner. Export filename templates retain their percent syntax while
RawQuery bytes remain unchanged. Legacy host userinfo/fragment behavior remains
unchanged; the new path form rejects ambiguous fields.

Upgrade all CNs before using the new Stage name/URI contract; mixed old/new CNs
cannot guarantee routing compatibility. Released 4.2.5 remains a separately
qualified negative target for native Arrow LOAD and the new Stage literal cases.

## Validation and compatibility

Core Python 3.8 remains available without Arrow. Analytics requires Python 3.9+.
SQLAlchemy >=1.4.24 is the supported floor; 2.1 requires Python >=3.11. Driver-bound
execution is explicit and does not reinterpret existing SDK text placeholders.

Validation maps shared compilation to real SQLAlchemy controls, async ownership
to deterministic barriers and real one-slot network faults, protocol changes to
actual driver readers and native imports, and Stage SQL to parser/race tests and
public SQL/SDK round-trips. Same-host dual-CN, a local S3-compatible emulator,
constrained-memory million-row streaming and SDK process-crash recovery provide
separate evidence. These are not cloud or backend-crash certifications.

## Driver SQL-mode status

MySQL OK, EOF and deprecated-EOF responses derive NO_BACKSLASH_ESCAPES from
the executing Session's existing parser flags. Transaction status remains with
its existing owner; no SDK mode cache or duplicate state is introduced. This
keeps DBAPI parameter escaping consistent after SET, reset and reconnect. The
pre-authentication handshake cannot assert the authenticated tenant's mode.
Drivers which discard authentication OK or result-set EOF status need an
acknowledged session-setting OK before direct binding when global mode changes.
Managed SDK connections and the tested ordinary SQLAlchemy setup provide that
acknowledgement; caller-owned engines may skip it and must arrange their own
initialization. Arbitrary raw-driver first-query behavior is separately
qualified rather than promised by the SDK.
