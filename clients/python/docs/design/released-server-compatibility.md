# SDK contracts on released servers

Revision: `PR32671-RELEASE-CONTRACT-2026-11-13-r1`.
Implementation: [#32671](https://github.com/matrixorigin/matrixone/pull/32671).

## Problem and invariant

The SDK targets released MatrixOne servers. A future kernel fix is not a
substitute for usable behavior on the current release. For each operation the
SDK must either preserve its values and semantics through an equivalent
implementation, or report an actionable limitation without claiming success.
An unsupported result must not hide a permission, connection or unrelated SQL
failure. Native feature support and SDK compatibility are separate outcomes.

The pinned latest release is v4.2.8, revision `10d204b`, published 2026-11-04.
Direct probes prove that inserting whitespace between numeric tokens and dollar
aliases preserves values and names; adopting the SDK dialect already reflects
BOOL and uppercase view types correctly. Native CREATE STAGE ENABLE=FALSE
silently creates an enabled Stage. Quoted Stage values fail catalog SQL;
quoted ASYNC FULLTEXT can acknowledge creation without producing correct index
results. Empty branch identity, some ENUM/SET SHOW CREATE output and Parquet
FLOAT32 infinities require native fixes. FLOAT64 and SQL FLOAT casts preserve
infinity; they do not make silently widening a requested FLOAT32 acceptable.

This changes the compatibility contract and crosses execution, reflection and
analytics consumers. Review this design before implementation.

## Decision and alternatives

Use the selected physical driver's handshake banner to recognize the exact
qualified official v4.2.8 profile. Do not use test environment variables, a
client-wide version assertion, or a guessed minimum fixed release. Main,
source builds and unknown banners retain their native execution paths. A
patched custom build should identify itself separately from the official
release. Future support decisions require qualification of that identity.

Install two bounded, engine-local SQLAlchemy listeners with the existing
compiler installation owner, covering sync/async Client, Session, adopted
engines, reflected operations and analytics on the actual checkout:

1. Before execution, normalize only numeric/dollar boundaries using the
   existing quote/comment-aware literal scanner. Insert whitespace without
   changing the value, alias, parameters, SQL mode or transaction owner.
   Leave malformed SQL for its existing error path. Read executing SQL mode
   only when backslashes/ambiguous double quotes make parsing necessary.
2. Before known unsafe DDL on the exact release, raise an actionable QueryError
   for quoted/reserved FULLTEXT identifiers, explicit ASYNC gojieba, apostrophe
   Stage names/values, disabled Stage creation and absent VECF4/VECF8 types.
   Recognize SQL tokens outside literals/comments. Do not replace identifiers,
   remove options, switch ASYNC to sync, or widen storage types automatically.
3. On a narrowly matched real native failure, add an actionable QueryError for
   empty-branch identity rejection, ENUM/SET SHOW CREATE serialization and
   Parquet FLOAT32 infinity rejection. Preserve the real exception, errno and
   SQLSTATE. Never retry a write or compensate with DROP.

Pre-execution rejection uses phase `compatibility`, committed=False and
statement_acknowledged=False for that statement. Native failures retain unknown
effect status until the existing transaction/analytics owner proves rollback;
the original exception remains reachable. Messages identify the release,
operation and usable alternative/upgrade requirement without copying SQL,
credentials, values or object paths. The release profile is not a claim that
every historical patch has the same limitations.

Existing Arrow-input Parquet staging and SDK BOOL/type-registry adaptation
remain equivalent implementations. An unadopted MySQL engine remains owned by
SQLAlchemy; document and test Client.from_engine as the SDK reflection entry.
Do not patch MySQL's global registry or invent lost ENUM view provenance.

Alternatives considered: requiring an unreleased kernel leaves SDK users
without a contract; swallowing native errors produces false success; automatic
DROP/name replacement/synchronous substitution/type widening changes requested
semantics; retry after rejection can repeat durable effects. A new negotiated
capability cache or destructive feature probes add state and cleanup with no
safe read-only oracle for these defects. The selected exact-release profile
requires no persistent capability state or destructive probes.

## Ownership, cost and unhappy paths

The existing engine owns listener lifetime; no worker, cache, queue, permit,
timer or secondary checkout is introduced. Reading the physical handshake is
constant-time. Nonmatching profiles return immediately. Scanning relevant SQL
is linear and retains no statement; unrelated release statements take the
existing path. Any required SQL-mode lookup uses the current connection and
cannot recurse into the guarded DDL or normalization path.

Cancellation, transport errors and unknown native failures propagate unchanged.
Preflight acquires no resource and performs no mutation. It does not roll back
caller-owned work. Native failure translation does not choose another write
path; pool invalidation, rollback, commit uncertainty, staged-file unlink and
async pending-close admission retain their current owners and bounds. No
catalog, storage, wire, authentication or upgrade format changes occur.

## Validation and reporting

| Contract | Proof |
|---|---|
| Numeric portability | Independent native spaced SQL and all Client/Session/export facades preserve Decimal/exponent values, alias names, comments and exact JSONL bytes; identifier/quote/malformed controls remain. |
| Safe release refusal | Known unsafe DDL raises the exact compatibility phase/feature/advice before native execution, no catalog object or altered source, and pool/connection remains reusable. |
| Real native failure | Independent raw driver reproduces the matched errno; SDK preserves cause/errno and reports the alternative. Permission/transport/wrong-text/wrong-statement controls cannot become a compatibility pass. |
| Reflection | Positive SDK-adopted BOOL/table/view values on release; retain native MySQL outcomes separately. Unsupported ENUM serialization has a precise SDK diagnostic, not invented domain/replay success. |
| FLOAT32 | Release retains the precise unsupported-type outcome and full cleanup/rollback checks; an explicit FLOAT64 alternative preserves every special value. Main/candidate retain all FLOAT32 positives. |
| Version/routing | Exact banner and suffix/unknown/source controls, different physical pool targets, adopted sync/async and SQLAlchemy 1.4/2.0/2.1; plain engines remain unaffected. |
| Complete qualification | Full release/main online and analytics plus owning offline suites, static checks and packaging. Preserve all unique inputs, privileges, errors and resource-lifecycle assertions. |

JUnit feature properties distinguish `supported`, `equivalent-implementation`
and `unsupported-with-actionable-error`. A green SDK contract suite must never
be described as complete native feature support. Keep the original failing
native baseline and all previous evidence. Update adjacent comments, workflow
policy, compatibility guide and both PR descriptions to this release-first
contract. New server releases and main require their own positive qualification.
