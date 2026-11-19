# SDK follow-up contracts: boolean OR and mapped file availability

Revision 2 — 2026-10-26. Design supplement for PR #31262, issues #31259/#31260
and the failing SDK workflow contract. Actual designer/reviewer:
`gpt-6.1-sol` with `xhigh` reasoning, session `/root/sdk_quality_design`.
Implementation input HEAD: `46c106fa153d9fc4cc0ce0767fc1d5119e5491d7`.
The external task record contains the exact design hash and gate decision.
The prior three quality designs and complete overall reviews remain authoritative;
this supplement changes only the boundaries below. Revision 2 narrows filename
compatibility after native reserved-character probes; the bounded validation and
connection ownership mechanisms are unchanged.

## Evidence and invariants

Issue snapshots and causal FulltextQueryBuilder/FulltextFilter, FileRelation,
ArrowStream, read_files, write-transaction and workflow/test code were read.
Reuse `wave3-native-baseline.log` / `probe-wave3.py` and
`wave3-file-baseline.log`: native old group forms fail/drop matches; +(alpha beta)
and +match +(alpha beta) are usable. Missing second mapped file produces partial
query/import despite the client files existing. Native bounded DATALINK reads on
4.2.5 distinguish missing (exception), existing empty (NULL) and nonempty (one
byte), including Stage sources. No completed real-server discovery is repeated.

Required invariants:

1. OR grouping preserves each supported branch's eligibility. Unsupported branch
   structure rejects before changing the parent or executing SQL; it is never
   flattened by deleting a conjunction/exclusion.
2. Each explicit mapped server source is checked using the server's file-service
   authority, against the same identity scanned by the external table. A missing
   mapped file never passes an already-observable availability check as empty.
3. Checks are bounded, foreground-owned and retain the scan/write connection.
   They do not materialize entire files or acquire a second pool connection
   while a stream/write already owns one.
4. A late source failure cannot be labeled a successful completed stream/import.
   Import checks before commit; emitted stream batches cannot be retracted.
5. Version-scoped CI targets retain their manual-only, closed-target admission;
   a stale test expectation is corrected without weakening the gate.

## 1. One supported boolean OR group (#31259)

Replace the per-child parenthesized groups with one required positive OR group,
for example +(alpha beta). Requiring that group matters when the parent also has
must('match'): the OR clause must not become merely optional scoring. Preserve
other parent terms/operators; no SQL-level MATCH OR or second MATCH is introduced.

Use one pure validator/converter shared by the query-builder entry and its filter
wrapper. A supported child is either a single required positive atom or a flat
set of ordinary optional positive atoms (an OR). A single optional atom is included.
Reject multiple required atoms in one child (conjunction), required-plus-optional
mixtures, exclusions, negative/weighted terms, phrases, nested groups or other
raw boolean syntax that this representation cannot preserve. Initially admit
plain Unicode word atoms and explicitly recognized trailing-prefix-star atoms;
reject whitespace/embedded boolean operators rather than guessing their meaning.
This is a bounded subset, not a new fulltext parser or DNF expansion.

FulltextFilter.group additionally requires the same search columns and BOOLEAN
mode across recipient and children. Direct FulltextQueryBuilder has no columns/
mode to validate. Reject empty/malformed children and empty argument lists with
clear existing TypeError/ValueError. Validate every child first, copy elements,
then append one group atomically; never mutate children or retain their mutable
lists. Failed input leaves the parent unchanged. Repeated build is stable, and
later child edits cannot alter an already accepted parent group. Existing explicit
FulltextGroup APIs outside this entry retain their prior contracts.

Acceptance on current and 4.2.5: the reported must/must and encourage/encourage
forms return exactly alpha-only, beta-only and alpha+beta rows; exclude neither/
NULL rows. Compare public ORM, compiled direct SQL and an independent verified
native OR string, plus a parent required-term control with extra nonmatching rows.
Test multiple optional atoms, prefix where supported, incompatible columns/modes,
unrepresentable AND/exclusion/phrase/nested cases, no parent mutation on rejection,
child reuse and repeated builds. Reuse the existing term STRING renderer and
algorithm/connection lifetime; no new SQL escaping or cache owner is added.

## 2. Bounded mapped-source checks (#31260)

A small pure normalizer returns each source's canonical external scan path and
bounded DATALINK URI together. Store immutable source identities in FileRelation.
Support POSIX absolute file paths, local file:/// URIs and stage:// references.
DATALINK encodes file path bytes so reserved characters do not accidentally become
URL query/fragment syntax. This preserves intended identity; it does not guarantee
that every filename is representable by each server/file-service provider. A file
URI is decoded once to its bare absolute scan path and encoded consistently for
its probe.
Stage paths retain the same stage authority and normalized path for scan/probe.
Never resolve a server-relative path against the client's working directory.

Reject NUL, unsupported schemes/remote file authorities, user-info, user-provided
URI query/fragment/range and relative paths before DDL/I/O. The external scanner
uses Go path.Match: reject decoded path glob metacharacters (* ? [ ]) and backslash
rather than claiming they are literal filenames. Apply this after local glob
expansion and URI decoding, so encoded metacharacters cannot bypass it. Local
input discovery may still expand normal globs; each resulting server mapping
must meet this explicit-file contract. Qualify reserved-character identity with
both native probe and actual external scan, not just URI serialization. On the
observed 4.2.5 server, local/file-URI percent and apostrophe names are positive
cases, but hash/space file paths are natively rejected; a Stage percent path can
resolve to an unreadable directory and errors even when encoded, while Stage
apostrophe works. These inputs must fail clearly in pre-DDL native validation
when unsupported, never retry against a stripped, decoded-again or different
file. No kernel/path-provider workaround or universal reserved-name support is
promised. Keep static ambiguity rejection distinct from native provider errors.

For each immutable source run a server-native query equivalent to
`SELECT length(load_file(CAST(%s AS DATALINK)))` with SDK-owned offset=0,size=1.
Use the actual qualified DATALINK overload, never bare VARCHAR load_file or a
whole-file fallback. At most one byte per source is read by this operation;
return only length, not file content. Both native NULL (empty) and length 1
succeed; NULL is not classified as missing. Native missing/permission/protocol/
unsupported-function errors propagate with their category/cause. Do not swallow
errors as unknown, retry as a different path, or drop that source from the UNION.
An existing empty file can still fail its format parser; availability is not
Parquet/CSV/JSON validity. DATALINK/file-service read authorization is required.

Keep local schema/record checks as their existing owner, once per public ingress.
Use a separate bounded server-check helper accepting an execution adapter:

* Construction validates all normalized mappings before exposing a usable relation
  (prefer before its first CREATE); failed construction preserves the existing
  reserved-table cleanup owner if DDL has already begun.
* .sql validates local sources and server availability through one ordinary
  checkout. Returned SQL text does not pin files or promise future availability.
* query validates local inputs, then passes a private validation callback into
  ArrowStream. Immediately before its SELECT, validate every server source on the
  stream's existing raw connection; at true cursor EOF validate again on that
  same owner. A normal short-lived DBAPI cursor executes each one-row check and
  closes in finally; run the post-check only after the streaming result is fully
  consumed. No second checkout or concurrent cursor command during an active
  unconsumed result, and no whole-result buffering for validation.
* import_into validates local inputs, then checks every server source through the
  existing write connection before INSERT and again after INSERT but before COMMIT.
  A post-check failure raises through the existing rollback/settings/cleanup owner,
  preventing a successful partial import. Do not rerun expensive local full-file
  checks after DML or create independent per-source write transactions.

A post-stream failure is terminal and must propagate before successful exhaustion/
to_arrow completion is reported. Already yielded batches remain partial data;
callers must observe successful stream completion before treating a dataset as
complete. Early close does not certify completion or require a second scan.
Preserve primary read/check errors over cleanup errors under existing lifecycle
rules. No new task, background validator, staging table, session-mode switch or
retry loop is introduced. Sync driver timeouts govern these synchronous calls;
keep the stream's existing network deadline. Count O(number of explicit files)
checks, with existing max_files (default 256), constant probe payload/memory and
one live validation cursor. Provider range-read internals are server-owned.

This is observation, not an immutable filesystem snapshot. Removal after a
successful precheck but before scan is caught by the postcheck if still absent;
replacement, removal-and-restoration between checks or a change after final check
is not ruled out. .sql handed to arbitrary external execution has no later check.
Do not claim atomic reads, content equality between client/server files, or exact
object-version locking. A kernel change or materialized immutable copy would be
required for those stronger guarantees and is outside this correction.

Acceptance on both servers: two complete mapped files return/import all exact
rows; first/second/all missing fail without target changes; valid Stage files and
missing Stage files have the same contract. Delete a mapping after relation
creation, before stream SELECT, after first yielded batch and after INSERT; use
deterministic hooks to prove error/completion/rollback behavior and zero checked-
out resources. Include empty-file existence, qualified local/file-URI percent/
apostrophe and Stage apostrophe positive names, and native-rejected hash/space/
Stage-percent negative cases before DDL. Also cover invalid glob/URI query inputs,
small pool ownership, missing/permission/cleanup failures and successful reuse.
Check exact file identity for positive names and propagate native failures for
unsupported names; a positive fixture must not combine known unsupported
characters and then attribute that provider error to SDK validation. Verify file reads remain size=1 with a large-file discriminator
or bounded executor contract. Existing Arrow/FileRelation lifecycle tests remain.

## 3. SDK-only delivery and separate CI configuration

The user explicitly requires the SDK PR to avoid unrelated kernel tests. The
trusted change-scope classifier currently treats a python-sdk.yml change as full
scope. It is loaded from github.workflow_sha; an untrusted PR cannot safely change
that authority for its own run. Keep that trust boundary intact.

For SDK PR #31262, restore .github/workflows/python-sdk.yml exactly to trusted
main revision 861a2c4cb6 and retain no other .github path delta. All SDK product,
regression and design changes remain under clients/python/**. Move the complete
new tests/contracts collection into tests/offline/contracts, update the local
runner to select tests/offline once and update current testing references. The
existing trusted main workflow already collects tests/offline and analytics;
assert that the relocated contract collection is nonempty and included there,
without duplicate collection or name-based test selection. Preserve all original
regression assertions, optional-dependency separation and local/server runner
capabilities. Historical design/review paths remain historical references; current
TESTING gives the new path.

This makes the current SDK PR classify as SDK-only immediately and removes the
stale CI assertion mismatch from its changed workflow. Do not force a scope output,
checkout PR-head privileged scripts, silently skip failed security tests, or weaken
the manual-only online admission. Cancelling an unrelated expensive run follows
the user's explicit instruction; cancellation is not itself a future routing fix.

Preserve the previously delivered enhanced workflow in a separate supporting CI
configuration PR based on trusted main. Its coherent scope is the enhanced SDK
workflow, its exact closed-target contract, narrowly matching scope classification
and the independently unconditional Iceberg workflow dependency filter. In that
PR, update change-scope.test.cjs to assert the exact server matrix
[4.2.5,current-source] and matrixone-sdk-${{ matrix.server }} environment expression.
Retain executable event-gate assertions: self-hosted online jobs require
workflow_dispatch plus inputs.online=true, never PR/workflow_call/push or a
false dispatch input. Retain offline read-only fork checkout, provenance, unique
artifacts, fatal checks and required nonempty zero-skip server contract selection.
The classifier exception covers the exact SDK workflow path only; general CI
classifier/trust changes must not acquire an SDK-only exemption.

Iceberg's workflow filter must exclude Python/docs-only changes while retaining
all owning Go source/tests, Go module/workspace dependencies and relevant build/
workflow inputs. Reuse repository scope/dependency authorities and test path cases,
rather than inferring the dependency closure from a few currently failing files.
The supporting CI PR must run its Node/workflow contract suite and explain actual
trusted-base activation. It cannot claim to govern its own old-base run merely
because the PR contains the new classifier. Do not automatically merge it or alter
external environment configuration as part of this SDK fix.

No SDK GitHub environments are presently configured. Test PASS validates workflow
structure, not protection rules, credentials, runners or successful deployment.
Missing-target configuration must continue failing clearly. Until supporting
configuration enters the trusted base, a separate unconditional Iceberg workflow
can still trigger its Go dependency check even on a pure SDK PR; distinguish this
from eliminating kernel UT/BVT through SDK-only scope and disclose that limitation.

Run focused changed-file Python/static checks, new regression and affected
analytics lifecycle suites, the existing trusted offline selection, and required
two-server contract selections locally. Run Node/routing tests in the supporting
CI closure. Reuse unchanged complete reviews and dependency evidence. Record the
actual source server version/build identity when its binary predates the latest
merge; do not present that older binary as a newly built merged HEAD. #31258 is
the already-fixed Stage case behavior from W5: verify and link existing mixed-case
proof rather than inventing another production change.

Decision: accepted implementable design pending exact-hash gate record. No kernel
change, full-file materialization or unsupported blanket version claim is needed.
If native current-source qualification invalidates the bounded DATALINK behavior,
stop that mechanism and revise the design rather than silently falling back.
