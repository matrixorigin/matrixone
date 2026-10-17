# SQL file access ownership

Issue: #30185. Implementation: #30212.

## Contract

Database table privileges do not grant access to CN host files or configured
server file services. SQL access to those resources requires the existing `sys`
account's active `moadmin` role. This is the permanent server-resource contract,
consistent with other system-control SQL operations. A tenant's `accountadmin`
role controls its account, not the CN operating system.

A stage is tenant-owned location metadata. Resolving `stage://` identifies the
provider and target; it never adds file authority. Internal execution, absence
of table scans, a prepared plan, and a stored Datalink similarly confer none.
The resolved provider determines the rule, including S3 specifications that
select the local `disk` provider.

| Resource | Admission owner and behavior |
| --- | --- |
| Local path, configured named service, dynamic local-disk service | The current execution must carry `sys` / `moadmin` authority. |
| Remote argumented provider or remote stage | Use caller-supplied credentials or the existing explicitly selected MinIO anonymous mode. Stage status and tenant catalog rules remain applicable. |
| Dynamic provider auxiliary dependencies | SQL cannot name server certificate/keytab files or select server profile/instance credentials, regardless of role. |
| `LOAD DATA LOCAL`, INLINE | Client data: no CN filesystem authority is acquired. |
| Asynchronous indexed Datalink sources | Only argumented remote providers and remote stages are supported. Server-owned source references are rejected before installing the lifecycle or committing new indexed values, even for administrators. |
| Generated index storage files | The storage owner reads its own artifacts natively and supplies binary chunk values to ordinary SQL in the existing transaction. No SQL identity is elevated. |
| Saved result scan | Existing result metadata/privilege owner admits an engine-generated location; user SQL does not supply its raw path. |
| Backup | Existing system-admin statement owner controls backup execution. |

The existing OUTFILE disable switch is an additional operational control. File
admission applies when export is enabled. This fix does not claim to enforce
`secure_file_priv`; that compatibility variable does not define the policy.

## Owners and execution boundaries

Fileservice owns the single provider parser, dependency policy and actual
acquisition. `GetForETL`, its parsed-path form and `GetForBackup` require a
scoped SQL file policy; missing policy rejects every target before acquisition.
The parsed-path form checks agreement with the raw target and constructs from
its own authoritative parse. The parser preserves each factory's existing
provider semantics: ETL selects HDFS through the service name, while backup also
honors its `is-hdfs` option. Within each factory the admitted arguments are the
ones used for construction. Process helpers derive the policy from current
account/role names; planning preflight delegates to the same stateless rule.
Stageutil owns catalog resolution and confers no additional authority.

Policy is created only in actual file operations. It is immutable, scoped to
that operation and rebound from the executing process, including prepared and
remote execution. Existing statement identity propagation remains the source
of truth. No global query/process policy hook, grant cache or new wire field
is introduced. External owners bind both runtime and ExternParam contexts after
remote reconstruction; helpers consume that context rather than replacing it
with Background/TODO. Blank internal identity stays restricted.

Generic backup callers are SQL backup operations and follow the same policy.
Native configured file services and owned engine/index artifacts continue using
their existing constructors. Missing SQL context never selects a trusted route.
Local table dump has an existing native owner: its caller admits a service-less
target before construction, independently of punctuation in the filename.

Dynamic SQL providers use caller-only construction. Supplied optional tokens
stay empty when absent; AWS S3 and STS do not load default configuration. Role
acquisition uses explicit upstream credentials, propagates errors and keeps the
same upstream for refresh. The SQL-scoped MinIO STS client borrows its owning
operation context because the pinned SDK creates STS requests without one.

HDFS simple authentication retains its supplied username or fixed `hadoop`
default; that default does not read CN configuration or credentials. Explicit
password authentication keeps the existing HDFS contract; local keytabs are
rejected before construction. MinIO retains anonymous access only when
`no-default-credentials=true` is explicit and all credential/delegation inputs
are absent. Missing/partial object-store credentials otherwise fail; no new
anonymous syntax or mode is introduced for other providers.

Admission runs before local directory creation, stat, list, read or write in:

- Raw file functions, the non-inline `parse_jsonl_file` table function, and
  Datalink constructors/readers/writers/stat.
- OUTFILE, including CSV, JSONLine and Parquet output and split files.
- Server-side LOAD DATA and external-table readers, including automatic Hive
  partition inference before its first directory listing, at planning I/O and again
  when a prepared/retained source is compiled for execution.
- Writable external-table stage output, stage list/remove operations.
- Table dump/load and data-branch file output.

The active account/role pair is refreshed once per statement from the existing
session owner, propagated into child SQL and the existing process wire fields,
and rechecked at resource acquisition. No user expression can set the storage
owner's authority. A Datalink cannot retain a role grant across statements.

SQL statement identity remains the caller's identity, including target DEFAULT
expressions, source Datalinks, metadata views, provenance probes and deletes.
Index maintenance cannot change that identity. Fulltext2 CREATE, compaction and
CDC, HNSW CREATE and CDC, and GPU artifact persistence pass native chunk data to
the existing SQL executor. Tenant CDC resolves source Datalinks with source
authority. Asynchronous indexed Datalinks follow the remote-only source contract
below; background work never reconstructs a foreground administrator grant.

### Asynchronous Datalink source lifecycle

An asynchronous consumer receives committed row values and source-tenant catalog
scope, not the authenticated role of the statement that wrote each row. Therefore
source account zero, table ownership, successful administrator CREATE, stored URLs,
or stage ownership cannot authorize future server-file reads. Installing an index
must not succeed on a known source that its normal CDC consumer cannot read.

Fulltext2 and asynchronous ordinary fulltext accept remote Datalink inputs only.
One shared provider classifier admits the resolved remote provider without opening
it. Index installation/rebuild validates existing values; final-value DML admission
validates the indexed Datalink values that will actually be written. This includes
empty-table CREATE followed by INSERT, UPDATE, defaults, typed INSERT SELECT,
REPLACE, duplicate-key paths, prepared execution and LOAD. Unsupported local,
configured-service or dynamic-disk inputs fail the foreground statement and roll
back its database effects. NULL and unselected rows do not cause source access.

The existing final-row planning owner and plugin async metadata determine which
columns need this check. Column names are collected before index definitions are
stripped and bound to the final row image. Modern UPDATE checks indexed sources
assigned in that final projection, including explicit same-value assignments,
automatic ON UPDATE expressions and generated values. Columns carried unchanged
from OLD need no source resolution. Repeated writable aliases of one physical
table retain full admission because later merging can recompute values; legacy
UPDATE and insert/duplicate/replace paths retain their existing full checks.
An unrelated UPDATE may therefore commit while a retained source is unavailable,
disabled, retargeted or unsupported after upgrade. It introduces no source or
file authority: acquisition still checks the actual provider, and an invalidated
consumer still requires repair or migration before it can progress. A nonfoldable runtime check in a barrier
filter validates only eligible rows, without reevaluating their source expressions.
There is no new catalog privilege, persisted policy, worker or wire field. Unrelated columns and non-indexed ordinary SQL do not perform provider
or stage resolution. Synchronous ordinary fulltext retains its foreground caller
identity and local-file admission behavior.

Validation-only callers share URL/stage resolution and range checks with the
materializing Datalink APIs. Stage owns decoded service-field construction;
fileservice owns both path interpretation and the existing provider policy.
CAST retains its legacy checks, while final-row admission additionally validates
the concrete file path and async resource policy. Neither needs to serialize a
service path merely to discard it. Complete acquisition objects and public path
strings retain their existing representation. Local/named expressions, colons
in file components and CSV newline normalization use the original parser.
Parsed argument slices own their storage; no caller scratch or validation result
is retained across operations. This adds no validation cache or new I/O path.

Administrators can index local document contents by explicitly materializing them
into a TEXT column with foreground load_text, then using the normal committed TEXT
CDC lifecycle. The database stores actual document text, rather than asking a later
consumer to reconstruct historical file authority. Existing local-Datalink async
indexes require moving inputs to supported remote sources and rebuilding, or
materializing into TEXT under administrator authority. Retry/restart never invents
missing authority for old URLs.

External resources remain mutable. Deliberately dropping/retargeting a stage,
removing an object, changing credentials or losing storage/network can invalidate
a previously admissible remote source after commit. This retains the existing
observable failure/retry contract: acquisition rechecks the actual provider, a
failure does not publish partial index state or advance its watermark, and an
operator must restore a supported readable source before retry can progress.
Admission does not promise immortal external resources. There is no catalog/history
dependency scanner or new locking protocol to prevent all external invalidation.
Changing a source to a server-owned provider never grants background file authority.

Acceptance must exercise actual post-CREATE INSERT/UPDATE and durable CDC-tail and
search changes, including old-term removal. FORCE_SYNC REINDEX is a separate build
oracle and cannot substitute for CDC. Controls cover remote-stage source-tenant
resolution, synchronous denial of unsupported inputs, foreground local-to-TEXT
CDC, blank-identity background denial and repair/retry after source failure.

### Index artifact persistence

The existing segment, tail spool and model owners create and destroy their
artifacts. They supply an owned `io.ReaderAt` to a shared short-lived chunk
inserter in `vectorindex/sqlexec`; the inserter accepts no SQL path or Datalink.
The current transaction, execution context and statement identity are borrowed.
The helper creates no transaction, file, worker or global registration.

Each range is split into the existing 65536-byte chunks. Index IDs and data are
parameters; chunk IDs and tags are generated integer values. Explicit storage
columns and existing quoted table identifiers are retained. The data parameter
is cast to LONGBLOB, which retains binary bytes and admits a full 65536-byte
chunk. Ordinary declared BLOB has a narrower 65535-byte family width.

Pending parameters are immutable until synchronous execution completes. Native
read scratch is one chunk. Flush at the existing row ceiling (500 for fulltext2,
2000 for vector models) or an 8 MiB total parameter-byte ceiling, including IDs.
The smaller ceiling was selected from concurrent persistence measurements: it
reduces simultaneous planning/transport peaks while retaining tiny-frame batching.
Release references and close SQL results after each batch. Errors make an
inserter terminal; successful persistence requires a final flush. Validate
negative ranges, arithmetic overflow and short reads, and check cancellation
between reads and batches. The ceiling bounds transport data, not total
transaction or storage-writer memory.

Fulltext2 releases serialization buffers before reading its base spill. Tail
spools retain their existing packed ranges and chronological chunk numbering;
one reader is active at a time and the tail owner retains cleanup until the
outer transaction returns. HNSW/GPU model owners retain artifact destruction.
GPU packing and device-memory admission finish before persistence starts.
Metadata, replacements and watermarks remain in the existing transaction.

The existing internal parameter vector must report allocation failures and free
partial allocations before planning. Nonempty NULL bitmaps must match the value
count. Binary payloads use explicit casts without hex expansion, unsafe aliases,
value specialization or a new plan cache. Ordinary RunSql and RunTxn share the
same account/database/context and statement-option propagation contract.

Remove SQL-wide system-file identity overrides and deferred `load_file` INSERT
generators together. No generated statement, statement position or JSON
configuration constitutes file authority.

Admission and acquisition must agree on the provider. Non-stage table dump/load
always opens local storage, so it admits a service-less target independently of
commas or colons in its filename. Its existing local owner validates and cleans
the path; the stage branch retains the generic provider resolver.

LOAD and stage list/remove patterns are admitted using provider-only parsing before listing. That
parser reuses fileservice's service/argument parser and backend selection;
malformed or unsupported provider specifications fail before construction or
credential discovery. Concrete path validation remains in ParsePath and the
actual resource owner. Existing glob expansion selects concrete names, so its
character classes and wildcard syntax are never validated as concrete names.

## Alternatives and cost

Entry-only checks allow a newly added acquisition caller to forget its
preflight. Mandatory factory admission makes such a caller fail before resource
acquisition. A global SQL marker plus a trusted fallback would require extra
lifecycle propagation and make lost context meaningful; requiring policy for
all generic acquisition avoids both. Existing native configuration constructors
already provide the separate engine-owner boundary.

A capability registry, catalog privilege, root-directory configuration and
per-read success logging introduce state, migration or operational costs that
this contract does not need. Existing statement logging is retained. A bounded
source-owner inventory is a regression aid alongside actual factory and public
SQL tests; it is not a proof about arbitrary future Go code. New provider
options must be reviewed for dependencies; the current validator cannot claim
to automatically classify unknown future fields.

Ordinary queries add no file-service policy work. The complete PR refreshes the
existing current identity at the statement owner under one session-local lock;
DML target planning additionally scans column metadata when indexes exist, then
returns immediately for tables without Datalink columns. These bounded changes
still require ordinary workload measurements. Actual file policy has no
cross-session lock, storage, RPC or extra file I/O. Streaming and row encoding
do not repeat admission per byte or record. NULL/filtered/empty file-function
inputs short-circuit before admission.

Index transport adds immutable strings, parameter vectors and expression/INSERT
vectors. Their copies, vector-growth peaks and backend/transaction buffers must
be measured together. Compare actual persistence at the same rebased base with
identical artifacts, storage and native inputs. This machine has 10 logical CPUs
and 16 GiB RAM; use concurrency 1/4/8 rather than risk a 32-worker memory peak.
Include multiple batches and packed tiny frames, allocation/heap/mpool peaks,
SQL calls, throughput and latency. The byte budget may change only with renewed
concurrent peak and throughput evidence. Reader microbenchmarks alone do not
establish index persistence performance.

Readers and dynamically constructed services have one effective owner. Writers
report bytes only after successful completion; errors/cancellation abort and
release owned resources. Named services remain borrowed.

## Compatibility and acceptance

Local file access from public/custom/tenant-admin roles is intentionally
rejected, including previously stored local stages and external-table paths.
Such workloads must use a remote tenant stage or an explicitly trusted system
operation. Asynchronous local-Datalink sources are unsupported even for an
administrator; use foreground materialization into TEXT for that lifecycle. This is a behavior correction, without a temporary permissive mode.

Dynamic SQL providers require explicit credentials or the existing explicit
MinIO anonymous mode, and cannot name server certificate/keytab files, profiles
or instance roles, including for moadmin. HDFS simple authentication remains
available through existing endpoint-only URLs and stages. Native configuration
constructors retain their deliberate provider contracts; an explicit
no-default-credentials setting is no longer overridden by a destination role.
SQL callers of generic ETL Go APIs must bind the current operation policy; an
unmarked context is no longer implicitly trusted. The legacy
`MO_NO_DEFAULT_CREDENTIALS` / `NoDefaultCredentialsForETL` switch no longer selects
SQL credential behavior: SQL acquisition always disables ambient discovery.
`StatementOption.Params()`
now returns `(vector, error)`; external Go consumers must handle the error and
rebuild. Unused `ReadFromFile`/`ReadFromFileOffsetSize` Go wrappers are removed;
SQL file functions retain their public names. Backup SQL is restricted to
sys/moadmin rather than every sys role.

No catalog migration or new wire field is introduced. Older CNs do not enforce
this contract: the deployment must update all SQL-serving CNs before considering
the fix effective. Rolling back restores the old behavior.

Acceptance requires real SQL denial before filesystem side effects, administrator
foreground success, remote-stage foreground/CDC success, local-to-TEXT CDC
success and unsupported async-source rejection before commit, client LOCAL import success,
prepared role/source re-admission, write error/completion/cancellation evidence,
normal and race evidence for affected owners, and bounded-cost measurements.
Performance microbenchmarks establish admission/reader cost only; they do not
prove an end-to-end QPS or P99 service-level guarantee.

Artifact persistence acceptance also requires byte-for-byte binary controls
(NUL, all byte values, invalid UTF-8, 65535/65536/65537-byte boundaries), batch and
packed-frame ordering, unchanged index search behavior, target DEFAULT refusal,
source/view refusal, short-read/error/cancellation cleanup and transaction
rollback/retry behavior. Both normal and race execution must cover affected
owners. GPU compile/runtime and multi-CN evidence are separate platform proofs;
their absence cannot be reported as a pass.

## Review record

The issue task ledger contains the design-first approval for owned artifact
persistence and asynchronous Datalink admission, followed by provider and
acquisition amendments. The acquisition-only revision supersedes the earlier
global policy propagation proposal. This document states the resulting contract
as one design. The local first-principles correction preserves HDFS simple and
explicit MinIO anonymous behavior, closes acquisition cancellation and removes
dead reader wrappers. It does not approve an unvalidated implementation.

Delivery must pin the reviewed document revision, implementation revision and
validation record. GPU native execution remains unverified because no suitable
machine is available; the user explicitly permitted PR update with that gap
visible. Root confinement and bounded reading of trusted operator certificate
configuration are separate configuration/lifecycle contracts.
