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
| Remote argumented provider or remote stage | Existing provider credentials, stage status and tenant catalog rules remain applicable. |
| `LOAD DATA LOCAL`, INLINE | Client data: no CN filesystem authority is acquired. |
| Generated index storage files | The storage owner reads its own artifacts natively and supplies binary chunk values to ordinary SQL in the existing transaction. No SQL identity is elevated. |
| Saved result scan | Existing result metadata/privilege owner admits an engine-generated location; user SQL does not supply its raw path. |
| Backup | Existing system-admin statement owner controls backup execution. |

The existing OUTFILE disable switch is an additional operational control. File
admission applies when export is enabled. This fix does not claim to enforce
`secure_file_priv`; that compatibility variable does not define the policy.

## Owners and execution boundaries

`process.AuthorizeSQLFileAccessPath` is the single stateless execution policy.
Fileservice owns provider classification and construction. Stageutil owns
catalog resolution. The generic GetForETL remains usable by trusted internal
storage owners, without dependencies on SQL sessions.

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
authority. Background local-stage source reads follow the foreground rule.

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

A directory-only check leaves the question of which tenant may access that
server directory unanswered. Restricting generic ETL breaks storage and engine
owners that do not execute user SQL. A new catalog privilege, allowlist cache
or policy state machine is unnecessary for the existing server-admin contract.
The selected design uses existing identity and one shared bounded admission
function, with no policy storage, RPC, catalog lookup or extra file I/O.

Ordinary queries add no file-service work. Existing per-statement identity
refresh uses a session-local lock; file admission has no cross-session lock.
Parsed-path admission is reused where the target is already parsed. File
streaming and row encoding do not repeat authorization per byte or record.
NULL/filtered/empty file-function inputs short-circuit before admission.

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
operation. This is a behavior correction, without a temporary permissive mode.

No catalog migration or new wire field is introduced. Older CNs do not enforce
this contract: the deployment must update all SQL-serving CNs before considering
the fix effective. Rolling back restores the old behavior.

Acceptance requires real SQL denial before filesystem side effects, administrator
success, remote-stage foreground/CDC success, client LOCAL import success,
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
