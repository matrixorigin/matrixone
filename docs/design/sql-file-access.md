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
| Generated index storage files | The index storage owner grants authority to its own generated statements through a per-operation SqlProcess copy. User Datalinks are evaluated with caller authority. |
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

- Raw file functions and Datalink constructors/readers/writers/stat.
- OUTFILE, including CSV, JSONLine and Parquet output and split files.
- Server-side LOAD DATA and external-table readers, at planning I/O and again
  when a prepared/retained source is compiled for execution.
- Writable external-table stage output, stage list/remove operations.
- Table dump/load and data-branch file output.

The active account/role pair is refreshed once per statement from the existing
session owner, propagated into child SQL and the existing process wire fields,
and rechecked at resource acquisition. No user expression can set the storage
owner's authority. A Datalink cannot retain a role grant across statements.

An index storage grant is used for generated spill-file storage statements,
not user-provided source content. Tenant CDC resolves remote source Datalinks
without such a grant. Background local-stage source reads are denied just as
foreground reads are; there is no compatibility exception.

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
