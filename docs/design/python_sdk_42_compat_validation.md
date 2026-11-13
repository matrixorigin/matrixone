# 4.2 compatibility closure for the Python SDK

This records the native companion to [SDK PR #32671](https://github.com/matrixorigin/matrixone/pull/32671).
The latest stable release used for reproduction and physical-data upgrade is
v4.2.8 (`10d204b8763f19b2b37ba2147d1d4c1a7828d6d4`). It is immutable: a release
containing these native changes is required to ship the corrected behavior.

## Change and coverage map

| Closure | Implementation and retained oracles |
|---|---|
| Buffered TLS and peer shutdown | Backport #29996 and retain the review follow-up that unwraps only owned buffered/TLS connections. Coalesced ClientHello, payload, reset, FIN grace, timeout/cancel, certificate/authentication rejection and pool release remain checked. |
| SQL schema metadata | Adapt #29893 to the 4.2 type family. BOOLEAN and widthless numeric display names preserve storage OIDs, decimal/string/vector dimensions, ENUM/SET values and MySQL view provenance. Retain the legacy DUMP type renderer and checksums. Short type headers fail without mutating the receiver. |
| FULLTEXT identifiers | Adapt #31953 across planner/post-DML/ISCP: quote each identifier atomically and preserve async parser parameters. Exact identifier, async gojieba, values and rollback checks remain. |
| Numeric scanner | Adapt #27010/#31137 without promoting a dotted or signed-exponent token into an identifier. Retain identifier/hex controls and malformed-input errors. 4.2 DOUBLE metadata and PREPARE error wrappers remain specific to 4.2. |
| Stage and SQL mode | Adapt the relevant #31774 paths: literal/credential quoting, logical-name/URI equivalence, combined ALTER rollback, and response NO_BACKSLASH_ESCAPES status. CREATE also persists explicit ENABLE false/true; no option is silently ignored. |
| FLOAT32 Parquet | Accept representable +/-Inf, retaining NaN, signed zero and finite-overflow rejection in plain and dictionary pages for FLOAT/DOUBLE. |
| Database branch identity | Adapt #28272 to 4.2 without a new schema/RPC field or a main-only protocol gate. Mark new database branches in dat_type. Retain legacy table receipts, ordinary/subscription/unknown target rejection, every ordinary table's receipt, publication owner account, context scope and named/timestamp/PITR restore identity. |

The branch design revision and separate root self-review are in
[data_branch_identity_42.md](data_branch_identity_42.md) and
[data_branch_identity_42_review_r1.md](data_branch_identity_42_review_r1.md).
They precede implementation; they are not independent external approval.

## Stage URI and error boundary

The backport extends accepted Stage references. Existing valid host-form URIs
such as `stage://simple/file.csv` remain supported; a path-form URI such as
`stage:///two%20words/file.csv`, previously rejected as having an empty host,
now addresses the logical Stage `two words`. The first path segment is decoded
once, so `%2F` belongs to the name and `%252F` represents a literal `%2F`.
Empty names remain invalid, and dot-only names use the host form. See the
[Stage guide](../../clients/python/docs/stage_guide.rst) for examples, the
existing SDK helper limitation, and the all-CN upgrade requirement.

The CI Datalink regression exposed a separate missing-process error path after
path-form parsing succeeded. Catalog lookup now returns an internal error when
its process, process base or Stage cache is absent, instead of dereferencing it.
This does not turn malformed Datalink ranges such as `size=c` into valid input;
the range error is checked with an initialized Stage cache. Host and path forms,
all four public Datalink consumers, and the original failing input retain error
coverage.

## Ownership, concurrency and restore boundaries

Branch DELETE owns one pessimistic transaction and the existing mo_database
account/name key. Its locking SELECT goes through runSqlWithBackExec, whose
statement executor owns RC snapshot refresh after a lock wait. Direct LockRows
had leaked errno 20628; the embedded deterministic CREATE TABLE/DELETE test
exposed this and now verifies waiting followed by rejection of the added ordinary
table. No new lock owner, retry policy, worker, cache or queue is introduced.

Clone context is restored per statement, including multi-statement COM_QUERY.
All source database identities are checked before account creation/deletion or
other irreversible target effects. Account snapshot preflight uses the already
authorized timestamp when switching to the source account; it does not resolve
a sys-owned snapshot name in a tenant namespace. Original cross-account UDF,
Stage, stored-procedure and user restoration assertions are retained. Catalog
results have lexical cleanup, and transaction completion releases the key lock.
Work remains linear in database/table definitions with existing receipt batches.

## Oracle adaptations

The existing metadata result audit compares parsed commands, result shapes,
headers, values and errors, rather than line counts. In 104 files it preserves
18,606 original commands and 71,428 result cells; the allowed changes are 3,590
SQL type cells and 18 BOOL-DDL cells. A further 22 Boolean commands extend the
executable bool.test. No planner/explain query, table count, row count, statistics
or expected data result is reduced. Test-script discovery includes .sql and .test.

Four new branch identity headers use 4.2's dat_type width (-1), rather than main's
32. Sequence-only source coverage retains 4.2's existing clone filter, which
excludes sequences: the resulting marked empty target is deletable and the
source sequence survives. The source count, target count, deletion and source
preservation assertions are explicit.

One existing empty Stage result had a stale VARCHAR(152) JDBC precision. An
independent raw-protocol control on official v4.2.8 and the patched service
returns equal VARCHAR wire fields (length 0xffffffff) for both empty and populated
tables. Only that expected header changes. The original DDL bound, empty row set,
permissions and rejected local-file operations are retained. Combined ALTER now
succeeds; only its five obsolete rejection rows are removed from the existing
Stage/snapshot goldens, with all SQL and subsequent catalog assertions preserved.
The original metadata audit did not include four saved `result_scan` expectations:
three `INT(32)` cells and one `bool` DDL string now match the direct SHOW output
(`int` and `boolean`, respectively). The SQL and remaining result checks are
unchanged.

## Upgrade and release limits

A stopped v4.2.8 instance wrote BOOL/Unicode/Decimal table and view rows, a legacy
table-backed branch, legacy empty branch and ordinary empty database. The new
binary booted a physical copy using the original Dragonboat hostname through
the existing explicit-hostname configuration. Values survived in source/branch
tables and views, the legacy table receipt remained deletable, unmarked empty
targets stayed rejected, and new marker/context/snapshot-restore checks passed.
The original release directory was never opened by the new binary.

4.2 upgrades are shutdown-only. An older binary must not be started on data
written by the new binary. Existing unmarked empty databases cannot safely be
guessed to be branches. Existing persisted ENUM/SET views that had already lost
logical provenance remain VARCHAR until recreated. VECF8/VECF4 and native Arrow
LOAD are absent 4.2 features; this native PR does not claim to add them. The SDK
provides checked Parquet staging for Arrow input while preserving the native
capability fact and all positive write/atomicity/cleanup checks.

## Validation

Code build: `e54bcb763a23a0d3c6a8c746035c8f73982ed37e`, Go 1.26.4,
Linux/amd64, with worktree-local verified CGo artifacts. Merging the remote
branch update `64eb506d853` preserves the exact same complete tree; it only
joins already included upstream merges. No code or fixture is changed by it.

| Proof | Result |
|---|---|
| Types/plan/function metadata owners and standard MySQL/SQLAlchemy clients | Owning suites, scoped vet/lint and four actual positive reflection cases passed. |
| Metadata BVT | Eight real tester files: 1,435 comparisons passed; Boolean same-instance full-metadata repeat: 67 passed. |
| FULLTEXT/numeric | Owning packages and focused regressions passed; 30 normal BVT comparisons passed twice; 53 public SDK FULLTEXT/numeric/TLS checks passed. |
| Stage/Parquet/SQL mode | Four owning packages, scoped vet/lint and native build passed; all 35 difficult scalar cases and the complete 335-case analytics collection passed without skips. |
| Branch/restore final owners | Frontend and compile owning suites, frontend/compile/issues vet and incremental lint passed after the cross-account preflight fix. The new account-source regression fails before and passes after. |
| Final Stage/snapshot/branch BVT | Six scripts, run twice on the same instance: each run has 911 total statements, 903 successful, zero failed/abnormal, eight existing issue-marked ignored statements. The 75 new identity, 258 privilege, 365 snapshot-system-table, 156 Stage and seven literal comparisons all pass. |
| Public SDK native closure | 132 release/branch/clone/standard-MySQL cases pass without skips against the final native binary. Only an unselected analytics test file changed during that run; selected inputs and all production Python files matched its frozen manifest. |
| Concurrency | Complete frontend race suite, four exact buffered-TLS/SQL-mode roots at 100 repetitions each, and the embedded deterministic branch DDL lifecycle root under race all passed. These proofs use a5a1705b70; the raced resource/lock owners are unchanged by the later stateless restore-query fix. |
| Physical upgrade | Pinned official v4.2.8 data copy, legacy receipts, table/view values, empty-target protection, per-statement identity and snapshot/restore marker all passed. |

The eight issue-marked statements are unchanged existing coverage exclusions,
not eight newly certified permission behaviors. The exact positive and negative
privilege oracles in the 258-statement script and focused unit tests remain.
The complete SDK online/analytics qualification and its final source identities
are recorded on [PR #32671](https://github.com/matrixorigin/matrixone/pull/32671).
No runner was added. Local build/link time is not claimed as a case-speed benefit.

Final implementation self-review: Codex `/root`, self/delivery review. Each closure
above maps its producer, consumers, compatibility limits and unhappy paths;
previous valid evidence is reused only where those inputs remain unchanged.
No unresolved implementation blockers remain for this native change. This is
not independent external review or a claim that a skipped/pending CI job passed.
