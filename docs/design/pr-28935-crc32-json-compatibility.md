# CRC32(JSON) compatibility repair

This focused repair of PR #28935 retains the public SQL function and adds a
stable execution identity instead of migrating stored values. Compatibility
covers main and supported releases, with no migration of unmerged experimental
PR data.

## Contract

| Identity | Binding and execution |
| --- | --- |
| CRC32 overload 0 | Existing catalog/wire expressions hash binary JSON; ordinary non-JSON calls retain their original bytes and scalar casts. |
| CRC32 overload 1 | New JSON or unresolved bindings hash normalized MarshalJSON bytes for JSON; requires candidate MORPC v109. |

The result stays UINT64; the executor also retains the historic UINT32 wrapper.
A stored legacy generated value can intentionally differ from a freshly bound
`CRC32(j)` query. Existing expressions are never silently upgraded, including
INSERT, UPDATE, REPLACE, ODKU, prepared rebinding and index maintenance.

## Boundaries

Placement checks workers and falls back to one CN. Sending rechecks the actual
worker at candidate v109; receiving checks the new identity. Old coordinators send identity
0, which new workers still implement. Unknown capabilities fail closed.

Catalog read and authoring use the existing separate durable admission floors.
Internal ALTER COPY and catalog LIKE/CLONE must carry bound expressions because
regenerated SQL loses their identity. Copies detach expression trees from source
metadata. Old folded defaults without a source expression retain their stored
value; newly folded CRC32 defaults retain their source identity for admission.
CHANGE/MODIFY of a legacy generated column preserves its identity for
an unchanged expression/type; semantic conversion is rejected and requires an
explicit table rebuild. No automatic data or index migration is introduced.

Views are rebound SQL and acquire the new semantics and admission requirement.
Ordinary CTAS result columns remain historical values. Physical restore retaining
catalog identities preserves the old semantics. SQL dump/replay is new DDL and
is not a lossless way to preserve a legacy generated expression's algorithm.

## Upgrade and rollback

Supported sources include main through v108 and supported released versions, not
earlier unmerged experiment binaries that changed overload 0 in place. Such data
cannot be distinguished by identity and must not be admitted as a supported
upgrade source. The current maintenance candidate integrates main
`d2a0055ebe1a0b2423368e046c1812c18f6face8`, preserving all contracts through v108,
including v101's JSON source domains, v107's functional-index contracts and
v108's isolated user-variable NULL regexp history during migration. CRC32 uses
candidate v109. This number is not claimed unique among other unmerged maintenance
candidates. Before landing, reallocate it against the actual cumulative main and
rerun the predecessor/admission tests; a higher number cannot advertise missing
predecessor capabilities. Cross-PR allocation coordination remains open.

Publishing new persisted expressions requires the durable candidate v109 authoring barrier.
Once the durable floor is raised, old CNs must stay excluded, including after
restart. A failed activation does not imply the floor can be lowered. Rollback
must respect the existing admission mechanism; this change adds no floor reset.

Connection migration carries PREPARE SQL, not its bound overload identity. Source
export therefore pins JSON/ANY CRC32 and uncertain statements until DEALLOCATE;
the new destination checks the entire SQL payload before USE, state restoration
or PREPARE, including payloads from old sources. Both checks use parsed AST and
the source additionally checks bound plans and folded Literal.Src. No wire field
is added. Only closed scalar SELECTs with explicitly understood expressions are
replayable: ordinary literal CRC32(text/numeric) remains overload zero, while
CRC32(parameters/variables/casts) is conservatively pinned. Views, table-dependent
queries, UDFs and unrecognized forms can hide a CRC identity and are also pinned;
this intentional availability restriction does not assert they all contain CRC.
Evaluated typed numeric user-variable snapshots remain values, regardless of the
assignment SQL retained for diagnostics. Actual source EXECUTE after rejection,
destination zero-replay effects and mixed-version migration require new evidence.
The destination parses with the typed source sql_mode before installing it. When
legacy SET replay cannot supply that mode as a value, it must prove the closed
statement safe under all 64 combinations of the six existing parser flags; it
does not evaluate SET SQL to discover the mode. This adds bounded parsing work
only on that legacy path, not to normal expression execution.

## Validation obligations

Executor and binding tests distinguish both identities with fixed independent
checksum oracles. Codec/feature tests cover unresolved types and folded source
expressions. Sender tests isolate a new coordinator and old target. Catalog
fixtures must preserve identity through DML, COPY, LIKE and prepared rebinding.
The function BVT covers generated columns, indexes, DML, prepared reads and ALTER.
Real two-binary rolling upgrade, persistent old-table DML after restart and
post-floor downgrade rejection remain separate environment acceptance tests;
mock version values and same-version CI do not prove them.

## Current main integration candidate on 2026-10-09

The normal merge preserves the actual v107 functional-index and v108 migration
capabilities rather than merely advertising a higher version. CRC32 placement,
send, receive, persisted read and durable authoring admission now require v109.
Focused tests reject the immediate v108 predecessor; destination tests also
retain older v100/v101/v106/v107 boundaries. Existing overload-zero identities
remain unchanged. Main's regexp binary-cast/blob domain changes are preserved.

The reachable v4.0.11 CDC upgrade repairs `source_table_id` and
`owner_generation` before the two dependent columns, retaining their original
unsigned types/defaults and the common-writer v106 gate. The new regression
covers released and partial catalogs, injected prerequisite failure, retry and
idempotence. The canonical CRC32 BVT requires all four catalog columns.

- PASS (exact pre-commit content, Linux/amd64, genuine Go 1.27.1/CGo):
  selected owning planner regression tests, 18 roots / 65 named tests,
  including both generated-attribute orders and catalog-source preservation.
  The same new fixture on the pre-repair candidate produces the expected
  illegal-attribute acceptance failure (2 roots / 17 named tests), without
  parser failures. Generated identity preservation cannot silently discard
  DEFAULT, ON UPDATE or AUTO_INCREMENT.
- PASS: the unchanged compiled owning receiver test group, 1 root / 27 named
  tests, including immediate v108 rejection and v109 sender/receiver admission.
  Earlier selected identity tests (26 roots / 68 named tests) and unchanged
  planner/frontend/upgrade evidence (26 roots / 101 named tests) retain their
  original content scope; these counts are not whole-package execution claims.
- PASS: a genuine new production build of the repaired candidate, binary
  SHA256 cd92727345b2ff565e56bebb5f35eaa2315a6309e4f1abffebf25fbfc803b9fa,
  actual SQL/ISCP readiness, and canonical CRC32 (169 statements), CDC (11)
  and CDC syntax (4), each twice: 368 runner statements with zero failed,
  ignored or abnormal comparisons. Canonical expected files were not
  regenerated. Pre-next SQL cleanup, graceful TERM exit, exact container
  cessation, empty cgroup, stopped deadline units and zero OOM were
  independently verified.
- NOT_RUN: whole owning-package suites and race on this integrated content;
  selected tests and same-version BVT do not establish deployment acceptance.
- NOT_RUN: supported-release upgrade/restart/restore/downgrade and mixed-CN
  rolling execution for this integrated candidate. Earlier evidence below
  retains its original exact-content scope; it is not final-v109 acceptance.

## Validation before main integration on 2026-09-28

The following results apply to `29d9d489`, before the main integration and
protocol reassignment. Integration validation is recorded separately.

- PASS: complete function, plan codec, planner, compiler and executor utility
  package tests (6,528 top-level tests). Compiler tests use GOMAXPROCS=4;
  existing Parquet fanout tests require more than two worker slots.
- PASS: embedded single-CN SQL and binary PREPARE, parameter reuse, schema-change
  reprepare, generated-column DML, ordinary/unique indexes and ALTER. Persisted
  DDL waits for the actual HAKeeper-driven authoring floor, without overriding
  runtime protocol values.
- PASS: normal mo-tester comparison using runner commit
  `309a0ca650e0c55e90292767fd61e4976818d66d`: `func_crc32.sql` (115 statements)
  and `generated_column.sql` (334 statements), each twice on the same isolated
  single-CN instance. Input staging was byte-identical; ISCP readiness and
  explicit database teardown were checked. No statements were ignored.
- NOT_RUN: real v93/new mixed binaries, old-version persisted-table upgrade and
  restart, physical restore, and post-activation downgrade rejection. Component
  tests and same-version SQL execution do not replace those acceptance tests.

## Historical main integration validation on 2026-09-28

Integrated main `239fe81c82ae07a8c316576cac8555c445c92212` and reassigned
the new identity to v100, preserving all preceding main contracts. This result
predates the later v101 reassignment after main claimed v100 for View metadata.

- PASS: all five owning packages, 6,816 top-level tests with GOMAXPROCS=4.
- PASS: SQL and binary PREPARE integration, including reprepare and generated DML.
- PASS: normal mo-tester CRC32 (115 statements) and generated-column (334
  statements) cases, twice each, zero failures or ignored statements, with
  actual authoring-floor/ISCP readiness and database teardown checks.
- PASS (historical): immediate pre-feature v99 destination rejection and v100 admission.
- COVERED BY UNIT TEST (historical v101 candidate): a v100
  View-metadata peer is rejected for CRC32 overload 1, while a v101 peer is
  admitted.
- NOT_RUN: the real mixed-binary and persisted upgrade/restore/downgrade
  acceptance scenarios described above. These remain required QA evidence.

## Maintenance candidate on 2026-10-08

Normal merge preserves main's unified expression placement and send validation
and its legacy TIMESTAMP defaults. CRC32 participates in the unified maximum
floor and single destination probe. Mixed persisted owners also take the maximum:
a v101 JSON source contract cannot lower the candidate CRC32 v107 requirement.
The v4.0.12 CHARACTER_SETS handler retains its historical v106 requirement;
CRC activation uses the existing durable latest-floor admission chain, not a
relabeled historical bootstrap handler. The historical v4.0.10 View-metadata
handler remains at v100. The embedded prepared test waits
for the actual CRC32 authoring floor, rather than the older View-metadata floor.

- PASS: focused codec, CRC32 execution, planner/catalog, placement/send, and
  receiver tests on the integrated candidate explicitly reject main v106 and
  admit candidate v107. Owning codec, function, planner, compiler, executor, and
  upgrade packages also pass. Function fixtures follow main's explicit vector
  ownership contract; their final focused and owning revalidation also passes.
- PASS: fresh native generation through `make cgo`, its provenance verification,
  and `make build` with Go 1.27.0 on Darwin/arm64. Revalidated all seven owning
  packages with the worktree's controlled CGo wrapper: 26,916 tests/subtests,
  zero failures. This supersedes the earlier tests with unverified native
  artifacts; those earlier runs are not the acceptance evidence.
- PASS: `TestCRC32JSONBinaryPrepared` on the fresh native generation, including
  real SQL/binary PREPARE, schema-change reprepare and generated/index DML.
- PASS: canonical mo-tester `func_crc32.sql` (115 statements) and
  `generated_column.sql` (334 statements), each twice on an isolated single-CN
  instance using the freshly built candidate; zero failures, ignored statements
  or abnormal results. Inputs were copied byte-identically, not regenerated.
  Runner source: `b95f5c09930c42bdbe77828b6ae72d39f2be48ed`; Java 17.0.15.
- At the time of the earlier maintenance validation, real O/N rolling execution,
  old-created table upgrade/restart/restore and old-CN rejoin/downgrade were
  NOT_RUN. The real old-main evidence below now covers the latter lifecycle
  paths; it does not cover supported-release or rolling remote execution.
- OPEN: coordinated capability allocation at landing. This candidate does not
  resolve or request re-review of the outstanding compatibility acceptance CRs.

## Real old-main lifecycle acceptance on 2026-10-08

Two independently compiled binaries were executed on Darwin/arm64. The old
binary is main `bab4b3286a0dd5683a9b291763817722233e586c`, advertising v106;
the candidate is production content `5a884a0cc3cdcc2909ff248bc404414b640b9066`,
advertising v107. The tested PR head
`f9c00b54c10327f5fedab24662a0528d64b4ac3c` differs from that production content
only in this document. No runtime protocol values were overridden.

Binary SHA256:

- Old main: `be6b71abaaf9281889686a9bd443d583f60335bacb60bf7fc94051c2fff2f50d`.
- Candidate: `43020148613ac6176da04eee6a18e0e25977c1a8d1ad4cf8103887b525288398`.

The old Go executable used Go 1.27.1 without SIMD after a Go 1.27.0 SIMD
toolchain startup failure; the candidate used Go 1.27.0 with SIMD. This is a
CRC32 correctness/lifecycle test, not a matched-toolchain performance comparison.
Makefile, CGo and third-party sources are identical between these two source
versions. The independently compiled old Go executable reused the candidate's
verified native generation via explicit include/library/rpath settings.

The old binary created the following table and inserted `{"t1":"a"}`:

```sql
CREATE TABLE t(id INT PRIMARY KEY, j JSON,
  c BIGINT UNSIGNED GENERATED ALWAYS AS (CRC32(j)) STORED, INDEX idx_c(c));
```

| Actual execution | Persisted `c` | Fresh `CRC32(j)` | Result |
| --- | --- | --- | --- |
| Old binary INSERT | 3719146973 | 3719146973 | PASS |
| Candidate upgrade, UPDATE and INSERT | 3719146973 | 4012824821 | PASS |
| Candidate whole-launch restart | 3719146973 | 4012824821 | PASS |
| Restore old-created table snapshot, then INSERT | 3719146973 | 4012824821 | PASS |
| Restore old binary's offline whole datastore, then UPDATE and INSERT | 3719146973 | 4012824821 | PASS |
| Candidate recovery after rejected whole-launch downgrade | 3719146973 | 4012824821 | PASS |

`FORCE INDEX(idx_c)` returned both legacy rows; EXPLAIN confirmed the physical
`__mo_index` path. A newly created generated column materialized 4012824821.
The physical-restore arm stopped the old whole launch before copying all 23
datastore files, recorded their SHA256 manifest, verified the copy and restored
bytes, and retained the earlier activated datastore separately. This is an
offline local DISK-V2 whole-datastore restore, not SQL replay and not a claim
about production cloud/backup-tool restore. Table snapshot restore was a separate
SQL restore arm and is not mislabeled as whole-datastore restore.

After the durable floor had advanced and survived candidate restart, three
sequential old-main CN incarnations each terminated with the actual
`requires persisted expression protocol version 107 (local version 106)`
diagnostic. Actual MySQL handshake/query probes observed no usable SQL; sockets
were closed after termination and the candidate remained healthy. A separate
whole old-binary launch on the activated datastore was also rejected by that
durable floor, with no usable SQL. The candidate subsequently recovered both
identities without resetting the floor. Every experiment-owned process stopped.

Retained evidence includes frozen runner versions, executable hashes, configs,
SQL receipts, EXPLAIN output, service logs, cold-backup hashes and cleanup
receipts. Runner hashes are:

- Initial upgrade/restart/table-snapshot arm:
  `685c0bd54b4e706cde2203582030b349d0ab8879b85305eb040aa822438c8803`.
- Actual SQL-admission rejoin arm:
  `d992f414dc73d73db7be6a6f4886cfc9de181abc068abe53f18ee182effe974a`.
- Offline physical restore/whole-launch downgrade arm:
  `b2bf85ca418802939d79285ee1b16f400c5f0eedc9bc40aad8783832d7e0b2ae`.

The initial CN-only launch manifest failed before admission; a later immediate
TCP-connect assertion confused a bound socket with admitted SQL ingress. Both
failed receipts are retained. Corrected runs use the supported single-service
`-cfg` entry and actual SQL probes; these failures are not hidden product PASSes.

At this point, supported-release-created table lifecycle and real mixed-CN
rolling remote execution remained NOT_RUN. The actual release attempt below
supersedes that status only for the executed cases. Old main is not a
supported-release binary. The remaining aptend/aunjgr acceptance criteria and
cumulative capability allocation at landing are not waived.

## Supported-release attempt on 2026-10-08

Official Linux/x86_64 v4.2.5 (`1a44f3d`, MORPC10) and the coverage-instrumented
shared-build artifact for exact head `4f431b020093b6724a96a59a36642475925d08dc`
were executed separately on mo-50. `BUILD_INFO`, executable version output and
SHA256 were verified before execution. This was a correctness experiment with
an 8 GiB memory cap, four-CPU quota and bounded runtime, not a performance run.

- PASS: the release created the legacy generated/index table and materialized
  3719146973; an offline whole-datastore backup preserved all26 file hashes.
- FAIL: candidate upgrade did not reach SQL readiness. Its catalog4.0.11
  migration attempted `pending_source_table_id ... after owner_generation`,
  but the release's CDC watermark table had neither `source_table_id` nor
  `owner_generation`. Authentication waited on tenant upgrade and timed out.
  This migration file is byte-identical to main base `bab4b328`; the CRC32
  delta does not modify it. No exact-base Linux control was executed, so this
  is a diagnosed inherited prerequisite, not a completed regression attribution.
- PASS: restore of the preserved cold backup into a fresh private directory
  and actual v4.2.5 startup recovered the original row and legacy checksum.
  Its catalog version was4.0.6, with the original six CDC watermark columns.
  This proves pre-upgrade backup rollback, not downgrade of activated v107 data.
- NOT_RUN: release-to-candidate DML/restart/snapshot/cold-restore continuation,
  release downgrade after successful activation, and rolling remote placement.
  The failed upgrade must be fixed or satisfied before these cases proceed.

The initial rollback probe failed a wildcard-listener collision after a
loopback-only preflight; the failed receipt is retained. A separately reviewed
runner used the original free endpoint range and succeeded. All three owned
units/processes stopped; the successful endpoint range was independently
verified released. Failed upgraded data, both rollback directories, release
backup and raw evidence are retained; no other service or port owner was changed.

Binary SHA256: release
`5d136df6fab5e7a5bdf4ba3e678470d898c8d681f3fdae99095ecd9c4205d2ed`;
candidate `c3320fd306fd74c52390374c46c08aa2a5cd57235be30664167ac3721c4ab2d8`.
Frozen release runner SHA256
`b93157f996272da396b7b46227c7a90469417616fda306fe538c554b9745da56`;
successful rollback runner SHA256
`28b8da6b3ea13acd0b3c03be826d860b3afc89e54ddfef724caf7d0b018c493e`.
This evidence does not close any reviewer finding or authorize automatic re-review.
