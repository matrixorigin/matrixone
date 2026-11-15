# Python SDK companion fixes on 4.2

This document describes PR #32723, related to #29928/#26068 and SDK PR #32671.
The native companion preserves 4.2-dev scalar type display. It does not bring
main's canonical scalar formatter or its client-reflection guarantees to 4.2.

## Scalar metadata contract

`SHOW COLUMNS`, `DESCRIBE`, `information_schema.COLUMNS.COLUMN_TYPE` and
`SHOW CREATE TABLE` retain the existing 4.2 spelling for ordinary scalar types.
Examples include `INT(32)`, `BOOL(0)`, `BIGINT UNSIGNED(64)`, `DATE(0)`, `TEXT(0)`
and `FLOAT(5)`. SHOW CREATE retains `bool`. Internal OIDs, dimensions, DATA_TYPE,
value packets and storage encoding are unchanged by the display decision.
There is no new scalar formatter or legacy-formatter layer.

These are compatibility observations, not a claim that every old string is
canonical MySQL DDL: the old FLOAT display can omit scale, and unsigned suffix
order is retained. Standard SQLAlchemy BOOL/table and view reflection are not
fixed by this native PR. The native BOOL values, NULL/default semantics and
ordinary SQL replay remain covered; the withdrawn canonical-output BOOL client
feature is not replaced with a skip or negative-only acceptance.

Independent ENUM/SET correctness work remains: preserve direct view provenance
before projection remapping, retain exact domain members/case and quote members
through existing literal owners. The dedicated empty ENUM domain remains
`enum('')`. Public domain replay and the existing 4.2 DUMP/hash spelling have
separate existing owners; scalar normalization does not justify changing either.

## Other retained closures

| Closure | Owner and resulting behavior |
| --- | --- |
| TLS | Drain frontend-owned read-ahead bytes before underlying TCP input; both handshake branches keep the existing socket ownership. Linux terminal probing unwraps the known buffered/TLS wrappers and keeps FIN grace. |
| SQL mode | Derive response status from the executing session; no second cached SQL-mode state. |
| FULLTEXT | Quote identifier components through the existing SQL quoting owner in planner, post-DML and ISCP; keep asynchronous parser parameters. |
| Numeric parser | Preserve numeric token boundaries and incomplete-exponent errors, while retaining 4.2 wire metadata. |
| Stage | Preserve host-form URIs; support logical names with path-form URIs, one-time name decoding, existing URL/file authority, atomic combined ALTER and explicit CREATE ENABLE false. |
| Parquet | Accept representable FLOAT32 infinities; keep finite overflow, NaN and signed-zero behavior. |
| Branch | Store the database-owned data-branch category in existing dat_type; retain all ordinary-table receipt checks, DDL catalog-key serialization and existing RC retry ownership. |
| Restore | Preserve branch identity through named/timestamp/PITR restore; preflight source definitions before irreversible target effects and retain statement/account context boundaries. |

Branch design and accepted review are in `data_branch_identity_42.md` and
`data_branch_identity_42_review_r1.md`. No catalog column, RPC field, storage
format, new worker, queue, retry state machine or mixed-version gate is added.
The branch category is a new persisted value in an existing catalog column.

## Stage URI boundary

Existing `stage://name/path` references remain supported. Path form such as
`stage:///two%20words/file.csv` addresses logical Stage names which are not URL
hosts. Split the first encoded segment before decoding, so `%2F` belongs to the
name and `%252F` denotes a literal `%2F`. Empty names remain invalid; dot-only
names use host form. Datalink `size=c` remains invalid after Stage resolution.
A missing process/base/cache returns an internal error instead of panicking.

Published 4.2.8 is unchanged. Path-form native support requires a 4.2 release
containing this PR; existing SDK Stage helpers still use host form. An API that
accepts a complete encoded URI is required for names needing path form.

## Validation and evidence boundaries

The display follow-up restores the affected existing golden type rows from the
verified 4.2 baseline, including binary-classified result files. Other query,
value, error, Stage-state, branch and restore changes are retained. Added scalar
metadata tests use literal 4.2 outputs and real-service comparisons; they do not
accept the candidate's generated output without review.

On 57, Linux/amd64, Go 1.26.4 with matching CPU native libraries:

- All three owning packages (types, plan and function) passed normal and race
  runs with -short -tags=matrixone_test.
  The focused metadata/round-trip/decode selection passed all 10 root tests.
  The pre-existing TestDecimal128Float skip remains unchanged.
- gofmt, standard vet and configured incremental golangci-lint passed on the
  three owning packages. Pinned molint exited zero and retained three pre-existing
  checkrecover diagnostics in unchanged expr_opt.go and returning.go.
- An isolated service built from actual 4.2-dev
  ad39691b6b45f26688d42c47d9451bc89c957679 supplied the baseline. All eight
  query snapshots match exactly: SHOW CREATE, ordinary/full columns, view
  DESCRIBE, table/view information_schema, native type decoding and BOOL/NULL
  values. Both baseline and candidate shut down normally.
- The bool, create_table_like and show_create_table BVT scripts passed 274/274
  comparisons twice on the same candidate instance, with zero failed, ignored
  or abnormal statements and explicit database/fixture cleanup checks. The
  tester used its normal comparison mode; JDBC header comparison was disabled,
  while COLUMN_TYPE cells were compared. Existing golden rows were restored
  from the baseline, not generated from the candidate.
- Unmodified standard SQLAlchemy 2.0.43/PyMySQL passed the three retained positive
  ENUM/SET cases, covering both SQL modes and the empty-member domain. These
  cases do not load the SDK and do not certify BOOL or view reflection.
- Early private harness setup failures (Unix-socket path length and tester
  configuration/result lookup) were recorded separately, corrected without
  product or assertion changes, and are not counted as successful runs.

The scope design was independently reviewed with gpt-6.1-sol/max; the final
implementation review and exact source revision are recorded in the PR body.
Earlier canonical metadata/standard BOOL reflection results do not certify this
display policy. Unchanged TLS, Stage, FULLTEXT, parser, Parquet and branch/restore
closures reuse their matching evidence from the series through 428a9ea84ab1
with revision and scope attribution; those suites were not rerun by this
display-only follow-up. The later lexer/Parquet QA section below records the
new implementation and owning-package validation for those two paths.

## Release limits

Original published 4.2.8 remains unchanged. Supported SDK paths and release
limitations are maintained independently in PR #32671. Standard unmodified
SQLAlchemy reflection limitations remain outside this display-preserving PR;
see #32746 and the native BOOL metadata issues rather than claiming full client
compatibility from raw SHOW results.

Upgrades are full-cluster shutdown only; older binaries must not open newly
written data. Old unmarked empty databases cannot safely be inferred as branches.
Persisted ENUM/SET views that already lost provenance need recreation. Native
Arrow LOAD and VECF4/VECF8 are not introduced by this backport. No CI runner is
added, and local validation does not certify remote CI.

## PR-only upgrade and rollback qualification

If this change ships in 4.2.9, the following record qualifies this PR's increment,
not the complete future release. The paired targets are PR base
1a3e7fb6960995e9a143e5cc3d5a50f2feb0980f and candidate
27f71fd679a0d498d45672185a27f217b44874b2. No production code or additional
compatibility mode was introduced for this qualification.

### Released physical-data fixtures

| Source | Release commit | Immutable image digest |
| --- | --- | --- |
| v4.2.1 | d2393868a7aaa6343518d80849fe4696aa3577e9 | matrixorigin/matrixone@sha256:c95d1b468e3fc5f4f2b377da14f4601e7efefa6eb143758636d60114a5bbb436 |
| v4.2.8 | 10d204b8763f19b2b37ba2147d1d4c1a7828d6d4 | matrixorigin/matrixone@sha256:eec6cee1ab0d1ddaa5c81cbc9d79ef6a59ecf13f2b5835631ef3b03ee6822fc6 |

Binary version output and SHA-256 were recorded for both released fixtures.
The released binary creates the tables, data, views, FULLTEXT index, Stage,
nonempty/empty branch databases and a database snapshot, then stops normally.
Clone its complete physical data directory separately for base and candidate;
neither target ever opens the original source. Both original source mo-data content
hashes remain unchanged.

Each target reads the old objects, performs new writes, restores an old snapshot,
creates/recreates a view, executes CREATE STAGE ... ENABLE=false, creates
database branches, restores a new branch snapshot, and restarts on the resulting
data. The requested Stage remains in_use on base and persists disabled on
candidate. Only another
disposable stopped-data copy is opened by the source release for rollback
observations. This is one CN/TN/LogService with complete stops between versions.
All seven service phases per source exited normally. The test does not exercise
rolling upgrade, downgrade support, large catalogs or general performance.

### Paired results

| Contract | From v4.2.1 | From v4.2.8 |
| --- | --- | --- |
| Upgrade reads: scalar values/metadata, legacy view metadata, Stage, existing FULLTEXT search, legacy branch identity and snapshot reads | Candidate equals released source and base | Candidate equals released source and base |
| Existing host-form Stage URI reads | Base and candidate return the fixture bytes | Base and candidate return the fixture bytes |
| Old snapshot restore with SET members UP,Down | Base produces up,down; candidate preserves UP,Down | Same |
| New ENUM/SET view provenance | Candidate stores members and logical types; base stores VARCHAR without members | Same |
| New branch identity, disabled Stage state and new view data after restart | Preserved | Preserved |
| Old release reads of sampled post-write table/view/catalog data | Preserved in both base/candidate data copies | Preserved in both base/candidate data copies |
| Old release creates a publication on a newly created nonempty branch | Base data succeeds; candidate data rejects the database as not a user database | Same |
| New path-form Stage URI | Works on candidate; old parser rejects the URI after rollback | Same |
| Old release deletes the empty database branch | Fails for both base and candidate data; inherited old limitation | Same |

The SET restore mismatch is a real base counterexample, not a passing base
assertion or a new candidate failure. Initial harness failures were preserved:
a raw varchar load_file call bypassed Stage Datalink resolution, so the actual
public test was corrected to load_file(CAST(... AS DATALINK)). No product code,
golden output or candidate correctness assertion was weakened. Candidate
restoration still must equal the source's original values exactly.

### Compatibility interpretation

No PR-specific old-data read failure was found in these two source fixtures.
Ordinary scalar storage and display remain unchanged. Existing views that
already lost ENUM/SET provenance and old unmarked empty branches are not
silently repaired by startup; their existing limitations remain.

New writes can create semantics that old releases do not understand. The
publication failure is a concrete PR-specific rollback difference: candidate
stores data-branch in the existing dat_type column, while base stores an empty
value. Both source releases reject the candidate-written category when creating
a publication, although the sampled rows remain readable. This is a functional
rollback incompatibility; the probe did not observe data corruption.
Applications using the new path-form Stage URI also require the new parser.

CREATE ENABLE=false, SQL-mode response status, ENUM/SET quoting/provenance,
credential validation and rejection of malformed dotted exponents deliberately
correct old behavior. Existing stored credential rows are not rewritten merely
by startup; new CREATE/ALTER credential inputs use the corrected validation.

A rollback plan must retain a consistent pre-upgrade backup or data snapshot
and account for writes after that backup. Simply replacing the binary with
4.2.1/4.2.8 on candidate-written data is not a validated safe rollback procedure.
The isolated old-binary probes do not change the shutdown-only/no-downgrade
support contract or certify every user schema, snapshot, index or deployment.

## Lexer and Parquet QA follow-up

This follow-up closes the two nonblocking findings in the
[QA review on 6eb3293fef74](https://github.com/matrixorigin/matrixone/pull/32723#issuecomment-6892511924).
It adds no display mode or compatibility path.

### Signed exponent boundary

After recognizing e/E and a sign, the existing scanner requires the next
character to be a decimal digit. Otherwise it returns LEX_ERROR immediately.
This covers integer, dotted and leading-dot mantissas, both signs, missing
digits, whitespace and a second sign. Legacy unsigned identifier fallback
(1e, 1efoo, 1e2foo, 123name), complete exponents and existing dollar aliases
remain covered by positive controls.

The eight new invalid lexer assertions failed before the three-line fix and
pass afterwards. The QA comment's blanket base-ParseOne rejection claim needs
qualification: an independent AST-only probe found that base accepts
SELECT 1.5e+foo as an identifier expression; this does not establish successful
SQL execution. The corrected scanner rejects the malformed exponent lexically.

### Parquet conversion and measured cost

The existing range comparison keeps ordinary values and NaN on the direct
conversion path. Only values outside the finite FLOAT range call one private
cold converter. It accepts signed infinities and returns the original
contextual invalid-input error plus zero for finite overflow. Exact boundary
neighbors and both signed MaxFloat64 values test finite overflow; existing
plain/dictionary tests retain infinities, NaN, signed zero and finite boundaries.

On 57, Linux/amd64, AMD Ryzen 9 7900X, Go 1.26.4 and source-matched CPU native
libraries, the final converter is inlined under default compiler settings
(cost 77, budget 80). The standard benchmark binary has no outer converter
symbol or scalar call to it. No compiler directives, performance-only flags,
extra memory/state or input-specific shortcuts are used.

The same committed benchmark source runs on exact base 1a3e7fb696,
pre-fix 6eb3293fef and the final follow-up. Base/pre overlays only supply that
benchmark and change the shared page-test fixture argument to testing.TB;
their production source is unchanged. Five interleaved rounds per revision,
one second per case, use CPU 23 and GOMAXPROCS=1. Medians:

| Case | Base 1a3e7fb696 | Before 6eb3293fef | Final follow-up | Final/base | Final/before |
| --- | ---: | ---: | ---: | ---: | ---: |
| Scalar finite (ns/value) | 0.9541 | 2.0970 | 0.9532 | -0.09% | -54.54% |
| Scalar NaN (ns/value) | 0.9543 | 1.4580 | 0.9537 | -0.06% | -34.59% |
| 4096-value plain page (us/page) | 120.8120 | 123.8330 | 121.1160 | +0.25% | -2.19% |
| 4096-value dictionary page (us/page) | 111.2290 | 115.5350 | 112.0040 | +0.70% | -3.06% |

Both scalar cases allocate zero bytes/objects. Both page cases retain exactly
98,320 B/op and 2 allocs/op on all three revisions and all samples. Pages contain
4096 real Double values; setup asserts row/value counts, plain/dictionary
encoding and each mapped value (including signed-zero bits). File creation is
outside timing and the output vector is reused with its length reset.

These are scalar and in-memory page-mapping measurements. They do not measure
end-to-end LOAD, file I/O, Darwin/arm64 or a general workload speedup. Small
page differences are reported numerically rather than treated as significance.
Earlier candidates were retained and rejected: range-first still lost inlining,
and an inlined error-only checker retained an extra normal-path error branch
and about 19% scalar overhead against base.

### Validation and evidence reuse

- Final source: four focused root tests and all 593 root tests in
  pkg/sql/parsers/dialect/mysql, pkg/sql/parsers and pkg/sql/colexec/external
  pass with -short -tags=matrixone_test, zero skips.
- gofmt, standard vet, pinned molint and configured incremental golangci-lint
  pass on those three owners.
- The existing numeric_literal_exponent BVT passes 19/19 twice on the same
  real instance, zero failed/ignored/abnormal statements, normal shutdown and
  explicit fixture cleanup. It uses ordinary expected-result comparison,
  without genrs. This run preceded the final Parquet-only layout change;
  lexer and case/result bytes are unchanged and checked explicitly.
- The initial TestReadDirSymlink failure under a hidden task TMPDIR also
  reproduces on exact base. Base passes under the visible task TMPDIR, as
  does the final complete owner run, without test/assertion changes or skips.
  Failed performance candidates and task-runner setup failures remain recorded
  separately and do not count as passing evidence.
- Design and final review use gpt-6.1-sol/max. The PR body records the final
  reviewed source and outcome. Unchanged display, TLS, FULLTEXT, Stage,
  branch/restore and upgrade evidence retain their stated revision and scope;
  those broader suites are not rerun for this two-owner follow-up.
