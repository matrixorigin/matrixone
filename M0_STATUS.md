# M0 restricted metadata-only ADD prototype

Architecture follow-up, 2026-10-27: [R3 read-boundary proposal](docs/design/20261027-m0-read-boundary-r3.md)
is a draft for maintainer decision, not approval or an implementation change.
It reopens the R2 default-compatibility policy and identifies an unresolved
trusted-origin/classification obligation. Current source remains `a44bc4b3`;
CI and earlier tests do not resolve that obligation. R2 and its model review
remain unchanged as historical records. No new test or performance result is
claimed, and the maintainer architecture CR remains open.

R3 evidence publication, 2026-10-27: the [inspectable local evidence package](docs/design/m0-r3-evidence/README.md)
contains the original three-test patch, reproduction commands, sanitized result
summary and source/native/receipt hashes. The expanded R3 includes the scoped
CREATE/COPY/clone/checkpoint-restart SQL result and synthetic default-reader
comparison. This is publication of existing executions; no test was rerun for
the documentation update. Trusted classification and supported historical
reads under shutdown upgrade and backup/restore remain the design decisions.
Mixed-version operation and downgrade are not added as approval requirements.

Review handoff, 2026-10-23: **request formal maintainer review of the restricted,
default-disabled M0 candidate, not production enablement or merge approval.**
The reviewed kernel head is `b3c86e3f3375f6441d3326d08ac23ac6598d4d1d`.
This status refresh changes documentation only. The evidence below is bound to
its actual source and mode; a documentation successor does not create a new
kernel, Linux binary or performance result.

CI-only follow-up, 2026-10-24: the historical dump round-trip test now verifies
the TN flush response and waits for a post-flush CN logtail frontier before
each complete DUMP. TN completion alone did not make the replacement visible to
the CN. This changes test synchronization only, not kernel/native inputs or the
DUMP in-memory-row rejection. It does not create new Linux/performance evidence.

Design-gate repair, 2026-10-26: the formal review of `4c9092203a` requested
a separately approved design, not a new runtime fix. The complete proposed
[restricted M0 design](docs/design/20261026-m0-restricted-add.md) defines the
append invariant, CN/TN commit and rollback ownership, proof-context limits,
recovery, shared default compatibility and resource/performance tradeoffs.
This is a distinct retrospective approval phase; prior `PASS_CODE_REVIEW`
and green CI do not establish design approval. Isolated Astra medium rejected
R1's undecided historical boundary, then approved R2's explicit source/geometry
and migration contract. The exact hashes, reviewer session and separate
decisions are in [the design review record](docs/design/20261026-m0-restricted-add-review.md).
The design requires one implementation alignment: the legacy proof must bind
the persisted caller's real DB/table IDs and canonical column seqnum/type,
not names alone. Native inputs and all other kernel closures remain unchanged.
That correction is now implemented and independently delta-reviewed after the
design decision (`PASS_CODE_REVIEW`, not a full-PR approval). The direct wrong-
type admission counterexample failed before the fix. The canonical-system and
wrong-identity/type/seqnum/object controls, default/M0 owning-package tests with
`-vet=all`, and persisted `TestBlockRead`/`TestBlockRead2` passed locally. No new
shared state is introduced; no old race, Linux or performance evidence is
relabelled as a new execution. The review record binds the source hashes.
Owning-package incremental lint and molint passed in default/M0 modes, as did
the repository error check, gofmt and whitespace checks. The new publication
head still owns its remote CI and maintainer review outcomes separately.

Base: `68e642fdc533e96ed8f83ac44659119ad8be97af`.
Kernel candidate: `cf9f5ff0f6cfbc11066e24c5bf209234f154cee9` (CF9).
Publication branch: `codex/m0-restricted-experimental-draft`.
The original publication commit `971e405e53` added documentation and a SQL BVT
fixture only. After Draft creation, the repository bot automatically merged
`main` at `8f9c50468ea06cd52554f2a5ef94cfb137d930f6` as `7e22a398f254`.
That integration tree matches a local merge-tree computation. Subsequent
integration repairs were reviewed and qualified at
`e1d1ea829170cec09ed681eb1defce38135eb155` (Q1). The later public ordinal/View
repairs end at `b3c86e3f`; their local, CI and scoped Linux evidence is recorded
separately below. CF9 products are not relabelled as integrated-head products.
Neither block-materialization/shuffle diagnostics nor object-level UPDATE
replacement is included. PR #30958 is the independent BOOLEAN DEFAULT false
extension; it is not part of this PR's supported domain or acceptance claim.

## Scope

- Only `mo_m0` activates the append domain. Normal builds keep ordinary
  ADD/COPY selection. This does NOT make the whole patch tag-isolated: the
  generic compatibility changes listed below also affect normal builds.
- The domain is a non-temporary table with one hidden composite primary key,
  rowid last, dense ordinary seqnums, and no index, foreign key, check,
  partition, generated, auto-increment or backfill transition.
- Each ADD appends exactly one trailing nullable column in independent
  pessimistic RC autocommit. Clone preserves the complete layout and version.
  Unsupported alterations are refused or use the existing non-M0 path.

## Implemented

- Object and catalog proofs validate the complete typed prefix, dense identity,
  version transition and hidden-key position. A missing expected prefix is an
  error; only a proven nullable suffix is interpreted as NULL.
- CN ADD verifies every current persisted object before mutation, reconstructs
  the CN definition and derived indices, preserves version/extraInfo for undo,
  and rejects uncommitted data objects.
- Planner/executor admission chooses M0 ADD only under `mo_m0`. Clone planning
  preserves internal IDs, seqnums, hidden-key position, source version and
  appended columns.
- Read selection proves each object before metadata pruning or fast filtering.
  The M0 append domain bypasses the unproven fast-filter path. Metadata scan,
  statistics, `IS NULL`/`IS NOT NULL` expression pruning and block reads use
  absence-as-NULL only after the object proof marker is present. The predicate
  check uses the proof's written width, not the physical column descriptor.
- CN writers derive the object schema version from the complete write batch.
  TN flush and merge writers derive it from the complete current schema instead
  of reusing a stale in-memory block/catalog version. TN merge proves old input
  objects itself instead of relying on a CN caller context.
- CN catalog reconstruction uses the authoritative `attnotnull` field, and
  PRE_INSERT NULL checks resolve the written column by name. This prevents a
  stale default descriptor or hidden-column offset from turning a proven
  nullable suffix into a false NOT NULL failure.
- Three M0-only fault points cover private definitions, TN ALTER registration
  and catalog deletion. Default builds compile inert implementations and keep
  ordinary ADD selection on `COPY_ALTER`.
- Proof context is bound to the exact object name and written/schema widths.
  `mo_tables.rel_logical_id` keeps one narrow legacy-missing-column proof for
  pre-upgrade system objects.
- The real CN catalog tuple -> protobuf -> TN decoder preserves an existing
  nonzero `rel_version` as a `VersionDef`; zero-version decoding is unchanged.
  This closes the clone version-carrier loss, rather than weakening object
  proofs or blindly applying writer version inference to readers.
- Ordinary unordered M0 readers distribute complete block descriptors across
  readers, including persisted-object lists. Partition/tombstone ownership and
  the first memory sentinel are preserved; ordered and default paths retain
  their original split. This is not a universal scan-scheduling optimization.

## Changes Also Present In Default Builds

| Shared closure | Behavior and regression boundary |
|---|---|
| Generic object reads | Missing ordinary columns fail closed without an exact object/suffix proof; only the narrow legacy catalog exception remains |
| Catalog nullability | Rebuild `Default.NullAbility` from authoritative `attnotnull`, not a potentially stale default descriptor |
| PRE_INSERT | Resolve written columns by name, including the map fallback, and reject constant NULL for a required column |
| Catalog create decoder | Transport existing nonzero table versions to the real TN schema; do not add a persistent field |
| COUNT binding | Resolve column positions by identity/name and keep additional nullability guards; the broad COUNT rewrite disable is M0-only |
| Public ordinals and Views | Number visible physical columns densely without changing catalog identities; retain derived-View metadata filtering so unselected definitions are not evaluated |

Default hooks still select COPY and retain ordinary read splitting. The shared
reader/catalog/PRE_INSERT changes require their own review and default-build
tests; COPY selection alone is not a compatibility proof.

## Verified

The following original local evidence is retained on base `68e642fd`, with
owned Go cache and hash-bound native artifacts (`darwin/arm64`, CPU). Later CF9
version/reader deltas have additional focused evidence described below. These
rows retain their historical identity; integrated whole-candidate review and
subsequent delta evidence are listed separately below.

| Evidence | Result | Scope |
|---|---|---|
| `m0-unit-group-r2.json/.log` | PASS | Object proof, layout/version derivation, planner admission, writer derivation, same-relation cache, legacy `rel_logical_id` proof and M0 zone-map predicate control |
| `m0-success-concurrent-r2.json/.log`, `m0-concurrent-race-r1.json/.log` | PASS | Formal customer success closure plus concurrent ADD/ADD; focused `-race -count=3` |
| `m0-rollback-cancel-merge-r3.json/.log` | PASS | Private-def/TN/catalog rollback, first-commit/second-failure, cancellation window, old-object TN merge and reopened-session readback |
| `m0-insert-interleave-r6.json/.log` | PASS | DML-before-ALTER and ALTER-holds-lock deterministic barriers, timeout cleanup, old-layout insert, mixed update and NULL readback |
| `m0-missing-column-zm-r1.json/.log` | PASS | `IS NULL`/`IS NOT NULL` zone-map pruning follows the M0 object's written width |
| `m0-cache-legacy-unit-r2.json/.log` | PASS | Same-relation ColId/seqnum/name-map refresh and narrow legacy system-catalog proof |
| `m0-default-compile-closure-r2.json/.log` | PASS | Affected default closure, including inert `!mo_m0` plan stub |
| `m0-default-copy-plan-r1.json/.log`, `m0-default-copy-exec-r1.json/.log` | PASS | Default ADD chooses `COPY_ALTER`; ordinary COPY pipeline controls pass |
| `m0-vet-default-r2.json/.log`, `m0-vet-mo-r1.json/.log`, `m0-lint-incremental-r2.json/.log` | PASS | Default and M0 vet closures; incremental lint `0 issues` |
| `m0-native-build-r1.json/.log` | PASS | Hash-bound local `darwin/arm64`, CPU CGo native generation |

`TestM0MetadataOnlyAddColumnSQL` also checks:

- the formal customer schema and the original two ALTER statements;
- independent ADDs, catalog nullable flags, `IS NULL`, `IS NOT NULL`,
  `COUNT(column)`, insert, partial update, full update and `SELECT *` order;
- `metadata_scan` and full `table_stats` consume proven missing suffixes;
- mixed old/new objects, clone plus another ADD, historical snapshot reads,
  snapshot clone, active data branch, snapshot drop and parent drop;
- object fingerprints remain unchanged across both ALTERs;
- a CN restart before checkpoint, then flush plus an actual L0 merge and a
  TN+CN restart after `globalcheckpoint`;
- a failed non-NULL in-place ADD leaves the relation unchanged, and an explicit
  transaction control is rejected by the existing branch-lineage guard without
  schema mutation.

`TestM0ConcurrentAddColumnSQL` covers two independent concurrent ADDs using a
deterministic start barrier. It accepts either safe success subset, requires
`rel_version` to match the successful count, verifies catalog nullable rows,
keeps all old object fingerprints unchanged, and inserts through the resulting
schema. TN WAL and checkpoint-plus-later-WAL recovery remain separately covered
by the recovery fixture.

## Integrated Qualification And Review

| Closure | Evidence and validity for the reviewed kernel |
|---|---|
| Complete candidate review | Isolated `gpt-6-astra`, medium, reviewed `8f9c5046..22f01a2c` and its 13-file corrective delta to Q1. Final result: `PASS_CODE_REVIEW`. TN-applied fault coverage, private rowid identity and COMMENT admission findings were addressed. This is model review, not GitHub human approval. |
| Q1 Linux default/M0 | Fresh matching native and normal products at `e1d1ea82`; formal customer SQL, default COPY, partial/full UPDATE, NULLs, parent/child isolation, committed merge, rollback/retry and restart passed. R5 completed the TN-applied handler, cancel/deadline and ADD/DML interleave selections. |
| Q1 local unhappy paths | Actual TN-applied error/cancel/deadline, formal SQL, clone/merge and default COPY/COUNT/COMMENT passed. Concurrent ADD, cancel, deadline and interleave passed `-race -count=3`; two-mode vet and incremental lint passed. |
| Q1 to `b3c86e3f` | Only ordinal/View code, tests and the customer fixture changed (seven files). `Makefile`, `cgo`, `thirdparties`, `go.mod` and `go.sum` are unchanged. Unaffected proof/writer/rollback/recovery evidence is reused by content, not relabelled as a fresh whole-head run. |
| Ordinal/View review and regressions | Isolated Astra medium initial and corrective delta reviews found no blockers. Local default/M0 public ordinal, stored/filtered View SQL and planner regressions plus two-mode vet/lint and error/license/format checks passed. |
| Shared Linux supplement | D3 `64aa85d3` / C3 `0343d680` fresh products passed the shared ordinal/View regression, original customer fixture, content/NULL/isolation and restart checks. This is tiny-data scoped reuse, not exact-`b3` whole-product qualification or official mo-tester execution. |

D3/C3 planner bodies match the corrective ordinal/View implementation; helper
license indentation has no semantic effect. D3 also has a separate bound-column
ADD admission change in `m0_hook_mo.go`, absent from this PR. Their whole-product
PASS and DEFAULT false behavior are not imported here. Only the matching shared
closure is reused; ordinary M0 admission remains covered by this PR's Q1 tests.

Retained receipt identifiers outside the source tree:

- `software-dev-followup-20261023/INDEPENDENT_REVIEW.md`,
  `independent-delta-review.json`, `review-repair-manifest.json` and
  `QUALIFIED_BASELINE.json`: complete review, resolved findings, precise tests,
  native/products, R4 SQL and R5 remaining qualification.
- `m0-public-ordinal-20261023/DELTA_REVIEW.md` and
  `R2/DELTA_REVIEW.md`, `R2/MANIFEST.json`, `R2/CI_TERMINAL.json`: corrective
  review, both-mode regressions and actual CI case selection.
- `data-cleaning-default-false-20261023/CORRECTIVE_R2_LINUX_READBACK.json`
  and `D3_C3_IDENTITY.json`: shared tiny-data Linux supplement. The 212-file
  archive SHA-256 is
  `f5904fe7514c82943c836ab19f31907222f04a89cf93f0ceb105634760db4b10`.

Failed generations remain retained: `72422387` background HOLD, Q1 preparation
failures, initial ordinal HOLD and `55f3dec` View/license/coverage failures are
not rewritten as PASS. The data-cleaning performance line remains INCONCLUSIVE
and NOT_PROMOTED; it supplies no performance result for this PR.

## Historical CF9 Linux Qualification And Screen

Normal Linux products use Go 1.27.1, `GOAMD64=v3`, `GOEXPERIMENT=simd`, and the
same qualified native artifacts. No diagnostic or race binary supplied the
formal CF9 screen.

| Product | SHA-256 |
|---|---|
| B stock, `68e642fd` | `02a8e14d2ea58b9b19fe3e88325c3bc741810f612b86189db464c1e7d0773c54` |
| CF9 default | `fe4f650005a0d3d42b5a18a4e942d22dc56215c361888d86b356b591edaf2ecc` |
| CF9 `mo_m0` | `8dc621c6d5498a1be3bd967a2cda36065a24566d901898864335883b555641d2` |

The accepted small-data Linux evidence covers formal customer success, default
COPY control, actual TN-after-registration failure/rollback, retry, restart,
full-content equality, parent/child isolation and a committed L0 merge. Actual
CN/TN schema versions are parent 2, clone 2, child 4, preserved across restart.
Failed earlier products/collectors remain failures; they were not relabelled.

The later normal CF9 `software_dev` screen is SF100, 5 workers x 20 steps,
first/warm once each, with MO 8 CPU/16 GiB, client 2 CPU/2 GiB and aggregate
19 GiB. Each cohort completed 100 steps with zero SQL errors and passed
`benchmark_sanity`. Across both cohorts: 400 successful logical ALTERs with
400 M0 admissions, zero COPY attempts and passing background-health checks.

| Observation | Driver elapsed | First-run MO CPU |
|---|---:|---:|
| Existing B first-run reference | 319.88 s | 2505.60 CPU-s |
| CF9 first-run | 231.04 s | 1812.76 CPU-s |
| CF9 warm | 215.29 s | not used for the first-run comparison |

The first-run driver difference is -27.77%, for this candidate package and
historical reference only. B was not rerun alongside this screen and has no
accepted warm reference here. Actual operation graphs/workloads can differ;
this is not isolated metadata-only ADD causality or statistical significance.
The B reference completed its driver and sanity, but its old overall observer
phase remains FAIL because it misclassified an ETL successful-upload record.
Only the hash-bound driver reference is reused; no full baseline PASS is invented.
CF9 process wall was 321.49 s first and 302.56 s warm; the B reference did not
complete equivalent extra cleanup, so no end-to-end process-wall speedup follows.

Own MO/client/run swap and OOM stayed zero. Host-only swap-out was recorded as
WARN (0.348 MiB first, 5.168 MiB warm), not erased or declared harmless. The
screen did not run a full-content oracle over all SF100 branches. Background
health PASS and successful shutdown are not proof of complete background drain
or of lower maintenance debt.

Retained evidence identifiers, outside the source tree:

- `m0-update-fix-20261021`: CF9 normal products, small-data qualification and
  revalidation, version codec RED/GREEN, focused race x3, vet/lint receipts.
- `software-dev-rerun-v6-20261022`: `MEASUREMENTS.json`, `RESOURCE_SUMMARY.json`,
  operation graphs, result/seal/readback/cleanup receipts. Its
  `head-evidence.tar` SHA-256 is
  `6d0e62f37dfd12029200d6782db2029f4f5fe58a2d179fe87b3b311675bc17a0`.
- Historical fixed-delta reviews cover reader allocation and the version codec.
  The later complete integrated-candidate and corrective reviews are recorded
  above; none substitutes for formal maintainer approval or QA.

The older `72422387` pilot remains historical evidence under its original
background-correctness HOLD; it is not promoted into healthy CF9 evidence.

## BVT And CI Boundary

`test/distributed/cases/ddl/m0_customer_add_clone.sql` and its `.result` cover
the public customer ADD/UPDATE/snapshot-clone path, nullable inserted suffixes,
NOT NULL rejection, COUNT behavior and parent/child isolation. At `b3c86e3f`,
[MatrixOne ALL CI run 39876195531](https://github.com/matrixorigin/matrixone/actions/runs/39876195531)
passed, including UT, SCA, UT Coverage, Coverage and CI Required. PROXY job
`120165869158` actually selected and passed the customer fixture **48/48** and
`information_schema_views_metadata.sql` **44/44**, with zero failed/ignored/
abnormal items. The PESSIMISTIC job did not select these two fixtures; its green
result is not counted as duplicate case coverage.

The documentation successor `0955ad60f4` did not pass its own CI: run
`39932171577` attempt 1 hit embedded-cluster service/cleanup deadlines. Its one
failed-jobs-only retry did not repeat those failures but failed
`TestTimestampAddHistoricalDumpRoundtrip` after an `OK` TN flush because the CN
still saw in-memory rows. The CI-only follow-up above fixes that missing
visibility barrier without sleeps, DUMP retries or relaxed assertions. Retain
both failed attempts. Repair head
`4c9092203a49f5bba5fcd8e1d2f2a8c6bd0c4982` passed
[run 39987278483](https://github.com/matrixorigin/matrixone/actions/runs/39987278483)
and its automatic Ready-event
[run 39994658132](https://github.com/matrixorigin/matrixone/actions/runs/39994658132),
including UT/SCA/Coverage and CI Required. In the former, PROXY job
`120534512275` actually passed the customer fixture 48/48 and View metadata
44/44; the latter's job success is not additional case-selection evidence.
A later documentation-only commit has its own CI status; these runs remain
bound to `4c9092203a`, not relabelled as exact-head successor CI.

Default CI does not establish M0-path coverage: the M0 SQL integration files
require `//go:build mo_m0`, while the ordinary UT runner selects
`matrixone_test`. Explicit tagged execution is supplied by the local/Q1 Linux
evidence above, not inferred from green ordinary CI. For maintainer reproduction,
use the normal CGo setup and the focused selections below:

- `./pkg/tests/sqlintegration`: `TestM0MetadataOnlyAddColumnSQL`,
  `TestM0ConcurrentAddColumnSQL`, `TestM0CloneMergeProofUsesDerivedVersion`,
  `TestM0AlterRollbackAt*`, `TestM0SecondAlterFailureKeepsFirstCommit`,
  `TestM0AlterCancelWindow`, `TestM0AlterInsertInterleave` and
  `TestM0MergeReadsOldObjectsWithoutCallerProof`.
- Default `./pkg/sql/plan`: `TestDefaultAlterAddColumnUsesCopy`.
- Focused catalog, proof, writer, read-shard and recovery tests in both relevant
  modes; existing identical local receipts may be reused with explicit binding.

Passing SQL results alone does not prove COPY elimination. Q1 tagged/Linux
qualification includes path and object evidence; ordinary CI does not silently
enable the experimental tag. A documentation-only successor reuses these
semantic checks but must satisfy its own applicable CI checks. No single
exact-head Linux run of every closure is claimed.

## Formal Review, QA And Non-Goals

The formal review handoff targets `XuPeng-SH`, the CODEOWNER covering
the affected planner/objectio/disttae/TAE paths. Completed model review is not
an approval on GitHub. Ready-for-review is not Ready-to-merge.

At `4c9092203a`, XuPeng-SH requested changes for the missing approved design
revision; full implementation review was deferred, with no runtime defect
asserted. gouhongshen's automated full-diff review was formally APPROVED with
two non-blocking follow-ups. Both are retained, not dismissed by green CI:

- Shared fail-closed reads can reject historical objects previously NULL-filled.
  QA/release must inventory relevant upgrade sources and test ordinary/system
  historical layouts; unknown geometry needs a narrow proof, not blanket NULL.
- Repeated proof work remains a cost risk, including pristine eligible roots.
  Reader-local same-object/context reuse already exists; no cross-scan cache is
  claimed. The core proof chain was already present in CF9. Current-head cost
  acceptance is still open; this repair adds no speculative cache or benchmark.

**QA required: yes.** SQL/catalog visibility, clone/merge and recovery semantics
need deployed acceptance. QA should verify the restricted same-version build
and ordinary default build separately: exact product/native identity, default
COPY, M0 admission and unchanged old objects, nullable suffix reads, parent/child
and old-snapshot isolation, actual merge commit, TN-applied rollback/cancel,
ADD/DML ordering, restart and ordinary/subscription View metadata permissions
and filtering. Automated coverage is reusable; QA is not claimed PASS.

Production enablement, mixed-version/multi-CN rollout, power-loss/crash matrices,
new schema/default support and final maintenance-debt acceptance are outside
this review handoff. CI, model review and cleanup do not silently satisfy them.
Unsupported schemas/DDL retain the documented rejection or ordinary fallback;
the experimental tag is not a deployment recommendation.

No new matched SF100 pair, stability matrix, all-branch full-content check or
new performance claim is required to begin formal review. UPDATE cost attribution
remains inconclusive; no shuffle/full-delete/proof-cache, fast-filter,
object-replacement or P1/P2 optimization is included. Conservative M0 filter
and COUNT restrictions remain part of the package's tradeoffs.

## Lifecycle

The accepted CF9 screen completed its own service/worker/watchdog closure,
seal/readback and successful-volume retirement. Failed receipts remain retained.
This handoff does not access Ubuntu, mutate diagnostics/old volumes or shared
caches, merge the PR or close parent issue #30518. That tracker remains open.
PR #30958's independent extension and performance work do not block review of
this narrower PR. Maintainer review and QA remain separate from completed
code/regression evidence and from any later merge decision.
