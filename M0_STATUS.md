# M0 restricted metadata-only ADD prototype

Decision, 2026-10-22: **CF9 is a qualified, same-version experimental candidate
for a restricted Draft PR, not a production feature or a Ready-to-merge change.**
Focused local validation and normal-binary Linux small-data qualification passed.
A later CF9-only SF100 first/warm screen also passed. A newly completed matched
B/CF9 first/warm pair, statistical significance and final background-debt
accounting are NOT established. These distinctions supersede the historical
local-only status below.

Base: `68e642fdc533e96ed8f83ac44659119ad8be97af`.
Kernel candidate: `cf9f5ff0f6cfbc11066e24c5bf209234f154cee9` (CF9).
Publication branch: `codex/m0-restricted-experimental-draft`.
The publication delta after CF9 contains documentation and a SQL BVT fixture;
it does not include the separate block-materialization diagnostic or a
fully-deleted-block skip. No kernel behavior is changed by that publication delta.

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

Default hooks still select COPY and retain ordinary read splitting. The shared
reader/catalog/PRE_INSERT changes require their own review and default-build
tests; COPY selection alone is not a compatibility proof.

## Verified

The following original local evidence is retained on base `68e642fd`, with
owned Go cache and hash-bound native artifacts (`darwin/arm64`, CPU). Later CF9
version/reader deltas have additional focused evidence described below; these
rows must not be mistaken for a fresh whole-PR independent review.

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

## CF9 Linux Qualification And Screen

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
- Independent fixed-delta reviews exist for reader allocation and the version
  codec. They do not replace independent review of the complete original M0
  patch or approval of the publication head.

The older `72422387` pilot remains historical evidence under its original
background-correctness HOLD; it is not promoted into healthy CF9 evidence.

## BVT And CI Boundary

`test/distributed/cases/ddl/m0_customer_add_clone.sql` and its `.result` cover
the public customer ADD/UPDATE/snapshot-clone path, nullable inserted suffixes,
NOT NULL rejection, COUNT behavior and parent/child isolation. They are authored
for both default COPY and `mo_m0` result semantics. Their execution on the
publication head remains pending exact-head BVT CI; no new BVT PASS is claimed.

Default CI does not establish M0-path coverage: the M0 SQL integration files
require `//go:build mo_m0`, while the ordinary UT runner selects
`matrixone_test`. A maintainer-run opt-in tag job must provide the normal CGo
setup and run `go test -tags mo_m0 -count=1` with the focused selections below:

- `./pkg/tests/sqlintegration`: `TestM0MetadataOnlyAddColumnSQL`,
  `TestM0ConcurrentAddColumnSQL`, `TestM0CloneMergeProofUsesDerivedVersion`,
  `TestM0AlterRollbackAt*`, `TestM0SecondAlterFailureKeepsFirstCommit`,
  `TestM0AlterCancelWindow`, `TestM0AlterInsertInterleave` and
  `TestM0MergeReadsOldObjectsWithoutCallerProof`.
- Default `./pkg/sql/plan`: `TestDefaultAlterAddColumnUsesCopy`.
- Focused catalog, proof, writer, read-shard and recovery tests in both relevant
  modes; existing identical local receipts may be reused with explicit binding.

Passing SQL result semantics without path/object evidence is not a claim that
COPY was eliminated. Experimental-tag exact-head CI remains a separate open
gate; this Draft does not silently enable the experimental build in production CI.

## Open Gates

- New BVT exact-head execution and explicit experimental-tag CI/review remain
  pending. The publication head adds no kernel changes beyond CF9.
- No new complete matched B/CF9 first/warm pair, repeated/stability matrix or
  all-branch SF100 full-content acceptance. Do not wait for another optimization
  or a rerun merely to publish this restricted Draft.
- UPDATE remains a known performance tradeoff. Final background drain CPU,
  writes and debt are not closed. No full-deletion skip, proof cache or fast
  filter optimization is included.
- The experimental build conservatively disables broad COUNT rewrites, not
  only a single customer query. Together with proof/filter and allocation work,
  this is part of the measured prototype package, not a pure COPY-removal gain.
- No crash, torn-write or power-loss matrix.
- No multi-CN rollout, lease handoff or mixed-version reader/writer interop.
  The default reader fails closed, but a mixed binary deployment has not been
  exercised.
- No production enablement. Unsupported schemas and DDL remain outside the
  domain; ordinary builds still select COPY and `mo_m0` is not a deployment
  recommendation.

## Lifecycle

The accepted CF9 screen completed its own service/worker/watchdog closure,
seal/readback and successful-volume retirement. Failed receipts remain retained.
Publication preparation does not touch Ubuntu, ongoing diagnostics, old volumes,
shared caches, issue state or other branches. Keep the PR Draft and
`do-not-merge/wip`; QA is required before production or Ready consideration.
