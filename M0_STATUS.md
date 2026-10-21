# M0 restricted metadata-only ADD prototype

Decision: **the restricted SQL prototype PASSes under the `mo_m0` build tag.
The local candidate has closed the focused rollback, cancellation, ADD/DML
interleaving and default-build gates. Linux qualification and SF100 remain
HOLD until the final local commit is built there.**

Base: `68e642fdc533e96ed8f83ac44659119ad8be97af`.
Branch: `codex/software-m0-68e642fd` (local, unpublished).
Worktree: `/Users/violet/.codex/worktrees/software-m0-68e642fd/matrixone`.

## Scope

- Only `mo_m0` activates the append domain. Normal builds compile inert stubs,
  keep the ordinary ADD/COPY path, and do not mark missing metadata columns as
  NULL.
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

## Verified

All evidence below is on base `68e642fd` with owned Go cache and the
hash-bound `local-cpu-diagnosis` native artifacts (`darwin/arm64`, CPU).

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

## Open gates

- No Linux build, formal customer-schema small-data qualification, real-process
  TN-registered failure or restart proof yet.
- No SF100 B/M0 first-run plus warm pair, path ledger, operation graph or
  performance attribution yet.
- No crash, torn-write or power-loss matrix.
- No multi-CN rollout, lease handoff or mixed-version reader/writer interop.
  The default reader fails closed, but a mixed binary deployment has not been
  exercised.
- No production enablement. Unsupported schemas and DDL remain outside the
  domain; ordinary builds still select COPY and `mo_m0` is not a deployment
  recommendation.

## Lifecycle

Local runner-created processes completed. Successful temporary roots were
removed after evidence binding; failed receipts and their relevant evidence
remain. No mo-129, remote CI, PR, push or automation was used. Ubuntu and SF100
remain pending the final local commit.
