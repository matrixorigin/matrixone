# Native Rebase development plan v0.2

Status: P0 selected-PK contract and P1 prerequisite test implemented in an
isolated worktree; no production executor or public syntax implemented.
See [P0/P1 record](native_rebase_p0_p1.md) and machine-readable
`native_rebase_validation.json` for runtime status; test code is not PASS evidence.
Supersedes the informal proposal to immediately introduce a new branch catalog
and multi-transaction publication protocol. Neither is an approved dependency.
Semantic working draft: [native_database_rebase.md](native_database_rebase.md).

## Objective and exclusions

Deliver native net-state Rebase: replay the feature changes since its old base
onto a fixed upstream state, correctly retain the new base, and integrate with
the adapter without per-row client replay or silently weaker semantics.

Exclude Reset->Rebase root-cause investigation, Reset repair, unrelated Merge
optimization, full Git commit-by-commit rewriting, and general schema merge.
No-PK duplicate and hidden-key cases remain necessary Rebase correctness tests;
they do not require a Reset implementation or a large data_agent reproduction.

Two deliverables must be distinguished:

- R1: native database replay into a new destination with atomic data/ancestry.
- R2: integrated branch Rebase with publication/history and repeat-operation
  correctness. R1 is useful but is not completion of the whole objective.

## P0. Freeze dependencies and the semantic contract

Read exact current main and experimental-fork heads, compare ancestry and patches,
and inventory only endpoint resolution, database enumeration, shared transaction
execution, diff streaming, batch DML, privileges and ancestry retention.
Use fork commits only where required; no whole-fork import or speculative refactor.
Use isolated task files/worktree before production edits; preserve existing work.
Do not assume previously inspected SHAs are still the desired delivery base.

Inspect the adapter callers and tests, not just `_rebase_impl`, for conflict
direction, affected-row reporting, commit/log references, callbacks and retry.
Freeze Bf/F and Bu/U identities, source immutability, scope, and conflict policy.
Specify whether returned change counts mean touched keys or physical mutations.
Policies use explicit KEEP_UPSTREAM/KEEP_BRANCH names internally; adapter ours/
theirs mapping must be tested, never inferred from a Merge operation's naming.

Required table contracts:

- Explicit PK: whole-row three-way decision, absent distinct from NULL tuple;
  identical concurrent edits coalesce; conflicting edits obey explicit policy.
- No PK: hidden keys are an optimization only when origin and allocation prove
  identity. Visible duplicate-count semantics and identity/event replay are
  different contracts; neither may silently substitute for the other.
- Use current adapter behavior and tiny independently expected results to resolve
  no-PK semantics. Include independent identical inserts, overlapping deletes,
  delete versus update, NULL, duplicate rows and collation. Record deliberate
  incompatibilities; do not approve the existing Python bag model as evidence of
  compatibility. If unresolved, reject changed no-PK inputs and label R1 partial.
- Same-schema regular tables first. Enumerate exactly supported constraints,
  index kinds and generated columns. Reject unsupported categories before writes.
  Do not disable FK checks as a shortcut.

Output: dependency map, supported/rejected matrix and reviewed design revision.
Exit: no unresolved semantic choice for the selected implementation milestone.

## P1. Prove the existing transaction path before adding infrastructure

Try a single owner transaction for clone, batch replay and retained base metadata,
using existing lifecycle admission and typed internal entry points. Identify every
callee that starts/commits a transaction or takes ownership of result buffers.
Do not invoke independent public SQL handlers and assume atomic composition.

Use tiny deterministic tests for a late-table conflict, cancellation and commit
failure. Verify that no destination or ancestry becomes visible and sources are
unchanged. Verify commit then restart preserves both destination and its base.
Check concurrent writes/drop/snapshot GC using deterministic synchronization.

Prefer this path if existing machinery supports it with bounded resources.
Single-request does not require multi-transaction staging. Adopt private staging
plus short publication ONLY if the single-transaction path is shown inadequate.
Such a change requires a separately reviewed lifecycle: access isolation, durable
operation identity, quotas, snapshot pins, generation checks, crash recovery,
cleanup ownership, retries and backup/upgrade behavior. A specially named public
database is not a private stage.

Output: concrete transaction ownership map and narrow executable evidence.
Exit: chosen path's rollback/commit/recovery semantics established, not mocked
away by a unit test of a successful callback.

## P2. Implement native explicit-PK replay (R1)

Add parser/AST/formatter, dispatch, authorization and execution as one coherent
feature. Freeze syntax only after selecting the destination/publication contract.
Provide no reachable parser-only stub or success-without-work handler.

Bind and pin four endpoints; validate logical base correspondence and schema;
derive Bf->F and Bu->U changes; resolve conflicts; apply bounded typed batches;
retain U as the new base; commit the destination and ancestry together.

Reuse engine equality, collation, constraints and index maintenance. The Python
model is an independent scalar decision oracle only. It proves neither SQL type
semantics nor engine transaction correctness. Do not assume existing PICK is an
equivalent implementation of the four-endpoint operation.

Start with no new cache/background worker and sequential table coordination.
Use existing query budgets, cancellation and spill; fail explicitly when a
required bounded path is unavailable. Do not export full changes to Python.

Exit: public SQL result tests plus multi-table rollback, repeat-Rebase and
Rebase-then-Merge tests pass for every supported category. Runtime tests required.

## P3. Complete no-PK replay under the accepted contract

Implement the P0-selected semantics in the server with exact duplicate handling.
Retain/reuse hidden-key identity only where proven; independent branches with
equal generated key values must not be accidentally matched.

For a visible-state path, normalize/cancel equivalent content with typed equality;
hash matches require value verification. Delete exactly the required multiplicity
using batched physical-row selection, not per-row client SQL or deleting every
matching duplicate. Track scan/write volume and bounded memory/spill.

Exit: duplicate/NULL/type/collision cases and hidden-ID-remapping counterexamples
pass. No unsupported custom-resolution case silently switches to this path.

## P4. Publish branches and integrate the adapter (R2)

Inventory who owns branch name -> generation -> database and base/history today.
Prefer existing catalogs or transactional adapter tables if they support the
contract. Do not put benchmark-specific history tables into a generic kernel API.

If registry/history participate in the owner transaction, update them atomically
with the result, with an expected-generation check. If they do not, the adapter
needs an explicitly durable publication/recovery protocol; R1 alone is not atomic
end-to-end Rebase. Do not advertise full native Rebase before this is resolved.

Check stale-generation publishers, target writes during preparation, pinned old
readers, source snapshots/history, retries after lost acknowledgements, and
publication failures. Never delete an adopted generation on CAS failure.
Reclaim old generations only after publication and reader/reference safety.
Existing SQL/session database resolution must observe the selected generation;
changing a pointer that readers never consult is not a working implementation.

Adapter capability-checks before mutation. Record execution path and fallback
reason. Unsupported schema/callback cases fail or use explicitly selected legacy
mode before native work; never fallback after an uncertain native commit.

Exit: supported adapter calls, log/history references and future Rebase/Merge
behave as specified, including interruption and restart.

## P5. Validate cost, then introduce proven fast paths

First tiny matched fixtures, then the fixed W1 Rebase microbenchmark. Preserve
data, branch graph, changes, policy, resource limits and timing boundaries.
Compare baseline/native results before comparing elapsed. Record client SQL count,
internal statement/batch count, scan/write volume, CPU, allocations, operation
elapsed and preparation/cleanup separately. One request can still be expensive.

Fast-path candidates: exact zero delta, unchanged upstream, unchanged tables,
shared immutable objects with correct visibility, and proven inherited row IDs.
Each needs a deterministic counterexample; do not change result semantics for speed.
Run a small supported macro fixture after micro validation; full data_agent is
not a release gate for this bounded scope if it needs excluded schema/callbacks.
Do not claim that its existing failure has been fixed.

## Delivery and completion

Suggested stacked changes: required prerequisites; reviewed Rebase contract;
native PK executor; no-PK support; adapter publication integration. Keep production
changes and their tests together, not a separate late correctness patch.

Each implementation closure uses focused UT/BVT and risk-based fault/restart
tests. Reuse unchanged valid evidence; no automatic full SF100 suite, remote
resource takeover, broad cleanup or new agent coordination is part of this plan.

Done means supported end-to-end native Rebase, fixed input/output semantics,
atomic/recoverable publication, bounded resources and matched performance evidence.
Unsupported features must be visible. R1 success, model PASS and reduced SQL count
are separate milestones, none of which alone proves complete native Rebase.

Current evidence: 10 original model tests, 6 frozen adapter-method contract
checks and 11 Linux-wrapper failure-path checks pass. A real SQL prerequisite
probe passed on Linux, and separate commit-failure/graceful-restart and
deterministic server-cancellation probes produced scoped PASS evidence.
A later isolated startup aborted in native malloc before its fixture SQL;
That historical failure remains preserved. No production executor, native
adapter validation or performance run exists yet.
The bounded 2026-10-30 follow-up adds real GC survival evidence but reproduces
native heap corruption in the source-DROP observation and crash-helper startup.
See the follow-up evidence directory in `native_rebase_validation.json`. No unchanged retry
or allocator workaround was used for those results.
The subsequent native-namespace qualification runs the original binaries with
the same rootfs/native library bytes, without observed heap abort. It exposes
and corrects two test issues: mo_locks needs the outer physical key encoding,
and the crash fixture must allow the normal 60s old-generation expiry within
its CN discovery budget. Concurrent source DROP (success/cancel) and real
SIGKILL/fresh-process recovery now PASS in bwrap (4.77s/66.68s). This clears
the existing-transaction P1 substrate blocker, not the typed native R1/R2
implementation or its separate design/review obligations. The old proot abort
has no proven allocator/root cause and remains unresolved.
Do not estimate speedup or completion time until P0/P1 size and dependencies are known.
