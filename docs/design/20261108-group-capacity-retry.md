# Preserve evaluated input during Group capacity recovery

Tracking issue: [#32369](https://github.com/matrixorigin/matrixone/issues/32369).
Implementation PR: [#32368](https://github.com/matrixorigin/matrixone/pull/32368).
Discovery: [#32060](https://github.com/matrixorigin/matrixone/issues/32060).
Reviewed baseline: `89ca3dbbf6d594d7614b8e1ca80f6b0300ed3cd2`.
Design decision: **PASS** for the scope below; implementation and validation
must be reviewed against this design.

## Problem and scope

Group evaluates one complete child batch before committing it in chunks of at
most `hashmap.UnitLimit` rows. On an accounted capacity rejection, it can drain
exact DISTINCT state or spill the committed prefix before retrying an
unpublished chunk. Currently, spilling resets expression executors and the
retry path clears `evaluated`. Group consequently evaluates the entire batch
again, even if an earlier chunk was already published.

An operator test rejects one real allocation after 256 groups are published.
For 512 rows with an incrementing side-effecting expression, the clean
baseline spills and reloads, evaluates the root twice, invokes 1,024 side
effects, and returns group keys `1..256` and `769..1024`. The correct result
for this input is one evaluation, 512 side effects, and keys `1..512`. The
test uses the existing allocation-account controller, not a production hook.

This fix is owned by Group's batch and spill lifecycle. It does not fix the
integer DISTINCT performance reported in #32060, PAD SPACE equality, ordinary
PK/FD assumptions, or expression replay elsewhere in the pipeline.

## Contract

- Once all group roots, aggregate-argument roots, and rollup sentinels for one
  child batch are evaluated, every pre-publication retry reuses those exact
  vectors and the current chunk offset. A committed prefix is never replayed.
- If the initial `evaluateBuildInput` fails, return its error without retrying
  that partially evaluated batch. A failed root may already have produced
  side effects; the same is true when a later root or sentinel allocation
  fails after an earlier root succeeded. Effects from a failed query are not
  rolled back, but Group must not automatically repeat them.
- Only an existing retryable allocation-capacity error at an existing
  pre-publication boundary may trigger the existing drain or spill recovery.
  Reuse its progress checks and error handling. Errors after publication,
  cancellation, failed drain, and failed spill remain terminal.
- No input expression is shared merely because another expression has an
  equal AST. The invariant applies to the already evaluated occurrence.

## Design

Factor `container.resetForSpill` in `pkg/sql/colexec/group/types2.go` into a
resident-state reset and an evaluated-input reset. Resident reset retains the
existing `distinctGroupReset` transition, releases the hash and accounted
iterator scratch, detaches the reusable iterator, and releases group-by batches and
resident and spill aggregate lists. Input reset resets
the group and aggregate-argument executors and frees the Group-owned rollup
sentinels. Keep `resetForSpill` as the existing full-reset wrapper.

Keep `spillDataToDisk` in `helper.go` as the full-reset entry point. Factor a
private variant, or private mode parameter, which after a successful spill
resets only resident state. The retaining mode has one production caller:
`Group.retryBuildBatchAfterCapacity`, after the batch evaluation completed.
Preserve existing spill accounting, recovery floor, measurable-progress
checks, hash reconstruction, and aggregate-list rebuild.

In `Group.buildOneBatch` in `exec2.go`, return immediately if
`evaluateBuildInput` fails. After it succeeds, retain the evaluated input through
both adaptive-growth recovery and ordinary preflight recovery. Exact DISTINCT
drain does not reset the evaluation. On a successful retaining spill, retry
the same `i` with a fresh hash preview and the same evaluated vectors. Retain
the current distinction between explicitly pre-publication commit errors and
terminal failures in group publication, `GroupGrow`, or `BatchFill`.

`H0` has no group key: initial evaluation already fails directly; later
aggregate preflight may use exact DISTINCT drain and must keep the evaluated
argument vectors. It must not start ordinary group spill. SORT ROLLUP has a
separate call path and is outside this change.

| Existing operation | Evaluation state | Reset after spill |
| --- | --- | --- |
| Group capacity retry after successful batch evaluation | Current batch remains live; prefix may be committed | Resident only |
| Group post-batch spill, end-of-input spill, `vm.EvalReset` | No active build batch | Full |
| MergeGroup post-batch, end-of-input, and capacity-retry spill | No Group expression-root batch | Full |
| Spill reload entry, record retry, adaptive respill, and final respill | Replay is at a spill-record boundary | Full |
| Grouped exact DISTINCT finalization spill | Input has been consumed | Full |
| Group or MergeGroup terminal `Reset` / `Free` | Attempt has ended | Full cleanup |

## Ownership and failure boundaries

`Group.Call` processes `buildOneBatch` synchronously and does not request the
next child batch until it returns. A column executor may borrow a vector from
that child batch; Group must not free it or call the child during recovery.
Expression-owned vectors remain owned by their executors; rollup sentinels
remain separately owned by Group. A successful retaining spill may free only
the resident state that was written. The next normal evaluation or full reset
releases or reuses expression storage through its existing owner; terminal
cleanup releases all remaining resources without double-freeing borrows.

The retained storage is bounded by one existing input batch's evaluated
vectors and sentinels, not by a fixed byte count. No additional value copy,
goroutine, persistent state, or unaccounted allocation is introduced.
Expression allocations remain charged to their current account, child
vectors to their child, and spill files and descriptors to the existing
statement budget. Retention can reduce recovery headroom: a low-memory query
that previously succeeded by replaying pure expressions may now return its
capacity error. When no drain or spill can make progress, terminate using the
existing error behavior; never replay the expressions to create space.

There is no new wire, catalog, persistent format, configuration, tenant
boundary, or upgrade gate. The normal no-spill path is unchanged. This design
makes no performance-improvement claim.

## Alternatives

- Re-evaluate as today: rejected by the proven wrong-result counterexample.
- Retry expressions classified as pure: requires a complete proof for nested
  roots, session state, warnings, and partial failure; an incomplete whitelist
  is unsound and adds another execution policy.
- Copy every evaluated vector before spill: adds a potentially large allocation
  at the point of capacity pressure and duplicates ownership and cleanup.
- Move roots into a planner PROJECT: changes placement and ordering and does
  not automatically cover aggregate roots or partial projection failure.
- Retain the already evaluated batch and spill only resident state: selected;
  it requires no extra copy or persistent state and preserves one value per
  expression occurrence.

## Validation

1. Keep the deterministic account-controller reproduction as a regression.
   Assert a real spill and reload, one evaluation, exactly 512 side effects,
   and complete keys `1..512`; include ordinary and inactive-rollup groups.
2. Fail group-root and aggregate-argument evaluation after an earlier root has
   run, with resident groups available for the old retry. Assert the original
   error and no replay. Cover sentinel allocation failure, cancellation and
   spill failure, as well as post-publication errors remaining terminal.
3. Exercise adaptive spill, exact DISTINCT drain, H0, both `NeedEval` modes,
   child-borrowed and expression-owned vectors, no-progress / capacity-floor
   failure, and `Reset`/`Prepare` reuse. Preserve the existing full-reset
   sentinel and MergeGroup/reload tests. At success, error, and cancel assert
   account, Group mpool, spill disk, and FD charges are released by their
   owners.
4. Run focused regressions, affected owning-package normal and race tests,
   and incremental static checks. Record nonempty selection, commands, source
   fingerprints, and terminal results; reconcile the complete implementation
   and test diff with this design in independent review.
