# Full Outer Join — Implementation Plan (#24192)

## Problem
`FULL OUTER JOIN` is parsed and planned as `plan.Node_OUTER`, but the compile
layer panics with `NYI("join typ '0'")` because no compile case handles it,
and the hash-join runtime never invokes its existing `IsFullOuter()` predicate.

## Approach
Reuse the existing hashjoin operator. Full-outer = left-outer probe + right-outer
unmatched-build emission. Both sub-mechanisms exist in isolation; we wire them to
fire together when `JoinType == Node_OUTER`.

Phase 1 ships **broadcast equi hash-join** support, with **single-CN probe**,
**spill disabled**, **shuffle disabled**, and **non-equi rejected** (panic NYI).
This is the smallest correct surface.
Phase 2 (follow-up) adds shuffle + true multi-CN probe.
Phase 3 (follow-up) adds spill correctness.
Phase 4 (follow-up) adds non-equi `loopjoin` full-outer.

---

## Design decisions (from critique)

### D1. Use semantic helpers, not scattered `|| IsFullOuter()`
Add to `pkg/sql/colexec/hashjoin/types.go`:
```go
func (h *HashJoin) EmitUnmatchedProbe() bool {
    return h.IsLeftOuter() || h.IsLeftSingle() || h.IsLeftAnti() || h.IsFullOuter()
}
func (h *HashJoin) EmitUnmatchedBuild() bool {
    return h.IsRightJoin || h.IsFullOuter()
}
```
Replace all gates in `join.go` (and later `spill.go`) with these helpers.
Avoids the `IsRightJoin` overload risk and reduces miss-a-gate regressions.

### D2. Phase 1 disables spill for full outer
`spill.go` uses `IsRightOuter()` (which checks `JoinType == Node_RIGHT`) for
build-empty-bucket gates — setting `IsRightJoin=true` does **not** trigger them.
Fixing that properly is Phase 3 work. For Phase 1, in `constructHashJoin`
(`operator.go:1613`), force `ret.CanSpill = false` when `JoinType == Node_OUTER`.

### D3. Phase 1 forces single-CN probe for broadcast
Use `newProbeScopeListForBroadcastJoin(probeScopes, true /*forceOneCN*/)` for
`Node_OUTER`. This avoids distributed double-emission of unmatched-build rows
(no global cross-CN bitmap merge protocol exists today; the in-process `Channel`
merge only covers parallel operators within the same scope fanout).

### D4. Phase 1 rejects non-equi full outer
`loopjoin` has no full-outer support (`pkg/sql/colexec/loopjoin/join.go:86,
143-160, 283-345` — no build-bitmap, no finalize, `emptyProbe` only null-fills
LEFT/SINGLE). In the broadcast compile switch, when `JoinType == Node_OUTER`
and the join is non-equi, panic NYI (`"FULL OUTER JOIN with non-equi condition
is not yet supported"`). Phase 4 implements it.

### D5. Phase 1 strictly bars shuffle for `Node_OUTER`
Two layers of defense:
- `pkg/sql/plan/stats.go` `determineShuffleType` (or equivalent): never set
  `HashmapStats.Shuffle = true` when `JoinType == Node_OUTER`.
- Defensive early-return in `pkg/sql/plan/runtime_filter.go` before the
  `node.Stats.HashmapStats.Shuffle` branch (~line 88) for `Node_OUTER`.
- `compileShuffleJoin` (`compile.go:~2482`) leaves `Node_OUTER` in the default
  case so it would panic if it ever reached there (defense in depth).

---

## Phase 1 — Broadcast equi hash-join

### 1.1 Compile dispatch (`pkg/sql/compile/compile.go`)
- **`compileBroadcastJoinProbe` switch (~line 2570)**: add
  ```go
  case plan.Node_OUTER:
      if isEq {
          rs = c.newProbeScopeListForBroadcastJoin(probeScopes, true)
          // build constructHashJoin op + dispatch
      } else {
          panic(moerr.NewNYINoCtx("FULL OUTER JOIN with non-equi condition"))
      }
  ```
  Mirror the structure of the `Node_RIGHT` case (which already forces one CN).
- **`compileShuffleJoin` switch (~line 2482)**: leave `Node_OUTER` excluded.

### 1.2 `constructHashJoin` (`pkg/sql/compile/operator.go`)
- Add a branch / set fields for `Node_OUTER`:
  - `ret.CanSpill = false` (Phase 1).
  - `IsRightJoin = true` so existing `Probe → SyncBitmap → Finalize` machinery
    initializes `rightRowsMatched` and routes through `Finalize`.
- The result-column projection (`ResultCols`) is set by the planner; both sides
  are already nullable for `Node_OUTER` (`opt_misc.go:78-79`).

### 1.3 Hash-join types (`pkg/sql/colexec/hashjoin/types.go`)
- Add the two helpers from D1.
- Add `Node_OUTER` to the `String()` switch (`join.go:39-60`):
  `case Node_OUTER: ": full outer join "`.

### 1.4 Hash-join runtime (`pkg/sql/colexec/hashjoin/join.go`)
Replace gating predicates with the new helpers:

| Line(s) | Today | Change |
|---------|-------|--------|
| 116 | `if !IsLeftOuter && !IsLeftSingle && !IsLeftAnti { skip-probe-when-build-empty }` | use `!EmitUnmatchedProbe()` |
| 138-142 | probe-exhausted: only `IsRightJoin` → `SyncBitmap` | use `EmitUnmatchedBuild()` |
| 157 | empty-mp probe gate | use `EmitUnmatchedProbe()` |
| 301 | `needsProbeForEmpty` | use `EmitUnmatchedProbe()` |
| 361 | init `rightRowsMatched` | gate on `EmitUnmatchedBuild()` |
| 430, 489, 605 | unmatched-LEFT emit gates | use `EmitUnmatchedProbe()` |
| 460, 487, 543, 583 | `rightRowsMatched.Add(...)` during probe match | already runs when bitmap initialized — no change |

### 1.5 NULL semantics
SQL: `NULL = NULL` does **not** match. The existing hash function rejects NULL
keys (left/right outer rely on this). Add a regression test.

### 1.6 Tests

**Unit tests** in `pkg/sql/colexec/hashjoin/join_test.go` (mirror existing cases
near line 363):
- both sides non-empty, mix of matched / unmatched on both
- empty build → all left rows with NULL right
- empty probe → all right rows with NULL left (via `SyncBitmap`)
- both empty → no output
- NULL keys on left, right, both
- NULL non-key columns
- many-to-many duplicate keys
- batch-boundary stress (rows > `colexec.DefaultBatchSize`)
- (defensive) non-equi `ON` should panic NYI in compile layer, not crash here

**SQL integration tests** under `test/distributed/cases/...` (locate existing
left/right outer dir; add `full_outer_join.sql` + `.result`):
- `select * from l full outer join r on l.k = r.k`
- `select l.a from l full outer join r on l.k = r.k` (probe-side-only project)
- `select r.b from l full outer join r on l.k = r.k` (build-side-only project)
- `select r.b, l.a from l full outer join r on l.k = r.k` (interleaved)
- `select l.k as lk, r.k as rk ...` (alias both sides)
- asymmetric table sizes (large l + small r, then swap) — exercises optimizer
  child swap; verify columns route to correct side
- duplicate keys + NULLs combined

### 1.7 Optimizer child-swap verification
`stats.go:1783-1804` `determineBuildAndProbeSide` may swap `Children[0]` and
`Children[1]` for `Node_OUTER` based on cardinality. Full outer is row-symmetric
but our implementation labels "probe = left" and "build = right". Verify the
existing rewrite of `OnList`/`ProjectList`/`ResultCols.Rel` after swap is
side-agnostic (it should be — inner join already relies on this). The
asymmetric-size SQL test in 1.6 covers this.

---

## Phase 2 — Shuffle + true multi-CN broadcast (follow-up)
- Stats: enable shuffle for `Node_OUTER` when both sides are large.
- `compile.go:2482` shuffle switch: add `Node_OUTER` case.
- Design and implement a global cross-CN bitmap-merge protocol for
  `EmitUnmatchedBuild()`. Today's in-process `Channel` only synchronizes
  parallel operators within one scope fanout. For multi-CN probe, each CN's
  partial bitmap must be OR'd globally before any CN runs `Finalize`, otherwise
  unmatched-build rows duplicate or vanish.
- Tests: multi-CN execution with duplicate-row checks.

## Phase 3 — Spill (follow-up)
- `pkg/sql/colexec/hashjoin/spill.go` lines 526, 620, 649: change the
  `needsProbeForEmpty` / `needsBuildForEmpty` predicates to use the same
  semantic helpers from D1.
- Per-bucket `rightRowsMatched` reset/merge audit.
- Spill integration test for full outer (low `SpillThreshold` to force
  multi-bucket).
- Re-enable `CanSpill = true` for `Node_OUTER` in `operator.go`.

## Phase 4 — Non-equi `loopjoin` (follow-up)
- Add full-outer to `pkg/sql/colexec/loopjoin/join.go`:
  - build-side `matched` bitmap
  - finalize phase that emits unmatched-build rows
  - extend `emptyProbe` to null-fill RIGHT cols when full outer
- Remove the panic-NYI guard added in 1.1.

---

## Files to modify (Phase 1)
- `pkg/sql/compile/compile.go` (~30 LOC: broadcast switch case + non-equi panic)
- `pkg/sql/compile/operator.go` (`constructHashJoin`: ~10 LOC)
- `pkg/sql/colexec/hashjoin/types.go` (~15 LOC: 2 helpers)
- `pkg/sql/colexec/hashjoin/join.go` (~20 LOC: replace gates + String case)
- `pkg/sql/colexec/hashjoin/join_test.go` (~200 LOC: new tests)
- `pkg/sql/plan/stats.go` (~5 LOC: shuffle exclusion for Node_OUTER)
- `pkg/sql/plan/runtime_filter.go` (~5 LOC: defensive early-return)
- `test/distributed/cases/...` (~120 LOC: SQL + golden)

Total: ~400 LOC, no new files.

## Risks / open questions (after critique)
- **Bitmap allocation timing**: `rightRowsMatched` init at line 361 currently
  gates on `IsRightJoin`. Confirm setting `IsRightJoin=true` for `Node_OUTER`
  in `operator.go` is sufficient, or whether the gate should also use
  `EmitUnmatchedBuild()` (cleaner and decouples the two concepts).
- **`IsRightJoin` callers outside hashjoin**: grep `IsRightJoin` across the
  tree before merging to confirm nothing in compile/colexec/plan misinterprets
  `IsRightJoin=true && JoinType=Node_OUTER` as physical RIGHT JOIN behavior.
- **Runtime-filter probe side**: `runtime_filter.go:95` already excludes
  `Node_OUTER` from build-side bloom filters. Confirm probe-side filters are
  also excluded (would incorrectly drop rows that should null-extend).
- **Optimizer child-swap**: covered by SQL test in 1.6 but should also be
  manually traced through `stats.go:determineBuildAndProbeSide` for `Node_OUTER`.
