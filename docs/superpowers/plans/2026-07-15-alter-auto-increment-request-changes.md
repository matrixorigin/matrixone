# ALTER TABLE AUTO_INCREMENT Request-Changes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the same-transaction auto-increment reset gap, preserve the durable multi-CN version fence, keep COPY semantics correct, and rebase PR #24531 cleanly onto current main.

**Architecture:** Retain table-definition versions as the durable epoch checked by TN. Add transaction-private incrservice reset caches keyed by transaction and table, use them for post-ALTER DML in that transaction, and retire rather than publish them at transaction close.

**Tech Stack:** Go 1.22+, MatrixOne incrservice, disttae/TAE transactions, gomock, protobuf, distributed BVT.

## Global Constraints

- After ALTER serializes successfully, no later automatic value may commit below `max(requested, max-existing+1)`.
- Correctness must not depend on CN broadcast delivery or CN reachability.
- Optimistic and pessimistic transactions use the same TN version fence.
- Transaction-private cache state must never be visible to another transaction or survive transaction close.
- COPY must apply the requested offset, then reconcile it with the actual copied maximum.
- Generated protobuf code must match `proto/api.proto`.
- Every production change follows RED-GREEN-REFACTOR and all changed concurrency paths receive race coverage.

---

### Task 1: Rebase and preserve the durable fence

**Files:**
- Resolve: `pkg/incrservice/column_cache.go`
- Resolve: `pkg/incrservice/types.go`
- Resolve: `pkg/vm/engine/disttae/txn.go`
- Inspect: `pkg/incrservice/table_cache.go`, `pkg/sql/colexec/preinsert/preinsert.go`, `pkg/vm/engine/tae/rpc/handle.go`, `proto/api.proto`, `pkg/pb/api/api.pb.go`

**Interfaces:** Preserve `tableDefVersion` and `tableDefVersionKnown` through every user-table write transformation and adopt upstream's range-owned allocation timestamp model.

- [ ] Fetch `upstream/main`, record the old HEAD, and rebase `codex/pr-24531-rc-audit` onto it.
- [ ] Resolve `column_cache.go` by keeping retired-cache guards while adopting upstream range timestamp ownership.
- [ ] Resolve `types.go` by retaining table-version parameters and upstream timestamp semantics.
- [ ] Resolve `disttae/txn.go` using upstream service-scoped CN segment tracking while retaining version/known propagation through memory batches, S3 writes, flushes, compaction, and precommit entries.
- [ ] Regenerate protobuf Go code from `proto/api.proto` using the repository generator; verify generated-only differences.
- [ ] Run focused version-fence, cache, disttae, and TAE tests with the documented CGo environment.
- [ ] Commit as `chore: rebase auto increment fix onto main`.

### Task 2: Add and implement transaction-private reset caches (RED-GREEN)

**Files:**
- Modify: `pkg/incrservice/service_test.go`
- Modify: `pkg/incrservice/types.go`
- Modify generated mocks under `pkg/incrservice`, `pkg/frontend/test`, and `pkg/vm/engine/test`

**Interfaces:**
- `InsertValues(ctx, tableID, tableVersion, txnOp, vecs, rows, estimate)`
- `GetLastAllocateTS(ctx, tableID, tableVersion, txnOp, colName)`
- `DiscardOffsetReset(ctx, tableID, txnOp) error`

- [ ] Add `TestSetOffsetTransactionUsesPendingOffsetForInsert` using a real mem store and transaction operator: stage offset 999, generate one value in the same transaction, and require value 1000.
- [ ] Run only that test against the pre-fix implementation and record the expected failure showing a value from committed offset.
- [ ] Add focused failing lifecycle tests for rollback/close cleanup and failed-statement discard without changing production code.
- [ ] Update interface declarations and mocks only far enough for the intended API to compile; keep the behavior tests red.
- [ ] Initialize the private reset registry in `NewIncrService` and add helpers that acquire, install, discard, and retire private caches without holding the service lock while closing.
- [ ] Make `SetOffset` force the transactional store offset, read columns with the same `TxnOperator`, construct an uncommitted private cache, and install it only after full construction succeeds.
- [ ] Register `ClosedEvent` cleanup once per transaction/table; on every terminal status retire and remove private caches without promoting them.
- [ ] Route `GetLastAllocateTS` and `InsertValues` through the private cache when `txnOp` owns one; otherwise use the existing committed version cache.
- [ ] Pass `proc.Base.TxnOperator` from `preinsert` into both calls and update all implementations/mocks.
- [ ] Implement `DiscardOffsetReset` as idempotent synchronous retirement for failed ALTER statements.
- [ ] Run the Task 2 tests and existing version/cache/allocator tests; require GREEN.
- [ ] Run `go test -race` for the new incrservice lifecycle tests.
- [ ] Commit as `fix: isolate transactional auto increment resets`.

### Task 3: Close compile error and rollback paths

**Files:**
- Modify: `pkg/sql/compile/ddl.go`
- Modify: `pkg/sql/compile/alter.go`
- Modify: `pkg/sql/compile/ddl_test.go`
- Modify: `pkg/sql/compile/alter_test.go`

**Interfaces:** Every successful `SetOffset` is paired with either a successful ALTER statement or `DiscardOffsetReset` on the error return path.

- [ ] Add a failing compile test that injects an error after `SetOffset` and verifies the reset is discarded before the transaction continues.
- [ ] Add a failing COPY test for an error after destination reconciliation and verify the destination private reset is discarded.
- [ ] Add scoped cleanup tracking around inplace and COPY ALTER; cleanup is disarmed only when the statement completes successfully.
- [ ] Add cancellation injection immediately after the transactional store update and verify no usable private cache remains.
- [ ] Run compile tests and the existing schema-version statement rollback tests; require GREEN.
- [ ] Commit as `fix: clean auto increment resets on alter failure`.

### Task 4: Prove the distributed serialization contract

**Files:**
- Modify: `pkg/incrservice/service_test.go`
- Modify: `pkg/vm/engine/tae/txn/txnimpl/txn_test.go`
- Modify: `pkg/vm/engine/tae/db/test/db_test.go`
- Modify: `pkg/vm/engine/disttae/txn_test.go`
- Modify: `pkg/vm/engine/tae/rpc/rpc_test.go`

**Interfaces:** Known old-version writes fail with `ErrTxnNeedRetryWithDefChanged`; same-transaction local-version writes use the private reset; unknown-version behavior is explicitly tested and documented as compatibility-only.

- [ ] Add a deterministic two-service stale-range test: both services allocate old ranges, ALTER commits through one service, the stale known-version write is rejected at TN, and retry with the new version allocates at or above the effective offset.
- [ ] Cover ALTER-prepares-first and DML-prepares-first ordering with actual generated values and document serialization-order semantics.
- [ ] Run the same fence test in pessimistic and optimistic modes.
- [ ] Add version propagation assertions for memory, S3/flush, and compaction entry paths.
- [ ] Add an explicit unknown-version compatibility test and decision-log comment; do not claim mixed-version strict fencing.
- [ ] Run all affected disttae/TAE/RPC tests and race-enabled incrservice tests.
- [ ] Commit as `test: cover auto increment reset serialization`.

### Task 5: Revalidate COPY and end-to-end behavior

**Files:**
- Verify/modify: `pkg/sql/plan/build_alter_table_test.go`
- Verify/modify: `pkg/sql/compile/alter_test.go`
- Verify/modify: `pkg/sql/colexec/table_clone/table_clone_test.go`
- Verify/modify: `test/distributed/cases/auto_increment/auto_increment.sql`
- Verify/modify: `test/distributed/cases/auto_increment/auto_increment.result`

**Interfaces:** Final COPY offset is `max(requestedOffset, copiedMax)` and uses transaction-private reset lifecycle until commit.

- [ ] Re-run plan assertions for lower request, zero, combined COPY-required ALTER, and COPY without explicit request.
- [ ] Re-run compile/table-clone reconciliation tests for empty, deleted, copied MAX, overflow, cancellation, and quoted identifiers.
- [ ] Add explicit-transaction inplace ALTER→INSERT→COMMIT and ALTER→INSERT→ROLLBACK SQL cases in both pessimistic and optimistic configurations supported by the BVT harness.
- [ ] Run the auto_increment distributed BVT and record exact case counts/results.
- [ ] Commit any necessary test updates as `test: verify alter auto increment end to end`.

### Task 6: MatrixOne completion gate and independent review

**Files:** Review the complete diff against refreshed `upstream/main`; write decisions to `.superpowers/sdd/progress.md` or the task report.

- [ ] Run `gofmt` and regenerate mocks/protobufs through repository tools.
- [ ] Run `go build`, `go vet`, and fresh `go test -count=1` for every modified package plus dependent `preinsert`, compile, disttae, TAE RPC/txn/db packages using the mo-dev CGo environment.
- [ ] Run race tests for incrservice and any new concurrent TN tests.
- [ ] Run the relevant distributed BVT.
- [ ] Inspect `git diff --check`, `git diff --stat`, generated-code consistency, and unintended files.
- [ ] Run `mo-self-review` over the full branch diff, including unhappy-path Q1-Q3 for private-cache maps, refcounts, allocator waits, callback cleanup, and cancellation.
- [ ] Dispatch an independent whole-branch reviewer with the original issue, every request-changes invariant, the design, plan, task reports, and full review package.
- [ ] Fix every Critical/Important finding, rerun covering tests, and repeat review until clean.
