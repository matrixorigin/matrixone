# Context Management Improvement Plan (Issue #23457)

## 1. Background
The MatrixOne compute engine faced several critical issues in its context management:
- **Panic Risks**: Nil contexts in cleanup paths triggered panics when tracing was enabled.
- **Swallowed Errors**: Context cancellation errors (e.g., `context.Canceled`) were being caught and set to `nil` in `Scope.Run`, making it impossible for callers to detect interrupted queries.
- **Redundant Operations**: Excessive `ReplaceTopCtx` calls caused performance overhead and potential loss of context values.
- **Inconsistent Tracing**: Transactional operations (Commit/Rollback) were using request-level contexts instead of transaction-level ones.

## 2. Completed Tasks (Phase 1: Emergency Fixes & Core Improvements)

### 2.1 Reliability (P0)
- **Defensive Tracing**: Added nil context checks in `pkg/util/trace/impl/motrace/mo_trace.go` to prevent panics when `trace.Start` is called with a nil parent.
- **Cleanup Paths**: Fixed multiple locations where `ExecCtx` was initialized without a `reqCtx` during transaction rollback:
    - `pkg/frontend/routine.go`: Added `getCleanupContext()` and fixed `handleRequest` quit branch and `cleanup` function.
    - `pkg/frontend/back_exec.go`: Added `getCleanupContext()` for `backSession` and fixed `Close()`.
    - `pkg/frontend/session.go`: Added `getCleanupContext()` for `Session` and fixed `reset()` method.
- **Error Propagation**: Fixed `pkg/sql/compile/scope.go` to correctly return `context.Canceled` or `context.DeadlineExceeded` instead of `nil` when a scope is interrupted.

### 2.2 Consistency & Optimization (P1/P2)
- **Reduced Context Replacements**: Optimized `pkg/sql/compile/compile2.go` to combine context values (`EngineKey`, `CounterSet`) before a single `ReplaceTopCtx` call in `Compile()` and `Run()`.
- **Transactional Tracing**: Updated `pkg/frontend/txn.go` to use `txnCtx` for `Commit` and `Rollback` trace spans, ensuring accurate tracing regardless of request cancellation.

## 3. Pending Tasks (Phase 2 & 3: Architectural Refactoring)
The following larger tasks are recommended for subsequent development phases:
- **`CancelManager`**: Implement a centralized manager in `Session` to register and cascade cancellations across Routine, Transaction, Request, Query, and Pipeline levels.
- **`ContextBuilder`**: Create a unified builder to ensure all mandatory values (TenantID, NodeID, etc.) are consistently attached to every new context.
- **`ManagedContext`**: Implement a hierarchical context wrapper that enforces parent-child relationships and prevents out-of-order derivation.
- **`ExecCtx` Refactoring**: Decouple `ExecCtx` into specialized structures for context management, execution state, and resources.

## 4. Verification & Testing Strategy

### 4.1 Unit Testing
- **Panic Verification**: Run `go test ./pkg/frontend -run TestCleanup` (or equivalent) with tracing enabled to ensure no panics occur during session closure.
- **Error Propagation**: Verify `Scope.Run` returns `context.Canceled` when the context is cancelled during execution.
- **Context Integrity**: Use `context.Value()` checks in `Compile` to ensure `EngineKey` and `CounterSet` are correctly preserved after `ReplaceTopCtx` optimization.

### 4.2 Integration Testing
- **Kill Query Test**: Execute long-running queries and use `KILL QUERY` to verify that the cancelled state is correctly propagated and reported back as an error.
- **Stress Testing**: Perform high-concurrency connection/disconnection tests to verify the stability of the new cleanup paths.

### 4.3 Build Verification
- Ensure all affected packages build correctly:
    ```bash
    go build ./pkg/frontend/...
    go build ./pkg/sql/compile/...
    go build ./pkg/util/trace/...
    ```

---
*Created by Gemini CLI Agent on 2026-01-07*
