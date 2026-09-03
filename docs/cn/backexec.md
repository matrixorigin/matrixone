# BackExec Design Analysis

## Core Concept

`backExec` is a lightweight executor in MatrixOne used for executing SQL in the background, primarily for internal metadata queries (e.g., subscription, publication information).

## Two Transaction Modes

```txt
Session (parent)
    │
    ├─ TxnHandler
    │      │
    │      └─ txnOp ◄─── Transaction lifecycle managed by parent session
    │           │
    │           │ (shared reference)
    │           ▼
    └─ backExec (shared transaction)
           │
           └─ TxnHandler
                  │
                  ├─ txnOp ──► Points to the same txnOp
                  └─ shareTxn = true
```

| Mode | shareTxn | Transaction Management | Close() Behavior |
|------|----------|------------------------|------------------|
| Shared Transaction | true | Managed by parent session | No rollback, only Reset |
| Independent Transaction | false | Managed by backExec itself | Execute rollback, complete Close |

## Key Insights

### 1. Shared Transactions Don't Manage Transaction Lifecycle

```go
func (th *TxnHandler) rollbackUnsafe(...) error {
    if !th.inActiveTxnUnsafe() || th.shareTxn {
        return nil  // Shared transaction: return directly, no operation
    }
    // Only non-shared transactions actually rollback
}
```

**Meaning**: `backExec.Close()` for shared transactions has no effect on the transaction; commit/rollback is entirely controlled by the parent session.

### 2. Transaction References Can Be Safely Updated

Since shared transactions don't manage transaction lifecycle, we can:
- Directly update `txnOp` references without Close + rebuild
- In autocommit mode, only need `UpdateTxn(newTxnOp)` when switching transactions

### 3. Pessimistic Transaction Snapshot Elevation Is Automatically Visible

`backExec` holds a **reference** (pointer) to `txnOp`. When a pessimistic transaction acquires a lock and the snapshot is elevated:

```go
// txnOperator.UpdateSnapshot() directly modifies internal state
tc.mu.txn.SnapshotTS = newTS
```

`backExec` automatically uses the new snapshot when executing SQL, without any additional handling.

## Optimization Strategy

### Before: Create New backExec for Each Call

```txt
GetSubscriptionMeta()
    │
    ▼
GetShareTxnBackgroundExec()  ← Create backExec
    │
    ├─ newBackSession()
    │      ├─ InitTxnHandler()
    │      ├─ buffer.New()
    │      └─ FastUuid()
    │
    ▼
getSubscriptionMeta()
    │
    ▼
bh.Close()  ← Destroy backExec
```

**Problem**: High overhead from frequent creation/destruction

### After: Session-Level Reuse, Update Reference on Transaction Switch

```txt
getOrCreateBackExec()
    │
    ▼
cachedBackExec exists?
    │
    ├─ Yes → txn changed?
    │          │
    │          ├─ Yes → UpdateTxn(currentTxn)  ← Only update reference
    │          │
    │          └─ No → Return cached directly
    │
    └─ No → GetShareTxnBackgroundExec()  ← First-time creation
              │
              ▼
          Cache and return
```

## Summary

| Feature | Description |
|---------|-------------|
| Transaction Ownership | Shared transactions are managed by parent session, backExec only borrows |
| Lifecycle | Can be elevated to session level, reused across transactions |
| Transaction Switch | Only need to update txnOp reference, no rebuild required |
| Snapshot Update | Automatically visible, no handling needed |

## Related Files

- `pkg/frontend/back_exec.go` - backExec and backSession definitions
- `pkg/frontend/txn.go` - TxnHandler definition, shareTxn logic
- `pkg/frontend/compiler_context.go` - TxnCompilerContext.getOrCreateBackExec() implementation
