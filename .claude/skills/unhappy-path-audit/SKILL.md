---
name: unhappy-path-audit
description: Full-codebase MatrixOne unhappy-path audit for leak, double cleanup, hung, and OOM risks using the Q1-Q3 framework plus ownership and wait-for graphs. Use for resource lifecycle, cancellation/close/fail-fast paths, goroutines, locks/channels/RPC waits, restart/reuse generations, or unbounded growth audits.
---

# Unhappy Path Audit Skill

## Core Logic (First Principles)

**Unhappy path auditing = answering three propositions:**

| Q | Proposition | Violation Consequence |
|---|-------------|----------------------|
| **Q1: Every creation has one effective destruction owner** | Ownership may transfer or be ref-counted, but each resource reaches cleanup and cleanup executes effectively once | **Leak / double cleanup** |
| **Q2: Every wait dependency chain has a termination** | Every explicit or implicit blocking edge reaches a guaranteed release, cancellation, or bounded timeout | **Hung** |
| **Q3: Every accumulation has an upper bound** | For every unbounded-growth container → capacity limit or recycling mechanism | **OOM** |

**Key principle**: Not "looks like it might leak/hang", but "prove it definitely leaks/hangs". Q1 includes ownership cardinality, not only the presence of a destructor. Q2 includes mutex, channel, callback, RPC, I/O, and retry dependencies, not only functions named `Wait`. Exhaust every bypass and release path before ruling.

---

## Systematic Method (Drill-Down Algorithm)

```
Entry function
  └→ Layer 1: Execute Q1-Q3 on every resource/wait/accumulation point
      ├─ Termination condition satisfied at this layer → ✅, stop drill-down
      └─ Termination condition depends on a lower layer → ⬇ drill down to Layer 2
          └→ Recurse with Q1-Q3
```

### Drill-Down Example

```
proxy.Close()
  └→ tunnel.Close()                 ← Q1: Destruction guaranteed? ✅ (defer)
      └→ pipe.kickoff() exit        ← Q2: io.EOF guaranteed? → depends on connection.Close()
          └→ connection.Close()     ← Q2: Close guaranteed? → depends on morpc readLoop detection
              └→ morpc readLoop     ← Q2: Condition for detecting close?
                  ├─ readTimeout > 0 → periodic return + detect → ✅
                  └─ readTimeout = 0 → forever blocked on conn.Read → ❌ Bug
```

---

## Audit Workflow

### Phase 1 — Scope Definition

```
Input: target module list
Actions:
  1. Trace the complete call chain from entry to exit
  2. Number layers: Layer 1, Layer 2, ... Layer N
  3. Mark each layer's concern dimensions (goroutine/connection/lock/memory/...)
  4. Draw resource ownership transfers and wait-for edges
  5. Mark restart/retry/pool generation boundaries
Output: audit scope matrix
```

### Phase 2 — Per-Layer Audit

```
For each Layer:
  1. read_file ≤ 6 files (hard limit)
  2. Execute Q1-Q3 on this layer, including ownership cardinality and hidden wait edges
  3. Record verdict + layer number where termination condition is satisfied
  4. If termination depends on a lower layer → mark drill-down target, do not conclude at this layer
  5. Output this layer's audit table → then proceed to the next layer
```

**Anti-false-positive rule**: For every wait point, first exhaust all release paths (including defer, cancel watcher, timer goroutine). Follow indirect edges such as `reject → mutex → owner → RPC`; a control path is not fail-fast merely because it returns an error after the owner eventually releases it. Only rule it hung when every path is confirmed unreachable.

### Phase 3 — Synthesis

```
1. Merge all layer audit tables
2. Extract failed Q1/Q2/Q3 items
3. Verify termination chain integrity (no break from entry to exit)
4. Apply Bug Claim Verification Checklist (see Critical Rules) to each candidate
5. Generate Issue templates for confirmed Bugs
Output: final risk matrix + Bug list
```

---

## Output Format

### Per-Layer Audit Table (Phase 2 output)

```
Layer N: [Module Name] ([File Name])

| Q | Proposition | Code Path | Verdict | Termination Layer |
|---|-------------|-----------|---------|-------------------|
| Q1 | creation→destruction | go xxx() → defer close() | ✅ | Layer N |
| Q2 | wait→termination | cond.Wait() → Broadcast | ✅ | Layer N (cancel watcher) |
| Q3 | accumulation→bound | channel | ✅ | Layer N |
```

### Final Risk Matrix (Phase 3 output)

```
| # | Bug | Layer | Q | Severity | Issue |
|---|-----|-------|---|----------|-------|
| 1 | [description] | Layer X | Q2 | High | #12345 |
```

### Termination Chain Verification (Phase 3 output)

```
Entry
  └→ Layer 1: destruction call ✅
      └→ Layer 2: wait released ✅
          └→ Layer 3: **BREAK** ← Q1 destruction unreachable
```

---

## Critical Rules

### Core Rules

1. **Anti-confirmation-bias**: prove "definitely leaks/hangs", not "looks like it might" — exhaust all bypass paths before ruling
2. **Batch hard limit**: ≤ 6 read_file per batch, must output audit conclusion for that batch before continuing
3. **Exit self-check**: before ending the turn confirm — ①conclusion in first paragraph ②audit table present ③Bug has Issue
4. **Drill-down discipline**: when termination condition is not satisfied, mark "pending drill-down", do not speculate at the current layer

### Bug Claim Verification Checklist (BEFORE filing ANY bug)

**Every bug claim MUST pass all 5 gates before being included in the final report:**

| # | Gate | Verification Action | Anti-Pattern (caught by spill audit) |
|---|------|---------------------|--------------------------------------|
| **G1** | **FULL-GRAPH** | Trace ownership to ALL terminal nodes and prove one effective cleanup owner on every path | Bug: found a destructor but missed a second concurrent owner, or stopped before the real terminal holder |
| **G2** | **CAN-FAIL/BLOCK** | Open the alleged function and every wait-for dependency; confirm it can actually fail, panic, or block | Bug: called a path fail-fast without noticing its mutex owner was blocked in downstream I/O |
| **G3** | **SYMMETRY** | For every growth claim (append/push/alloc), grep the variable name + `= nil` / `= make` / truncate; confirm NO reset point exists anywhere in the lifecycle | Bug: saw `spillIndex = append(...)`, missed `spillIndex = nil` in `cleanupSpill()` |
| **G4** | **LINE-REREAD** | Re-read every cited line number AFTER forming the bug hypothesis — do not trust memory from the initial read | Bug: asserted `ctr.state` doesn't become `SendSucceed` on failure; line 127 shows it's unconditional |
| **G5** | **CALIBRATE-LAST** | Severity (Low/Medium/High) assigned ONLY after G1-G4 pass AND all bugs in the batch are confirmed. Never assign severity during discovery | Bug was Medium → turned out to be false positive (severity meaningless) |

**Process rule**: G1-G4 are per-bug gates applied during Phase 2→3 transition. G5 is a global gate applied after the full bug list is finalized. Any bug failing G1-G4 is **discarded**, not downgraded.

### G1 Detail: Full-Graph Traversal

For every resource (fd, lock, goroutine, memory allocation), construct the complete directed ownership graph:

```
Creation Point ──transfer──→ Holder A ──transfer──→ Holder B ──transfer──→ ...
                                  │                    │
                                  ▼                    ▼
                              Reset()              Destroy()
                              Free()               FreeMemory()
```

Required actions per node:
1. Identify every transfer function (hand-off, assignment, message send)
2. For each transfer target, locate its destruction path
3. Verify destruction path is reachable on error branches too
4. Prove competing branches cannot both execute irreversible cleanup, unless the destructor itself is verified idempotent
5. Repeat until reaching a node with NO further transfer (terminal)

**Stop condition**: Graph is complete when every leaf is either (a) a verified destructor call, or (b) a GC-managed resource with bounded lifetime.

### Q2 Detail: Wait-For And Control Paths

Construct a wait-for graph for each potentially blocking path:

```
caller -> mutex/channel/future -> owner/receiver -> RPC/I/O/callback -> release
```

Cancellation, close, abort, timeout, health-check, and rejection paths must be
independent of the work they control. If such a path acquires a lock held across
downstream work, sends into that work's queue, or synchronously waits for its
cleanup, it inherits the downstream termination condition. Trace to the local
edge that guarantees progress; a caller context alone is insufficient if the
blocked primitive does not observe it.

For restart/retry/pooling, include generation edges: old callbacks and handles
must terminate before reuse, and new state must remain unpublished until all
admission gates are ready.

### G3 Detail: Symmetry Check

For any variable `V` that exhibits growth (`append`, `map[K]=V`, channel send, allocation):

1. Find all assignments to `V` (grep `V\s*=`)
2. Confirm one of these assigns nil / empty / zero
3. Verify that assignment is ALWAYS reached before the variable goes out of scope

Common false positive: variable is reset in a function you haven't read yet (e.g., `Reset()`, `Free()`, `cleanup()`). Always search for these BEFORE filing a Q3 bug.

---

## Appendix A: Module Threat Model Reference

### A.1 Proxy / Execution Dispatch Layer

| Resource Creation | Destruction Path | Wait Point | Release Event | Accumulation | Bounded By |
|-------------------|-----------------|------------|---------------|--------------|------------|
| `go handleConnection()` | `defer cc.Close()` | `handleOneMessage()` reading from client | `io.EOF` / `ctx.Done()` / `err` | connection count | `maxConnections` |
| `go pipe.kickoff()` (×2) | `src.Shutdown()` → `io.EOF` | `cond.Wait()` in pause | cancel watcher goroutine + timer | `cnTunnels` map | CN count × per-CN connection count |
| placeholder tunnel | `selectOneFailed()` | `selectOne()` selecting CN | immediate return | — | — |
| transfer goroutine | `defer finishTransfer()` | `pause()` cond.Wait | `defaultTransferTimeout=10s` | — | — |

### A.2 morpc / RPC Transport Layer

| Resource Creation | Destruction Path | Wait Point | Release Event | Accumulation | Bounded By |
|-------------------|-----------------|------------|---------------|--------------|------------|
| `readLoop` goroutine | `conn.Read()` returns error / `readTimeout` triggers ctx check | `conn.Read(readOptions)` | **readTimeout > 0** → periodic return | backend pool | `maxConnections` |
| `writeLoop` goroutine | `defer closeConn(false)` | `writeLoop` ctx | ctx.Done() | — | — |
| backend connection | GC manager (idle check + inactive check) | — | — | idle backend | `maxIdleDuration` |
| Future | `Get(ctx)` return / GC | `f.Get(ctx)` | ctx deadline | — | — |

### A.3 Compile / Remote Execution Layer

| Resource Creation | Destruction Path | Wait Point | Release Event | Accumulation | Bounded By |
|-------------------|-----------------|------------|---------------|--------------|------------|
| sender goroutines | `sender.close()` in `RemoteRun` defer | `sendPipeline` each send | caller ctx | — | — |
| `messageSenderOnClient` goroutine | `<-receiver.connectionCtx.Done()` | stream.Get() | ctx / stream close | — | — |
| stream sender | `Close(true)` → pool return + gauge dec | `waitingTheStopResponse()` | `30s timeout` | stream pool | `sync.Pool` |
| `messageReceiverOnServer` goroutine | `<-receiver.connectionCtx.Done()` | `NotifyDispatch` | `connectionCtx.Done()` + `dispatchProc.Ctx.Done()` | — | — |
| dispatch goroutines | `proc.Ctx.Done()` / channel close | channel send (non-blocking) | non-blocking select + default fallback | channel buffer | `ChannelBufferSize` |

### A.4 Pipeline / Computation Execution Layer

| Resource Creation | Destruction Path | Wait Point | Release Event | Accumulation | Bounded By |
|-------------------|-----------------|------------|---------------|--------------|------------|
| `Pipeline` scope | `defer p.Cleanup(s.Proc, err, isPrepare, err)` | merge (non-blocking) | — | batches | `bat.Clean(mp)` per-batch release |
| pipelines in ants pool | pool full → `errSubmit` → `errC` | `Run()` waiting completion | `errC` / `errMergeC` / `scope.Run` completion | mpool alloc | pool cap |

### A.5 Txn / Transaction Layer

| Resource Creation | Destruction Path | Wait Point | Release Event | Accumulation | Bounded By |
|-------------------|-----------------|------------|---------------|--------------|------------|
| `txnClient` | `Close()` | `doCreateTxn` in paused state | `pausedC` close on Resume/client Close, or caller context | waiting user txns | request concurrency; entries removed on cancel/close |
| txn operator | `closeTxn()` commit/rollback path | `doSend()` 2PC commit | caller ctx (SQL executor timeout) | — | — |
| lock (unlock error path) | `lockService.Unlock()` infinite retry (max backoff 5s) | — | — | — | — |

### A.6 Lock Service Layer

| Resource Creation | Destruction Path | Wait Point | Release Event | Accumulation | Bounded By |
|-------------------|-----------------|------------|---------------|--------------|------------|
| `remoteLockTable` | `close()` + `RemoteLockTimeout=10min` TTL | Lock wait queue | txn commit/rollback → unlock / timeout | lock table size | bounded by active txn count |
| bind info | `handleError()` actively detects bind changes + allocator query | — | — | — | — |
| remote lock (on remote CN) | `RemoteLockTimeout=10min` TTL (remote CN cleanup) | — | — | — | — |

### A.7 Frontend / Session Layer

| Resource Creation | Destruction Path | Wait Point | Release Event | Accumulation | Bounded By |
|-------------------|-----------------|------------|---------------|--------------|------------|
| session | `session.RemoveSession()` | MySQL protocol read | `io.EOF` / `ctx.Done()` | session vars | bounded per session |
| `RoutineManager` routine | `deleteRoutine` + `rt.cleanup()` | — | — | — | — |
| prepared stmt cache | released on session close | — | — | cache size | per-session limit |

### A.8 CN Lifecycle Layer

| Resource Creation | Destruction Path | Wait Point | Release Event | Accumulation | Bounded By |
|-------------------|-----------------|------------|---------------|--------------|------------|
| CN services (net, query, txn, lock, pipeline, engine) | `Close()` in reverse order, independent try-catch per phase | — | — | — | — |
| RPC clients (txn/hakeeper/query/timestamp) | `stopRPCs()` | — | — | — | — |
| drain connections | rebalancer `handleTransfer` → `tunnel.Close()` | rebalancer migration | tunnel timeout | migrating connections | drain grace period |
| `PipelineClient` | `Close()` | — | — | — | — |

### A.9 TAE / Storage Engine Layer (disttae)

| Resource Creation | Destruction Path | Wait Point | Release Event | Accumulation | Bounded By |
|-------------------|-----------------|------------|---------------|--------------|------------|
| block handle / file | `refcount → 0` / `defer close` | IO wait | IO completion / timeout | block cache | LRU eviction / size cap |
| txn state (MVCC) | txn commit/rollback → version cleanup | lock contention on block | txn commit/rollback | active versions | GC watermark |
| partition reader | `Close()` | logtail fetch | `maxTimeToWaitServerResponse=60s` | — | — |
| partition state | `checkpoint` + `gcPartitionStateTicker=20min` | — | — | partition state entries | checkpoint cleanup |
| snapshot | snapshot release on txn completion | — | — | — | — |
| logtail consumer goroutine | `stopConsumers()` + `ctx.Done()` | `receiveOneLogtail()` | `maxTimeToWaitServerResponse=60s` | — | — |

### A.10 Log Service / Replication Layer

| Resource Creation | Destruction Path | Wait Point | Release Event | Accumulation | Bounded By |
|-------------------|-----------------|------------|---------------|--------------|------------|
| subscription goroutine | `ctx.Done()` → `stop()` | logtail receive | quorum ack timeout + retry | WAL buffer | size cap + flush |
| RPC connection | `Close()` | heartbeat timeout | leader lease expiration | — | — |
| checkpoint goroutine | `ctx.Done()` | checkpoint sync wait | quorum sync timeout | — | — |

### A.11 Hakeeper / Gossip Membership Layer

| Resource Creation | Destruction Path | Wait Point | Release Event | Accumulation | Bounded By |
|-------------------|-----------------|------------|---------------|--------------|------------|
| heartbeat goroutine | `ctx.Done()` / `Close()` | heartbeat timeout | next ticker interval | member list | cluster size × state |
| gossip connection | `Close()` | gossip sync timeout | peer timeout | pending messages | send buffer cap |

---

## Appendix B: Distributed Component Wait Termination Models

| Wait Type | Termination Condition | Common Protection Patterns |
|-----------|----------------------|---------------------------|
| **Local I/O wait** | IO completion / timeout | `ctx.Done()`, `readTimeout`, `writeTimeout` |
| **Quorum wait** | majority response / timeout | quorum ack deadline → retry or downgrade |
| **Heartbeat wait** | timeout → mark peer dead | heartbeat interval × failure threshold |
| **Lock wait (local)** | lock holder release / timeout | `RemoteLockTimeout` TTL, deadlock detector |
| **Lock wait (distributed)** | remote lock TTL + bind change detection | `handleError()` + allocator query |

---

## Appendix C: Common False Positive Patterns

| False Positive Pattern | Correct Adjudication Path | Gate Violated |
|------------------------|--------------------------|---------------|
| See `cond.Wait()` without timeout → report hung | Exhaust all `Broadcast()` call sites (including defer, cancel watcher, timer goroutine) | — |
| See goroutine launched without explicit stop → report leak | Check goroutine-internal exit paths: `ctx.Done()` / `io.EOF` / channel close | — |
| See `sync.Map.Store` without delete → report OOM | Check TTL / active GC cleanup / refcount→0 cleanup | — |
| See `close()` without sending RPC → report resource leak | Check if remote has TTL self-cleanup / bind change detection | — |
| See RPC wait without timeout → report hung | Trace upward context for deadline, check if caller guarantees cancel | — |
| See channel send potentially blocking → report hung | Verify non-blocking (select + default), or guarantee receiver always consumes | — |
| See fail-fast error return → assume prompt rejection | Draw `reject → lock/channel → owner → downstream` and prove the control path has an independent bound | G2 |
| See one cleanup callback → assume exactly-once cleanup | Trace all public terminal calls, retry paths, and callbacks; prove exclusive ownership or destructor idempotence | G1 |
| See reset/reopen under a lock → assume generation safety | Trace old callbacks/handles and publication order across restart or pool reuse | G1/G2 |
| See resource transferred to next holder, close not found in current scope → report leak | Trace ownership to ALL terminal nodes — next holder may have Destroy()/FreeMemory() | G1 |
| See append/push without bound in current scope → report OOM | Grep variable + `= nil` — cleanup function in sibling file may reset it | G3 |
| See function call that "might" panic → report fd leak on panic path | Open function body — nil-check+assign or simple arithmetic cannot panic | G2 |
| See `SendMessage` return error → assume upstream state machine doesn't clean up | Re-read the exact line where state is set — it may be unconditional | G4 |
