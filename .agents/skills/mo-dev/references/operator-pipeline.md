# Operator And Pipeline Reference

## Contents

1. Operator Lifecycle Contracts
2. Post-Modification Verification
3. Fault Diagnosis
4. Common Pitfalls
5. Distributed Dispatch And Receiver Hangs
6. Forbidden Patterns

## 1. Operator Lifecycle Contracts

### Call() vs Reset()

| Method | Purpose | Must NOT Do |
|--------|---------|-------------|
| `Call()` | Process one batch. Receive from upstream, compute, send downstream. | Send terminal signals (`End`, `Error`, `Abort`). That is `Reset()`'s job. |
| `Reset()` | Cleanup. Notify downstream the operator is done. | Block indefinitely waiting for receiver acknowledgment; choose cleanup from the actual terminal protocol and outcome. |

This is the most common source of subtle operator bugs: premature pipeline termination, spurious timeouts, dead receivers.

### PipelineSpool Lifecycle

| Method | Behavior | Use When |
|--------|----------|----------|
| `Close()` | Wait for consumers to acknowledge end of stream by consuming nil batches | Graceful legacy nil-batch protocol |
| `CloseWithTimeout()` | Same as `Close()` but bounded by a timeout | Legacy nil-batch cleanup only |
| `ForceCleanupAfterTerminalSignal()` | Release deferred spool state after a successfully delivered typed `EventEnd` and paired receiver cleanup | Graceful typed-signal completion |
| `Abort(cause)` | Immediately release unconsumed slots and record failure/abort state | Error, cancel, abort, or terminal-delivery failure |

Critical rule: typed termination has two outcomes. A successfully delivered `EventEnd` defers spool reclamation until paired receiver cleanup returns, then calls `ForceCleanupAfterTerminalSignal()`. Error/abort/cancel paths and terminal-delivery failures call `Abort(cause)` immediately. Do not use legacy `CloseWithTimeout()` for either typed path.

### Pipeline Signal Types

| Signal | Constructor | Meaning |
|--------|-------------|---------|
| `EventData` | `NewDataSignal(batch)` | Normal data batch |
| `EventEnd` | `NewEndSignal()` | Graceful end of stream |
| `EventError` | `NewErrorSignal(err)` | Operator encountered an error |
| `EventAbort` | `NewAbortSignal()` | Forceful termination |

Dual-protocol compatibility: `GetNextBatch` handles both explicit typed signals and legacy nil-batch convention (`content == nil`). Old operators using implicit nil-batch continue to work.

## 2. Post-Modification Verification

### Test Freshness

Test output must be from the current turn:

1. `go test` command appears in current turn's tool calls.
2. Exit code is 0.
3. Timestamp is after the last edit.

### Completion Gate

Before declaring a change done, all boxes must be checked:

```
□ explicitly name each changed owning package and prove `go list -mod=readonly` selects at least one package
□ GOWORK=off go build -mod=readonly <each named owning package> -> exit 0
□ GOWORK=off go vet -mod=readonly <each named owning package>   -> exit 0
□ GOWORK=off go test -mod=readonly -v -count=1 -timeout 120s <each named owning package> -> exit 0, no hangs
□ git diff --stat                            -> inspected, no unintended files
□ Regression: at least one test from dependent package passes
```

Hang = failure. If `go test` produces more than 10s of no output, investigate.

## 3. Fault Diagnosis

| Symptom | Look At |
|---------|---------|
| Test hangs near a repeatable deadline (for example 30s) | Locate the timer/deadline owner from complete stacks. Check `CloseWithTimeout` only when the wait-for graph reaches it. |
| Test hangs >5s, no output | Deadlock or blocking channel send. Check whether `done` channel is closed and whether `sendSignal` uses non-blocking `select`. |
| `context deadline exceeded` after 30s | `WaitingEndWithTimeout` timed out. Check whether all senders called `Reset()` and sent typed terminal signals. |
| `CGO_CFLAGS` not working | Run `go env CGO_CFLAGS` to verify. Use `export` if the package has sub-packages. |

## 4. Common Pitfalls

### Structural Changes Need Both Ends

When changing communication protocol between operators, such as connector <-> merge or dispatch <-> merge, update both sender and receiver. A sender-only change can deadlock.

Identification: a repeatable cleanup timeout is a locator, not a root cause.
Trace the actual timer owner and protocol on both ends before attributing a typed
signal / `CloseWithTimeout` mismatch.

### CGo Link Errors Are Usually Environment

CGo link errors (`Undefined symbols`, `cannot find -lmo`) are environment issues until proven otherwise. The C shared libraries (`libmo.dylib`, `libusearch_c.dylib`) must be pre-built via `make cgo` and `make thirdparties`.

### Pipeline Cleanup: Abort, Do Not Wait

During pipeline cleanup, operators should use non-blocking or timeout-gated communication. Never block waiting for a receiver that may have already exited.

### Channel Full Edge Cases

When sending terminal signals into a bounded channel, the send may fail because the channel is full. Ensure the terminal state is still recorded even when channel send fails; the `done` channel must close regardless of delivery success.

## 5. Distributed Dispatch And Receiver Hangs

Collect synchronized goroutine stacks from every CN before naming storage, network, or transaction locking as the cause. Trace this registration protocol end to end:

```text
compile constructs remote receiver UUID and placement
  -> registerLocalDispatchReceivers / registerRemoteDispatchReceivers
  -> Dispatch.RegisterRemoteReceiversWithHandle
  -> prepareRemote publishes ownership with PutProcIntoUuidMap
  -> PrepareDoneNotifyMessage and remote GetProcByUuid rendezvous with that registration
  -> dispatch.Prepare reuses the registration and selects the send path
  -> the first remote send (or empty-input completion) calls waitRemoteRegsReady
  -> waitRemoteRegsReady consumes remoteInfo until every receiver attaches
  -> batch forwarding starts
```

The exact publication owner differs on older release branches. Inspect the
checked-out branch instead of assuming current `main`; on current `main`,
registration happens before scope goroutines run, `dispatch.Prepare` does not
wait for attachment, and `waitRemoteRegsReady` in the first send path owns that
wait.

A characteristic broken-registration wait graph is:

```text
primary CN
  sendNotifyMessageWithFactoryAndWait
    -> receiveMsgAndForward
      -> messageSenderOnClient.receiveBatch

remote CN
  handlePipelineMessage
    -> messageReceiverOnServer.GetProcByUuid
      -> wait for dispatch registration
```

When this graph appears:

1. Count complete stack signatures on each CN at the same UTC timestamp. Compare the exact notify-sender subtree with the sum of remote UUID-registration waits; a broad `receiveBatch` count may include unrelated remote runs.
2. Compare onset, stable-hang, and pre-cancel snapshots. Stable counts and waiter ages support a missing or unreachable registration owner.
3. Trace each UUID from scope construction and local/remote placement through
   `registerLocalDispatchReceivers` / `registerRemoteDispatchReceivers`,
   `RegisterRemoteReceiversWithHandle`, `prepareRemote`, and
   `PutProcIntoUuidMap`, then through `dispatch.Prepare` and the first
   `waitRemoteRegsReady`. Check local-conversion rewrites, fan-out
   multiplication, pre-registration order, early return, and cancellation. On
   a release branch, first locate the equivalent publication and attachment
   owners in that version.
4. Separate trigger from cause. `LOAD DATA`, deletion, or index maintenance may create the DAG without object storage being the blocked resource.
5. Treat long RPC deadlines as an amplifier or missing guardrail unless they created the ownership break.
6. Treat post-cancel cleanup as proof that teardown eventually releases the graph, not proof that the pre-cancel wait was healthy.

Do not infer an object-store stall from `runtime.netpoll`, TLS, or `net/http.(*persistConn).readLoop`. Require a query-owned response-body consumer stack through fileservice/storage SDK calls. MORPC/goetty socket reads belong to the cross-CN data plane and must be attributed by their complete callers.

## 6. Forbidden Patterns

1. Never send terminal signals (`End`, `Error`, `Abort`) from `Call()`. Only `Reset()` sends terminal signals.
2. Never call `sp.CloseWithTimeout()` after switching to explicit typed terminal signals. Use deferred graceful cleanup after delivered `EventEnd`, or `sp.Abort(cause)` on failure.
3. Never claim "pre-existing" from `git stash`; reproduce the same command at the correct clean baseline in an isolated worktree.
4. Never declare done without fresh test output.
5. Never assume `go build` success means `go test` will pass.
6. Never skip bottom-up testing.
7. Never add per-algorithm `switch`/`if` on index algo names in the SQL layer. Route through `indexplugin.Get(algo)`.
