# Issue 24158 Root Cause and Fix

## Summary

The hang/failure in issue #24158 is **not** caused by the proxy disconnect cleanup work.

It is a separate bug in the **compile/remoterun notification path**:

- `Scope.RemoteRun` already avoids remote-running a scope on the local CN by checking `ipAddrMatch`
- but `Scope.sendNotifyMessage` still opens an RPC stream for every `RemoteReceivRegInfo.FromAddr`
- when one dispatch operator is scheduled on the **same CN** as the coordinator, `FromAddr` equals the local service address
- `pipelineClient.NewStream` rejected that self-connection and returned:
  `remote run pipeline in local: <addr>`

That leaves the dispatch side without a valid prepare-done notification stream, which can surface as query failure or hang depending on the execution timing.

## Root Cause

The failure requires all of the following:

1. A multi-CN plan that uses remote dispatch / shuffle.
2. At least one dispatch operator lands on the **local CN**.
3. `sendNotifyMessage` tries to create the prepare-done stream to that local CN address.

Code path:

1. `Scope.RemoteRun` handles the local-scope case correctly:
   - `pkg/sql/compile/scope.go`
   - `if s.ipAddrMatch(c.addr) { return s.MergeRun(c) }`
2. Later, `Scope.sendNotifyMessage` iterates `RemoteReceivRegInfos` and unconditionally calls:
   - `newMessageSenderOnClient(..., fromAddr, ...)`
3. `newMessageSenderOnClient` calls:
   - `cnclient.GetPipelineClient(sid).NewStream(ctx, toAddr)`
4. `pipelineClient.NewStream` previously rejected `backend == c.localServiceAddress`.

So the real bug is:

> `RemoteRun` has a local-address guard, but `sendNotifyMessage` still depends on a pipeline client implementation that refused valid loopback notification streams.

## Why this can hang

Remote dispatch waits for its remote receiver information to be delivered through the notification path:

- `dispatch.prepareRemote` registers UUID -> process/channel mapping
- `dispatch.waitRemoteRegsReady` waits on `dispatch.ctr.remoteInfo`
- `handlePipelineMessage(Method_PrepareDoneNotifyMessage)` is what normally resolves that wait

If the prepare-done notification stream cannot be created for the local CN case, that coordination path is broken. Depending on where cancellation/error propagation wins the race, the query may fail immediately or appear hung.

## Fix

Use the existing notification RPC path for local CN addresses too.

Implemented change:

- remove the self-connection rejection from:
  - `pkg/cnservice/cnclient/client.go`
  - `pipelineClient.NewStream`

After the fix:

- `RemoteRun` still keeps its `ipAddrMatch` guard, so scopes are **not** remote-run onto the local CN
- but `sendNotifyMessage` can now establish the local loopback stream needed by the dispatch notification protocol
- this is the smallest behavior change that fixes the broken path without refactoring dispatch/local receiver handling

## Why this fix is low risk

This change is narrow:

- it only affects `pipelineClient.NewStream`
- the intended beneficiary is the prepare-done notification path used by compile/remoterun
- it does **not** change the planner, shuffle layout, dispatch protocol, or proxy code

The existing `RemoteRun` local guard remains in place, so this does **not** re-enable “remote run the whole scope on the same CN”.

## Validation

Validated with:

- `go test ./pkg/cnservice/cnclient ./pkg/sql/compile -count=1`

And a new unit test:

- `TestPipelineClient_NewStreamAllowsLocalBackend`

This test pins the actual regression point: local backend addresses must be forwarded to the underlying morpc client instead of being rejected inside `pipelineClient.NewStream`.
