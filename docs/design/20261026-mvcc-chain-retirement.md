# Retire unconsumed MVCC chain helpers

Owner: ongoing engineering-quality issue #29249. Implementation PR pending.
Base: `7b80dbdbd3` (2026-10-26 main). This is a bounded continuation of the
repository-wide retirement inventory, not completion of the ongoing cleanup.

## Contract and evidence

The live catalog embeds `txnbase.MVCCChain[*MVCCNode[T]]`. It inserts fully
constructed nodes and uses the existing visibility, conflict, commit/rollback,
prepared-range and replay-search methods. Node encoding/decoding remains owned
by `MVCCNode.WriteTo` / `ReadFromWithVersion` and command-specific factories.

Retire the unconsumed `HasCommittedNodeInRangeLocked`,
`HasUncommittedNodeLocked`, `HasCommittedNodeLocked`, and
`IsCreatingOrAbortedLocked` methods. Retire `MustOneNodeLocked` with its sole
caller, `IsCreatingOrAbortedLocked`. None belongs to `txnif.MVCCNode` or a
registered interface. Whole-repository AST references include test and tagged
files; separate documentation, registration, function-value and reflection
checks must confirm no incoming live edges before implementation.

`MVCCChain.newnodefn` is initialized but never read. Remove this private field
and its constructor argument. `catalog.NewBaseEntry` merely wraps that factory
and passes it to the chain; remove its factory argument and use explicit generic
types at its six callers. Every removed caller argument is an unevaluated
zero-argument closure, so no side effect or node initialization is discarded.
Retain the comparator, mutex option, zero value and list allocation unchanged.

Remove `TxnMVCCNode.Compare2`, a disconnected prepare-time ordering convenience;
retain the live `Compare` method and transaction timestamps/codecs unchanged.

## Boundaries and alternatives

Keep all reserved replay/checkpoint/read-instance paths, including
`MVCCChain.GetPrepareTsLocked`, `SearchNodeLocked`, command factories and
`NewEmptyMVCCNodeFactory`. Preserve the complete `MVCCSlice` implementation in
this batch: its replay annotation requires separate retirement/retention proof.
Do not delete its methods merely because the discovery inventory has no caller.
No tests or assertions are removed, and no replacement helper/state is added.

Keeping stubs retains unused obligations; moving these routines into tests or
adding another factory owner does not simplify the system. Select direct removal
of the proved dead closure and its unused argument propagation. The chain's
in-memory shape loses one function pointer; it is not a serialized schema.
Audit containing-type size accounting and all encoding owners. No wire, catalog,
checkpoint or WAL format changes are permitted. Internal exported helpers have
repository consumers audited; this does not promise unknown out-of-tree Go
source compatibility.

## Validation and review

Before editing, freeze the reachability/ownership evidence and review this design.
Check overlap with open PR files, particularly catalog/transaction changes.
After editing, compare every surviving declaration modulo positions/comments;
only the constructor signature/call arguments and dead declarations may change.
Run existing normal/race suites for txnbase and catalog, including catalog
transaction visibility, rollback, prepared replay and command round trips.
Compile directly affected consumers and run incremental gofmt/vet/configured
lint with exact Go 1.27.1 and matching native provenance. No new BVT or benchmark
is justified: no live SQL path, transaction protocol, hot-path work or storage
encoding changes. Reuse unchanged concurrency/compatibility evidence only within
its actual boundary. The required `gpt-6.1-sol / xhigh` overall review and final
correction re-review gate apply before PR publication.
