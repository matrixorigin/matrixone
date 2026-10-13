# Scalar RC TRUNCATE admission

Status: design approved; implementation and validation tracked in the linked PR.
Issue: https://github.com/matrixorigin/matrixone/issues/29400
Implementation: https://github.com/matrixorigin/matrixone/pull/29837

Design input: `2d0be48dd912497d2d861da60939efdc6591c780` against base
`64b715a8234e272794185e2acc98ad58a8346c65`. Actual `gpt-6.1-sol`, reasoning
`xhigh`, session `01a12710-062c-7840-86a8-c66927427fd1`, approved this smaller
design before scoped production edits on 2026-10-10. It supersedes the broader
FK/subtree proposal: that proposal is coherent but unnecessary for ordinary
scalar progress. Design approval does not establish implementation or CI success.
Deployment requires maintenance cutover, with drained old transactions.

## Problem and acceptance boundary

A retains UPDATE on dbA.t; B holds lifecycle G S while DROP waits for A;
independent TRUNCATE on dbC.u currently requests G X and joins the convoy.
Waiting on a fresh G request fixes premature lock conflicts but leaves this
availability problem. C must commit before A releases while B is still waiting,
with empty data, a new physical ID and the same logical ID.

The scoped path accepts permanent ordinary scalar tables under pessimistic RC,
including primary keys and auto-increment. Both the live constraint definition
and FK catalog must show no incoming, outgoing or self FK relationships. There
must be no secondary index, index ownership, partition definition/feature flag,
parent ownership, Snapshot/PITR protection or branch component. Uncertifiable
structures, history, optimized DELETE, SI, temporary and special tables retain
the existing broad path. Historical G-X convoy limits remain; this change does
not close all of issue #29400.

## Existing responsibilities and protocol

Reuse the current distributed lifecycle keys and replacement executor:

1. Discover scalar eligibility and history without effects. Positive facts
   select broad admission before lower locks.
2. Acquire C S, G S and root D S through existing exact admission. Install the
   TN-applied RC frontier and verify registry identity.
3. Reuse the component loader to pin K even for an absent root. Require the
   complete touched component to be empty.
4. Acquire exact root catalog T X and existing destructive storage X with definition-change intent. This outer lock
   publishes the retired definition fence on commit; nested DROP borrows it. Install
   the final frontier, reopen the relation and verify physical/logical IDs and
   scalar shape.
5. Authoritatively read both FK catalog directions and history in this same
   transaction without FOR UPDATE. Include physical and SnapshotTableID
   identities, existing scope rules and account identity resolved by account ID.
   User snapshots are checked in the actor catalog as well as SYS history.
   Reserved protection snapshots select broad handling as well.
6. Run existing SHOW CREATE/DROP/CREATE, omitting the G write, lineage preparation
   and preservation, and branch reclaim only for this certified root.

Late history or structure changes promote G without waiting and return the
existing definition-change retry before effects. Promotion conflict preserves
whole-transaction rollback semantics. There is no late domain growth or D upgrade.

CREATE performing FK publication or repair takes actual G X before D/T locks or
physical/catalog effects. Detect it from existing typed plan fields: UpdateFkSqls,
Fkeys, FkDbs/FkTables and FksReferToMe. This covers forward references without SQL
parsing or a new plan field, including the shared CREATE route used by clone.
Ignore-FK options do not bypass the guard. Validate planned parent generations
before publication; ADD FK validates at its existing named-parent locking seam.
The deliberate tradeoff is broad serialization of FK CREATE/repair.

Extend the existing synchronous receipt rather than add another framework.
Broad nested CREATE borrows actual G-X ownership without new admission/frontier
advancement. Scalar nested DROP is authorized only for its immutable operator,
account, database identity/name, table name and physical/logical IDs; it receives
the existing skip-reclaim option. Validate before effects, expire and restore the
receipt with defer on success, error or panic. Locks remain transaction-owned.

Full FK RMW refactoring is outside this boundary: scalar replacement changes no
external constraint. Simple lost-update arguments must also account for existing
TAE ALTER version conflicts and rejection of retired physical IDs. No recursive
inventory, partition cache API, new state machine, durable owner or execution
path is introduced. Existing cancellation, statement retry and rollback own
termination and cleanup.

## Required evidence

Use the existing two-CN fixture for before-A-release progress and same-object
exclusion, identity/data preservation on cancellation/timeout, and subsequent
progress. Cover CREATE/forward-FK/repair/clone and ADD FK publication competitors,
eligibility fallback, physical/logical and tenant history, receipt misuse/expiry/
panic, and post-DROP/CREATE rollback. Preserve branch successor/history and
DIFF/MERGE behavior, SI, temporary tables, optimized DELETE and auto-increment.
Run affected normal/race/static checks and final gpt-6.1-sol/xhigh overall review.
Unchanged v1 evidence remains reusable only where dependencies did not change.

## Historical compatibility boundary

The scoped path excludes tenant snapshots, including renamed tables identified
by their stable logical ID. The existing broad path remains unchanged. QA found
an existing tenant historical-lineage retention defect: broad positive probes
and compaction read only SYS snapshots. This change does not claim to repair
that defect or close historical convoy limits. Its red reproduction is retained
outside the source tree for a separate correction. The same design session
explicitly approved this narrower boundary after challenging retention scope.
