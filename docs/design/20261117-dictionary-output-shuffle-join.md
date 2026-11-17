# Dictionary shuffle and join outputs

Owner: #32387; parent: #32379. Design revision 1, approved by the requester on
2026-11-17 before implementation. The implementation is independent of #32388
and starts at main 47957ef450. Foundations and selected-input consumers through
#32492 are merged. This document records the approved design, not measured gains.

## Behavior and boundaries

Keep shuffle routing kernels, producer lanes, batching, queue credits and
transport protocols. A partition can escape as a dictionary only when it fills
an available output slot from one immutable input. Short mixed-source fragments
retain ordinary coalescing. Freeze source columns once per pending input and
share their backing across admitted partitions; receivers own separate headers
and selections. Whole-bucket forwarding retains its existing no-selector path.

Use the existing batched hash-join match paths for residual-free proven-unique
INNER and probe-only SEMI/ANTI output. Preserve output batch boundaries and probe
order, including repeated probe rows and existing NULL semantics. Probe columns
share one output selection. INNER build columns can be views when all selected
rows belong to one build batch, otherwise use existing flat bulk gathers. Other
join modes, residual predicates, spill recovery and unknown consumers retain
their existing accounted representation boundaries. Eligible view-producing
joins must not ask upstream to copy payload solely to satisfy owned-input
preference.

No multi-base vectors, new cache, public configuration, SQL or wire changes.

## Ownership, refusal and cleanup

Reserve existing queue credits before constructing a partition. Sources are
immutable snapshots, never borrowed mutable upstream headers. Unpublished
headers have one producer cleanup owner; publication transfers them to the
existing queue/receiver lifecycle. A shared backing's physical charge is held
once until its final lease releases; logical selected size never discounts it.
Producer reuse uses existing COW, without an acknowledgement barrier. Snapshot
and row-map workspaces are execution-local and bounded by the existing pending
input, output batch size and queue capacity. Retention refusal falls back to the
existing accounted gather; cancellation and real allocation failures propagate.

Join views are retired before result reuse. Leases may outlive the probe input
or build JoinMap header; Reset/Free and partial construction release unpublished
state once. Preserve the existing terminal protocol and cancellation edges.

## Validation and delivery

Extend existing fixtures for one-source partitions, mixed coalescing, full
transfer, blocked/resumed writes, consumer exit, partial publication, cancellation
and source reuse. Join controls cover unique/membership output, duplicates,
NULLs, selected inputs, multi-batch build payload and fallback. Check final
account/pool release; run affected packages and focused lifecycle race checks.

First require component work/copy savings without mixed/tiny-path regressions.
Then qualify this branch alone against fresh main using local SF10 memory/disk
and SF100 disk, with matched configured budgets and no spill/swap/OOM or build
interference. One process per build/configuration opens once and executes six
Q1-Q22 suites; discard the first and profile/account all five warm suites. Report
all queries and Total, wall/CPU/instructions, allocation/copy work, retained
memory and I/O. Require attributable Q9 benefit; investigate confirmed regressions
before readiness and obtain approval for material design changes. Do not verify
datasets/results in benchmark scripts. Append results as comments; use the PR
template and retain draft until acceptance. Remove only task-owned run clones
after completion, retaining raw datasets and evidence.

## Approved revision 2: identity probe forwarding

On 2026-11-17 the requester approved the measured-cost correction recorded in
`build/tpch-dictionary-output-wave-20261117/evidence/join-output-cost-revision.md`.
It supersedes partial-output join dictionary construction in revision 1; shuffle
full-slot partition production is unchanged. The original constructor experiment
and allocation profile remain in local evidence, not in the delivered path.

Start the existing bounded unique INNER/probe-only SEMI/ANTI match path with an
optimistic identity projection. Continue gathering build payload normally. Skip
probe-payload copies while every row survives. At the first mismatch, copy the
preceding matched prefix once into existing owned result buffers, then continue
ordinary selected gathers without rehashing or reevaluating predicates. If all
rows survive, borrow the original probe headers until the next Call through one
private result batch. Retire that batch by clearing references, never by freeing
or mutating its borrowed columns. Existing downstream retention rules still apply.

Use the ordinary path for oversized/continued input, residuals, other join modes,
spill and owned-only destinations. No new row-map buffer, lease, core API or
per-row kernel change is introduced. Gate all-match/first/middle/last-mismatch,
all-rejected and selected-source controls against the existing path: no extra
steady-state Go allocations or repeated payload copies, with an identity-case
work saving before all22 qualification. Keep the original per-query gates.

Identity forwarding stays inside the original child Call lifetime, so revision 2
retains the existing owned-input preference. It avoids the second join payload
copy without forcing upstream readers to construct immutable dictionary metadata.
Already-selected inputs remain supported and can be forwarded unchanged.

## Approved revision 3: nested owned-preference admission

The requester approved this repair before implementation. Mechanical destination
admission remains mandatory. An owned-output preference permits identity
forwarding when every projected probe vector is flat and owns detachable data
and area backing. Borrowed/dictionary inputs under that preference retain the
existing gather. Keep uniqueness, mode, bounded-input, spill and continuation
restrictions and the upstream owned-input preference. No selector or lease is
added. Prove nested join negotiation, original header identity, first mismatch
fallback and source lifetime before native Q9 activation and isolated all22
qualification. Existing benchmark tables remain current until replaced.

## Approved revision 4: bounded nonunique INNER payload gathering

The requester authorized extending residual-free nonunique INNER projection to
the existing bounded tuple scratch and vector gather kernels. Collect resolved
probe/build ordinals in stream order, including repeats, at most one Find chunk
per gather. Preserve current probe state/selection continuations, NULL handling
and output boundaries. Keep lookup/join placement and residual/outer/single-row
error modes unchanged; spill remains on its established route. No new dictionary
constructor, multi-base vector, allocation state or hash/routing kernel. Require
component instructions/copy benefit and native Q9 activation before all22 timing.
