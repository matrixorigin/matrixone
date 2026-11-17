# Reader membership before payload construction

Owner: #32388; parent: #32379. Design revision 1, approved by the requester on
2026-11-17 before implementation. Independent of #32387; baseline main 47957ef450.
Selected reader output, leases and normalization through #32492 already exist.
This revision adds earlier filtering, not a second dictionary-output API.

## Behavior and interfaces

Extend the existing internal ReaderFilterRequest with optional membership key
positions and a read-only selection callback. Nil preserves ordinary execution.
Admit compatible filtered probe scans using the existing equality-lineage,
independent-source, capacity and profitability proofs. Only predicates whose
skipped evaluation cannot suppress errors or side effects admit this ordering.
Do not change join placement, persisted annotations or native filtering kernels.

Preserve storage visibility, tombstones and native storage-filter handling. Read
membership keys, apply the ready domain, read remaining predicate columns for
survivors, and evaluate the existing ordered residual chain. Compose mappings
back to original physical rows, then construct remaining payload once using the
existing retained-view/owned-output policy. Empty results skip payload reads.
Keep intermediate mappings reader-local and publish final output once. Reuse
existing low-rejection bypass rather than introducing another threshold.

Existing exact-filter, Top-K and unsupported reader paths retain their proven
implementations. Compatible exact programs consume the membership survivors as
an initial physical-row selection through the existing exact-read function;
this does not replace their native predicate/payload implementation. Fall back through the established route without reevaluating
predicates, changing their errors, or rejecting valid input. Default output stays
flat; view publication still requires existing account/capability admission.

## Ownership and boundedness

The prepared scan owns the sequential request and callbacks. Borrowed selections
are consumed before the next callback; reader-local composition scratch is at
most one block of row indexes. A scoped loader owns cached input until evaluation
and any final gather finish. Escaping backing requires the existing physical pin
lease; refusal uses an accounted selected-row gather. Restore private reader
headers on success/error/cancel and release unpublished state on all paths.
Retain the existing allocation, queue, cache and terminal protocols. No new cache,
background worker, user-facing flag, codec or wire format.

## Validation and delivery

Extend existing reader and planner fixtures for partial/all/empty membership,
storage selections, tombstones and physical-row composition, NULLs, reordered
input views, ordered residual errors, unavailable/unprofitable domains, pin
refusal/eviction, cancellation and prepared reuse. Verify full account release.
Run owning packages, focused lifecycle race checks and a small SQL fixture.

Count decoded, gathered and retained bytes and physical I/O separately: fewer
selected rows do not imply fewer decoded bytes for a touched block. Require a
component work benefit and cheap all-match/bypass behavior before full timing.
Qualify this branch alone against fresh main using local SF10 memory/disk and
SF100 disk. Use one process/build/configuration, one open and six full Q1-Q22
suites; discard cold suite 1 and profile/account suites 2-6. Require attributable
Q19 benefit and no concealed per-query/Total regression; no spill/swap/OOM or
competing builds. Keep configured budgets matched. Benchmark scripts do not
verify input/results. Append results as comments, use the PR template, and keep
draft until acceptance. Preserve raw datasets/evidence; remove task-owned run
clones at terminal completion. Material design changes need requester review.
