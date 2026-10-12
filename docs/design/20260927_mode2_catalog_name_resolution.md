# Case-preserving catalog name resolution

Revision 10. Owner: issue #29418; implementation: PR #29422.
The distinct design approval is recorded in [Revision 10 design approval](#revision-10-design-approval).
The integration target is main `cd8d5abf9665a97d13e50b04840fb74b877fd339`.

## Revision 10 design approval

Change scope: the complete mode-2 catalog-resolution and namespace-admission
change for issue #29418 and PR #29422. The design gate applies because the change
crosses catalog visibility, transaction/lifecycle, authorization, tenant and
distributed-admission ownership boundaries.

Reviewed design: [revision 10 at commit
2a26e7c503cefce29e2cb711bf963026839cd277](https://github.com/matrixorigin/matrixone/blob/2a26e7c503cefce29e2cb711bf963026839cd277/docs/design/20260927_mode2_catalog_name_resolution.md).
SHA256 of that document before this traceability addendum:
`c34fa36a745e25ac60e0d379beb62ce3a62a503a71c6e549cd63fc8af8a9acb6`.

Distinct design-first phase: the revision-10 implementation plan was approved by
native `gpt-6.1-sol/xhigh`, session `01a12eb1-da69-73c0-b3a8-4eabe4904833`, turn
`01a130de-f59a-79f2-b3a4-68c1991cfd2e`. Its decision was **PASS for implementation
plan only**; it required contraction of the independent migration extension and
integration with the target admission owners before final implementation review.

Renewed exact-revision decision: **PASS** for the contract linked above, by native
`gpt-6.1-sol/xhigh`, session `01a12e1c-8887-7b32-8c01-41c0fe8887fe`, turn
`01a13271-bf53-7d42-8350-1d1681954e3d`, on 2026-10-13. No blocking design finding
remains. This addendum records approval without changing the revision-10 contract.

Decision log: parser-only spelling preservation cannot resolve aliases in an
exact-keyed catalog/workspace. A session-dependent shared comparator cannot
preserve one stable index order. A unified-index alternative was rejected for
additional exact-lookup allocation/default-path cost. The selected sparse folded
index and transaction/session overlays retain their existing visibility,
cleanup and bounded-resource owners. Physical identity flows into authorization,
publication and recovery; folded admission uses the existing transaction owner
and TN-ordered frontier before effects. SI never advances its fixed snapshot.

Compatibility scope: fixed naming modes retain existing migration safety gates.
The independent ModeAtSource/ModeAtSourceExported fields and mandatory marker are
removed. Arbitrary global/default drift during connection migration is a
separate capability. No new persisted catalog-key format or rollout exception
is part of this contract.

Accepted tradeoffs: mixed-name maintenance and retained heap have extra cost;
forward-FK discovery is O(tenant FK-name cardinality). Exact-lookup allocation
and canonical-update budgets, cleanup, public SQL and contention controls remain
the acceptance criteria in this document. Measurements do not claim universal
SQL throughput. External CHANGES_REQUESTED remains outstanding.

Implementation deviations: none introduced by this documentation repair.
Design approval does not replace normal/race/SCA/SQL/resource evidence or the
separate native final review of the complete corrected committed candidate.

## Scope and contract

Mode 0 preserves and compares exact spelling. Mode 1 lowercases names. Mode 2
preserves stored spelling and compares the shared `identifier.Fold`: Unicode
lowercase for valid UTF-8 and ASCII-only lowercase for malformed input bytes.
At one tenant and transaction snapshot, lookup returns absence, one physical
name/generation ID, or ambiguity for every spelling in a collision group.
Transaction-local inserts/deletes override committed versions before deciding
uniqueness. IF EXISTS suppresses only typed absence; it never hides ambiguity,
cancellation or storage failures.

Preserving an AST spelling repairs the narrow DROP grammar bug, but cannot make
an exact-keyed catalog/workspace find a case variant. The common engine resolver
and its physical-identity consumers therefore remain in scope. Physical
generation IDs and logical privilege/lineage IDs have distinct owners.

This fixes production with a fixed naming mode. Runtime SET GLOBAL remains
available; arbitrary default drift during connection migration is a separate
capability. Remove the independent ModeAtSource/ModeAtSourceExported extension,
its mandatory marker and mode installer. Retain the target migration protocol,
LAST_INSERT_ID/FOUND_ROWS, temporary replay, prepared-state and cancellation
safety gates. There is no additional rollout exception or new wire format.

## Visibility, ownership and resources

* The existing session system-variable snapshot supplies the statement mode.
  Internal callers without the mode context retain exact lookup.
* The catalog cache owns MVCC, logtail watermarks and GC. Retain the exact index
  and a sparse folded index containing the same noncanonical version objects.
  Insert/delete/GC update both under the existing owner, not a second visibility
  state machine. Exact lookups do not allocate additional index state.
* Database/table operation chains own a lazy folded membership map under their
  existing mutex. Its size is bounded by transaction-touched names; rollback and
  terminal transaction cleanup remain in that owner.
* Session temporary aliases retain their mutex and mutation journal. Folded
  membership follows publication, replacement, rollback and retirement. No
  second cached mode or uniqueness summary is maintained.
* Check cache snapshot completeness before and after visits. If GC invalidates
  the answer, discard it entirely and use the scoped fixed-snapshot historical
  stream. Its existing owner cancels, drains/releases batches and joins the
  producer on every terminal path.

Consumers carry resolved names and IDs into later catalog predicates. Preserve
pre-plan authorization, publisher membership and unpublished-table rejection,
reserved-system tenant routing, private-View tenant ownership, historical
Snapshot/PITR identity and exact-AST account teardown. Caller temporary aliases
cannot shadow foreign publisher or private-View relations. Background current
and historical wrappers share one transaction-owner validator while explicitly
choosing physical or logical identity.

## Namespace and lifecycle admission

Synthetic keys represent a folded name, not a catalog row. They must not use
row-version probes as their freshness proof. A narrow name-admission result API
shares lockop's validated keys, binding checks, exact-row capacity handling and
transaction-owned cleanup. The existing RC-only lifecycle API remains RC-only.
No second lock manager, manual unlock protocol or background owner is added.

The CREATE owner pins sorted database names, all required hidden-prefix gates,
sorted/deduplicated folded namespaces, then the target exact-name domain. Include
positive and negative FK dependencies and known hidden-index names. Missing FK
databases retain a negative database-creation namespace dependency. Folded grants
contribute to the existing refresh decision. A grant timestamp invalidates facts;
it is not a TN read frontier. Use the target TN-ordered barrier and pre-effect
plan/domain revalidation. Retained locks and FK publication preserve FastFail.
`createNamePinsAdmitted` is set only after target and folded obligations succeed.
Execution must not reacquire those pins or refresh an admitted CREATE again.

COPY/TRUNCATE collect the target endpoint/FK/partition domain once under the
outer lifecycle admission. Prefix/folded and exact-name pins precede physical
table locks. The same bounded domain becomes the replacement receipt. Dynamic
hidden-index naming retains its prefix gate. A nested CREATE borrows the checked
actor/transaction/domain and frontier; it does not refresh after DROP or silently
expand the receipt. Temporary and non-RC paths retain their existing ownership.

Pessimistic SI keeps its fixed snapshot. A namespace grant with an empty/unusable
watermark or a watermark/definition newer than that snapshot is a typed WW
conflict before effects, even for a nonwaiting late locker. It never refreshes SI.
Fresh grants and validated reentry can succeed. Binding/capacity/cancellation
failures propagate; capacity failure never widens to a whole-catalog lock.

## Validation and performance acceptance

Required evidence is tied to the complete final delta and build inputs, not an
old commit SHA or historical PR-body claim. Current target requires Go 1.27.1.
Use matching native artifacts, bounded owning-package normal and race tests,
go vet, configured molint/golangci/license checks and named terminal results.
The source export/native build must have an exact manifest; no stale artifacts
or fabricated provenance stamps are accepted.

Regressions cover modes 0/1 controls, case variants, Unicode/malformed bytes,
ambiguity/IF EXISTS boundaries, same-transaction create/read/drop/rollback,
physical name and ID, temporary cleanup, authorization, FK positive/negative
parents and self-reference, SHOW/publication membership, system routing,
private Views, historical recovery and tenant cleanup. Namespace tests cover
complete lock order/domain, retained FastFail, borrowed scope/frontier, fresh SI,
waiting and nonwaiting stale SI, abort, reentry, binding, capacity and cancellation.
Two controlled creators and replacement/hidden-index competitors must prove one
physical winner or a typed conflict, progress and unrelated-name independence.

Public SQL validation includes all three mode-2 suites, mode controls and changed
CREATE LIKE/FK/publication/view/recovery/tenant cases. Retain RC inter-CN visibility,
namespace contention and SI fresh/stale controls on isolated existing fixtures.
Assertions include catalog IDs/names and cleanup, not merely absence of panic.

Matched serial cache controls require no added exact-lookup allocation and no
more than a 20% median canonical-update increase; declare three alternating
samples per version in advance. State scales with catalog GC, transaction and
session lifetime. Historical scans are scoped. Forward-FK discovery remains
O(tenant FK-name cardinality), cancellable and measured on representative data;
cache microbenchmarks do not prove universal SQL throughput.

## Maintenance and review boundary

The reviewer requested a redesign and suggested no more than 1,000 lines. Report
production, tests, generated code, SQL/goldens and documentation separately over
the entire pinned-target delta. Remove the independent migration extension,
superseded FK persistence and duplicated admission/recovery ownership rather than
compressing syntax or deleting distinct oracles. The numerical suggestion is a
review objective, not an operator hard cutoff; external CHANGES_REQUESTED remains
outstanding. Snapshot/workspace uniqueness, namespace/generation admission,
tenant/authorization safety and historical recovery require real owners.

Implementation and validation records belong to the coordinator's task ledger.
Formatting, a successful native build, historical UT or pending CI are not final
approval. Applicable runtime/SCA/race/SQL/resource checks and a separate native
final review of the exact corrected committed candidate remain required.
