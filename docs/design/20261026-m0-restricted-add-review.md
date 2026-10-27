# Restricted M0 design review decision

Scope: complete restricted feature in [PR #30816](https://github.com/matrixorigin/matrixone/pull/30816),
including shared default-build changes. Owning parent: [#30518](https://github.com/matrixorigin/matrixone/issues/30518).
Date: 2026-10-26. Implementation reference:
`4c9092203a49f5bba5fcd8e1d2f2a8c6bd0c4982`.

## Independent design phase

Reviewer: isolated `gpt-6-astra`, reasoning effort `medium`, agent session
`01a1765c-2036-7871-a7a2-25c17e6a9c24`. The reviewer did not implement the
feature or author this design. It read the frozen document and repository
design contract, checked source where necessary, and returned the decisions
below without edits, test execution, Ubuntu access or GitHub submission.
This is model design review, not approval submitted by a human maintainer.

Trigger: cross-owner planner/CN/TN/ObjectIO/recovery feature, persistent reading
and shared default compatibility changes, plus the mandatory size threshold.
This is a distinct retrospective approval phase responding to
[XuPeng-SH's CR](https://github.com/matrixorigin/matrixone/pull/30816#pullrequestreview-5632814494),
not a claim of historical design-first approval. Prior `PASS_CODE_REVIEW`
and CI did not satisfy this gate. Full implementation approval was held while
the following design decisions were made.

### R1: REQUEST_CHANGES

Frozen R1 SHA-256:
`3aa976b08c7656d4ab156208404cfdf1fa9a6eadb6cbbe8858374c02d0da8149`.

Finding D1: shared default missing-column behavior acknowledged a compatibility
change but left the supported historical source boundary to future inventory.
Without a decided boundary it could not distinguish intended rejection from
an upgrade regression. Fix: specify source/geometry acceptance, rejection and
migration before approval; actual release qualification may remain QA work.

The review also identified an implementation discrepancy: the legacy
`mo_tables.rel_logical_id` helper checked names only, not real catalog-owned
DB/table identity. A design must require that identity, not claim that names
already prove the system relation. R1's failed decision remains retained.

### R2: PASS (design only)

Reviewed [R2 proposal](20261026-m0-restricted-add.md), SHA-256:
`444e7bfc9f0a5b333435b90a7f9b93a70109aeb5232749a0ab018bcc3324eeed`.
The reviewer checked this hash at the beginning and end of the delta review.
This separate record avoids changing the reviewed proposal after approval;
the containing Git commit supplies its immutable public revision.

Blocking design findings: **none remaining**. D1 is resolved by section 6's
geometry matrix and pre-upgrade source-reader export/recreate/import contract.
QA verifies the chosen contract rather than choosing silently supported shapes.
Sources without a compatible reader/migration are not admitted. The explicit
implementation correction requires real DB/table IDs, canonical seqnum and
type, plus exact object/requested-column binding, before implementation approval.

Accepted decisions/tradeoffs:

- Restricted typed-prefix proof, not zero table IDs or lookalike layout as
  provenance; keep existing transaction commit, CN undo and TN MVCC/WAL owners.
- Ordinary complete objects remain readable independent of age; only the
  canonical system-column exception receives the legacy proof. Other missing
  geometries require the explicit migration or a reviewed design revision.
- Same-version, default-disabled experiment with conservative filter/COUNT
  behavior and repeated metadata cost; reader-local reuse is not a global cache.
- Removing the tag is not data migration. Restore a compatible backup or
  separately validate migration; no production/mixed-version promise.

Limits: migration is a design contract, not a newly validated migration tool.
Historical-source acceptance, maintainer approval, deployed QA and performance/
maintenance-debt acceptance remain separate. No runtime evidence was created
by this design review. R2 approval does not certify the reference implementation
already corrected its name-only legacy helper.

## Implementation alignment after design approval

After the R2 decision, the narrow legacy identity correction was implemented
and returned to the same independent reviewer in a separate implementation
delta phase. Decision: **PASS_CODE_REVIEW**, no blocking delta findings. This
does not claim full-PR implementation approval from that phase.

Reviewed source SHA-256:

- `pkg/vm/engine/tae/tables/pnode.go`:
  `77218d0ab7527b4598151ed667629e0d598225d6a5ec744f9e0b662be0f280ad`.
- `pkg/vm/engine/tae/tables/pnode_legacy_proof_test.go`:
  `27233212f51f084bebac13353ab5528bd6959199654cdb9a11bd04245b3b7a68`.

The reviewer traced `Scan`'s existing `ObjectEntry.AsCommonID()` to the owning
catalog table/database, confirmed the canonical checks and exact object/column
consumer boundary, and checked that negative tests clone rather than mutate
the global system schema. There is no new shared state, lifetime or API.
The review did not execute or certify test logs; author validation is separate.
Model design/code decisions cannot clear a formal GitHub `CHANGES_REQUESTED`
on the maintainer's behalf. Deployed QA and migration acceptance remain open.
