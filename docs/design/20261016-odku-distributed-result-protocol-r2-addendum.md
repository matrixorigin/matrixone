# Distributed ODKU Result Protocol: r2 alignment addendum

Status: approved by independent scoped design review r2; see
`20261016-odku-distributed-result-protocol-review-v2.md`.
No build, regression, BVT, deployment, performance, or implementation acceptance
is claimed by this document. Root owns reviewer and lifecycle decisions.

- Revision: r2, 2026-10-16, owner VioletQwQ-0, PR #28444 / issue #28162.
- Accepted source before this repair: `0c2090984b2aebef9cf10fba304465b14e3f5c77`.
- Necessary normal-merge target: `b3d6b21fe9d260c91e1777529a48a9d93061af33`.
- Original r1 inspected implementation: `286b60837041d819573a260f7ab6e346125c6d3a`.
- Original reviewed r1 content: Git blob `914d5ddd19635d07924ebbe22cbb5eaa6f192fa2`
  at head `038207ff2e5fd84d79d7d6c851c467b68b05a4e8`, SHA256
  `ddff85b82ca7775ce492d68fb904a3bd5952c59e4f6390a8733ddf347953e756`.
- The immutable [v1 approval record](20261016-odku-distributed-result-protocol-review-v1.md)
  remains an approval of that exact r1 content. It is not approval of this r2
  addendum or of the repaired implementation.

## Subsequent candidate allocation overlay (not part of r2 approval)

For the current conflict-repair candidate, actual main is
`743e749a1e33d6c845bb73999e8c82d297a2b657`, whose capability prefix reaches
MORPC124. The candidate preserves main's MORPC123 row-lock admission and
MORPC124 fractional TIMESTAMPADD SECOND contracts and provisionally allocates
JSON ELT/opaque visible text at MORPC125, typed JSON MIN/MAX at MORPC126, and the
complete ODKU/session-LAST_INSERT_ID contract at MORPC127. Independent unmerged
branches also use provisional MORPC125; this is not a coordinated allocation or
cross-branch deployment guarantee. Revalidate it at authoritative main admission.
This supersedes only the r2 allocation for the current candidate; it does not
claim design approval or implementation acceptance. The r2 review record and
allocation below remain historical and bound to their stated source and target.

The approved r2 addendum below supersedes only the allocation and compatibility
boundaries in the [retained r1 specification](20261016-odku-distributed-result-protocol.md).
The existing coordinator ordering, provenance, bounded reduction, transaction
publication, generation/retry ownership and performance budgets remain the
implementation's intended design. Their existing evidence keeps its original
revision binding; it is not automatically upgraded to r2 acceptance.

## Current cumulative allocation

| Capability | Current floor | Allocation owner |
| --- | --- | --- |
| Collation-aware LEAST/GREATEST overload 4 | MORPC117 | Authoritative merged target b3d6b21f |
| Selected JSON ELT and canonical opaque visible text | MORPC118 | Inherited JSON capability, moved after the target prefix |
| Typed JSON MIN/MAX aggregate comparison | MORPC119 | Inherited typed comparison capability |
| Ordered ODKU summary and captured session LAST_INSERT_ID baseline | MORPC120 | This PR's complete result contract |

The old candidate's 117/118/119 allocation cannot be retained after the merge:
upstream117 does not implement JSON ELT, and a typed-JSON119 peer does not
implement this ODKU contract. Source admission, cached/persisted-plan admission,
placement, actual destination, encode/decode, and same-stream checks must compose
the actual maximum above. ODKU and captured-baseline plans reject119 and accept
120; JSON ELT rejects117 and accepts118; typed JSON MIN/MAX rejects118 and
accepts119. LEAST/GREATEST keeps its actual117 owner.

The r1 wire fields and terminal JSON schema are unchanged. The semantic floor
changes to120; lowering the runtime floor does not permit silently discarding
tracking metadata. Unknown/older destinations still fail before executable
dispatch, and a tracked remote pipeline still requires baseline presence even
when its value is zero. Previously retained 115/119 capability or binary results
are historical evidence, not measurements of the current120 implementation.

## Result owner and result column

| Supported shape | Result owner and observable rule |
| --- | --- |
| Modern ODKU with visible AUTO_INCREMENT columns | Select the first visible AUTO_INCREMENT column in table-definition order for both PRE_INSERT generated provenance and DEDUP_JOIN stored/incoming fallback ID references. Additional columns still allocate and write normally; their presence does not reject the statement. |
| Multiple visible columns, all IDs explicit | Accept DML. No generated ID from the selected column means no generated session publication. Existing source-specific OK fallback rules still apply. |
| Multiple visible columns, selected column generates | A successful inserted row can publish that selected column's ID using the existing first-generated logical ordinal rule. |
| Selected column explicit, only a later column generates | Accept and persist the row. The later column does not acquire modern ODKU result ownership or replace the session ID; existing non-generated OK fallback rules use the selected column. |
| No primary/unique user key | Retain the existing legacy plain-INSERT fallback, including prepared update-clause parameters. Its PRE_INSERT publishes the legacy statement ID rather than an ODKU summary. Use that actual plan contract for frontend and internal-executor result selection, retaining ordinary allocator reporting. |
| Visible user column named `__mo_fake_pk_col` with an explicit user primary key | Treat it as a visible user AUTO_INCREMENT column. The SQL spelling alone does not make it internal. |
| Allocator-owned hidden fake primary key | Exclude it using hidden/internal ownership metadata; it must not publish a user insert ID. |

The first-visible policy is specifically the modern tracked-ODKU result policy;
it does not redefine ordinary INSERT/REPLACE or the legacy no-key allocator's
existing multiple-column behavior. Additional AUTO_INCREMENT columns are not
new deduplication or result owners. This adds no wire field or new reducer.

Both result consumers inspect the actual successful execution plan. The frontend
uses its computation wrapper's synchronized plan; the internal executor uses
Compile.GetPlan(), including a plan rebuilt by retry. An ODKU AST clause or an
empty/nonempty summary is not sufficient to choose the owner. An empty tracked
summary must not fall back to a stale legacy statement ID.

## Validation obligations and limits

Focused plan tests must prove actual no-key legacy ownership and retain prepared
parameter arity, plus matching PRE_INSERT and DEDUP result-column selection for
explicit/generated multi-column and visible-name schemas. PRE_INSERT tests must
retain rejection of hidden, invalid and out-of-range source columns. Frontend
and internal-executor regressions distinguish raw OK ID, durable session state,
statement ID and current-plan/retry selection.

The canonical `on_duplicate_key_edge.sql` and `last_insert_id_hidden_pk.sql`
extensions cover real no-key/prepared insertion, multiple visible allocators,
later-only generation, changed/no-op conflicts, visible internal spelling and
the existing unique-column assignment rejection with unchanged stored rows.
Their `.result` additions require actual normal comparison. Typed/helper checks
or static SQL/result parity do not establish that execution evidence.

Actual-client validation must assert raw OK, next session value and persisted
rows independently. Preserve the existing transaction/retry publication timing;
after an error, reseed independently before testing session reuse rather than
claiming a universal after-error session value. Capability simulations must
exercise the new predecessor/floor boundaries; they do not establish real
mixed-binary deployment or measured performance acceptance.

This document records the intended contracts, not execution receipts. Actual
regressions, canonical comparisons and developer validation need separately
recorded results bound to the tested revision. Root separately decides r2
re-review, any review-thread resolution, design acceptance and deployment/
performance/QA acceptance. The v1 record is not silently relabelled as any of
those outcomes.
