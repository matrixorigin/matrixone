# PR #28935 bounded migration repair

Normative reviewed version: D2-DTS-1 with D2-DTS-1.1 saved-AST amendment.
Scope is only fully qualified direct
INT32 base-table SELECT with captured empty source/current target policy.
All other table-backed replay remains fail-closed.

The following artifacts are preserved byte-for-byte:

- [D2-DTS-1](pr-28935-direct-table-migration-d2.md), SHA-256
  `6f370b6097c9d892562355c83c8ee5b8ba815d564da51c42916c6c42a5f69a33`.
- [Independent preimplementation decision](pr-28935-direct-table-migration-preimplementation-review.md),
  SHA-256 `ba8e82bf1e0223a4a6e8de65e1c763163424120ee864563e5e647a9d59f6d790`.
- [Independent saved-AST amendment decision](pr-28935-direct-table-migration-saved-ast-review.md),
  SHA-256 `f7058e4297cc0b5ebd84f616fce303cdcd9d4d14b4bbc8d1585101ddd32ab46c`.

Decision PASS_FOR_BOUNDED_IMPLEMENTATION_ONLY is technical design acceptance,
not test evidence, GitHub approval, or closure of broader CRC32 compatibility.
Reviewed source: `2d616a3847a59cb09560fa119ff6b35b745d3caf`;
base: `e0314478159c80b062483c307180b02dc009dbc0`.
The reviewed D2 text's v123 CRC32 and provisional v124 direct-migration numbers
are historical placeholders from its original base. Actual main
`743e749a1e33d6c845bb73999e8c82d297a2b657` owns v123 for row-lock admission and
v124 for fractional TIMESTAMPADD; this candidate provisionally assigns CRC32
v125 and direct-table migration v126. The original D2 and review artifacts
remain byte-for-byte unchanged; this allocation refresh does not change their
bounded semantic design. These candidate-local numbers do not imply maintainer
allocation approval or cross-branch deployment support and must be revalidated
against authoritative main before final admission. Broader design acceptance,
old-release lifecycle evidence and genuine older-CN rejoin evidence remain
open.

The local uncommitted candidate passes the real issue29148 SQL/procedure
migration fixtures, including the original transferred rows, later policy,
NeedReprepare, explicit reprepare and cleanup assertions. The whole frontend
package also passes (its existing TestRuleCacheDoubleCheckLocking remains
skipped). These are local results, not current-head CI or publication acceptance.

The approved amendment permits only
saved-AST exact same-table qualifiers introduced by the binder; raw SQL remains
strictly unqualified. It does not remove any object/catalog/policy/generation/
capability guard or expand the supported SQL language.

Two implementation corrections were necessary after source export became
reachable: ordinary signed INT32 metadata from GetTypeFromAst uses Scale=-1,
so only that sentinel and zero canonicalize to no decimal scale (other scales
and hidden-type sentinels still reject); certified PREPARE skips its optional
cached Compile because NewCompile would advance the already-owned statement
boundary a second time. EXECUTE constructs its ordinary Compile later. The
publication visibility/stamp checks remain unchanged. Neither correction is
a generic SQL/catalog/plan transport or permission to admit other types.

Race and BVT qualification remain missing; no large race rebuild was attempted
under the disk constraint. Real post-replay auth/cancel/panic disposal and
fresh-target retry are not established by the synthetic lifecycle unit tests.
Materialized CRC upgrade, nonempty-policy positive migration and preserving
preexisting target state after replay remain open. Root independently reviews
this candidate and its evidence before publication.
