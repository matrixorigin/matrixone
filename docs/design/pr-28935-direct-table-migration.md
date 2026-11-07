# Bounded direct-table prepared migration

This design admits a fully qualified direct INT32 base-table SELECT with an
empty captured source and current target policy. Other table-backed replay
remains fail-closed. The detailed bounded proof, failure ownership and saved-AST
amendment are described in the preserved historical design artifacts:

- [D2-DTS-1](pr-28935-direct-table-migration-d2.md), SHA-256
  `6f370b6097c9d892562355c83c8ee5b8ba815d564da51c42916c6c42a5f69a33`.
- [Preimplementation decision](pr-28935-direct-table-migration-preimplementation-review.md),
  SHA-256 `ba8e82bf1e0223a4a6e8de65e1c763163424120ee864563e5e647a9d59f6d790`.
- [Saved-AST amendment](pr-28935-direct-table-migration-saved-ast-review.md),
  SHA-256 `f7058e4297cc0b5ebd84f616fce303cdcd9d4d14b4bbc8d1585101ddd32ab46c`.

These artifacts document their original reviewed source and are retained
unchanged. Their proposed CRC32 and direct-migration protocol allocations,
feature-specific capability checks, mixed-version matrix and associated
publication requirements are superseded by the current shutdown-only upgrade
contract. They do not define the current deployment policy. Existing unrelated
protocol checks are retained.

## Current admission and ownership

The source checks the retained SQL and saved AST, bound plan, schema identity,
current statement generation and empty policy. The bounded witness carries the
account, database/table identity and version, columns, projection and order;
it is not an executable plan or authorization grant. The target checks the
witness, typed session snapshot, target catalog and current policy before
replay. Its actual PREPARE still performs authorization and is guarded against
binding a different object or publishing a stale generation. Later schema or
policy drift requires an explicit reprepare.

The target must be disposable if replay starts. Preflight rejection does not
consume the once-only migration or mutate target user state. After replay begins,
an error closes the disposable target after releasing the owned operation;
the proxy retains the source until successful handoff. The source and target
use the same binary under the supported contract, so this path does not require
a new feature-specific MORPC floor or a new proxy capability check. Safety
checks on witness completeness, typed state and failure disposition remain.

The saved-AST amendment permits only exact qualifiers for the same table
introduced by the binder; raw SQL remains within the narrow grammar. Ordinary
signed INT32 metadata accepts the canonical scale sentinel -1 or zero. Certified
PREPARE avoids advancing the already-owned statement boundary a second time;
EXECUTE performs its ordinary compilation later.

Regression coverage must exercise source refusal, proof/typed-state validation,
post-replay disposal and the real issue29148 SQL/procedure migration and policy
lifecycle. Test results and outstanding release-upgrade acceptance are recorded
in task evidence, not inferred from this design.
