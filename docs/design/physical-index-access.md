# Physical index relation access

MatrixOne's physical index relations are implementation details of their owning
base table. [Issue #31436](https://github.com/matrixorigin/matrixone/issues/31436)
and [PR #32032](https://github.com/matrixorigin/matrixone/pull/32032) address
direct SQL and maintenance functions that otherwise expose those relations.
The design was reviewed before implementation by `gpt-6.1-terra` at `max`
reasoning. The rebased design revision is CI-D1 through CI-D3 plus the approved
maintenance-TVF and no-evaluation amendments (2026-11-02).

## Boundary

An authenticated sys/moadmin session may inspect a physical index relation.
Other sessions cannot explicitly read, mutate, clone, or request metadata about
one. Classification uses the requested and resolved names, relation kind, and
durable index feature. Missing definitions fail closed. Server-authored index
work is tied to its already-authorized base and exact generated relation, and
does not confer access to a user expression, view, subquery, or stored routine.

SHOW INDEX's generated metadata sources carry exact AST-node identities and
their parent object. User-supplied WHERE expressions bind normally. Metadata
listings exclude physical relations using their decoded feature as well as the
legacy name and kind; the current persisted information-schema view is upgraded
through the catalog owner. Clone source snapshots are resolved in the source
account, while destination checks use the authorized destination account before
mutation. Prepared cross-account clone validates its source during PREPARE and
rechecks the destination at EXECUTE.

## Index maintenance functions

Every caller-configured relation consumed by a maintenance table function is
resolved and classified before reads, persistence, cache eviction, or other
effects. Internal SQL issued by that operator carries a restrictive relation
policy, including full caller identity. A consumed view is recursively bound
without executing or folding its expressions; actual child SQL is checked again
when bound. Untrusted contexts cannot inherit an internal access capability.

Each index plugin maps its own generated function and operands to the live base
and child definitions. The SQL owner selects roles through that descriptor;
callers supply the function and index name without repeating the role list.
Vector plugin validators share config decoding, while runtime admission retains
its independent checks of the consumed relations. A bounded attestation carries the account, database,
base, function, and role-specific child identities on exactly one generated
operator. The internal codec transports this record when the operator runs on a
different CN. A restricted nested statement may mark only the exact generated
AST source nodes corresponding to that attestation. ISCP initialization derives
a fresh record from the fenced source table ID and current index group; legacy
non-function initialization SQL remains on its existing path. Admission is
repeated after reset and operator reuse, without per-row catalog lookups.

The CPU implementations cover fulltext2, HNSW, and IVF-FLAT; CAGRA and IVF-PQ
use the same boundary without changing their distributed placement. GPU runtime
behavior requires validation on a CUDA-capable runner.

CAGRA and IVF-PQ ignored-row targets follow the same optional-operand contract as HNSW: the plugin validates and attests each present ignored relation, synchronous rebuild uses the descriptor's role list, and runtime admission checks that target before any effects. ISCP initialization derives the same role list from the fenced live index group. An absent optional target grants no authority.
