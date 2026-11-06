# Physical index relation access

MatrixOne's physical index relations are implementation details of their owning
base table. [Issue #31436](https://github.com/matrixorigin/matrixone/issues/31436)
and [PR #32032](https://github.com/matrixorigin/matrixone/pull/32032) address
direct SQL and maintenance functions that otherwise expose those relations.

## Boundary

An authenticated sys/moadmin session may inspect a physical index relation.
Other sessions cannot explicitly read, mutate, clone, or use table-specific
metadata operations on one. Classification uses the requested and resolved
names, relation kind, and durable index feature. Missing definitions fail closed.
Server-authored index work is tied to its already-authorized base and exact
generated relation, and does not confer access to a user expression, view,
subquery, or stored routine.

SHOW INDEX's generated metadata sources carry exact AST-node identities and
their parent object. User-supplied WHERE expressions bind normally. Metadata
listings exclude physical relations using their decoded feature as well as the
legacy name and kind; current persisted information-schema views are upgraded
through the catalog owner. Historical TABLES views bind a filtered catalog
source without changing their stored definition. Clone source snapshots are
resolved in the source account, while destination checks use the authorized
destination account before mutation. Prepared cross-account clone validates its
source during PREPARE and rechecks the destination at EXECUTE. New clones of
system metadata views use the local listing policy. Existing user-owned views,
including older clones, keep their definitions and grants and may expose catalog
metadata; they do not grant direct access to index rows.

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
Maintenance matches the exact name from the owned definition; SQL spelling is
normalized by the planner, preserving distinct Unicode index identities.
Synchronous maintenance and ISCP share role projection while retaining their
own source admission and error policy.
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
