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

## Decisions and alternatives

The security invariant is that knowing an internal relation name does not
grant access to its rows or a way to invoke maintenance on it. A privileged
sys/moadmin session can still inspect it; an ordinary session retains its
existing access to authorized base tables. Current TABLES, PARTITIONS and
COLUMNS listings exclude physical children, including a child renamed after
its creation. The existing-user-view exception described above concerns
metadata in previously saved definitions, not authority to read index rows.

Leaving the existing behavior unchanged would preserve the direct-access
exposure. Filtering only the reserved name prefix would miss a renamed child
whose catalog identity still marks it as physical. Treating a complete internal
executor or nested query as trusted would instead risk extending that authority
to caller-provided SQL, views, or routines. The chosen classifier and bounded
operator-specific provenance close both boundaries, at the cost of catalog
checks when an operation is admitted or reset. They do not add per-row
authorization or a new worker, queue, or long-lived cache. Privilege probes
used to choose the catalog path are side-effect-free; only a real denied
relation access constructs and reports a privilege error.

The earlier versioned design record at
[`8c9f4111ce`](https://github.com/matrixorigin/matrixone/blob/8c9f4111ce/docs/design/physical-index-access.md)
states that CI-D1 through CI-D3 and the maintenance-TVF and no-evaluation
amendments were reviewed before implementation. The original review artifacts
are not available in the current task record; that statement is historical
context, not a substitute for reviewing this revision and its implementation.

## Ownership, failure and compatibility

The plugin descriptor projects the roles in a live index group. The compiler
accepts the generated SELECT and function only after validating its shape,
literal arguments and provenance. Its one-operator specification carries the
account, database, base and role-specific target identities. The pipeline
codec transports that specification to a same-version receiver, which resolves
and checks the live identities before maintenance effects. Nested SQL receives
access only to the marked generated sources; binding a consumed view does not
evaluate its expressions. ISCP independently fences its source ID and derives
the live group. Missing, malformed or mismatched provenance is rejected at the
relevant admission boundary; reset and reuse perform admission again.

Each specification contains at most 16 targets, and copies are scoped to the
statement or operator. Transaction completion, cancellation, retries and
resource cleanup remain owned by the existing executor and transaction paths;
this change does not introduce a separate retry or rollback mechanism. An
admission error is returned through the existing SQL or ISCP error path so the
caller can diagnose the denied operation.

Current persisted system views are refreshed through the existing tenant
upgrade owner. Published historical view and migration definitions remain
unchanged; historical TABLES binding and new metadata-view clones use the
policies described above. Upgrade and recovery must preserve the released
source definition until the existing upgrade transaction commits. MatrixOne's
upgrade contract is shutdown upgrade: mixed-version operation and downgrade are
outside this design. A released definition installed on a current binary is a
useful upgrade control, but is not an old-binary upgrade test. Subscription
and local view branches must both retain the same physical-index boundary.

## Validation map and remaining limits

`TestPhysicalIndexRelationClassification` covers the classifier;
`TestInformationSchemaCurrentColumnsFiltersPhysicalIndexes` and
`TestAdaptLegacyInformationSchemaColumnsDDL` cover current and historical view
definitions. `TestHiddenIndexUserAccess` and the information-schema BVT cover
ordinary-user access and visible metadata. `TestTablePrefixMetadataUpgrade`
covers the persisted-view upgrade path. The maintenance plan, codec and runtime
admission tests cover statement shape, identity and reset; ISCP initialization
tests cover its source fence. `TestIssue29566IndexRebuildCoordinator` exercises
the distributed rebuild path. These checks must be assessed at the actual
source revision and execution mode; a recorded test name alone is not a pass.

Real CAGRA/IVF-PQ execution on a GPU and an upgrade from a running old binary
remain unverified. Device-free GPU-tag builds and released-definition tests do
not establish those results. The BVT result snapshots for SHOW CREATE retain
the full current view definition and must change when that definition changes.
