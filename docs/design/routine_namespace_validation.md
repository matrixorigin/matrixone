# Routine overload namespace validation

A cached or prepared routine call depends on two distinct things:

* the exact selected FunctionRef and immutable revision contracts;
* the complete candidate set for its account/database/name at binding time.

The revision-local `namespace_version` stored on an identity/revision cannot be
used as the second dependency: CREATE of a sibling does not update that tuple.
Updating all sibling heads would make them disagree with immutable revisions.

`RoutineCall.namespace_fingerprint` and `RoutinePlanDependency.namespace_fingerprint`
now carry a separate state fingerprint. The resolver and reuse validator read
all same-name candidates, across SQL/Python and signatures, in their catalog
read transaction (and with the explicit historical snapshot when applicable).
The internal reads are marked derived after BEGIN and restored before finishTxn,
so READ COMMITTED cannot advance the snapshot between enumeration and digest
capture. Database identity uses the same read transaction and snapshot tenant.
The selected ID locates the namespace; account context and the independently
validated database ID fence its scope. Execution continues to use the exact
FunctionRef, never a name or a latest-revision lookup.

The query deduplicates namespaces before joining candidates, and projects fixed-width
SHA-256 hashes of text/JSON metadata instead of transferring source bodies.
The canonical encoding hashes a versioned JSON tuple per candidate: identity,
active revision, revision-local namespace, base arguments/body/language/return/
database/SQL mode/security, plus effective revision arguments/body/language/
return/security. Sorted fixed-length candidate digests form the namespace
state digest. Thus newly introduced better/ambiguous matches invalidate even
when the selected identity is unchanged. DROP, replacement, clone and restore
follow visible catalog state. Restoring exactly the same candidates need not
invalidate: this is not an event counter. Selected revision integrity and
security checks still run independently.

There is no new persistent catalog or backfill. Older plans without the digest
cannot be reused; the normal bind path regenerates the dependency. A missing
selected identity invalidates rather than reporting catalog corruption. Empty
candidate results are distinct from malformed multi-result responses.

Validation reads are bounded to 65,536 candidate rows (an extra row detects
overflow) and 64 MiB of decoded metadata per read. Exceeding either marks the plan non-reusable and forces rebind,
never accepts a truncated digest and does not reject otherwise valid SQL. This increases reuse validation cost with the
number of overloads and their definition size. It does not add worker RPCs or
per-row lookups. Dependency count/serialized-byte bounds remain in force.

This replaces the earlier proposal to advance a separate mutable namespace
generation for every overload-set change. It preserves that proposal's cache
correctness property without introducing a second publication/restore index.
