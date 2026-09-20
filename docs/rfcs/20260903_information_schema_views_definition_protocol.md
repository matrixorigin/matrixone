- Status: proposed — implementation complete; pending independent approval
- Start Date: 2026-09-03
- Authors: MatrixOne maintainers
- Implementation PR: https://github.com/matrixorigin/matrixone/pull/27716
- Issue for this RFC: https://github.com/matrixorigin/matrixone/issues/27655

# Parser-derived `information_schema.VIEWS` definitions

## Summary

`information_schema.VIEWS.VIEW_DEFINITION` must expose the defining SELECT,
not the original CREATE statement. New views persist a parser-derived definition
and legacy rows are read through parser-aware metadata functions. The functions
are new distributed plan functions (IDs 581 and 582), so the catalog contract is fenced by MORPC
v89.

## Problem and invariant

Schema-diff and migration clients replay `VIEW_DEFINITION`. A full CREATE
statement is not a standalone SELECT and falsely marks aggregate views as
updatable. Current rows, and legacy rows that have already acquired the additive
metadata, return their parser-derived frozen SELECT. A legacy row that contains
only the historical `Stmt` is parsed into a defining SELECT, but a raw
`SELECT *` cannot be expanded to its creation-time column list because that
snapshot was never persisted. Such a row therefore preserves the stored
wildcard expression instead of claiming a historical freeze. Malformed catalog
rows may return NULL. No CN that cannot resolve either function ID can receive a
pipeline or catalog view that references it.

## Design

The CREATE/ALTER owner derives `ViewData.Definition` from the stabilized view
AST, after wildcard expansion and separately persists `CheckOption`. Explicit
view column names are exposed through a derived-table column list around that
frozen SELECT, including UNION output, so replay preserves the view's public
column names without renaming aliases referenced by the inner `ORDER BY` or
`HAVING`. The same parser-tree helper is used when regenerating a legacy
definition and when the metadata function parses a legacy `Stmt`. The catalog
remains the single owner of that frozen metadata.
`mo_view_definition(viewdef)` and `mo_view_check_option(viewdef)` return the
stored fields without writes; for an older row that lacks them, they parse only
the stored statement using its persisted SQL mode and identifier-case settings.
This bounded, side-effect-free fallback avoids a second SQL regexp lexer and
does not depend on background recovery.

When either function identity occurs in a persisted view expression, the
planner writes `required_protocol_version: 89` into `ViewData`, including when
the call is nested under another expression. The real view bind/Prepare path
reapplies that marker. It uses the existing two-floor admission lifecycle:
HAKeeper publishes the durable read floor during the v89 decoder barrier, while
the authoring floor remains closed until the all-CN admission and catalog fence
complete. Therefore an authoring or read floor of 0 or v88 fails closed, an
all-v89 fenced CN can create the VIEWS definition, and an ordinary view without
either function remains unmarked.

The fallback cannot infer metadata that is absent from a legacy row. In
particular, it does not promise to reconstruct the historical expansion of a
wildcard; the authoritative freeze begins when the additive fields are written
by CREATE/ALTER or by the bounded regeneration path. Adding a historical column
snapshot to the legacy catalog format would be a separate compatibility
migration and is outside this PR.

MORPC v89 is allocated as `MORPCLatestVersion + 1` from official main v88 at
`6ad0c48b0567eda5db6c3daf843bf1823c388f93`, which already owns v70 through v88;
v88 is the canonical vector HLL_ADD_AGG capability. The two function IDs
are the next available IDs after main's exclusive function bound 581, and the
bound advances to 583.
The capability is specific to these functions and the persisted VIEWS definition.
A sender probes the selected destination CN as well as its local runtime before
encoding a pipeline containing either function ID; an unknown or unavailable
destination capability fails closed. The v4.0.6 VIEWS upgrade waits for common
v89. New tenant initialization installs the new VIEWS DDL only after the local
coordinator and every CN in the current inventory have positively confirmed v89;
a mixed, unknown, RPC-failing, or incomplete capability probe records the
predecessor VIEWS definition while still committing the final-version tenant
row. A bounded post-upgrade reconciliation pass later rechecks common v89 and
reuses the guarded transactional entry to replace only that predecessor
definition. Any cluster with a CN below v89, including the immediate predecessor
v88, preserves all existing metadata definitions, including the v58 COLUMNS
contract. Pipeline preparation, remote marshal, and
remote unmarshal reject a
pipeline containing either function ID below v89. The receiver check protects
stale prepared work as well as normal sender dispatch. Before the v89 HAKeeper
protocol-floor activation is durably committed, a cancelled rollout may stop
the v89 upgrade, keep or restore `InformationSchemaViewsLegacyDDL`
transactionally, wait for catalog and in-flight work to converge, and admit
v88 only after verifying that no v89 maintenance worker can re-install the
new definition. After the v89 floor is committed, the floor is monotonic and
v88 binaries must not be admitted: restoring the view text alone cannot undo
the decoder barrier. The supported recovery is forward recovery with v89-
compatible log/CN services, or a coordinated restoration of cluster state from
before the activation barrier; an ordinary in-place downgrade is unsupported.
Merely draining below-v89 requests is not sufficient because the new persisted
view text references the functions. The new JSON fields are additive and old
binaries keep treating them as unknown.

## Alternatives

Keeping raw SQL regexp extraction was rejected because it repeatedly diverged
from the SQL lexer for comments and quoted strings. Eagerly rewriting every
legacy row was rejected because the existing recovery lifecycle is deliberately
inactive and a metadata read must not perform unbounded catalog writes. Allowing
the DDL before v89 was rejected because an old CN cannot bind the metadata functions.

## Bounds, security, and operations

The compatibility parse is per visible legacy row and is linear in that row's
stored statement; current rows return their stored definition directly. If both
legacy metadata columns are requested, the row may be parsed once per function;
each parse is still bounded by the stored statement and query cardinality. It
creates no durable work, goroutine, queue, retry, or cache. Existing visibility
joins remain the authorization boundary; the functions do not broaden the
selected row set. A mixed-version request fails before dispatch with a stable
NotSupported error rather than returning wrong metadata.

## Validation

Focused parser/function tests cover current and legacy definitions, quoted and
commented inputs, malformed rows, frozen wildcard expansion, the documented
legacy raw-wildcard boundary, explicit derived-table column lists with inner
alias preservation, and CHECK OPTION.
Protocol tests cover the v88 predecessor
rejection and v89 acceptance at prepare, sender, and receiver boundaries,
including a mixed-version destination probe and the all-CN capability fence.
Generated ViewData tests cover both function IDs, nested detection, the
authoring/read floor values 0, v88, and v89, and binding the exact generated
metadata at the immediate predecessor and current protocol.
System-view tests prove mixed, unknown, and RPC-failing CN capability probes
fall back to the predecessor VIEWS DDL, while an all-v89 inventory uses the
parser-derived DDL. Tenant initialization keeps the final account version but
records the predecessor VIEWS definition when capability discovery is
incomplete. The post-upgrade bounded reconciliation pass rediscovers that
durable definition marker, retries only after a positive all-CN v89 check, and
uses the same guarded transactional entry to publish the parser-derived
definition once it is safe. Upgrade tests prove both the v4.0.7 handler and
its VIEWS entry require v89. The predecessor-init test proves that the
restoration target has no function reference before an older CN is admitted;
the cancellation-after-staging test additionally proves that a cancelled
transaction preserves the legacy marker and page cursor, and that a later
generation can retry and publish only after a committed v89 gate.

## Unresolved questions

None. This RFC is proposed pending independent design approval; it documents
the delivery contract and does not self-approve the design.
