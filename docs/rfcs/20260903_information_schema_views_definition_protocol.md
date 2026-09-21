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
v92.

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
planner writes `required_protocol_version: 92` into `ViewData`, including when
the call is nested under another expression. The real view bind/Prepare path
reapplies that marker. It uses the existing two-floor admission lifecycle:
HAKeeper publishes the durable read floor during the v92 decoder barrier, while
the authoring floor remains closed until the all-CN admission and catalog fence
complete. Therefore an authoring or read floor of 0 or v91 fails closed, an
all-v92 fenced CN can create the VIEWS definition, and an ordinary view without
either function remains unmarked.

The fallback cannot infer metadata that is absent from a legacy row. In
particular, it does not promise to reconstruct the historical expansion of a
wildcard; the authoritative freeze begins when the additive fields are written
by CREATE/ALTER or by the bounded regeneration path. Adding a historical column
snapshot to the legacy catalog format would be a separate compatibility
migration and is outside this PR.

The post-upgrade reconciliation uses the exact current
`InformationSchemaViewsDDL` as its idempotence marker. After the common v92 gate
is available, a bounded page may repair any tenant whose
`information_schema.VIEWS` is missing or whose definition is not exactly that
current DDL. This includes the v92 predecessor definition and older
metadata-view definitions; it does not modify user views, user data, or any
information-schema object other than `VIEWS`. The broader convergence rule is
intentional: a tenant can have been created while capability discovery was
incomplete, can have stopped between a drop and create, or can be carrying an
older view definition from a prior upgrade. Treating only one literal
predecessor as repairable would leave those states permanently stale.

The reconciliation transaction owns one account page (32 accounts by default)
and publishes its process-local account cursor only after commit. A replacement
failure, cancellation, or mid-page protocol loss rolls back the complete page
and leaves the durable VIEWS definition and cursor as the retry marker. If an
account disappears after page enumeration, its account-local objects are
already gone; the transaction intentionally skips that account and may commit
the remaining repairs and cursor. This is the only account-disappearance
exception to page-wide rollback. The periodic upgrade owner retries the same
page on its next 10-second maintenance tick; a successful empty-page scan wraps
the cursor so late-created accounts remain discoverable. The final-version pass
intentionally invokes VIEWS reconciliation before orphan-privilege maintenance
and returns a visible error when reconciliation fails. Consequently,
orphan-privilege maintenance is skipped on every failed VIEWS pass, including
repeated failures; once VIEWS succeeds, that same pass proceeds to orphan
cleanup. This serial ordering keeps a pass that failed to establish the v92
public catalog contract from being reported as fully successful, while the
bounded VIEWS page remains independently rollback-safe.

MORPC v92 is allocated as `MORPCLatestVersion + 1` from official main v91 at
`a873bcf555aad2bf5eab0d1754f935454ba3043e`, which already owns v70 through v91;
v91 is the canonical CHAR and JSON HLL_ADD_AGG capability. The two function IDs
are the next available IDs after main's exclusive function bound 581, and the
bound advances to 583.
The capability is specific to these functions and the persisted VIEWS definition.
A sender probes the selected destination CN as well as its local runtime before
encoding a pipeline containing either function ID; an unknown or unavailable
destination capability fails closed. The v4.0.6 VIEWS upgrade waits for common
v92. New tenant initialization installs the new VIEWS DDL only after the local
coordinator and every CN in the current inventory have positively confirmed v92;
a mixed, unknown, RPC-failing, or incomplete capability probe records the
predecessor VIEWS definition while still committing the final-version tenant
row. A bounded post-upgrade reconciliation pass later rechecks common v92 and
reuses the guarded transactional entry to converge missing or stale VIEWS
definitions to the current contract. Any cluster with a CN below v92, including the immediate predecessor
v91, preserves all existing metadata definitions, including the v58 COLUMNS
contract. Pipeline preparation, remote marshal, and
remote unmarshal reject a
pipeline containing either function ID below v92. The receiver check protects
stale prepared work as well as normal sender dispatch. Before the v92 HAKeeper
protocol-floor activation is durably committed, a cancelled rollout may stop
the v92 upgrade, keep or restore `InformationSchemaViewsLegacyDDL`
transactionally, wait for catalog and in-flight work to converge, and admit
v91 only after verifying that no v92 maintenance worker can re-install the
new definition. After the v92 floor is committed, the floor is monotonic and
v91 binaries must not be admitted: restoring the view text alone cannot undo
the decoder barrier. The supported recovery is forward recovery with v92-
compatible log/CN services, or a coordinated restoration of cluster state from
before the activation barrier; an ordinary in-place downgrade is unsupported.
Merely draining below-v92 requests is not sufficient because the new persisted
view text references the functions. The new JSON fields are additive and old
binaries keep treating them as unknown.

## Alternatives

Keeping raw SQL regexp extraction was rejected because it repeatedly diverged
from the SQL lexer for comments and quoted strings. Eagerly rewriting every
legacy row was rejected because the existing recovery lifecycle is deliberately
inactive and a metadata read must not perform unbounded catalog writes. Allowing
the DDL before v92 was rejected because an old CN cannot bind the metadata functions.

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
Protocol tests cover the v91 predecessor
rejection and v92 acceptance at prepare, sender, and receiver boundaries,
including a mixed-version destination probe and the all-CN capability fence.
Generated ViewData tests cover both function IDs, nested detection, the
authoring/read floor values 0, v91, and v92, and binding the exact generated
metadata at the immediate predecessor and current protocol.
System-view tests prove mixed, unknown, and RPC-failing CN capability probes
fall back to the predecessor VIEWS DDL, while an all-v92 inventory uses the
parser-derived DDL. Tenant initialization keeps the final account version but
records the predecessor VIEWS definition when capability discovery is
incomplete. The post-upgrade bounded reconciliation pass rediscovers that
durable definition marker, retries only after a positive all-CN v92 check, and
uses the same guarded transactional entry to publish the parser-derived
definition once it is safe. Upgrade tests prove both the v4.0.7 handler and
its VIEWS entry require v92. The predecessor-init test proves that the
restoration target has no function reference before an older CN is admitted;
the cancellation-after-staging test additionally proves that a cancelled
transaction preserves the legacy marker and page cursor, and that a later
generation can retry and publish only after a committed v92 gate.

Maintenance acceptance additionally covers the broader convergence rule: a
missing VIEWS object and a stale/non-current VIEWS definition are both repair
candidates, while an exact current definition is an idempotent no-op. The
retry, mid-page protocol-loss, cancellation, commit-failure-after-restart, and
late-account tests prove that a failed page publishes neither a partial
replacement nor its cursor; the persisted predecessor definition remains the
recovery marker. Account disappearance is deliberately skipped so that other
accounts in the bounded page can still commit, while replacement and
transaction failures retain page-wide rollback. The service-level ordering is
intentionally visible in logs and return status: a failed VIEWS page is retried
by the periodic owner before the same pass is considered complete, and
orphan-privilege cleanup is skipped on every failed VIEWS pass rather than used
to hide that failure. Once VIEWS succeeds, the same pass proceeds to orphan
cleanup.

## Unresolved questions

None. This RFC is proposed pending independent design approval; it documents
the delivery contract and does not self-approve the design.
