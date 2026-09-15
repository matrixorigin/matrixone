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
are new distributed plan functions (IDs 579 and 580), so the catalog contract is fenced by MORPC
v73.

## Problem and invariant

Schema-diff and migration clients replay `VIEW_DEFINITION`. A full CREATE
statement is not a standalone SELECT and falsely marks aggregate views as
updatable. The invariant is that every visible current or legacy view returns
its parser-derived frozen SELECT (or NULL only for a malformed catalog row),
and no CN that cannot resolve either function ID can receive a pipeline or catalog view
that references it.

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

MORPC v73 is allocated as `MORPCLatestVersion + 1` from official main v72,
which already owns v70 through v72. The two function IDs are the next available
IDs after main's exclusive function bound 579, and the bound advances to 581.
The capability is specific to these functions and the persisted VIEWS definition.
A sender probes the selected destination CN as well as its local runtime before
encoding a pipeline containing either function ID; an unknown or unavailable
destination capability fails closed. The v4.0.6 VIEWS upgrade waits for common
v73. New tenant initialization installs the new VIEWS DDL only after the local
coordinator and every CN in the current inventory have positively confirmed v73;
a mixed, unknown, RPC-failing, or incomplete capability probe aborts the account
transaction, so it cannot commit a final-version tenant with the predecessor
metadata. A v72-or-earlier cluster preserves all existing metadata definitions,
including the v58 COLUMNS contract. Pipeline preparation, remote marshal, and
remote unmarshal reject a
pipeline containing either function ID below v73. The receiver check protects
stale prepared work as well as normal sender dispatch. Before admitting any
v72-or-earlier CN during rollback, operators must pause related metadata plans,
restore `InformationSchemaViewsLegacyDDL`, wait for the catalog change and
in-flight work to converge, and only then admit the older binary. Merely draining
v72-dependent requests is not sufficient because the new persisted view text
references the functions. The new JSON fields are additive and old binaries keep
treating them as unknown.

## Alternatives

Keeping raw SQL regexp extraction was rejected because it repeatedly diverged
from the SQL lexer for comments and quoted strings. Eagerly rewriting every
legacy row was rejected because the existing recovery lifecycle is deliberately
inactive and a metadata read must not perform unbounded catalog writes. Allowing
the DDL before v73 was rejected because an old CN cannot bind the metadata functions.

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
commented inputs, malformed rows, frozen wildcard expansion, explicit
derived-table column lists with inner alias preservation, and CHECK OPTION.
Protocol tests cover the v72 predecessor
rejection and v73 acceptance at prepare, sender, and receiver boundaries,
including a mixed-version destination probe and the all-CN capability fence.
System-view tests prove mixed, unknown, and RPC-failing CN capability probes
abort before any tenant metadata is written, while an all-v73 inventory uses the
parser-derived DDL; upgrade tests prove the VIEWS entry requires v73. The
predecessor-init test is also the rollback guard: it proves that the restoration
target has no function reference before an older CN is admitted.

## Unresolved questions

None. This RFC is proposed pending independent design approval; it documents
the delivery contract and does not self-approve the design.
