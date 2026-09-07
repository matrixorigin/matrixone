- Status: draft
- Start Date: 2026-09-03
- Authors: MatrixOne maintainers
- Implementation PR: https://github.com/matrixorigin/matrixone/pull/27716
- Issue for this RFC: https://github.com/matrixorigin/matrixone/issues/27655

# Parser-derived `information_schema.VIEWS` definitions

## Summary

`information_schema.VIEWS.VIEW_DEFINITION` must expose the defining SELECT,
not the original CREATE statement. New views persist a parser-derived definition
and legacy rows are read through `mo_view_definition`. The function is a new
distributed plan function (ID 578), so the catalog contract is fenced by MORPC
v44.

## Problem and invariant

Schema-diff and migration clients replay `VIEW_DEFINITION`. A full CREATE
statement is not a standalone SELECT and falsely marks aggregate views as
updatable. The invariant is that every visible current or legacy view returns
its parser-derived frozen SELECT (or NULL only for a malformed catalog row),
and no CN that cannot resolve ID 578 can receive a pipeline or catalog view
that references it.

## Design

The CREATE/ALTER owner derives `ViewData.Definition` from the stabilized view
AST, after wildcard expansion and excluding CHECK OPTION. The catalog remains
the single owner of that frozen text. `mo_view_definition(viewdef)` returns the
stored field without writes; for an older row that lacks it, it parses only the
stored statement using its persisted SQL mode and identifier-case settings.
This bounded, side-effect-free fallback avoids a second SQL regexp lexer and
does not depend on background recovery.

MORPC v44 is allocated from official main v43 specifically for the function and
the persisted VIEWS definition. The v4.0.6 VIEWS upgrade waits for common v44.
New tenant initialization at v43 or below installs the predecessor VIEWS DDL,
which has no function reference; v44 installs the new DDL. Pipeline preparation,
remote marshal, and remote unmarshal reject a pipeline containing function ID
578 below v44. The receiver check protects stale prepared work as well as normal
sender dispatch. Rolling back is safe after v44-dependent requests drain: the
new JSON fields are additive and old binaries keep treating them as unknown.

## Alternatives

Keeping raw SQL regexp extraction was rejected because it repeatedly diverged
from the SQL lexer for comments and quoted strings. Eagerly rewriting every
legacy row was rejected because the existing recovery lifecycle is deliberately
inactive and a metadata read must not perform unbounded catalog writes. Allowing
the DDL before v44 was rejected because an old CN cannot bind function ID 578.

## Bounds, security, and operations

The compatibility parse is per visible legacy row and is linear in that row's
stored statement; current rows return their stored definition directly. It
creates no durable work, goroutine, queue, retry, or cache. Existing visibility
joins remain the authorization boundary, so parsing happens only after the view
row is selected. A mixed-version request fails before dispatch with a stable
NotSupported error rather than returning wrong metadata.

## Validation

Focused parser/function tests cover current and legacy definitions, quoted and
commented inputs, malformed rows, frozen wildcard expansion, and CHECK OPTION.
Protocol tests cover the v43 predecessor rejection and v44 acceptance at
prepare, sender, and receiver boundaries. System-view tests prove v43 tenant
initialization uses the predecessor DDL and v44 uses the parser-derived DDL;
upgrade tests prove the VIEWS entry requires v44.

## Unresolved questions

None. This RFC is draft pending independent design approval; it documents the
delivery contract and does not self-approve the design.
