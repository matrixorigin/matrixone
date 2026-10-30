# FLUSH PRIVILEGES compatibility no-op

Status: proposed; design review required before implementation delivery.

Issue: #30468 (FLUSH PRIVILEGES sub-item only). Implementation PR: pending.

## Problem and scope

Clients that issue `FLUSH PRIVILEGES` encounter a syntax error even when their
privilege changes were made through MatrixOne's own account-management SQL.
The compatibility target accepts this exact statement as a no-op for the active
`sys:moadmin` or non-system `accountadmin` role. Other active roles are denied.
The statement has no effect on any catalog, privilege or rewrite cache,
prepared handle, tenant, or other session. It preserves an open transaction.
`FLUSH` is non-reserved; unsupported variants remain errors. PREPARE (text and
binary) is unsupported. Other #30468 sub-items are out of scope.

## Contracts and alternatives

MySQL 8.0 requires RELOAD, reloads grant tables and performs additional cache and
account operations, and FLUSH causes implicit transaction commit. MatrixOne's
RELOAD is an AST privilege token without a corresponding grantable frontend
privilege. This compatibility subset is deliberately narrower: administrator
roles substitute for delegable RELOAD; it does not load MySQL grant tables or
commit. See https://dev.mysql.com/doc/refman/8.0/en/flush.html and
https://dev.mysql.com/doc/refman/8.0/en/implicit-commit.html.

Alternatives: leave the syntax unsupported (safe but breaks clients); implement
full RELOAD privileges and refresh semantics (more compatible but requires new
catalog/authorization and cache contracts); silently accept for all users
(unsafe). The selected admin-only no-op bounds the compatibility change without
creating new state or authorization grants. It does not promise that direct
writes to MySQL grant tables or account unlocks will take effect.

## Ownership and execution

The parser recognizes only the top-level exact statement. A dedicated AST routes
to the frontend status response. The existing privilege classifier identifies
the operation as special, and the object-type-none authorization handler checks
the session's active administrator role before execution. The frontend then
returns success without mutation. Rejection occurs before the no-op. Transaction
admission accepts the statement without adding it to the implicit-commit class.
Prepared statements are rejected before publication so EXECUTE cannot bypass
this authorization. There are no new resources, caches, persistent data,
background work, retries, cross-node messages or migrations. Failure or
cancellation does not change any state; no shutdown-upgrade migration is needed.

## Validation

Parser tests cover case, identifiers and unsupported variants. Authorization
tests cover system and tenant admins and ordinary/wrong-tenant roles. Public SQL
validation must verify OK and denial, BEGIN/ROLLBACK and autocommit=0 on both
success and rejection, cache on/off, GRANT/REVOKE visibility, preservation of
existing prepared statements, unsupported FLUSH variants and PREPARE in both
protocols. A service-backed case is required in addition to unit tests.
