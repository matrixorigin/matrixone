# FLUSH PRIVILEGES compatibility no-op

Status: design review passed on 2026-11-12 for further implementation review; see the review record below.

Issue: #30468 (FLUSH PRIVILEGES sub-item only). Implementation PR: #31834.

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
background work, retries, cross-node messages or migrations. The no-op adds no state mutation on failure or cancellation. Existing error
handling, including `mo_rollback_txn_on_error`, still governs transaction
rollback; no shutdown-upgrade migration is needed.

## Validation

Parser tests cover case, identifiers and unsupported variants. Authorization
tests cover system and tenant admins and ordinary/wrong-tenant roles. Public SQL
validation must verify OK and denial, BEGIN/ROLLBACK and autocommit=0 on both
success and rejection, cache on/off, GRANT/REVOKE visibility, preservation of
existing prepared statements, unsupported FLUSH variants and PREPARE in both
protocols. A service-backed case is required in addition to unit tests.

## Design review record

- Scope: the complete `FLUSH PRIVILEGES` compatibility sub-item in #30468.
- Trigger: new user-visible SQL behavior crossing parser, authorization,
  transaction, and prepared-statement paths.
- Reviewed proposal: commit `c0de195f10d64cd036e99d9053ca1c1d676b9579`, this
  file's Git blob `86b9b362e58a91ffaef3586744e73eb4755022b9`.
- Blocking design findings: none. The issue-requested no-op is bounded to the
  active system `moadmin` or tenant `accountadmin` role; it preserves the
  existing transaction/error policy and introduces no mutable state. The
  alternatives of unrestricted acceptance and full MySQL `RELOAD` semantics
  would respectively weaken authorization or require new privilege/cache
  contracts.
- Decision: PASS for further implementation review.
- Reviewer: Codex maintenance review, 2026-11-12; this is not a GitHub human
  approval. The implementation already existed when this review was recorded;
  this record does not claim that design approval preceded the initial
  implementation. It gates subsequent implementation review and delivery.
- Implementation follow-up: exercise the actual binary `COM_STMT_PREPARE`
  request path and validate the rebased exact head before implementation
  acceptance.
