# Iceberg Access Unregister Lifecycle

- Status: in-progress
- Start Date: 2026-08-27
- Authors: MatrixOne maintainers
- Implementation PR: #27715
- Issue for this RFC: #27668

## Summary

`iceberg_register_access` persists principal and residency-policy rows that are
intentionally treated as dependencies by `DROP ICEBERG CATALOG`. Before this
change, users had no supported SQL operation that could remove those rows, so a
catalog with no external table mappings could remain permanently undeletable.

This design adds `CALL iceberg_unregister_access(<catalog>[, <options>])`. It
removes the requested access scope in the same transaction as any required
principal cleanup. The existing `DROP ICEBERG CATALOG` dependency rule remains
unchanged: it neither guesses intent nor silently cascades through live table
mappings.

## Motivation and Contract

The lifecycle invariant is: every supported registration has an authorized,
idempotent, transactionally atomic supported exit path. A catalog can be
dropped after its table mappings and access metadata have been explicitly
removed; a normal drop with remaining dependencies is rejected without changing
catalog metadata.

The first owner of access metadata is the frontend built-in procedure. It owns
the transaction that reads the catalog row with `FOR UPDATE`, deletes the
selected residency-policy row(s), conditionally deletes the principal mapping,
and commits or rolls back as one unit. `handleDropIcebergCatalog` remains the
consumer that checks all catalog-owned dependencies before deleting the catalog.

## API and Authorization

```sql
CALL iceberg_unregister_access('catalog_name');
CALL iceberg_unregister_access('catalog_name', 'scope=account,account_id=9');
CALL iceberg_unregister_access('catalog_name', 'scope=cluster');
CALL iceberg_unregister_access('catalog_name', 'scope=all');
```

Only `account_id`, `scope`, and `scope_type` are accepted. `scope` and
`scope_type` must agree when both are provided. An account administrator may
remove only its own account scope. A system tenant administrator may remove the
cluster scope or all scopes; its omitted scope means `all`. Unsupported options,
cross-account access, and unsupported scopes fail before a transaction begins.

The result contains operation `unregister_access`, status `committed`, account
ID, catalog ID, and resolved scope. Repeating a permitted scope removal is safe:
the delete has no residual effect, and the response still represents a committed
cleanup transaction.

## State Transitions and Atomicity

| From | Operation | Guard / linearization point | To | Failure behavior |
| --- | --- | --- | --- | --- |
| registered | unregister scope | `BEGIN`; lock catalog row | scope policy removed | rollback on every query or delete error |
| policy removed | count remaining relevant policies | same transaction | principal retained or deleted | rollback on count/read error |
| no relevant policies | delete principal mapping | same transaction | no access metadata | rollback on delete/commit error |
| metadata removed and no mappings | normal drop | existing dependency check | catalog deleted | rejected drop leaves every row intact |

The policy count includes a cluster policy and the target account policy because
either continues to authorize the account. Principal deletion occurs only when
that count is zero. The transaction's successful commit is the linearization
point; no background workers, queues, retries, or asynchronous cleanup are
introduced.

## Compatibility, Security, and Operations

The change adds a new optional SQL procedure and does not alter catalog table
schemas, stored records, existing registration semantics, or ordinary drop
semantics. Older clients continue to receive the previous dependency error until
they issue the new procedure. Mixed-version routing is safe because the call is
parsed and executed by the selected CN; deployment must not route this new call
to a CN without the implementation.

Authorization is evaluated from the session tenant before a background executor
is acquired. The procedure never accepts caller-supplied principal or policy
fields, preventing it from becoming a general metadata mutation surface. SQL is
formed from validated numeric IDs and fixed scope predicates.

The work is cold-path catalog maintenance: residency-policy cleanup uses separate
cluster and account probes rather than an `OR` scan. Cluster registrations
normalize `account_id` to zero, so both probes use the existing residency-policy
primary-key prefix `(scope_type, account_id, catalog_id)` without requiring an
unversioned secondary index for already-installed tenants. It adds no per-query
background work, cache, or retained state. Existing frontend errors and the E2E
report provide operational diagnosis; rollback preserves the original metadata
for inspection.

## Alternatives

1. Add implicit cascade behavior to ordinary `DROP ICEBERG CATALOG`. Rejected:
   it would make a familiar destructive DDL silently remove access state and
   could conflict with live mappings or operator expectations.
2. Permit direct deletion from `mo_catalog` access tables. Rejected: this leaks
   internal schema details and bypasses scope authorization and transactional
   cleanup.
3. Require manual system-table repair. Rejected: it is unavailable through the
   supported privilege model and does not offer a stable user contract.

## Verification and Acceptance

Focused frontend unit tests prove authorization, scope predicate selection,
commit/rollback behavior, principal retention while another scope remains, and
invalid option/scope rejection. The local Iceberg E2E lifecycle case proves the
public sequence: create catalog, register access, create a mapping, verify
ordinary drop is atomically rejected, remove mapping, unregister access, drop
catalog, and verify zero catalog-owned rows. Its SQL-mock tests additionally
prove cleanup after registration failure, invalid catalog identity, and a
non-atomic blocked-drop observation.

Acceptance requires the focused frontend and local Iceberg tests to pass and
the changed-code coverage gate to exceed 75%.

## Unresolved Questions

None for the initial single-CN lifecycle contract. Future work may define an
explicit administrative cascade DDL only if it has a separately reviewed safety
and multi-CN compatibility design.
