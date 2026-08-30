# Information Schema Metadata Visibility and Active-Role Closure

- Status: Proposed — awaiting design approval
- Owning issue: [#27656](https://github.com/matrixorigin/matrixone/issues/27656)
- Implementation PR: [#27695](https://github.com/matrixorigin/matrixone/pull/27695)
- Version: 1
- Last updated: 2026-08-30

## 1. Problem and evidence

A non-admin tenant user can query `information_schema` views and discover database, table, column, index, constraint, partition, and view metadata for objects that the user's active authorization context cannot access. This differs from the authorization boundary enforced by normal object access and commands such as `SHOW DATABASES`.

The first owner of the authorization decision is the tenant catalog together with the session's active role. An `information_schema` view is a consumer of that decision; it must not invent a weaker visibility rule.

## 2. Scope and design triggers

This design covers metadata visibility for `SCHEMATA`, `TABLES`, `COLUMNS`, `STATISTICS`, `TABLE_CONSTRAINTS`, `CHECK_CONSTRAINTS`, `KEY_COLUMN_USAGE`, `REFERENTIAL_CONSTRAINTS`, `VIEWS`, and `PARTITIONS`.

A design review is mandatory because the implementation crosses frontend bootstrap, tenant upgrades, planner, table-function execution, catalog access, and sysview ownership boundaries. It changes an authorization boundary, rolling-upgrade behavior, statement-local materialization, and a metadata-query hot path.

Non-goals:

- changing the privilege grant/revoke model;
- changing role activation semantics;
- changing partition-table, charset/collation, UDF, stored-procedure, or warning compatibility;
- exposing `mo_current_roles()` as a new user-facing authorization mechanism outside this metadata implementation;
- making metadata visibility imply permission to read object data.

## 3. Authorization invariant and trust boundary

For tenant account `A`, active role `R`, and metadata object `O`:

> A protected `information_schema` row is visible if and only if `O` is an intended system object, or the normal authorization context rooted at `R` has an ownership or metadata privilege path that makes `O` visible.

The role set is the cycle-safe transitive closure of roles granted to the stable active role ID. Role names are not identities and renaming a role must not affect visibility. The closure is evaluated at statement execution time, so `SET ROLE`, prepared-statement reuse, and ordinary plan-cache reuse cannot retain a previous session role.

A database is visible when one of these conditions holds:

1. it is an intended system schema;
2. an active/inherited role owns it;
3. an active/inherited role has applicable account- or database-level metadata privilege;
4. it contains an object visible under the table/object rule.

A table-like object is visible when one of these conditions holds:

1. it is in an intended system schema;
2. an active/inherited role owns it;
3. its database is owned by an active/inherited role;
4. an applicable account-, database-, table-, or view-level privilege grants metadata visibility.

Constraint, index, column, partition, and view rows derive visibility by joining to the visible table set. They may not independently enumerate hidden objects.

The trust boundary is tenant-local: catalog queries execute under the current account and must not expose another tenant's objects. The explicitly retained system-catalog rows are the only cross-account exception already required by system-schema behavior.

## 4. Role-closure mechanism

### 4.1 Interface

`mo_current_roles()` is a zero-argument table function returning one fixed-width `INT64 role_id` column. It emits the active role and every transitively inherited role exactly once.

The function reads the active role from the execution-time process/session context. Planning must not replace it with a role name or a role ID captured when a prepared/cached plan was built.

### 4.2 Traversal

The implementation performs breadth-first frontier expansion:

1. initialize `visited` and the frontier with the active role ID;
2. query `mo_catalog.mo_role_grant` for grants whose `grantee_id` is in the current frontier;
3. add unseen `granted_id` values to `visited` and the next frontier;
4. repeat until the frontier is empty.

Frontier SQL is split into batches of at most 256 role IDs. The `grantee_id` catalog index bounds each query to the relevant graph instead of scanning every tenant grant. `visited` makes cycles finite and suppresses duplicates.

Resource complexity is proportional to the reachable closure and reachable edges, not the tenant's disconnected role graph. Memory ownership is statement-local and released with the table-function operator. There is no background goroutine, global cache, retry loop, or cross-statement mutable state.

### 4.3 Errors and cancellation

Every internal executor result is closed by the function. Internal SQL errors and cancellation propagate to the caller; partial closure results are not published as a successful authorization set. Cancellation terminates further frontier expansion. Empty or malformed internal results fail rather than widening visibility.

No retry is performed because replaying nested catalog work inside the same statement cannot repair an authorization or transaction error and would increase resource use. A failed metadata query may be retried by the normal statement owner.

## 5. Statement-local sharing and materialization bound

The canonical visibility CTE references the active-role set multiple times. Inlining every reference would execute the nested closure SQL three times for most protected views and six times for `SCHEMATA`.

The planner therefore permits statement-local CTE sharing only for the exact bounded shape:

- one childless, zero-argument `mo_current_roles()` function scan;
- optionally wrapped only by cardinality-preserving projections;
- exactly one fixed-width `INT64` output;
- no join, additional scan, filter, aggregate, limit/offset, or variable-width output.

That exact producer is evaluated once and consumed through the normal query-scoped sink/source ownership path. Its maximum row count is the reachable role count and its row width is fixed.

A CTE that merely contains `mo_current_roles()` does not qualify. In particular, joins with user/catalog tables and early-stop `LIMIT`, SEMI, or ANTI consumers retain the normal full-drain, profitability, 32 MiB estimate, and spill gates. This prevents a small authorization optimization from forcing eager materialization of an unrelated large subtree.

Sink/source cleanup, cancellation wakeups, memory admission, and spill lifecycle remain owned by the existing CTE materialization machinery. This change adds no new lifecycle state.

## 6. Bootstrap and rolling-upgrade contract

`mo_current_roles()` and canonical protected views are a cumulative CN capability identified by `MORPCVersion41` in the implementation revision associated with this design. Version 41 follows capabilities v38-v40 already present on `main`; a service advertising v41 therefore includes those earlier contracts and this role-closure capability.

The capability number is not an independently negotiable feature bit. Before merge, the implementation must merge latest `mo/main` and verify that v41 remains the next unique cumulative version. If another capability lands first, this document and every producer/consumer gate must be revised together to the next version.

Gated consumers are:

- planner admission for `mo_current_roles()`;
- tenant bootstrap selection of canonical versus compatibility view definitions;
- same-version tenant upgrade entries for the role-grant index and protected views;
- protocol tests and user-visible compatibility errors.

Canonical views may be installed only when every participating CN reports at least the required version. Pre-capability bootstrap uses compatibility definitions that do not reference the unavailable function and fail closed relative to the new visibility boundary.

### Upgrade

1. deploy binaries that understand the capability while the common protocol remains below it;
2. retain compatibility definitions during the mixed-version phase;
3. once every CN advertises the capability, create the role-grant index and install canonical views;
4. new plans may then use `mo_current_roles()`.

### Downgrade and rollback

Do not lower the common protocol while canonical views referencing `mo_current_roles()` remain installed. Operational rollback must first restore compatibility definitions (or complete tenant rollback using the normal upgrade framework), then remove v41-only participants. Catalog data and role grants themselves are unchanged, so no role data migration or destructive rollback is required.

A restarted old CN cannot safely join a cluster whose common protocol and canonical views require v41; normal protocol admission must reject or hold that mixed state rather than treating the old CN as capable.

Backup/restore carries ordinary catalog/view definitions. Restoring into a pre-v41 binary set requires compatibility definitions to be selected before tenant queries are admitted.

## 7. Security and denial-of-service analysis

The design is fail-closed: protocol uncertainty, internal executor failure, malformed closure output, or cancellation fails the query rather than treating every role/object as visible.

The closure cannot cross tenant boundaries because catalog access uses the current account context. Stable numeric role identity prevents rename-based visibility drift. Cycle detection prevents malicious or accidental cyclic grants from hanging traversal.

Disconnected catalog grants do not contribute to query work. Reachable closure size is still proportional to legally reachable roles; batching limits SQL statement size but does not impose a semantic role-depth limit. Existing tenant role/grant governance is the capacity control for a deliberately enormous reachable closure. The operator owns all temporary memory, and cancellation remains available throughout frontier expansion.

## 8. Alternatives

### A. Embed recursive SQL in every view

Rejected. Recursive CTE execution in the affected pipeline previously exposed hang/lifecycle risk, repeats a complex expression across views, and makes execution-time role and plan-cache behavior harder to control.

### B. Scan all role grants and compute closure in Go

Rejected. It costs `O(all tenant grants)` CPU and memory even when the active role has no inherited grants. Concurrent metadata discovery can amplify disconnected catalog size into latency and OOM.

### C. Maintain a global/session role-closure cache

Rejected for this change. Correct invalidation must cover grants, revokes, active-role changes, transaction visibility, tenant isolation, and restart. It introduces long-lived state and stale-authorization risk. The indexed frontier design keeps ownership statement-local and scales with relevant data.

### D. Duplicate the frontier closure at every CTE reference

Correct but rejected for performance. Typical views would execute three to six complete nested closures per statement.

### E. Materialize every CTE containing `mo_current_roles()`

Rejected. An arbitrary surrounding join/scan can be unbounded and may have early-stop consumers. Only the exact fixed-width closure shape is admitted specially.

## 9. Validation map and acceptance criteria

| Contract | Evidence |
|---|---|
| Active plus complete inherited closure, cycles, duplicates | table-function unit tests |
| Work proportional to reachable graph, disconnected 100k-edge case | focused benchmark/stress test |
| Internal results close on success/error; cancellation propagates | table-function executor tests and ownership review |
| Runtime active role under prepared and ordinary cache reuse | public SQL BVT and planner/compile tests |
| One closure producer per protected metadata query | reachable plan-shape tests for all protected views |
| Amplifying JOIN plus LIMIT/SEMI remains inlined | negative real-planner tests |
| No privilege/direct/inherited/database ownership/admin boundaries | public SQL BVT matrix |
| Sibling views and `SCHEMATA` cannot bypass visibility | public SQL BVT matrix and sysview definition tests |
| Pre-v41 definitions do not reference the function | bootstrap/sysview compatibility tests |
| Canonical upgrades wait for common v41 and are idempotent | v4.0.6 upgrade tests |
| Unique cumulative allocation after latest main | merge-time `MORPCVersion` audit and exact-head CI |

Acceptance requires all focused owning-package tests, affected BVT, race tests on role/protocol/plan-cache paths, `go vet` for affected packages, and `git diff --check` to pass. Structural one-scan tests prove multiplicity; a real metadata-query latency/allocation benchmark remains desirable performance coverage but is not an authorization-correctness oracle.

## 10. Risks, rollout, and observability

Primary risks are privilege overexposure, hidden authorized objects, stale active-role capture, protocol misallocation, repeated nested SQL, and unbounded closure work. The gates and validation above address each risk independently.

No new metric or background health signal is introduced. Failures surface as normal query/bootstrap errors with the required protocol version. Operators can diagnose rollout state through existing common-protocol reporting and tenant-upgrade logs.

Rollout is contained by the common-protocol gate: compatibility definitions remain active until all CNs are capable. If scale evidence regresses, rollback restores compatibility definitions before withdrawing capable binaries.

## 11. Decision log

- Numeric active role ID is the authorization root; role names are display metadata.
- Complete transitive inheritance is required; limiting traversal depth is not acceptable.
- Indexed frontier expansion is preferred over global scans or a mutable cache.
- Closure evaluation is shared once per statement only for the exact bounded function/projection shape.
- Canonical view installation and planner admission use the same cumulative protocol capability.
- System schemas remain intentionally visible; tenant-private metadata follows ownership and privilege paths.

## 12. Open decisions

No known blocking design question remains. The protocol number must be revalidated immediately before merge because concurrent PRs allocate from the same cumulative sequence.

## 13. Design review record

To be completed by an authorized reviewer:

```text
Change scope: information_schema metadata authorization and active-role closure
Trigger: authorization boundary; protocol/upgrade contract; cross-package lifecycle and hot-path change
Design: docs/design/CLAUDE_INFORMATION_SCHEMA_METADATA_VISIBILITY.md, version 1, <reviewed commit>
Blocking findings: <none or findings>
Decision log: <accepted tradeoffs and resolved questions>
Decision: PASS | REQUEST_CHANGES
Implementation deviations: <none or affected sections>
```
