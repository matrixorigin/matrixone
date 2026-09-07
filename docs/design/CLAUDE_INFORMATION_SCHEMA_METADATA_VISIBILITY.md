# Information Schema Metadata Visibility, Active-Role Closure, and Subscription Metadata

- Status: Approved for implementation
- Owning issues: [#27656](https://github.com/matrixorigin/matrixone/issues/27656), [#27794](https://github.com/matrixorigin/matrixone/issues/27794)
- Implementation PRs: [#27695](https://github.com/matrixorigin/matrixone/pull/27695), issue #27794 follow-up TBD
- Version: 3
- Last updated: 2026-09-01

## 1. Problem and evidence

A non-admin tenant user can query `information_schema` views and discover database, table, column, index, constraint, partition, and view metadata for objects that the user's active authorization context cannot access. This differs from the authorization boundary enforced by normal object access and commands such as `SHOW DATABASES`.

The first owner of the authorization decision is the tenant catalog together with the session's active role. An `information_schema` view is a consumer of that decision; it must not invent a weaker visibility rule.

A second defect affects subscription databases. A subscription table is resolved and read through the subscriber's local database alias, but its physical `mo_tables` and `mo_columns` rows belong to the publisher account and source database. `SHOW TABLES` and direct reads use `SubscriptionMeta` to cross that catalog boundary. The current `TABLES` and `COLUMNS` views read only the subscriber's physical catalog rows, so a valid, readable subscription table is omitted. On `mo/main` commit `33af6ae3c0c23e4f277137460577ea64678f7ecf`, the issue #27794 reproducer succeeded for `SHOW TABLES` and direct reads in three consecutive runs while both views returned zero matching rows.

## 2. Scope and design triggers

This design covers metadata visibility for `SCHEMATA`, `TABLES`, `COLUMNS`, `STATISTICS`, `TABLE_CONSTRAINTS`, `CHECK_CONSTRAINTS`, `KEY_COLUMN_USAGE`, `REFERENTIAL_CONSTRAINTS`, `VIEWS`, and `PARTITIONS`. Version 3 extends only `TABLES` and `COLUMNS` with subscription metadata; the other protected views retain their version 2 local-catalog behavior.

A design review is mandatory because the implementation crosses frontend bootstrap, tenant upgrades, planner, table-function execution, catalog access, and sysview ownership boundaries. It changes an authorization and tenant-isolation boundary, rolling-upgrade behavior, statement-local materialization, goroutine/channel lifecycle, and a metadata-query hot path.

Non-goals:

- changing the privilege grant/revoke model;
- changing role activation semantics;
- changing partition-table, charset/collation, UDF, stored-procedure, or warning compatibility;
- adding subscription rows to `STATISTICS`, constraint, key, partition, or view-definition views;
- persisting mirrored publisher table or column rows in the subscriber catalog;
- exposing any cross-account subscription metadata primitive as a general user-facing API;
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

The trust boundary is tenant-local: ordinary catalog queries execute under the current account and must not expose another tenant's objects. The explicitly retained system-catalog rows are the only cross-account exception already required by system-schema behavior.

For a subscription alias `S` in subscriber account `A` and a publisher object `P`, a subscription metadata row is visible if and only if all of the following hold at the statement snapshot:

1. `S` is a local subscription database owned by `A`;
2. the corresponding `mo_subs` row is normal, the publisher account is not suspended, the current `mo_pubs` row still exists, and its current account list still permits `A`;
3. `P` is in the current publication table scope;
4. after rewriting the row to `A` plus the local alias/database identity, the existing active-role ownership or privilege predicate makes it visible.

Publisher role IDs are never interpreted as subscriber owners. The bridge rewrites owner and database identity from the local alias and retains publisher table identity only where existing table-level privilege and catalog joins require it. Publication state may narrow or remove visibility during a later statement; it is never captured in a prepared plan or a process-global cache.

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

The closure has an explicit fail-closed admission limit of 4,096 distinct roles, including the active role. The limit is checked before a newly discovered role is published to `visited` or a frontier. A statement that would admit role 4,097 returns an error and emits no partial authorization batch. This caps retained Go workspace for `visited`, frontier/next slices, and final role slices. Admission conservatively budgets 128 bytes per role, so closure workspace is bounded to 512 KiB per metadata statement before the output vector. The fixed-width output vector adds at most 32 KiB (`4,096 * 8` bytes) and is charged through the query's existing process mpool. The conservative 128-byte workspace estimate covers an `int64` map key, map bucket/load overhead, and simultaneous frontier/final slice storage without relying on runtime-specific minimum object sizes.

The role count and byte budget describe one query-owned closure generation. Concurrent metadata queries each have their own generation and cannot share or retain another statement's workspace; aggregate closure workspace is therefore bounded by 512 KiB times already-admitted query concurrency, rather than tenant graph size times concurrency. This design does not add a second global concurrency controller.

### 4.3 Errors and cancellation

Every internal executor result is closed by the function. Internal SQL errors and cancellation propagate to the caller; partial closure results are not published as a successful authorization set. Cancellation is checked before each frontier query and while admitting returned roles, and terminates further expansion. Empty or malformed internal results fail rather than widening visibility. Capacity rejection follows the same terminal path: the current internal result is closed, temporary workspace becomes unreachable on return, and the table-function batch remains empty.

No retry is performed because replaying nested catalog work inside the same statement cannot repair an authorization or transaction error and would increase resource use. A failed metadata query may be retried by the normal statement owner.

## 5. Subscription metadata bridge

### 5.1 Interface and private admission

Two zero-argument table functions provide the minimum raw rows needed by the affected views:

- `mo_subscription_tables()` returns the `mo_tables` subset used by `TABLES` and visible-table authorization;
- `mo_subscription_columns()` returns the `mo_columns` subset used by `COLUMNS`, the publisher-computed key priority, and the minimum colocated table fields required to apply the subscriber's table-visibility and temporary-object predicates without starting a second cross-account function.

Neither function accepts an account, database, publication, or table argument. Planner binding admits them only when the bind context's trusted view lineage is the canonical system `information_schema.TABLES` or `information_schema.COLUMNS` consumer that needs the function. Binding the persisted definition itself is admitted only when the planner-recorded CREATE/ALTER/regeneration target is that exact owning system view; this target is supplied structurally by the DDL path rather than inferred from SQL text. A direct SQL call or a call embedded in a user-created view is rejected. Cloning `information_schema` restores cloned `TABLES` and `COLUMNS` from their v41 local-only definitions before remapping the destination schema, so a user-owned clone remains useful without becoming a second cross-account trust boundary. This is an authorization control, not an optimization: raw cross-account rows must not be available without the canonical system view's local active-role filter.

The planner and executor use fixed schemas and a cumulative protocol capability. The execution identity override is constructed only from trusted catalog rows by the operator; user SQL cannot supply or alter it.

### 5.2 Discovery and identity mapping

Candidate subscriptions are discovered under the system-account catalog identity by explicitly restricting `mo_database.account_id` to the execution-time subscriber account and joining current rows from `mo_database`, `mo_subs`, `mo_account`, and `mo_pubs`. A candidate is retained only when:

- the local database type is `subscription` and its alias matches the subscribed name;
- `mo_subs.status` is normal;
- the publisher account exists and is not suspended;
- the current publication exists for the same publisher account/name;
- the current publication account list is `all` or contains the exact subscriber account name.

The current `mo_pubs.database_name` and `table_list` are authoritative; `table_list = '*'` means all tables. The historical `all_table` flag is not used for admission because existing `ALTER PUBLICATION ... TABLE ...` paths can leave it stale while updating `table_list`, which is also the scope consumed by subscription resolution and `SHOW PUBLICATIONS`. Copied publication fields in an older `mo_subs` row do not widen visibility. The source catalog query runs under the discovered publisher account, filters `account_id = current_account_id()`, filters the current source database and exact current publication table scope, and quotes every catalog-derived SQL literal with `sqlquote`.

Returned rows are rewritten to the subscriber account, local subscription alias, local database ID, and local database owner. Source schema names and publisher owners are not exposed as subscriber identity. Source table physical/logical IDs and source column relation IDs remain available only for the existing catalog association and table-level privilege semantics. Two local aliases that expose the same publisher objects through distinct publications therefore produce distinct logical metadata namespaces without duplicating persistent catalog state. MatrixOne separately enforces that one subscriber account cannot subscribe to the same publication twice.

### 5.3 View composition and authorization

`TABLES` keeps the existing local `__mo_visible_tables` CTE and local final projection. A separate subscription projection reads `mo_subscription_tables()` exactly once and applies the same active-role ownership/grant and object filters directly; it does not place the subscription function inside a CTE referenced by both final branches. Only the local projection evaluates `internal_auto_increment` against a local physical relation, while the subscription projection returns zero as an explicit unsigned bigint, matching the existing subscription `SHOW TABLE STATUS` behavior and preserving the public `AUTO_INCREMENT` column type. This split prevents both repeated producer binding and rewritten subscriber-alias physical lookup. `COLUMNS` likewise retains the existing local visible-table CTE and local `mo_columns` branch, then combines it with a single `mo_subscription_columns()` branch using `UNION ALL`. The subscription function colocates each column with rewritten table authorization fields, so that branch applies the same active-role ownership/grant predicate and temporary-object predicate directly. The local branch retains its existing key-index join, while the subscription branch consumes the publisher-computed key priority. Hidden/internal/temporary object filters, type decoding, generated/default expressions, and `COLUMN_KEY` mapping remain equivalent across both branches.

The subscription-aware CTEs are used only by `TABLES` and `COLUMNS`. The shared version 2 CTEs for other protected views remain local-only, preventing unrelated scans and accidental name-based joins to publisher metadata.

### 5.4 Snapshot, lifecycle, and resource bounds

All discovery and publisher scans use the caller's transaction and statement cancellation context. There is no retry, global/session cache, background worker, or state surviving the table-function generation.

Each affected view reaches only one subscription function and therefore owns at most one producer goroutine. This also guarantees that one statement transaction does not run concurrent cross-account internal SQL streams. Candidate subscriptions are read with `dat_id` keyset pagination in pages of at most 64, so retained discovery state is bounded independently of tenant subscription count. Publisher results use a channel of at most eight executor result envelopes and are copied into query-mpool-owned output batches of at most 8,192 rows. Source executor batch size remains governed by the existing internal SQL executor; the bridge does not retain results after their rows are copied.

The producer executes one internal statement at a time: fetch one bounded candidate page, fully close it, then stream each publisher catalog query before advancing. It never pauses one transaction cursor while starting another. Work is `O(number of subscription pages + valid subscriptions + emitted source metadata rows)`. There is no work proportional to unrelated publisher accounts or unrelated databases; a table-list publication narrows source queries to that exact list.

The table-function state owns the cancel function, current executor result, result/error channels, producer completion channel, output batch, pagination generation, and limit accounting. Success closes each result exactly once. Error, malformed result, cancellation, `LIMIT`, reset, and free first cancel production, close any consumer-held result, drain and close producer-owned results, wait for producer completion, and then release the output batch. Cleanup never sends pipeline terminal signals from `Call()` and never waits on a producer without first making both cancellation and channel draining possible.

Errors from candidate or publisher catalog execution and malformed result schemas fail the statement rather than widen visibility. A subscription invalidated before the statement snapshot is absent from the live joins. Cancellation and early limit stop remaining subscription scans; there is no partial successful metadata result after a terminal error.

### 5.5 Standards and alternatives

SQL information-schema semantics describe logical schemas and table/column metadata but do not standardize MatrixOne publications. Exposing the subscriber alias as `TABLE_SCHEMA` follows MatrixOne's existing direct-resolution and `SHOW TABLES` namespace rather than exposing physical publisher placement.

Rejected alternatives are:

- switching `current_account_id()` directly in a static view, which cannot safely establish multiple publisher identities;
- planning-time subscription expansion, which captures mutable publication state in cached/prepared plans and grows plan size with subscription count;
- persistent subscriber-side catalog mirroring, which introduces dual-write, invalidation, upgrade, and recovery state;
- an unrestricted raw cross-account table function, which bypasses the system view's role filter;
- retaining all subscription or source rows in one batch, which creates an avoidable tenant-controlled memory amplifier.

## 6. Statement-local sharing and materialization bound

The canonical visibility CTE references the active-role set multiple times. Inlining every reference would execute the nested closure SQL three times for most protected views and six times for `SCHEMATA`.

The planner therefore permits statement-local CTE sharing only for the exact bounded shape:

- one childless, zero-argument `mo_current_roles()` function scan;
- optionally wrapped only by cardinality-preserving projections;
- exactly one fixed-width `INT64` output;
- no join, additional scan, filter, aggregate, limit/offset, or variable-width output.

That exact producer is evaluated once and consumed through the normal query-scoped sink/source ownership path. Its maximum row count is the reachable role count and its row width is fixed.

A CTE that merely contains `mo_current_roles()` does not qualify. In particular, joins with user/catalog tables and early-stop `LIMIT`, SEMI, or ANTI consumers retain the normal full-drain, profitability, 32 MiB estimate, and spill gates. This prevents a small authorization optimization from forcing eager materialization of an unrelated large subtree.

Sink/source cleanup, cancellation wakeups, memory admission, and spill lifecycle remain owned by the existing CTE materialization machinery. This change adds no new lifecycle state.

## 7. Bootstrap and rolling-upgrade contract

`mo_current_roles()` and the version 2 protected views are a cumulative CN capability identified by `MORPCVersion41`. On the merged `mo/main` baseline `bf63172c0691e917ed2613c2d3a2d3d76a7f682e`, v42 is transactional SQL-task child cleanup, v43 is scalar-predicate runtime-filter terminal state, v44 is validated MongoDB explicit-query scan payload, and v45 is bounded Parquet whole-file fanout. Subscription metadata functions and the version 3 `TABLES` / `COLUMNS` definitions therefore use the next available cumulative capability, `MORPCVersion46`.

The capability numbers are not independently negotiable feature bits. Before merge, the implementation must merge latest `mo/main` and verify that v46 remains the next unique cumulative version. If another capability lands first, this document and every producer/consumer gate must be revised together.

Gated consumers are:

- planner admission for `mo_current_roles()` at v41;
- planner admission for both subscription metadata functions at v46;
- tenant bootstrap selection among pre-v41, v41-v45, and v46 definitions;
- every tenant upgrade entry that can persist the latest `TABLES` or `COLUMNS` DDL;
- same-version v4.0.6 definition reconciliation;
- protocol tests and user-visible compatibility errors.

Subscription-aware views may be installed only when every participating CN reports at least v46. Bootstrap below v41 uses the prior role compatibility definitions; bootstrap at v41-v45 uses the current active-role/local-catalog definitions; bootstrap at v46 or later uses subscription-aware definitions. This preserves local metadata availability during mixed-version operation without allowing an old CN to receive an unknown function scan.

### Upgrade

1. deploy binaries that understand v46 while the common protocol remains below it;
2. retain v41-v45 local-catalog definitions during the mixed-version phase;
3. once every CN advertises v46, same-version tenant upgrades install the subscription-aware `TABLES` / `COLUMNS` definitions;
4. new plans may then contain the subscription metadata scans.

All historical and current upgrade entries that reference the mutable latest `InformationSchemaTablesDDL` or `InformationSchemaColumnsDDL` constants must use the v46 protocol barrier. Their complete-definition checks remain idempotent, so an existing v41 definition is refreshed after common v46 while an already-current definition is left unchanged.

### Downgrade and rollback

Do not lower the common protocol below v46 while subscription-aware view definitions remain installed. Operational rollback must first restore the v41 local-catalog `TABLES` / `COLUMNS` definitions, then remove v46-only participants. A rollback below v41 must additionally restore the original role compatibility definitions. No publication, subscription, role, grant, table, or column data migration is introduced, so rollback changes only view definitions and binary capability.

A restarted old CN cannot safely join a cluster whose common protocol and persisted views require a newer capability; normal protocol admission must reject or hold that state rather than treating the old CN as capable.

Backup/restore carries ordinary catalog/view definitions. Restore admission must select definitions compatible with the destination common protocol before tenant queries are admitted.

## 8. Security and denial-of-service analysis

The design is fail-closed: protocol uncertainty, internal executor failure, malformed closure output, or cancellation fails the query rather than treating every role/object as visible.

The role closure cannot cross tenant boundaries because its catalog access uses the current account context. Stable numeric role identity prevents rename-based visibility drift. Cycle detection prevents malicious or accidental cyclic grants from hanging traversal.

The subscription bridge is the only new cross-account path. It derives subscriber identity from execution context, requires a live system-catalog join restricted to that account, checks current publication authorization, quotes all catalog values, and constructs publisher execution identity internally. The raw functions cannot be called outside their exact trusted system-view lineage. Publisher owner IDs are rewritten and cannot collide with subscriber roles to grant visibility accidentally.

Disconnected catalog grants do not contribute to role-closure work. Reachable closure size is proportional to legally reachable roles only until the 4,096-role / 512-KiB admission boundary. Subscription discovery retains at most 64 candidates, eight streamed result envelopes, one current result, and one 8,192-row output batch per function generation. It scans only current subscriber candidates and current publication databases; no global account scan result is retained. Concurrent metadata queries have independent query-owned bounds and no shared cache or worker queue.

## 9. Alternatives

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

## 10. Validation map and acceptance criteria

| Contract | Evidence |
|---|---|
| Active plus complete inherited closure, cycles, duplicates | table-function unit tests |
| Work proportional to reachable graph, disconnected 100k-edge case | focused benchmark/stress test |
| Role 4,096 succeeds; role 4,097 fails before publication | deterministic table-function boundary tests |
| Cancellation before/between frontier queries publishes no partial batch | injected cancellation tests |
| Concurrent closures have independent 512-KiB admission generations and all reject over-limit graphs | barrier-controlled concurrency test |
| Internal results close on success/error/capacity rejection; cancellation propagates | table-function executor tests and ownership review |
| Runtime active role under prepared and ordinary cache reuse | public SQL BVT and planner/compile tests |
| One closure producer per protected metadata query | reachable plan-shape tests for all protected views |
| Amplifying JOIN plus LIMIT/SEMI remains inlined | negative real-planner tests |
| No privilege/direct/inherited/database ownership/admin boundaries | public SQL BVT matrix |
| Sibling views and `SCHEMATA` cannot bypass visibility | public SQL BVT matrix and sysview definition tests |
| Pre-v41 definitions do not reference the function | bootstrap/sysview compatibility tests |
| Canonical upgrades wait for common v41 and are idempotent | v4.0.6 upgrade tests |
| Unique cumulative allocation after latest main | merge-time `MORPCVersion` audit and exact-head CI |
| Live subscription/publication/account/table-scope filtering | table-function query-builder/decoder UT and public SQL BVT |
| Subscription `TABLES.AUTO_INCREMENT` and unpruned `SELECT *` avoid local-alias physical lookup | sysview/planner structural tests and public SQL BVT |
| Subscriber alias/local DB identity; no publisher owner collision | table-function typed-output UT and role-isolation BVT |
| Existing local active/inherited/database/table privilege behavior applies | public SQL BVT matrix with no-grant and granted controls |
| Raw function direct/user-view calls rejected; trusted system lineage admitted | planner unit tests and public SQL negative BVT |
| Candidate page, result channel, output batch, and producer count bounds | table-function boundary tests and ownership review |
| Success/error/malformed/cancel/limit/reset/free close results and join producer | injected lifecycle UT, focused race stress, and unhappy-path audit |
| Publication add/remove/drop and aliases over the same publisher objects update at statement time | public SQL BVT without plan invalidation hooks |
| v41-v45 definitions omit subscription functions; v46 definitions include only affected views | sysview/bootstrap protocol tests |
| Every persisted latest TABLES/COLUMNS writer waits for common v46 | upgrade inventory tests and definition-check tests |
| Original issue succeeds on a clean single-CN instance three consecutive times | exact-head runtime reproduction with recorded output hash |

Acceptance requires all focused owning-package tests, affected BVT, race tests on lifecycle/planner/protocol paths, `go vet` for affected packages, `git diff --check`, `unhappy-path-audit`, and `mo-self-review` to pass. Structural tests prove private admission and DDL placement; public SQL tests independently prove reachability and authorization.

## 11. Risks, rollout, and observability

Primary risks are privilege overexposure, hidden authorized objects, stale active-role/publication capture, publisher/subscriber ID collision, protocol misallocation, repeated nested SQL, leaked producer goroutines/results, and excessive metadata work. The role and subscription bounds, private planner admission, live catalog joins, local identity rewrite, cleanup state machine, and protocol gates address each risk independently.

No new metric or background health signal is introduced. Failures surface as normal query/bootstrap errors with the required protocol version. Operators can diagnose rollout state through existing common-protocol reporting, tenant-upgrade logs, and ordinary query errors. The implementation must not log publication table lists or cross-tenant metadata at per-row cardinality.

Rollout is contained by the common-protocol gate: v41-v45 local definitions remain active until all CNs are capable. If correctness or scale evidence regresses, rollback restores those definitions before withdrawing v46-capable binaries.

## 12. Decision log

- Numeric active role ID is the authorization root; role names are display metadata.
- Complete transitive inheritance is required within the admitted 4,096-role closure; an oversized closure fails the entire metadata query rather than truncating authorization.
- Closure workspace admission is capped at 4,096 roles and a conservative 512 KiB per statement; output-vector memory remains process-mpool-accounted.
- Indexed frontier expansion is preferred over global scans or a mutable cache.
- Closure evaluation is shared once per statement only for the exact bounded function/projection shape.
- Canonical view installation and planner admission use the same cumulative protocol capability.
- System schemas remain intentionally visible; tenant-private metadata follows ownership and privilege paths.
- Subscription metadata uses the subscriber alias/local database identity and the current publication scope; no persistent mirror is added.
- Cross-account execution is private to exact system-view lineage and has no user-supplied identity inputs.
- Candidate discovery is keyset-paged at 64; each function owns one producer, eight result envelopes, one current result, and an 8,192-row output batch.
- `TABLES` / `COLUMNS` use v46 while other version 2 protected views remain at v41.
- `TABLES` evaluates `internal_auto_increment` only in its local-row projection; subscription rows return zero without resolving the rewritten alias as a local relation.
- All persisted writers of the mutable latest TABLES/COLUMNS DDL share the v46 barrier.

## 13. Open decisions

No known blocking design question remains. The protocol number must be revalidated immediately before merge because concurrent PRs allocate from the same cumulative sequence.

## 14. Design review record

```text
Change scope: issue #27794 subscription metadata bridge plus the existing information-schema authorization design
Trigger: authorization/tenant-isolation boundary; cumulative protocol and persisted-view upgrade contract; cross-package goroutine/channel lifecycle; metadata hot path
Design: docs/design/CLAUDE_INFORMATION_SCHEMA_METADATA_VISIBILITY.md, version 3, decision set reviewed before implementation on 2026-09-01
Blocking findings: none
Decision log: preserve existing active-role predicate; use private runtime streaming bridge; live publication catalogs are authoritative; no persistent mirror/cache; v46 three-tier compatibility
Decision: PASS (user approved the recorded implementation plan with “go ahead” before product/test changes)
Implementation deviations: none; any change to identity, private admission, protocol, or lifecycle requires re-review
```
