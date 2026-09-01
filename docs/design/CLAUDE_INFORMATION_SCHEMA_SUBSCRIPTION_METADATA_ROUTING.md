# Information Schema Subscription Metadata Routing

- Status: Proposed — awaiting independent design approval
- Owning issue: [#27759](https://github.com/matrixorigin/matrixone/issues/27759)
- Implementation PR: [#27778](https://github.com/matrixorigin/matrixone/pull/27778)
- Related local-visibility design: [Information Schema Metadata Visibility and Active-Role Closure](CLAUDE_INFORMATION_SCHEMA_METADATA_VISIBILITY.md)
- Version: 2
- Last updated: 2026-09-01

## 1. Problem and evidence

A subscription database is a subscriber-side schema alias for objects owned by a
publisher account. Normal table resolution already routes a subscribed table to
the publisher catalog. Index discovery did not: `SHOW INDEX` and
`information_schema.STATISTICS` continued to read the subscriber's local
`mo_indexes`, `mo_tables`, and `mo_columns`. The table was queryable, but its
index rows were absent. MySQL Connector/J consequently returned incomplete
`DatabaseMetaData.getIndexInfo()` and `getPrimaryKeys()` results.

Fixing only the literal `SHOW INDEX FROM subscription.table` syntax is
insufficient. Connector/J queries `STATISTICS` through prepared statements, and
applications may place schema predicates in `WHERE`, `JOIN ON`, derived tables,
correlated subqueries, or omit them for account-wide discovery. Subscription
membership may also change between planning and execution or be read through a
historical account snapshot.

The change therefore defines an account-wide metadata contract and a narrow
cross-account authorization exception. It requires design review even though
the originating user-visible symptom is a bug.

## 2. Scope and non-goals

This design covers:

- `SHOW INDEX` for a table resolved through a subscription database;
- every occurrence of the built-in `information_schema.STATISTICS` view;
- Connector/J index and primary-key discovery shapes built on `STATISTICS`;
- current and account-snapshot subscription enumeration;
- ordinary plan-cache and prepared-statement lifecycle behavior;
- persisted `STATISTICS` view definitions during upgrade and downgrade;
- subscriber active-role visibility plus publisher-account,
  publication-database, and publication-table isolation.

This design does not:

- expose arbitrary publisher `information_schema` or `mo_catalog` rows;
- make subscriber role IDs meaningful in the publisher account;
- grant data or metadata access beyond the intersection of subscriber RBAC and
  the existing publication contract;
- copy publisher index rows into the subscriber catalog;
- change publication creation, subscription creation, or withdrawal semantics;
- add SQL syntax, a catalog schema migration, or a protocol capability bit;
- promise subscription-aware routing for metadata views other than
  `STATISTICS`.

## 3. Terms and normative result contract

Let:

- `S` be the subscriber account;
- `P` be a publisher account;
- `N` be a subscription database name visible in `S`;
- `D` be the publisher database named by the publication;
- `T` be the publication's table set, including the existing all-tables form;
- `V` be the set of tables visible to the subscriber session's active-role
  closure through the subscription schema;
- `X` be the current statement snapshot or an explicitly requested historical
  account snapshot.

A publisher index row is visible through `N` at `X` if and only if all of the
following hold at `X`:

1. `S` has a normal, active subscription record for `N`;
2. that record identifies publisher `P` and publication database `D`;
3. the indexed table is a member of publication table set `T`;
4. the indexed table is visible in subscriber RBAC set `V`;
5. the `mo_indexes`, `mo_tables`, and `mo_columns` rows belong to `P` and join
   to that published table;
6. the persisted built-in view has a statement shape the planner can rewrite
   without weakening these predicates.

The returned `TABLE_SCHEMA` and `INDEX_SCHEMA` are `N`, not `D`. The table,
column, and index names and attributes remain publisher metadata. Rows for a
withdrawn/deleted subscription, another publisher database, or an unpublished
table are absent.

The subscriber-local branch retains the local metadata-visibility contract
defined by `CLAUDE_INFORMATION_SCHEMA_METADATA_VISIBILITY.md`. This document
adds only the subscription branch exception described below; it does not widen
the local branch.

## 4. Authorization and tenant-isolation boundary

### 4.1 Two independent authorization boundaries

The publication database/table set is the existing publisher-controlled grant
that makes a subscribed table definition and its read-only data available to a
subscriber. Index and primary-key metadata are structural metadata required to
describe and correctly use that same table. Requiring a second publisher role
grant would make a valid subscription unreadable to standard clients and would
create an authorization rule that publication owners cannot express through
the publication contract.

Publication membership is the sole **publisher-side** authorization boundary;
it is not a replacement for subscriber RBAC. Before entering a publisher
catalog, the compiler evaluates the subscription database against the current
subscriber session's active-role closure. Database ownership, database-wide or
global table grants, and database metadata grants admit all publication-member
tables. Exact table/view grants admit only their recorded logical table IDs. A
connect-only user therefore gets no subscription metadata branch.

After that subscriber-local decision, the publisher branch must not evaluate
subscriber role IDs against publisher `mo_role_privs`: role IDs are
account-local identities and collisions across accounts have no authorization
meaning. It also must not use an implicit publisher session role because no
publisher login participates in the subscriber statement. Instead the planner
intersects the subscriber-visible table IDs with the publisher account,
database, and publication table set.

This exception is narrow. It authorizes only the catalog rows needed to
describe tables already admitted by the publication. It does not expose
publisher users, roles, databases, unpublished tables, other metadata views, or
arbitrary catalog queries.

### 4.2 Enforced predicates

Each publisher branch carries the full subscription identity on its catalog
object references. Planning enforces all four scopes independently:

1. subscriber active-role visibility, computed only from subscriber-local
   `mo_database`, `mo_role_privs`, and `mo_current_roles()`;
2. publisher-account scope on `mo_indexes`, `mo_tables`, and `mo_columns`;
3. publication-database scope on the publisher `mo_tables` scan;
4. publication-table scope on that same `mo_tables` scan.

Logical table IDs are globally unique catalog identities. For an exact grant,
the subscriber-side privilege row supplies only those IDs; publisher table
names are not disclosed while RBAC is evaluated. The publisher `mo_tables`
scan then requires both `rel_logical_id IN V` and membership in publication set
`T`. When exact grants exist, the compact globally unique ID set may be
attached to each candidate subscription branch; unrelated publishers match no
ID and return no row. If there is neither broad visibility nor any exact grant,
the subscription branches are omitted entirely.

The joins in the canonical `STATISTICS` view then restrict index and column
rows to the admitted table IDs. Output schema expressions are rewritten from
publisher database `D` to subscription name `N` only after source scoping is
attached.

The canonical view's `__mo_visible_tables` CTE normally evaluates the current
tenant's role closure. The account-wide provider evaluates that same role
closure locally first. In a publisher branch the planner then replaces the
CTE's subscriber-role predicate with the publisher-account predicate and adds
the captured subscriber-visible logical IDs when visibility is table-specific.
Publication database/table filters remain on `mo_tables`; replacing the CTE is
not a standalone authorization grant.

An unsupported `__mo_visible_tables` shape fails planning. The planner never
falls back to an unscoped publisher scan. Compiler-context subscription state
is restored after every branch on success or error so a later local resolution
cannot inherit publisher identity.

## 5. Account-wide planning semantics

Every logical occurrence of `information_schema.STATISTICS` is bound as one
relational source:

```text
subscriber-local STATISTICS
UNION ALL publisher branch for active, subscriber-visible subscription N1
UNION ALL publisher branch for active, subscriber-visible subscription N2
...
```

Outer predicates are applied to this source by normal relational planning.
They do not decide which catalogs are present. Consequently these shapes are
semantically equivalent with respect to subscription discovery:

- `WHERE table_schema = ?`;
- a schema predicate in `JOIN ON`;
- an outer predicate over a derived `STATISTICS` query;
- a correlated or nested occurrence;
- an account-wide query without a schema predicate;
- a prepared Connector/J query with schema and table parameters.

Each sibling or nested occurrence receives its own complete branch set. The
subscriber-local source is never dropped when subscriptions exist.

Subscription names are sorted for deterministic plans and deduplicated using
the server's database-identifier comparison rules. Under
`lower_case_table_names=0`, differently cased names are distinct. Modes 1 and
2 compare them case-insensitively while preserving the selected subscription's
display spelling. Empty, nil, withdrawn, deleted, and subscriber-invisible
entries are omitted.

`SHOW INDEX` already identifies one database and follows that subscription's
publisher identity directly. It shares the same publisher-account and
publication-table restrictions but does not require account-wide enumeration.

## 6. Snapshot consistency

For a current query, subscription enumeration and publisher catalog binding use
the session transaction. For a named historical account snapshot older than
the session transaction:

1. the compiler clones the transaction operator at snapshot timestamp `X`;
2. it applies the snapshot tenant identity to the background context;
3. it enumerates `mo_subs` and subscriber-local RBAC visibility through a
   short-lived background executor bound to that cloned transaction;
4. all local and publisher catalog branches retain the same plan snapshot;
5. the background result and executor are closed on every return path.

This prevents a plan from combining today's subscription set with historical
publisher catalogs, or the inverse. A subscription present at `X` and removed
now remains visible only to the historical query. A subscription created after
`X` remains absent from that query.

Snapshot scope validation remains owned by the existing snapshot subsystem.
This design does not permit a subscriber to use a snapshot for an account it
could not otherwise address.

## 7. Cache and prepared-statement lifecycle

The complete visible subscription set is captured in the plan's UNION shape,
but create/drop/withdraw/reauthorize transitions do not necessarily change a
table schema version. Schema-version invalidation alone is therefore
insufficient.

### 7.1 Ordinary statements

Every plan originating from built-in `STATISTICS` retains an origin-view
dependency marker. The ordinary `COM_QUERY` plan cache rejects that dependency
even when the account currently has zero subscriptions and no publisher node
exists. Repeating the same SQL after creation of the first subscription must
build a new branch set.

### 7.2 Prepared statements

A prepared `STATISTICS` plan is rebuilt on every `EXECUTE`. The rebuild decision
is computed before validating captured table references. Because a guaranteed
rebuild will resolve the current branch set, stale publisher references from a
withdrawn subscription are not resolved first; doing so would surface an
obsolete authorization error instead of returning the current result.

This contract covers zero-to-one creation, additional subscriptions,
withdrawal, reauthorization, drop, and case-mode resolution. Parameter values
remain execution-time values and do not alter the branch enumeration rule.

Prepared compile artifacts may be reused only when their plan is reused. A
subscription-metadata dependency forces a new plan and therefore does not
retain a compile pipeline for the previous branch set.

## 8. Persisted-view and rolling-version compatibility

No persisted table or publication format changes. The built-in `STATISTICS`
definition remains stored as an ordinary view and is parsed into a query-owned
AST. Publisher rewrites modify only that branch's AST; they do not mutate the
catalog definition or a parser object shared with another query.

Compatibility rules are:

- legacy definitions without `__mo_visible_tables` receive the publisher
  account rewrite in the top-level predicate and retain publication filters on
  catalog scans;
- the canonical visibility-aware definition receives both the top-level
  account rewrite and the narrow `__mo_visible_tables` replacement described
  in Section 4;
- a present but structurally unsupported named visibility CTE fails closed;
- local branches continue to use the persisted definition unchanged.

This change needs no new MORPC capability because it introduces no persisted
wire or catalog format that an older CN cannot parse. During a rolling binary
upgrade, an older CN may still show the original empty subscription metadata,
while an upgraded CN returns the corrected rows. This is a temporary
availability/compatibility difference, not an authorization widening: the old
behavior exposes fewer rows. Operators requiring consistent JDBC metadata must
wait until all query-serving CNs are upgraded before relying on the new
contract.

Downgrading restores the old incomplete behavior but requires no catalog
rollback. Backup and restore carry the existing publication, subscription, and
view rows unchanged. The local metadata-visibility design's independent
protocol/view-installation gates remain authoritative for selecting canonical
versus compatibility view definitions.

## 9. Cardinality, complexity, and explicit planning budget

Let `A` be the number of active, subscriber-visible subscription schemas and
`R` the number of logical `STATISTICS` occurrences. Each source view contains
a fixed planner shape `V`. Plan construction and the number of catalog branches are
`O((A + 1) * R * V)`. Execution work is also proportional to those branches,
but every publisher `mo_tables` scan is constrained by publisher account,
publication database, and table set before index rows are returned.

Enumeration adds one batched subscriber-local visibility query per logical
`STATISTICS` source; it does not issue one RBAC query per subscription.

The subscription feature currently has no catalog-enforced per-account hard
maximum. This design therefore does not truncate subscriptions or silently
return partial metadata. Instead it defines this explicit supported planning
budget for the current UNION implementation:

- expected operating range: 0–16 active subscriptions and 1–2
  `STATISTICS` occurrences per statement;
- validated envelope: at most 64 active subscriptions and at most 4
  `STATISTICS` occurrences, or 256 publisher view expansions;
- reference compile budget at the validated envelope: 500 ms and 256 MiB of
  cumulative Go allocations per plan build on an Apple M1 with 8 planner
  threads;
- queries above the validated envelope are not truncated, but are outside the
  performance contract of this implementation and require a follow-up runtime
  metadata operator or equivalent shared representation before that envelope
  is raised.

The checked-in benchmark is reproducible with:

```text
go test ./pkg/sql/plan -run '^$' \
  -bench '^BenchmarkSubscriptionStatisticsPlanning$' \
  -benchmem -benchtime=3x -count=1
```

Reference evidence on 2026-09-01 (`darwin/arm64`, Apple M1) is:

| Active subscriptions | Occurrences | Planning time | Cumulative allocations |
|---:|---:|---:|---:|
| 0 | 1 | 3.4 ms | 1.1 MiB |
| 0 | 4 | 11.9 ms | 4.4 MiB |
| 16 | 1 | 31.1 ms | 12.9 MiB |
| 16 | 4 | 87.8 ms | 52.5 MiB |
| 64 | 1 | 69.5 ms | 49.7 MiB |
| 64 | 4 | 399.4 ms | 204.2 MiB |

Wall-clock values are reference evidence, not a timing assertion in unit tests.
The deterministic boundary test compiles 64 subscriptions across four
occurrences and verifies all 256 publisher branches. CI executes the functional
publication/subscription matrix against real catalogs; timing remains observed
through existing statement and subscription duration metrics.

## 10. Failure handling and ownership

Subscription enumeration errors, snapshot executor errors, view parse errors,
and unsupported visibility-CTE shapes fail the statement. They do not degrade
to local-only or unscoped publisher results. A publication filter construction
error likewise aborts the affected plan.

All temporary subscription slices are query-owned. Enumeration copies and
sorts the provider result before deduplication. Historical background executors
are closed with `defer`; compiler-context subscription identity is restored
after each publisher branch. There is no global subscription metadata cache,
background goroutine, retry loop, or cross-statement mutable branch list.

Cancellation and memory admission continue to use the existing planner/query
ownership paths. The explicit planning budget in Section 9 is the acceptance
envelope for the current representation, not permission to bypass those limits.

## 11. Alternatives

### A. Rewrite only literal `SHOW INDEX` and JDBC predicates

Rejected. It is syntax-dependent and fails account-wide, JOIN, derived,
nested, and future connector query shapes.

### B. Evaluate subscriber role IDs inside the publisher account

Rejected. Numeric role identities are tenant-local. Reusing them can both hide
legitimate published tables and, on an ID collision, express an authorization
meaning the publisher never granted.

Subscriber RBAC is still mandatory, but it is evaluated against the
subscriber-local subscription database and privilege rows before publisher
routing. Only the resulting broad-visibility flag or globally unique logical
table IDs cross that boundary.

### C. Execute under a publisher user or role

Rejected. A subscription does not create a publisher login session, and no
stable publisher role is part of the publication contract.

### D. Copy index metadata into subscriber catalogs

Rejected. It introduces asynchronous refresh, withdrawal cleanup, snapshot
versioning, and stale-security-state problems for data already owned by the
publisher catalog.

### E. Add one planner branch only after extracting a schema predicate

Rejected. Predicate extraction is not complete across JOIN, derived, nested,
OR, prepared, and account-wide queries. Authorization and semantics must not
depend on optimizer predicate placement.

### F. Runtime subscription-aware metadata operator

Deferred. It can share one compact runtime representation and is the preferred
direction if the supported envelope must exceed Section 9. It is substantially
more invasive because it needs runtime catalog routing, predicate pushdown,
snapshot ownership, distributed execution, and observability contracts.

### G. Cache the account's subscription branch set

Rejected for this change. Correct invalidation must cover creation, drop,
publication withdrawal/reauthorization, snapshots, transaction visibility,
case mode, restart, and tenant isolation. Rebuilding keeps the ownership and
freshness rule explicit.

## 12. Validation map and acceptance criteria

| Contract | Evidence |
|---|---|
| `SHOW INDEX` routes index/column scans to publisher | planner unit test and public BVT |
| Account-wide source retains local plus every active subscription | planner unit tests and public BVT |
| WHERE, JOIN ON, derived, nested, sibling, OR, and prepared shapes agree | planner unit tests and public BVT |
| Publisher account/database/table isolation; unpublished table absent | plan-shape tests and public BVT |
| Connect-only subscriber cannot discover published table/index names | restricted-user public BVT and omitted-branch planner test |
| Database-wide and exact-table subscriber grants intersect publication scope | database-wide public BVT plus visibility-provider and exact-filter unit tests |
| Subscriber role IDs do not authorize publisher catalogs | canonical CTE rewrite and publisher-RBAC negative tests |
| Canonical and legacy persisted-view shapes remain safe | real canonical-DDL rewrite test and fail-closed shape tests |
| Current and historical membership use one snapshot | compiler-context ownership review and public snapshot BVT |
| Ordinary zero-to-one transition cannot reuse stale cache | cache-admission test and repeated COM_QUERY BVT |
| Prepared create/withdraw/reauthorize/drop transitions rebuild | frontend lifecycle tests and public prepared BVT |
| Guaranteed rebuild skips obsolete captured-reference validation | injected resolver test |
| Identifier modes 0/1/2 and malformed bytes deduplicate correctly | planner unit tests and case-sensitive BVT |
| 64 subscriptions × 4 occurrences preserve all 256 branches | deterministic planner budget test |
| Planning cost remains measurable across 0/16/64 and 1/4 | checked-in benchmark and Section 9 reference results |
| Connector/J index and primary-key result shapes | public BVT plus Connector/J integration run |

Acceptance requires focused planner and frontend tests, affected public BVT,
race tests for planner and prepared/cache lifecycle paths, `go vet` on owning
packages, `git diff --check`, and exact-head CI. Timing numbers are reviewed
against the explicit budget but are not used as flaky wall-clock unit-test
assertions.

## 13. Risks, rollout, and observability

Primary risks are cross-tenant metadata leakage, unpublished-table leakage,
stale membership, historical/current state mixing, identifier-case omission,
and planner amplification. Sections 4–10 assign a separate invariant and test
to each risk.

The rollout is binary-only. No backfill or destructive rollback exists. The
feature may be disabled operationally only by routing metadata clients to old
behavior or rolling back the binary; publication data remains unchanged.

Existing statement duration, memory, error, and publication/subscription
duration telemetry cover operational regressions. A dedicated branch-count
metric is not introduced in this bug fix. If production accounts approach or
exceed the validated envelope, a follow-up runtime representation must add
branch/cardinality observability before raising the budget.

## 14. Decision log and open decisions

- Subscriber active-role visibility and publication membership are both
  required. Publication scope, not publisher RBAC, authorizes the narrow
  cross-account catalog scan after subscriber RBAC succeeds.
- `STATISTICS` is account-wide and syntax-independent.
- Local and publisher rows are combined with `UNION ALL`; existing catalog
  uniqueness prevents semantic duplicate index rows within one branch.
- Historical enumeration and catalog binding use one snapshot.
- Ordinary plans are not cached; prepared plans rebuild every execution.
- Publisher rewrites are query-owned and support legacy plus canonical
  persisted definitions without a catalog migration.
- Identifier deduplication follows `lower_case_table_names`.
- The current representation's explicit supported envelope is 64 active
  subscriptions and four `STATISTICS` occurrences.
- No blocking design question is intentionally left unresolved. Raising the
  performance envelope is a separate design change.

Independent design approval is still required. This document must remain
`Proposed` until an authorized reviewer records a decision against an exact
commit. Code review approval of an earlier implementation revision is not
implicitly design approval.

## 15. Design review record

To be completed by an authorized reviewer:

```text
Change scope: cross-account subscription index metadata routing
Trigger: authorization/tenant boundary; account-wide semantics; snapshot and cache lifecycle; planner amplification
Design: docs/design/CLAUDE_INFORMATION_SCHEMA_SUBSCRIPTION_METADATA_ROUTING.md, version 2, <reviewed commit>
Blocking findings: <none or findings>
Decision log: <accepted tradeoffs and resolved questions>
Decision: PASS | REQUEST_CHANGES
Implementation deviations: <none or affected sections>
```
