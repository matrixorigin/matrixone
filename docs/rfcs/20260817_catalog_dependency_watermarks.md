- Status: draft
- Start Date: 2026-08-17
- Authors: VioletQwQ-0
- Implementation PR: none
- Issue for this RFC: #27235

# Precise Catalog Dependency Watermarks

# Summary

This RFC evaluates how MatrixOne should invalidate prepared plans and RC transaction table-cache entries after catalog changes. It compares precise dependency watermarks with extending the existing fixed-size account-bucket watermark. It does not change production behavior.

The current `CatalogCache.HasNewerVersion` uses exact database and table history lookups for named dependencies. A 4096-bucket monotonic watermark is used only for account-level dependencies where `DatabaseId`, `DatabaseName`, and table name are absent. Broadening the table-change bucket check to named table dependencies can remain conservative only if the exact database identity check is retained. Unrelated DDL and account hash collisions can still cause unnecessary plan rebuilds and RC table-cache reloads.

# Motivation

The exact named-dependency path constructs temporary BTree keys and scans retained catalog history. MAIN1 profiling attributed about 3.10% of sampled allocation bytes to this area. An experimental A+C stack removed that selected allocation owner, but its formal no-profile gain was only `+0.7026%`. That result is close to cohort noise and combines two changes; it is not evidence for an isolated catalog TPS gain.

The design goal is therefore correctness and measurable invalidation precision, not a predetermined throughput target. A production change is justified only if it removes material lookup cost without causing an unacceptable rebuild or cache-miss rate.

The common negative path is an explicit non-regression requirement: after cache and object warm-up, a `HasNewerVersion` check that finds no newer catalog version must add zero heap allocations attributable to the check and must meet a pre-agreed latency budget for the production entry point. The budget must be measured against the current exact-history implementation at p50/p95/p99 under representative cache state. Cold initialization, caller-owned allocations, and unrelated rebuild work are outside this requirement; an implementation that cannot satisfy both the allocation-free steady-state path and the latency budget remains rejected.

## Measured Attribution Evidence

The allocation cost is present in a real TPCC attribution window, not only in a local microbenchmark. The comparison uses M-profile run `31691323156` with 220,625 completed transactions and MAC-profile run `31998721900` with 225,236 completed transactions.

In the M-profile window, `CatalogCache.HasNewerVersion` owned:

- 17,060,022,442 allocation bytes, or approximately 75.5 KiB per completed transaction;
- 52,996,163 allocation objects, or approximately 240 objects per completed transaction;
- 1.98 seconds of flat CPU and 52.88 seconds of cumulative CPU, or approximately 8.97 microseconds flat and 239.7 microseconds cumulative CPU per transaction;
- 312.41 seconds of cumulative mutex delay, or approximately 1.416 milliseconds per transaction.

The complete, unfiltered MAC allocation-space and allocation-object profiles contain no matching `HasNewerVersion` owner. This shows that the experimental watermark fast path removed the original lookup's direct allocation owner and call tree in the measured workload. The owner represented about 3.10% of M allocation bytes and 1.21% of M allocation objects.

The whole M-to-MAC stack reduced CPU per transaction by 2.64%, allocation bytes per transaction by 6.33%, and allocation objects per transaction by 2.95%. Those changes include P0-A and are directional stack evidence only. They are not an isolated C throughput result, and the formal no-profile A+C gain remains `+0.7026%`.

No transaction P95/P99 claim is made for Catalog C. The observed commit-pipeline tail changes are confounded by P0-A, while steady-state TPCC contains too little DDL to quantify bucket-collision or unrelated-DDL rebuild tails. Those claims require the targeted invalidation workload described below rather than another full TPCC cohort.

# Current Behavior and Consumers

Catalog logtail application updates exact table/database BTree history. Table inserts and deletes also advance a fixed `accountID % 4096` high-watermark. The watermark is monotonic and bounded independently of tenant churn.

`HasNewerVersion` has three distinct contracts:

1. A database name detects database deletion or recreation by comparing stable database identity.
2. A named table detects deletion, recreation, or a higher table version through exact account/database/name history.
3. An account-only marker uses the bucket watermark to detect any table change for that account. A database-only marker intentionally ignores unrelated table changes after database identity is validated.

The two production consumers are:

- prepared statement execution in `TxnComputationWrapper`, once per non-snapshot schema dependency;
- RC transaction table-cache reuse in `Transaction.getCachedTableByKey`.

Prepared plans additionally use a separate prepared-metadata watermark for subscription and named-snapshot validation. This RFC does not collapse those dependency classes.

# Required Semantics

A replacement must preserve these invariants:

- No false negative: every DDL that can make a cached schema or plan stale must invalidate it.
- Monotonicity: replay, GC, eviction, checkpoint restore, and out-of-order delivery cannot lower a watermark.
- Stable identity: drop/recreate and ID/name reuse are detected even when the visible name is unchanged.
- Isolation: changes in another account cannot hide a required invalidation.
- Historical plans: dependencies bound to a valid snapshot remain immutable and must not be invalidated by current-object changes.
- Database-only dependencies do not become dependent on every table in the database unless the plan actually has that dependency.
- Conservative collisions may add work, but their rate must be measured and bounded before deployment.

Views, SQL UDF bodies, subscriptions, named snapshots, PITR DDL, temporary tables, and session DDL generations remain separate dependency types unless an implementation explicitly models and tests them.

# Candidate A: Precise Dependency Watermarks

Maintain monotonic watermarks using stable dependency keys:

- account dependency: account ID;
- database dependency: account ID plus database ID and database generation;
- table dependency: account ID plus database ID, table ID, and table generation/version.

Names remain lookup inputs but are not sufficient identity. Drop/recreate must publish a new generation even if an ID or name is reused. A cached consumer records the dependency identity and observed watermark at plan/table construction. Invalidation compares that observation with the latest watermark without constructing temporary BTree items or scanning history.

The design must define:

- the logtail event that advances each dependency class;
- publication ordering relative to exact BTree catalog state;
- representation of create, alter, truncate, rename, drop, and recreate;
- memory ownership, cardinality bounds, tenant eviction, and GC;
- replay/checkpoint reconstruction and compatibility during mixed-version restart;
- how a dependency key is re-resolved when legacy cached state lacks a generation.

No map keyed only by mutable database or table name is acceptable. GC may remove old exact entries only after a retained monotonic summary prevents false negatives.

# Candidate B: Broadened Account-Bucket Watermark

This option retains the exact database-name and database-ID history check, then replaces only the named table-history scan with the existing 4096-bucket table-change watermark. The table portion of the hot path is a bucket calculation, read lock, and timestamp comparison, with fixed memory.

The exact database check is required because `InsertDatabase` and `DeleteDatabase` do not advance the current table-change buckets. A bucket-only replacement could therefore miss drop/recreate of an empty database. A future design may instead add a separately specified database watermark, but it must not treat the current table bucket as a database-change oracle.

With the exact database check retained, the table check is conservative: accounts `A` and `A+4096` collide, and every table DDL in an account invalidates all named table dependencies in that account. It does not encode which database/table changed, why it changed, or whether the plan depends on that object.

This option must not proceed based only on allocation removal. It is acceptable only if measured false-positive costs remain within an agreed budget under realistic tenant and DDL distributions.

# Measurement Plan

Add diagnostic counters in an instrumentation-only experiment, not in the production candidate:

- `HasNewerVersion` calls by prepared-plan and RC table-cache consumer;
- exact invalidations and bucket-only invalidations;
- same-account unrelated-DDL false positives;
- cross-account collision false positives;
- prepared-plan rebuild count and latency;
- RC table-cache delete/reload count and latency;
- dependency-map entries and retained bytes by account;
- DDL event rate by create/alter/truncate/rename/drop/recreate class.

Replay the same event stream through exact history, precise watermark, and bucket watermark oracles. Report false negatives as correctness failures and false positives per 10,000 dependency checks.

Workloads must include:

- one account with many unrelated tables and repeated DDL;
- many accounts below and above 4096, including forced collisions;
- DDL-heavy and read-heavy tenants sharing a bucket;
- prepared text and binary protocols;
- same-session, cross-session, and multi-CN execution;
- RC transactions that reuse table-cache entries across statements;
- table/database drop and recreation with name and identity reuse;
- empty-database drop and recreation, proving the exact database check remains authoritative;
- replay, checkpoint restore, concurrent update/check, and GC.

The instrumentation experiment must report hit ratio, rebuild/cache-reload latency, CPU, allocations, and retained memory. It must not report profile-mode TPS as production throughput.

# Production Test Plan

Any future implementation PR must include:

- unit differential tests against the current exact BTree behavior for no change and every supported DDL;
- same-account unrelated DDL and forced bucket-collision controls;
- equality, older-than, and newer-than watermark boundaries;
- concurrent update/check race tests with deterministic barriers;
- replay in order and out of order, checkpoint restore, and GC monotonicity;
- drop/recreate, truncate, alter, and rename identity transitions;
- empty-database drop/recreate with no table event available to advance an account bucket;
- RC table-cache reuse and invalidation through the production transaction entry point;
- text and binary prepared statements across sessions and CNs;
- BVT cases for public prepared-statement behavior and multi-statement RC reuse;
- focused allocation/latency benchmarks and an independent no-profile performance cohort.

# Ready Gate

This RFC remains Draft until all of the following are complete:

1. Catalog, frontend, and RC table-cache owners accept the dependency identity and publication model.
2. Every invalidation producer and consumer is mapped, including alternate dependency classes that remain out of scope.
3. The chosen design has a proof and executable matrix showing no false negatives.
4. Memory bounds, tenant churn, replay reconstruction, and GC ownership are specified.
5. Prepared-plan and RC cache false-positive rates are measured on representative workloads.
6. An acceptable rebuild/cache-miss budget is agreed before implementation.
7. A production implementation and rollback plan is separated from diagnostic instrumentation.

# Drawbacks

Precise watermarks add state, replay rules, and GC obligations. They can move allocation cost from each lookup into retained metadata and increase logtail update work. A bucket watermark has lower implementation and memory cost but can amplify unrelated DDL into compilation and cache-reload storms.

Keeping the current exact history scan is the safest fallback if neither alternative demonstrates a favorable total cost.

# Rationale and Alternatives

Directly restoring the experimental bucket fast path is rejected because allocation removal alone does not quantify downstream invalidation cost. Reusing mutable names as precise keys is rejected because drop/recreate and rename require stable identity and generation. A global catalog watermark is rejected because it is even coarser than account buckets.

The preferred direction is a precise watermark only if its retained-state lifecycle is bounded and its update/replay contract remains simpler than the exact history lookup it replaces. Otherwise the current implementation remains the reference behavior.

# Unresolved Questions

- Which stable generation is authoritative for table/database identity across replay and upgrade?
- Can existing logtail rows reconstruct precise watermarks without a persisted format change?
- What false-positive budget is acceptable separately for prepared plans and RC table-cache entries?
- Should dependency watermarks live in CatalogCache, plan-cache metadata, or a dedicated immutable snapshot?
- How should view, UDF, subscription, and named-snapshot dependencies compose with table/database dependencies?
