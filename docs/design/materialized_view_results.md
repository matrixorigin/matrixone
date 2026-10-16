# Materialized view implementation validation

Validated on 2026-10-13 against the local task worktree based on
`dd38098853ea197699ed3fac3e69dc53975ac061`. Go 1.27.1, Linux amd64,
4 CPU quota, 16 GiB memory, one local CN/TN/LOG deployment with native disk
storage for correctness, restart and performance; a second CN joined the same
TN/LOG/storage for the topology checks. The implementation remains experimental
and disabled by default.

## Executed behavior

- 66 main SQL checks passed, including scheduled refresh, exact numeric/NULL
  oracles, empty aggregates, joins, projection/filter, repeated refresh, read-only
  and dependency guards, SQL mode/fixed timezone, prepared queries, revoked
  source permissions, owner checks, concurrent readers/publishers and tenant isolation.
- Three checks after a graceful stop/restart passed: stored materialization,
  persisted history and a new refresh.
- Five native failure/cleanup checks passed: bigint overflow aborts refresh,
  last-good rows remain, failure history is recorded, drop removes metadata,
  and dependency cleanup permits dropping the database.
- Nine two-CN checks passed: deferred population rejection, publication
  visibility, independent second-CN refresh, five rounds of simultaneous refresh
  with coherent results/stable IDs/nonregressing source timestamps, and terminal
  history. Conflicts may reject one concurrent request with native w-w conflict;
  at least one publisher must succeed and both nodes must see the complete result.
- In total, 83 SQL checks passed. Focused tests passed 22 top-level tests and
  65 subcases across ten packages. Six relevant package suites passed in full.
  Targeted race tests ran 100 repetitions; the three changed task/failure tests
  each passed a separate exact-name, 100-repeat race run at delivery.
  Incremental vet, build, formatting and diff checks passed.

The startup tests discovered nested statement ownership, task ID width and
missing background transaction deadlines. They also found failed DROP DATABASE
cleared the session's database and general tenant initialization attempted to
recreate global catalogs. These paths were fixed, with regression coverage.

## Performance

Deterministic one-million-row fact table; 128-group first materialization.
Each scenario uses the same binary, account and committed source data, fully
retrieves results, verifies equality, warms both queries and alternates 50 pairs.
The median averages the two central observations. Initial CLI output used the
upper central observation; the accompanying JSON preserves all raw samples and
records the corrected median method. No timing samples were replaced.

| Scenario | Raw median ms | MV median ms | Ratio | Full refresh ms |
|---|---:|---:|---:|---:|
| B01 grouped aggregation | 27.647 | 3.724 | 7.43× | 151.270 |
| B02 explicit rollup | 29.392 | 3.308 | 8.89× | 0.000 |
| B03 repeated join | 18.910 | 3.530 | 5.36× | 40.947 |
| B05 high cardinality control | 16.352 | 15.749 | 1.04× | 34.560 |

The rollup uses the existing summary; its refresh column is zero because no
additional view was built. The join materializes eight groups over one million
facts and a 128-row dimension. The high-cardinality control has 10,000 groups
from 10,000 qualifying facts and shows little benefit. These are explicit MV
queries: transparent optimizer rewrite is not implemented.

Raw observations and plans: [materialized_view_performance_results.json](materialized_view_performance_results.json).
Runner: [test/materialized_view](../../test/materialized_view/README.md).
Case-level behavior evidence: [materialized_view_behavior_results.json](materialized_view_behavior_results.json).
Runtime logs and original main/restart/failure/two-CN JSON are in `/workspace/mv-build`.
Change map and verification boundaries: [materialized_view_self_review.md](materialized_view_self_review.md).

The implementation reuses MO transaction snapshot timestamps and physical/logical
source identities as version provenance. This is the foundation for using
Git4Data semantics; branch-aware refresh, historical MV reads and incremental
change extraction are future work.

## Limits

This proves the documented experimental full-refresh scope on a local
cluster, with a bounded two-CN publication check. It does not establish production-tail, cold-cache,
mixed-version/downgrade, crash-at-commit or large-scale scheduler behavior.
B04 and B06–B08 remain broader workload experiments. Logical MV clone/restore,
historical MV reads, incremental refresh and automatic query rewrite reject or
remain outside this phase. Configured golangci-lint was unavailable; incremental
vet is not a claim that the full lint pipeline passed.

## Publication validation — 2026-10-13

The publication branch is based on freshly fetched main
`3e50e1b32d8da61bbbf70c89989fb08b671809de`. Main already reserves protocols 110
and 111, so the MV protocol floor is now 112. The original observations above
retain their original baseline; they are not relabeled as new-head results.

New-base verification passed 22 focused top-level tests / 66 subcases, the six
owning package suites, native library/service/runner builds, and configured
incremental golangci-lint 2.14.0 with zero issues. The extra protocol case proves
that authoring floor 111 cannot admit the new relation. Results and scope:
[materialized_view_publication_results.json](materialized_view_publication_results.json).

The final native run passed **84 SQL checks**: 67 main, three after graceful
restart, five native failure/cleanup and nine two-CN checks. Prepared and
authorized reads now retrieve/compare all rows and check terminal stream errors;
the cancellation assertion checks the actual context error. An added escaped
source-name case reproduced a refresh failure under NO_BACKSLASH_ESCAPES. The
fix keeps metadata SQL on its own default parser/literal rules while applying
captured mode/timezone only to persisted source SQL. The native reproducer and
new case passed after the fix; source identity and schema checks remain enabled.

Publication timing samples are retained as diagnostics because lint/race build
work ran concurrently. Use the separately recorded prototype measurements above
for the isolated warm-query comparison. Full CI and the remaining production
release gates are still pending; publication is a Draft PR under issue #30079.
