# Multi-CN Architecture

## Overview

MatrixOne CNs are stateless and can scale horizontally. The Proxy routes each
client connection to an appropriate CN.

## Proxy (`pkg/proxy/`)

- `Router` defines the CN selection policy.
- `RefreshableRouter` refreshes the cluster topology dynamically.
- Connection routing includes load balancing.
- Label-based routing is supported.

## ClusterService (`pkg/clusterservice/`)

- `MOCluster` exposes cluster topology and metadata.
- `ClusterClient` provides inter-CN communication.
- `labelSupportedClient` supports label-based routing.

## QueryService (`pkg/queryservice/`)

- `QueryService` handles distributed queries.
- `Session` manages query-service sessions.
- QueryService routes messages between nodes.

## Gossip (`pkg/gossip/`)

- Memberlist-based node discovery.
- Node heartbeats and metadata exchange.

## Multi-CN Deployment

See `etc/launch-multi-cn/`. Each CN has an independent configuration with:

- Different service ports.
- The same TN and LogService addresses.
- Optional routing labels.

## Intelligent Query Scheduling Boundaries

Multi-CN scheduling must answer four separate questions in order:

1. **Where may the query run?** Resolve the target CN pool from the
   authenticated tenant and workload class.
2. **What resources are available?** Read CN capacity, health, and live load.
3. **May the query enter?** Apply admission and resource budgets by tenant,
   workload, and pool.
4. **Which CN should run it?** Place the query only within the authorized and
   admitted worker set.

The Proxy selects the connection's ingress CN. The SQL scheduler determines the
execution topology for queries on that connection. Remote Run only transports
and executes pipelines already selected by compile; it must not select another
worker, widen the resolved pool, or apply an independent fallback.

The query scheduler must not arbitrarily migrate a TP transaction after it has
acquired CN-local or transactional state. Multi-CN TPCC therefore needs a
routing decision before that boundary: either the Proxy establishes session
affinity, or a pre-execution router maps a warehouse/partition key to its owner
CN and pins the transaction there. Phase 9 conservatively verifies that the
current CN belongs to the workload's target pool. AP and LOAD workloads may
select a remote CN or multiple CNs when their semantics allow it.

## Scheduling Roadmap

### Phase 9: Deterministic Workload-to-Pool Routing

This phase establishes a hard boundary between policy and execution:

- A dynamic account-level policy maps workload classes such as `tp`, `ap`,
  `load`, `maintenance`, and `internal` to labeled target pools.
- The server injects the authenticated account label. User hints may only
  reduce the worker count or strengthen fallback behavior; they cannot widen
  the target set across tenants.
- TP must remain on the current CN and verifies that it is a member of the
  target pool. AP and LOAD may select one remote worker, while multi-CN AP may
  select a deterministic worker subset.
- Strict policies fail closed on invalid configuration, unresolved pools,
  unroutable workers, or empty pools. Historical behavior remains only when no
  policy is configured or a compatibility fallback is explicitly requested.
- Normal execution, prepared statements, retries, and `EXPLAIN` consume the same
  immutable policy snapshot. Traces, logs, and metrics expose the workload
  class, policy generation, pool, routing mode, candidates, and selected
  workers.

Phase 9 establishes workload isolation and the legal candidate set. It does not
select by instantaneous load, and it does not by itself provide multi-CN TPCC
connection distribution, resource protection, or closed-loop scheduling.
Version 1 deliberately keeps target pools account-isolated; a shared AP pool
across accounts requires explicit shared-pool authorization and per-account
admission budgets in a later phase.

The policy is configured by an account administrator through
`query_workload_policy`. An empty value preserves historical scheduling. The
versioned JSON form is:

```sql
SET GLOBAL query_workload_policy = '{
  "version": 1,
  "policies": {
    "tp": {
      "pool": "tp",
      "labels": {"role": "tp"},
      "current_cn": "required"
    },
    "ap": {
      "pool": "ap",
      "labels": {"role": "ap"},
      "max_workers": 4
    },
    "load": {
      "pool": "etl",
      "labels": {"role": "etl"}
    }
  }
}';
```

`labels` must not contain `account`; the server adds the authenticated account.
`fallback` defaults to `strict`. `legacy-compatible` is the only compatibility
mode, and `empty_worker: local-fallback` is valid only with that mode.
`max_workers` is an upper bound. TP policies must use
`current_cn: required`.

### Phase 10: Unified Resource and Capacity Model

Provide a scheduler-readable CN resource snapshot covering CPU, memory,
concurrency slots, queue depth, health, and data freshness. Define conservative
semantics for missing, stale, and draining state. Resource facts remain
separate from the Phase 9 authorization pool so load data cannot alter tenant
isolation.

### Phase 11: Hierarchical Admission and Resource Budgets

Add account → workload → pool budgets before placement. Protect TP tail latency,
and give AP/LOAD independent budgets that can queue, throttle, or reject work.
All waits must be bounded and cancellable; an unbounded queue must not hide
overload. Shared pools may be enabled only with explicit authorization and
per-account fairness at this boundary.

### Phase 12: Load-Aware Placement Within a Pool

Select workers only from the Phase 9 pool and within the capacity admitted by
Phase 11. Score available capacity, current load, data locality, and stability.
Use deterministic tie-breaking, reservations, and hysteresis so concurrent
schedulers do not stampede toward the same apparently idle CN.

### Phase 13: Execution Feedback and Failure Closure

Feed queue time, actual resource use, execution latency, cancellation, failure,
and draining outcomes back into the resource and admission models. Feedback
must carry the policy generation and execution attempt so retries and stale
results cannot corrupt current state. Recovery must remain inside the original
authorized pool.

### Phase 14: Multi-CN TPCC Acceptance and Operations

Validate Proxy connection distribution, warehouse/partition-owner routing,
transaction affinity, TP pool verification, admission protection, and failure
recovery as one system. Single-owner transactions should avoid distributed
transaction work, while cross-owner transactions must remain correct. Use
throughput, P95/P99 latency, fairness, overload behavior, and single-CN failure
recovery as acceptance criteria. Provide rollback controls and per-account
rollout instead of validating only that queries reached multiple CNs.

## Test Coverage

| Change area | Required coverage |
| --- | --- |
| Proxy routing | Tenant BVT for multi-tenant routing |
| Connection balancing | Sysbench stability under high connection concurrency |
| CN failure recovery | Chaos tests that terminate a CN |
| Node discovery | Network-partition chaos tests |
| Cross-CN queries | Distributed execution with large data volumes |
| Workload pool routing | Tenant isolation, strict fallback, and prepared/retry snapshot unit tests |
| Resources and admission | TP/AP/LOAD contention, overload, bounded wait, and cancellation tests |
| Load-aware placement | Multi-CN balance, stampede, draining, and failure-recovery tests |
| Multi-CN TPCC | E2E/chaos for connection distribution, transaction affinity, P95/P99, and CN failure |
