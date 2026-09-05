# #23684 Arrow LOAD release-readiness evidence

Review date: 2026-09-05. Rebased base: `up/main@845fff8ee9`. The versioned
[Arrow LOAD design](../23684_arrow_load_design.md) defines the protocol,
ownership, rollout, and acceptance contracts. This record covers
the local release rehearsal; it does not claim cloud-provider or human-owner
approval.

## Gate status

| Gate | Local result | Release status |
| --- | --- | --- |
| F-031 through F-040 | fixed, tested, committed in the branch history | complete |
| Rebase | branch rebased onto the stated `up/main` base | recheck immediately before delivery |
| Local default availability and flag rollback | no-config local Arrow LOAD plus explicit disable/drain/restart coverage | local complete |
| S3/stage and distributed admission | explicit per-CN opt-in is required | fail-closed pending aggregate quota, provider, and owner gates |
| Mixed-version upgrade | local-only default does not advertise the new remote pipeline capability | rerun required before enabling distributed execution |
| Commit failure/CN shutdown/cross-node cancel | deterministic fault injection and 2-CN BVT passed | local complete |
| Aggregate pin quota/range planner/deployment stress | deliberately deferred | blocker for S3/distributed production |
| Real AWS/OSS/COS | delegated to provider test owners | external blocker |
| A/B, alerts, rollout/rollback | local E2E A/B and reference gates recorded in runbook | deployment acceptance pending |
| Arrow-Go supply chain | license/SBOM/size/platform/CVE review recorded | security and packaging blockers remain |
| Formal owner approval | packet below prepared | pending human approval |

## Default local-only and mixed-version status

The current product policy keeps the unaccepted surfaces fail-closed: a candidate
CN using an existing configuration with no Arrow section enables local File and
Stream Arrow LOAD only. Direct S3-compatible sources, S3-backed stages, and
distributed record-batch fanout require explicit `s3-enabled=true` and/or
`distributed-enabled=true` configuration on every participating CN.
`TestArrowLoadBVT` and `TestArrowLoadMultiCN` exercise those opt-in paths;
configuration and planner tests prove that omitted settings keep them closed.

The earlier two-binary rehearsal remains evidence that the old binary rejects
Arrow syntax. Before distributed execution can be enabled in a release artifact,
the exact artifact must repeat the mixed-version upgrade test, including routing
a parallel statement while an old CN is present and documenting the supported
upgrade order.

`TestArrowLoadRolloutRollbackDrain` separately stops a cluster only after an
active large LOAD is visible. On restart with every gate disabled, the table is
either fully committed when shutdown drained the statement or empty when it
canceled; a partial commit is forbidden. A missing-file LOAD proves rejection
occurs before I/O. Re-enabling local LOAD while distributed execution remains
off makes `parallel 'true'` fall back to serial execution and commit all rows.

## Failure and cancellation evidence

- `CommitPhaseFailureRollback` injects failure after workspace dump and before
  commit visibility. It leaves only the seed row, then succeeds on retry.
- 2-CN `KILL QUERY` and client-context cancellation use distributed fanout and
  leave zero rows.
- `WorkerCNShutdown` closes the worker only after the coordinator exposes the
  active LOAD. Completion is accepted only as all fixture rows or zero rows.
- Existing File/Stream, transaction/isolation, malformed input, object-change,
  MinIO, race, fuzz, and formal distributed SQL cases remain part of the branch
  evidence described by the design and shared-substrate records.

## Performance and operational acceptance

The end-to-end materialization A/B and raw results are in
`23684_arrow_bridge_benchmark.md`. The runbook defines local reference gates:
zero correctness/leak failures, immediate internal-error escalation, bounded
error-rate escalation, p99 within 20%, throughput at least 90% of the accepted
control, and pinned bytes back to baseline within 60 seconds. No dedicated
Arrow Grafana dashboard is introduced; signals belong in existing FileService
and Pipeline views.

On the final rebased candidate, the three-run median materialize control was
5.3% lower latency and 5.6% higher throughput than borrow. The broad sample
spread means neither policy has a demonstrated repeatable performance lead;
the local comparison passed the reference regression gate but deployment A/B
must still use representative data and exact release artifacts.

These values make local rehearsal deterministic. They do not replace workload,
provider, topology, cache-pressure, or exact-release-binary acceptance by the
deployment owner.

## Owner approval packet

| Owner | Review surface | Evidence | Decision |
| --- | --- | --- | --- |
| SQL/Planner/Compile | LOAD-only syntax, binding, shard plan, additive protobuf, mixed version | planner/compile UT, remote roundtrip, mixed binary rehearsal | pending owner |
| Execution | External lifecycle, fanout, cancellation, shutdown | Arrow E2E, 2-CN cancellation/shutdown, race/fuzz | pending owner |
| Container/Resource | leases, borrowed vectors/nulls, COW, accounting | owning-package UT/race and consumer inventory | pending owner |
| FileService/S3 | conditional range, cache pin, identity, credentials, request policy | provider UT and local MinIO; aggregate quota/provider cloud gaps explicit | pending owner |
| Transaction/Storage | statement atomicity, retry, encoding boundary | transaction BVT and post-workspace-dump commit fault | pending owner |
| Security/Release | licenses, SBOM/CVE, binary size, platform artifacts | supply-chain review | blocked/pending owner |

An approval must name the owner, reviewed commit/artifact, decision, date, and
any accepted exception. Author self-review cannot substitute for these entries.

## Release decision

Local Arrow LOAD is available by default. S3/stage and distributed execution are
not generally reachable without an explicit deployment opt-in. Deferred aggregate
pin quota/range-planner pressure work, real-provider testing, deployment A/B,
exact Linux artifacts, mixed-version rerun, and formal owner approval remain
release-readiness gates for those opt-in modes and must not be inferred complete
from a local Arrow LOAD deployment.
