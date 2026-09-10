# #23684 Arrow LOAD release-readiness evidence

Review date: 2026-09-08. Original evidence base: `up/main@479c445b1d6d66295344fc32e75e0dbcebb00e35`. The #28517 candidate was rechecked against
`up/main@269d59addd032d20897cc4d86f58de3e387a6d76` before delivery. The versioned
[Arrow LOAD design](../23684_arrow_load_design.md) defines the protocol,
ownership, rollout, and acceptance contracts. This record covers
the local release rehearsal; it does not claim cloud-provider or human-owner
approval.

## Gate status

| Gate | Local result | Release status |
| --- | --- | --- |
| F-031 through F-040 | fixed, tested, committed in the branch history | complete |
| Rebase | branch rebased onto the stated `up/main` base | recheck immediately before delivery |
| Default admission and flag rollback | no-config File/Stream, S3/stage, and distributed Arrow LOAD plus explicit disable/drain/restart coverage | default-on contract; validation rerun required |
| S3/stage and distributed admission | omitted fields enable the paths; explicit per-CN `false` values remain kill switches | pending aggregate quota, provider, and owner gates |
| Mixed-version upgrade | MORPC v57 remains the remote compatibility gate; rollback uses explicit distributed/primary switches | rerun required for exact release artifacts |
| Commit failure/CN shutdown/cancellation | deterministic commit fault injection, cluster lifecycle, and blocked S3 request cancellation passed | local complete |
| Aggregate pin quota/range planner/deployment stress | deliberately deferred | blocker for S3/distributed production |
| Real AWS/OSS/COS | delegated to provider test owners | external blocker |
| A/B, alerts, rollout/rollback | local E2E A/B and reference gates recorded in runbook | deployment acceptance pending |
| Arrow-Go supply chain | license/SBOM/size/platform/CVE review recorded | security and packaging blockers remain |
| Formal owner approval | packet below prepared | pending human approval |

## Default-on and mixed-version status

The current product policy enables every Arrow surface when the Arrow section or
its gate fields are omitted. Local File/Stream, direct S3-compatible sources,
S3-backed stages, and distributed record-batch fanout are therefore available by
default. Explicit `enabled=false`, `s3-enabled=false`, or
`distributed-enabled=false` values remain per-surface rollback switches, and
configuration, planner, worker, and public-path tests must prove those values
survive repeated validation and restart. `TestArrowLoadBVT` and
`TestArrowLoadMultiCN` exercise the no-config paths.

The earlier two-binary rehearsal remains evidence that the old binary rejects
Arrow syntax. Before treating default-on distributed execution as release-ready,
the exact artifact must repeat the mixed-version upgrade test, including routing
a parallel statement while an old CN is present and documenting the supported
upgrade order.

`TestArrowLoadRolloutRollbackDrain` separately holds a small LOAD after range
admission and conversion, before batch publication, then initiates cluster
shutdown. On restart with every gate explicitly disabled, the table is either fully
committed when shutdown drained the statement or empty when it canceled; a
partial commit is forbidden. A missing-file LOAD proves rejection occurs before
I/O. Re-enabling local LOAD while distributed execution remains off makes
`parallel 'true'` fall back to serial execution and commit all rows.

## Failure and cancellation evidence

- `CommitPhaseFailureRollback` injects failure after workspace dump and before
  commit visibility. It leaves only the seed row, then succeeds on retry.
- Client-context cancellation blocks an in-flight conditional S3 range request,
  then verifies request-context cancellation, statement failure, and zero
  committed rows. The prior 2-CN processlist observer is not evidence: the
  local fixture can complete before an observer establishes a stable point.
- The former 2-CN processlist-based worker-shutdown observer is not evidence:
  the local fixture can complete before an observer establishes a stable point.
  `TestArrowLoadRolloutRollbackDrain` now uses a deterministic single-CN,
  test-owned post-admission boundary and its complete-or-empty assertion
  preserves the transaction-boundary contract. This does not replace
  topology-specific remote worker-loss/cancellation evidence; that evidence
  remains deferred until a deterministic multi-CN fixture is available.
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
| Execution | External lifecycle, fanout, cancellation, shutdown | Arrow E2E, deterministic lifecycle/cancellation, race/fuzz | pending owner |
| Container/Resource | leases, borrowed vectors/nulls, COW, accounting | owning-package UT/race and consumer inventory | pending owner |
| FileService/S3 | conditional range, cache pin, identity, credentials, request policy | provider UT and local MinIO; aggregate quota/provider cloud gaps explicit | pending owner |
| Transaction/Storage | statement atomicity, retry, encoding boundary | transaction BVT and post-workspace-dump commit fault | pending owner |
| Security/Release | licenses, SBOM/CVE, binary size, platform artifacts | supply-chain review | blocked/pending owner |

An approval must name the owner, reviewed commit/artifact, decision, date, and
any accepted exception. Author self-review cannot substitute for these entries.

## Release decision

Arrow LOAD is enabled by default when its gate fields are omitted. Explicit
`false` values remain rollback controls. Deferred aggregate pin quota/range-
planner pressure work, real-provider testing, deployment A/B, exact Linux
artifacts, mixed-version rerun, and formal owner approval remain release-
readiness gates and must not be inferred complete from local validation.
