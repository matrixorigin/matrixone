# COPY ALTER QA handoff

Candidate: `d1fdf00bf6b315b06a6133d2cce07c91af52da3f`  
Issue: [#28319](https://github.com/matrixorigin/matrixone/issues/28319)  
PR: [#28418](https://github.com/matrixorigin/matrixone/pull/28418)

Status: **BLOCKED_ENVIRONMENT / owner not assigned**. This document prepares
the handoff; it does not claim that deployment QA has started or passed. The
issue remains open and the PR remains Ready.

## Required deployment

Use a clean deployment built from the exact candidate SHA, with at least two
CNs so the clients can be pinned to different CNs. Record the MatrixOne build,
CN/TN/Log topology, tool revision, data directory identity, client version,
timeout settings and the source SHA. Do not reuse a business database.

The performance driver is in
`pkg/tests/issues/issue_28319_perf_test.go` behind the `issue28319_perf` build
tag. Its run contract is documented in
`docs/design/evidence/20260916-copy-alter-performance-runbook.md`.

## Required QA cases

1. Pin clients A and B to different CNs. Pause A after `data-copied` and before
   publication; B must complete a different-table `ALGORITHM=COPY` ALTER and
   COMMIT. Check table data, schema, relation IDs and exactly one copy per
   request.
2. Repeat on the same table. B must wait for the table owner, then use the
   current definition without a lost row, half replacement or duplicate copy.
3. Publish the first Snapshot/PITR owner during preparation, and publish an
   owner after ALTER commits. Cover table, database, account and cluster
   scopes, including ordinary-tenant/system-tenant transitions.
4. Run prepared text and binary ALTERs across a default-database change. Inject
   the private coordination conflict and verify two distinct transaction IDs,
   complete cleanup of the first attempt and reuse of the prepared binding.
5. Cover View dependencies, invalidation/revalidation, synchronous and
   asynchronous index tasks, foreign keys, auto-increment and final-task
   ownership. Temporary relations must leave no View, task or lineage record.
6. Inject failure or cancellation after copy-table creation, copy, index build,
   each gate, catalog refresh, history protection, old-table deletion, rename,
   task registration and commit. Resolve uncertain commit results by querying
   final transaction state; do not assume rollback.
7. Repeat the parallel and cancellation cases across both CNs, then verify a
   subsequent ALTER succeeds and no lock waiter, temporary relation, task or
   lineage edge remains.

## Performance handoff

The merge-before proposal keeps the reconstructed 8192-row/2-worker/80-ALTER
counterexample, a one-worker and representative two-worker paired A/B, and
three exact original-nightly runs. The 4/8-worker and extra large-scale matrix
is proposed for follow-up QA only after a maintainer explicitly accepts the
scope change. For every run, include all requests and failed attempts, P50,
P95, max, CPU, I/O, copy starts/completions, retry counts and the exact
baseline/candidate identities.

The original nightly runner parameters and an isolated environment are not
available in this workspace. Until a maintainer supplies them and names the
QA owner, the formal performance and deployment rows remain
`BLOCKED_ENVIRONMENT`/`NOT_RUN`; local driver screening is diagnostic only.

## Handoff owner and acceptance

Developer responsibilities are implementation, deterministic regressions,
driver and evidence files. A maintainer must decide the revision-3 protocol
and acceptance-scope proposal. A named QA owner must accept the deployed SHA,
topology and workload, then report each case as PASS, FAIL or NOT_RUN with raw
artifact locations. No owner is inferred from the issue assignee.
