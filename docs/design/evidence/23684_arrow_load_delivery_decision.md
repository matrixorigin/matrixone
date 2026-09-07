# #23684 Arrow LOAD implementation delivery decision

**Status:** implemented with a fail-closed default. This is a versioned
technical decision for the implementation delivery; it is not an approval to
enable Arrow LOAD in a production deployment. Independent rollout and owner
approvals remain pending as recorded in
[`23684_arrow_load_release_readiness.md`](23684_arrow_load_release_readiness.md).

## Decision

The mergeable scope is the static Arrow IPC File/Stream `LOAD DATA`
implementation plus the minimal reusable safety substrate it requires:

- bounded IPC metadata validation in `arrowipc`;
- reference-counted Arrow buffer and range-lease ownership, borrowed-vector
  COW/materialization fallback, and statement-capacity accounting;
- transactional LOAD binding/conversion, FileService conditional reads, and
  worker-side admission before I/O; and
- an additive, MORPC v53-gated remote pipeline representation.

This scope does not authorize a deployment to turn on the feature. The shipped
configuration defaults `enabled`, `s3-enabled`, and `distributed-enabled` to
`false`; a CN without an explicit setting rejects the corresponding Arrow LOAD
before I/O. Every participating worker rechecks its own setting, so a
coordinator's more-permissive or stale configuration cannot grant remote work.
Keeping the implementation present but unreachable by default lets normal
release integration validate its contracts without silently broadening a
deployment's data-plane surface.

## Contract preserved by this decision

The implementation remains LOAD-only: it does not add Arrow external tables,
result scanning, a Flight listener, or a Python-UDF ABI. A statement either
commits through the normal LOAD transaction path or releases its ranges,
capacity, Arrow backing, and vectors without publishing partial data. A
conditional object range is admitted only after both reading and the provider
`Close` complete successfully. The remote payload is sent and accepted only by
v53 peers; v52 and older peers reject it.

The shared substrate is deliberately bounded to these consumers. It does not
confer policy authority on `arrowipc` or `arrowbridge`: FileService retains
object identity and provider closure, `External.Prepare` owns worker admission,
and the statement allocation account owns capacity. The detailed package and
ownership boundaries are in
[`23684_arrow_shared_substrate.md`](23684_arrow_shared_substrate.md).

## Deferred, separately approved rollout decisions

The following are not implied by this implementation decision and remain
blocked until their named evidence and owner decisions are recorded:

1. enabling local Arrow LOAD for a deployment;
2. enabling S3/stage or distributed execution, which additionally requires a
   cross-worker aggregate admission/range-pressure design and real-provider
   evidence;
3. enabling remote execution during an upgrade, which requires exact-release
   mixed-version validation and an explicit supported order; and
4. deployment A/B, security/release, and SQL/execution/resource/FileService/
   storage owner acceptance.

These separations are intentional: an operator can always retain the
conservative rollback by leaving the gates unset, or stop new admissions by
turning them off on every CN. `force-materialize=true` remains a diagnostic
rollback for borrowed backing without changing SQL semantics.

## Acceptance evidence

The exact behavioral contract and tests are defined in the versioned
[`Arrow LOAD design`](../23684_arrow_load_design.md): configuration/planner and
worker gate coverage, v52/v53 predecessor/current protocol coverage,
conditional-close failure cleanup, record/dictionary window bounds, public
File/Stream and multi-CN paths, rollback, and cancellation. The release
readiness record carries the remaining evidence matrix rather than presenting
it as complete.

Any future change that makes one of the gates default-on, broadens the shared
substrate to a new transport or ABI, or changes the remote compatibility
contract requires a new decision revision and its own acceptance evidence.
