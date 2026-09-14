# #23684 Arrow LOAD implementation delivery decision

**Status:** implemented with a default-on policy. This is a versioned technical
decision for the implementation delivery; operational provider, aggregate-
admission, and owner approvals remain pending as recorded in
[`23684_arrow_load_release_readiness.md`](23684_arrow_load_release_readiness.md).

## Decision

The mergeable scope is the static Arrow IPC File/Stream `LOAD DATA`
implementation plus the minimal reusable safety substrate it requires:

- bounded IPC metadata validation in `arrowipc`;
- reference-counted Arrow buffer and range-lease ownership, borrowed-vector
  COW/materialization fallback, and statement-capacity accounting;
- transactional LOAD binding/conversion, FileService conditional reads, and
  worker-side admission before I/O; and
- an additive, MORPC v57-gated remote pipeline representation.

The shipped configuration defaults `enabled`, `s3-enabled`, and
`distributed-enabled` to `true`; explicit `false` values are deployment kill
switches for the corresponding surface. Every participating worker rechecks
its own setting, so a coordinator's more-permissive or stale configuration
cannot grant work after a worker-side rollback. Operational provider,
aggregate-admission, mixed-version, and owner gates remain separate acceptance
work; they do not change the default-on product contract.

## Independent implementation approval record

The independent approval for the original mergeable, fail-closed implementation
substrate is recorded in [PR review #5127791633](https://github.com/matrixorigin/matrixone/pull/28145#pullrequestreview-5127791633), submitted against exact revision
`53af58d64c2e1d928445cd8104511346a5a156a3`. Its decision is `APPROVED` for
merging that substrate, while expressly retaining provider, aggregate-admission,
mixed-version, and rollout-owner gates. This record predates the default-on
policy tracked by issue #28517 and does not claim approval for that policy or
for a deployment: the separately deferred decisions remain governed by the
readiness matrix below.

## Contract preserved by this decision

The implementation remains LOAD-only: it does not add Arrow external tables,
result scanning, a Flight listener, or a Python-UDF ABI. A statement either
commits through the normal LOAD transaction path or releases its ranges,
capacity, Arrow backing, and vectors without publishing partial data. A
conditional object range is admitted only after both reading and the provider
`Close` complete successfully. The remote payload is sent and accepted only by
v57 peers; v56 and older peers reject it.

The shared substrate is deliberately bounded to these consumers. It does not
confer policy authority on `arrowipc` or `arrowbridge`: FileService retains
object identity and provider closure, `External.Prepare` owns worker admission,
and the statement allocation account owns capacity. The detailed package and
ownership boundaries are in
[`23684_arrow_shared_substrate.md`](23684_arrow_shared_substrate.md).

## Deferred, separately approved rollout decisions

The following operational decisions remain pending until their named evidence
and owner decisions are recorded:

1. accepting the cross-worker aggregate admission/range-pressure design and
   real-provider evidence for the default-on S3/stage and distributed paths;
2. accepting remote execution during an upgrade, which requires exact-release
   mixed-version validation and an explicit supported order; and
3. deployment A/B, security/release, and SQL/execution/resource/FileService/
   storage owner acceptance.

These separations are intentional: an operator can retain a conservative
rollback by setting the gates to `false` on every CN, or stop new admissions by
turning off `enabled`. `force-materialize=true` remains a diagnostic rollback
for borrowed backing without changing SQL semantics.

## Acceptance evidence

The exact behavioral contract and tests are defined in the versioned
[`Arrow LOAD design`](../23684_arrow_load_design.md): configuration/planner and
worker gate coverage, v56/v57 predecessor/current protocol coverage,
conditional-close failure cleanup, record/dictionary window bounds, public
File/Stream and multi-CN paths, rollback, and cancellation. The release
readiness record carries the remaining evidence matrix rather than presenting
it as complete.

Any future change that makes one of the gates default-off, broadens the shared
substrate to a new transport or ABI, or changes the remote compatibility
contract requires a new decision revision and its own acceptance evidence.
