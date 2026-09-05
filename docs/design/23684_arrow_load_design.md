# #23684 Arrow LOAD design

Status: proposed; independent design approval is pending.  This is a versioned
design artifact for implementation PR #28145, not evidence of approval.  The
release-readiness matrix is maintained in
[`evidence/23684_arrow_load_release_readiness.md`](evidence/23684_arrow_load_release_readiness.md).

## Problem and scope

`LOAD DATA` previously accepted only text-oriented formats.  Arrow IPC File and
Stream inputs need a bounded, transactional ingestion path that preserves SQL
mapping and error semantics.  This design adds Arrow only to `LOAD DATA`; it
does not make Arrow a general external-table, result-scan, or Flight protocol.

The invariant is: every accepted Arrow input is decoded against the planned
schema, charged to the executing statement, and either becomes one ordinary
transactional LOAD result or leaves no visible partial result.  A remote source
or distributed worker is admitted only when **that executing CN** has enabled
the applicable rollout gate before it opens I/O.

Non-goals are aggregate cluster quota/range planning, automatic cloud-provider
enablement, and a new long-lived cache or controller.  Those are deliberately
separate release gates, not implied by local Arrow support.

## Contract and alternatives

The input contract follows Apache Arrow IPC (File and Stream container framing)
as implemented by Arrow-Go v18.  File footer/message metadata and decoded-size
claims are checked before allocation.  Schema binding is compile-time and the
execution reader rejects schema/type drift.  Arrow fields map by name by
default, or by the explicit positional option; normal LOAD target rules still
own casts, generated columns, constraints, and transaction commit.

We considered (1) materializing every Arrow value, (2) accepting Arrow only
through a conversion sidecar, and (3) bounded borrowing with materialization
fallback.  Always materializing is simpler but makes large varlen loads pay an
avoidable copy; a sidecar adds an operational data-plane boundary and retry
contract.  The selected option borrows only reference-counted Arrow backing
that passes a pin-amplification bound, otherwise materializes.  `force-
materialize` is an explicit rollback/diagnostic switch, not a different SQL
semantics mode.

## Configuration, rollout, and compatibility

`frontend.arrow-load.enabled` defaults to true for local File/Stream LOAD.
`s3-enabled` and `distributed-enabled` default to false.  Planning samples the
settings for its scope, but every `External.Prepare` repeats the gate using the
worker CN's `ParameterUnit` before constructing an Arrow reader:

| Source/execution | Required worker settings | Default |
| --- | --- | --- |
| local File or Stream | `enabled` | enabled |
| direct S3 or S3-backed stage | `enabled`, `s3-enabled` | rejected |
| distributed Arrow scope | `enabled`, `distributed-enabled` | rejected |
| distributed S3 scope | all three | rejected |

This prevents a coordinator's stale or more-permissive configuration from
bypassing a worker's fail-closed policy during a rolling change.  The execution
scope remains a positive compile authorization and MORPC v48 remains the
receiver compatibility gate.  v48 is `up/main` v47 plus one on the delivery
rebase; older peers reject the additive Arrow pipeline fields, so mixed-version
deployments must drain or keep remote Arrow modes disabled.  Downgrade is safe
under the same gates because local Arrow does not advertise a remote capability.

## Ownership and failure model

The compiler owns scope construction and immutable object identity snapshots.
`External.Prepare` owns worker admission; `ArrowReader` owns one open IPC
reader; FileService owns provider response closure; and the statement allocation
account owns capacity reservations.  A range is published only after bytes are
read **and** the conditional provider `Close` succeeds.  Read, probe, close,
parse, conversion, cancellation, and commit failure all abort the reservation;
the released lease returns its exact charge once.

For a record batch, the reader validates immutable shape and validity once at
record admission.  It then budgets and converts windows.  Budgeting performs
only O(columns) structural validation; each conversion checks its selected
window and its cancellation checkpoint.  Thus a record split into K output
batches has linear total validity work rather than K full-record scans.  The
statement account bounds retained range and vector capacity; pin amplification
forces materialization instead of retaining an oversized source allocation.

Terminal ownership is explicit: EOF calls `ArrowReader.Close`, which releases
IPC references and returns the underlying stream close result.  `External.Call`
propagates that terminal error rather than reporting successful completion.
`Reset`/`Free` remain best-effort cleanup only after the execution result is
already determined.

## Validation and acceptance

Focused unit coverage proves worker-side local/S3/distributed gates, positive
compile scope propagation, conditional read success plus close-only
`ErrObjectChanged` with zero committed capacity, terminal external close-error
propagation, malformed IPC/schema/null cases, and budget behavior.  Existing
Arrow File/Stream, identity, MinIO, multi-CN, rollback, and SQL BVT cases cover
the consumer and public paths.  The immediate predecessor compatibility test
is retained with the MORPC v48 gate.

Before remote production enablement, the readiness record requires a bounded
cross-worker aggregate admission design, real-provider evidence, exact-release
mixed-version validation, deployment A/B, and independent SQL/execution/
resource/FileService/storage/security-release decisions.  Until then the
defaults above are the conservative rollback and failure-containment plan.

## Open decisions

Independent owners must approve the proposed remote rollout only after the
listed acceptance evidence exists.  No product choice is needed to ship this
documented local-only default: unapproved remote modes remain fail-closed.
