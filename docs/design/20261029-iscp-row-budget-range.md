# Row-budgeted ISCP tail ranges

- Tracking issue: #26973
- Implementation: PR #31372
- Revision: 2, requester selected bounded range probing on 2026-10-29

## Contract

Index-sync jobs select from their own watermarks with a default source-change
target of 100,000 rows. This includes existing catalog jobs whose
`TriggerSpec.TargetRows` is zero. New index jobs persist that target explicitly
and use the existing `AlwaysUpdate` trigger. A non-index job can opt in by
setting a positive target with `AlwaysUpdate`. No new trigger code or catalog
format is required; older binaries ignore the optional JSON field. MatrixOne
upgrades by full-cluster shutdown, so no mixed-version owner protocol is needed.

Index jobs with the same source table, watermark, range, and row target share
one source probe/read and fan out to their own sinks. A lagging index reads its
own range until it catches up. Index and non-index jobs never share a read.
The existing 30-minute timestamp cap continues to apply to every catch-up
job before dispatch or clean-table watermark advance.

The target counts source insert and delete operations before PK net-effect
coalescing. A completed iteration applies one complete timestamp range and
advances the existing atomic sink/watermark boundary to its selected upper
bound. The first commit timestamp in a range may exceed the target; it is
admitted whole so the job cannot starve. A later commit that would exceed the
target is left for the next iteration. Initial snapshots retain their current
protocol. The 30-minute index range cap remains an outer bound.

This is a soft row limit. A single large commit can exceed it; wide rows,
source scan work, and HNSW/cuVS model persistence are not bounded by this
change. It does not close every part of #26973.

## Why range probing is needed

The current `ChangeHandler.Next` returns after reaching its coarse batch target;
that return can split one commit timestamp. An in-memory source sorts rows by
commit time, and disk objects expose row counts, but the physical table reads
objects by creation time. A rewritten object can contain rows committed before
it was created. Thus a partially consumed handle cannot safely tell ISCP that
all changes before the most recently observed timestamp have been delivered.

The selected implementation probes complete timestamp prefixes under the same
source transaction snapshot that will perform the final read. A probe requests
all historical versions so the row count is monotonic as the upper timestamp
increases; it counts inserts and tombstones together and may stop once the count
exceeds the target. Probe handles are closed and their batches cleaned before
the next probe. If the full scheduler range fits, use it. Otherwise bisect the
full physical and logical timestamp order to find the largest complete prefix
that fits.
If no nonempty prefix fits, select the first timestamp as the oversized-commit
exception. Finally reopen the selected range for normal consumer delivery.

Probe reads disable `quickNext` so a pure insert/delete range cannot be
materialized in one unbounded batch. The final read retains `quickNext` when
the probe proved the entire selected range fits the target. The oversized-first
commit final read disables it as well. This permits the fast path when its
complete output fits, while keeping the exceptional case streamed.

Because the probe and final read use the same source transaction snapshot, the
selected range cannot acquire new committed rows between those reads. A probe
error, cancellation, or incomplete group fails without publishing progress.

## Commit and retry

ISCP sets `iterCtx.toTS` to the selected bound before creating Running status,
consumers, or data retrievers. The selected bound is the value used by
`DataRetriever.GetToTS`, the sink transaction, durable watermark update, and
in-memory state. No partial probe result is ever persisted. After a failed
iteration, the next attempt starts at `watermark.Next()` and may choose a
different safe range.

## Validation

The focused tests cover a full range, later-commit deferral, an oversized first
commit, a leading gap, raw streaming probe options and cleanup on error. The
existing ISCP package and neighboring change-handle tests must pass. Further
review should examine end-to-end index equivalence, the cost of repeated
probing, and rolling-upgrade behavior before this mode is enabled in a release.
