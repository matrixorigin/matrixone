# Backup GC scan evidence

Owner: #28473; implementation: [PR #29504](https://github.com/matrixorigin/matrixone/pull/29504), `4.2-dev`.
Design: gpt-6.1-sol / xhigh, session `/root/pr29504_design`, revision 1,
against `85f5cedf97e086618d78fe57c18d23c62a0c8c97`. The parent accepted the
payload-proof design before implementing it; final review uses gpt-6-astra / xhigh.

## Problem and invariant

`CopyGCDir` advances a GC filename's recovery watermark without scanning the
new interval. Backup treated that name as a complete census. A real two-backup
counterexample (one row, value 42, create 25, snapshot 27, drop 30) succeeds twice
but loses the physical object after GC metadata scanned through 20 is renamed
to cover 20..40. The historical checkpoint still references the missing file.

A deleted lifecycle may be omitted only when a successfully published GC census
covers it and its remaining set excludes the physical object. Compacted historical
references and every other live owner still retain the file. Partial reads fail
before backup completion; unknown evidence never authorizes omission.

## Existing owner and representation

`GCWindow` owns one additional contiguous proven interval. The existing filename
and `tsRange` continue to own recovery progress. No new GC policy or execution
path is introduced.

Metadata block 0 remains the original single varchar stats column. An optional
block 1 has the same schema, exactly one row: `GCS1` followed by start and end TS
bytes (12 bytes each). This fixed-size payload is committed by the same object
write as the remaining set, even when that set is empty. Explicit malformed
proof fails closed. Legacy one-block metadata has unknown coverage.

Only completely read incremental checkpoint intervals grant proof. Globals and
compacted checkpoints filter history and cannot certify absence. Overlapping or
adjacent proven ranges may join; a gap retains a proven suffix, never a hull.
Proof is installed only after scan, sinker sync, and metadata publication succeed.
GC filtering preserves it; clone/replay carry it; close clears it. Restoring or
renaming metadata leaves the payload unchanged. An old reader consumes block 0;
an old writer may discard proof, which makes the next new-version read conservative.

Backup inspects eligible GC metadata and chooses one usable payload interval,
preferring the earliest start then latest end. It reads only that window's
remaining rows. Lifecycle resolution, compacted-reference retention, copy-required
OR, destination reuse, and required-file errors keep their existing owners.
The descriptor loader and GC collector must propagate row-read failures; the
unused `softDeletes` loader parameter and collection branch are removed.

## Rejected alternatives and cost

Filename heuristics cannot distinguish inherited records. `.snap/.acct` files
are written before scan completion and cannot prove it. Protecting only globals
misses older incremental history; reconstructing snapshot policy in backup would
duplicate GC responsibilities. An interval list or new manifest is unnecessary.

State grows by two timestamps per window and one tiny metadata block per rewrite.
Backup reads candidate metadata and streams one remaining set. No worker, lock,
retry, global cache, or unbounded coverage history is added. Legacy objects whose
proof was lost can still cause conservative backup failure; new scans recover
safe omission only inside their proven interval. This cannot repair incomplete
backups already published or make an old buggy selector safe after rollback.

## Validation contract

Use actual checkpoint/GC writers and cache-disabled object readback: repeated
backup with both legacy and new metadata; compacted history; genuine reclaimed
objects; owner/type/epoch and timestamp boundaries. Verify real scan publication,
read/write/cancel failures, empty and nonempty proof round trips, gap merging,
filtered checkpoint kinds, malformed proof, and the old block-0 reader. Keep the
existing lightweight file-service fixtures; no service or large dataset is needed.
Run focused tests, owning backup/logtail/GC packages and direct datasync consumers.
Actual customer-cluster restore and throughput gains are not claimed.
