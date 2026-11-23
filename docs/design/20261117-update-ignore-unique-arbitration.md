# Unique arbitration for repeated-target UPDATE IGNORE

Issue: [#31824](https://github.com/matrixorigin/matrixone/issues/31824)

Implementation: [#32827](https://github.com/matrixorigin/matrixone/pull/32827)

## Problem and contract

An UPDATE can write the same physical table through more than one alias. Under
IGNORE, each alias can change a row that an earlier alias already changed or
rejected. Checking each alias against the statement snapshot independently is
not sufficient: a later alias can claim a key already accepted for another row,
or a surviving key can be mistaken for a released snapshot key. In either
case the table or its unique index can contain conflicting entries.

Keep the existing ordered alias stages. For each physical row, S is its
statement-snapshot image and C its latest accepted image, initially S. Each
active alias forms a candidate N from C, including recomputed generated
columns. If its constraints reject it under IGNORE, retain C and report the
rejected candidate; otherwise N becomes the next C. Earlier rejected
assignments are not resurrected. Each accepted physical key must have at most
one owner under the actual index equality and encoding, including NULL,
composite, prefix and collation keys. Internal rows and inactive or no-op
assignments do not themselves create duplicate warnings. A later failure of any
constraint must reject the candidate atomically.

The existing transaction remains the publication and rollback owner. A normal
non-IGNORE UPDATE continues to validate the final merged physical image; this
design does not replace that path. A single writable target and another physical
table in the same statement retain their existing independent checks. Existing
guards for repeated targets changing primary or partition identity, including
generated dependencies, remain in force.

## Existing owners and data flow

The planner owns ordered candidate stages, S, C, and the projections used
by each affected unique index. A candidate's old key is derived from C, not
from S. Before checking an alias stage, the planner also represents:

* **Reservation:** an accepted C key, even if its owner has no surviving
  candidate after CHECK or a preceding unique-index check.
* **Release:** the snapshot key of a row whose accepted C has a different
  key for this unique index, using the index's actual encoding and null-safe
  equality. A payload-only change does not release an unchanged key. This
  evidence travels with the snapshot key and only exempts that old snapshot
  entry from a probe; it cannot claim a candidate.

These are auxiliary rows in the existing build input; a NULL physical row ID
distinguishes them from candidate rows. The delete marker distinguishes release
from reservation. They cannot reach the DML output or create warnings.

HashBuild owns per-key arbitration using the priority **current-key owner >
reservation > changed claimant > release**. Thus a current owner survives its
own reservation, and a reservation defeats a competing changed claimant
regardless of input arrival order. Release evidence survives compaction and
rebuild, but never displaces an owner or claimant. The ordinary key and NULL
rules remain authoritative; priority does not impose an order between unrelated
keys. Within the existing alias-order contract, a failed candidate retains C.

DedupJoin probes snapshot keys, observes releases, filters rejected candidates
and renders diagnostics for real candidates. Compiler/operator-copy and
pipeline serialization preserve the auxiliary-row column reference, including
parallel copies; spill rebuild carries the same metadata and row roles. A
selection mapping used for warnings must refer to the retained physical build
row after compaction, including NULL keys and batch boundaries.

## Alternatives and bounds

Keeping per-alias snapshot checks cannot establish the accepted-image invariant.
Depending on UNION arrival order is invalid when inputs execute concurrently.
A separate global arbiter or new storage/state machine duplicates the existing
materialized alias stages and introduces additional ownership and failure
contracts. Arbitration inside the existing build/probe path keeps one owner for
each part of the work and requires no new long-lived service or persistent
format.

For N retained rows, A affected alias stages and I affected unique
indexes, each index stage can materialize up to approximately N candidate,
N reservation and N release rows. It must also probe the snapshot index: if
M is the number of index rows examined per stage, work can scale with
A * I * (N + M), and M may be much larger than the affected-row set. The
existing base also scans/probes the index; the comparison must measure the
whole stage. Transient candidate state scales with N and carried key/row width,
plus the existing hash, selection and spill costs. HashBuild owns and accounts
for its vectors, selection maps,
bitmaps and per-group priority/survivor scratch; the existing spill subsystem
owns spill partitions, files and rebuild. Cancellation, allocation failure,
partial initialization, retry and reset/free must release these resources and
leave the transaction able to roll back. Prepared execution may reuse a plan,
but no per-execution ownership or warning state may survive into the next run.

The correction is allowed to add work on the affected path to establish
correctness. Compare latency and memory against the same fresh base on narrow
and wider keys, conflicting and non-conflicting inputs, and an unaffected
single-target control. Investigate a repeatable regression above 10% on the
unaffected control; report the measured range and affected-path cost rather
than claiming a speedup from a different workload. Spill and memory-accounting
tests must demonstrate bounded cleanup; the result does not set a new global
memory or disk limit.

## Compatibility and validation

The added plan/pipeline references are transient in-cluster execution
metadata; they do not change persisted catalogs, table or index formats, or the
client protocol. MatrixOne's supported upgrade stops the whole cluster before
replacing binaries. Under that contract all in-cluster peers run the same
binary; rolling mixed-version operation and downgrade are unsupported, so this
change adds no per-feature compatibility branch or version gate. A new binary
must still read data written by supported released versions.

The validation must cover:

* Public SQL for previous-accepted ownership, snapshot release, candidate and
  reservation arrival orders, CHECK rejection, multiple unique indexes,
  generated keys, NULL/prefix/composite/collated keys, successful controls and
  exact warnings; compare base-table and forced-index results.
* Independent MySQL observations for the no-op owner plus one or two genuinely
  rejected candidates, including affected rows, warning count and row values.
* Rebased non-IGNORE final-image behavior, protected-key guards, a single
  writable target and a different physical target in the same statement.
* HashBuild/DedupJoin selection, copies and serialization, resident execution
  and spill/rebuild with the same role ordering; errors and cancellation clean
  up execution resources and roll back partial changes.
* Relevant owning-package tests, a real multi-CN execution path, targeted
  static checks, and same-host cost comparisons. Evidence must identify the
  tested source, topology, workload and terminal result.
