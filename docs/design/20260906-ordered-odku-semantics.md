# Ordered `ON DUPLICATE KEY UPDATE` Semantics

Status: Implemented; pending reviewer acceptance

## Problem

`INSERT ... ON DUPLICATE KEY UPDATE` is both an ordered logical program and a
physical table mutation. Collapsing assignments or duplicate input keys loses
observable SQL semantics, while treating every logical action as a physical
write needlessly rewrites base/index data and fires implicit `ON UPDATE`
expressions.

The same statement can also cross planner, DEDUP, distributed pipeline,
`MULTI_UPDATE`, regular and irregular index maintenance, partition/direct/S3
writes, client affected-row reporting, and rolling-upgrade protocol boundaries.
Those consumers need one explicit contract rather than independently inferring
"changed" from the final batch.

## Invariants

1. Assignment expressions execute left-to-right. A repeated target is not
   deduplicated; each RHS observes the row image produced by the prior RHS.
2. Every duplicate input row is a logical action. Constraints that can be
   affected by ODKU are validated for every action before any non-final action
   is discarded.
3. Logical affected rows and physical mutation are independent:
   a changed duplicate contributes 2, a no-op contributes 0 or 1 under
   `CLIENT_FOUND_ROWS`, while storage/index writers receive only the final row
   and only when its physical image changed.
4. A pure no-op restores the stored row image before CHECK/FK/index/RETURNING
   consumers. It must not leak an implicit timestamp or regenerated value.
5. FK validation is eligible only for an inserted row or an action that changed
   that FK tuple. An unrelated update must not revalidate historical orphan
   data created while `foreign_key_checks=0`.
6. Value-change detection follows the target SQL type: NULL-aware, CHAR PAD
   SPACE, JSON structural comparison, declared FLOAT scale, signed zero and NaN
   peers, and element-wise vector semantics. The hot path performs no
   `interface{}` conversion or heap allocation.
7. Malformed/missing metadata fails closed. A mixed-version CN that cannot
   interpret the action/count/physical markers must not execute the plan.
8. Target selection is itself statement-ordered. For each input row, PRIMARY
   then UNIQUE constraints are considered in definition order against both the
   pre-statement snapshot and earlier successful INSERT actions. The first
   conflict wins. An UPDATE action does not publish unused incoming keys.
9. The resolved target identity is also the base-row lock identity. A secondary
   UNIQUE conflict must not lock the unrelated incoming primary key, and a
   synthetic-primary-key table must not omit the base-row lock after resolution.
10. In WriteS3 plans, the final `UpdateFlushS3Info` operator is the sole owner of
    client-visible ODKU affected rows. Every writer transfers its accumulated
    logical count in-band exactly once, independent of physical blocks, scope
    placement, partition routing, or whether the final action is a no-op.

## Plan and execution model

The planner retains an ordered `(target-column, expression)` stream. DEDUP
replays it against a stable row image and emits:

- the materialized current row;
- an accumulated affected-row weight;
- a physical-change marker for the final image;
- when action validation is required, an action-final marker and constraint
  eligibility markers.

Action validation is enabled only when a changed target (including its
generated-column closure) can affect CHECK, FK, or NOT NULL semantics.
CHECK/FK/NOT NULL assertions are barriered before the action-final filter. The
filter then reduces each key group to its final row. Constraints independent of
the update still validate newly inserted groups through a compact final-row
eligibility marker; they neither revalidate an existing historical row nor
force every duplicate action through the validation pipeline.
The action stream and each constraint's metadata have separate gates. In
particular, FK eligibility columns are produced, propagated, consumed, and
validated only when `foreign_key_checks` enables non-self FK checking. CHECK or
NOT NULL validation may still require the action stream while FK checking is
disabled; those states must never manufacture or consume FK metadata.
`MULTI_UPDATE` consumes the count marker independently and applies the physical
marker uniformly to base, regular-index, irregular-index, partition, direct,
and S3 writers.

For tables with secondary UNIQUE constraints, the planner also emits one
ordered target-arbitration stage before DEDUP. Each constraint contributes its
incoming key and nullable pre-statement target identity. The stage owns one
hash map per constraint, a single vector of identities for rows accepted as
INSERTs, and compact map-group-to-identity ordinals. Rows with no conflict
publish all non-NULL keys atomically; rows resolved as UPDATE publish none.
The resolved identity becomes the DEDUP key for both explicit and synthetic
primary-key tables and the base-table lock key. This prevents a static snapshot
probe from turning two same-statement ODKU actions into a duplicate error or an
insert of the wrong row, and prevents a secondary-UNIQUE conflict from updating
one row while locking an unrelated incoming primary key.

WriteS3 writers cannot own client-visible ODKU counts because parallel and
remote writers may live below merge `PreScopes`, outside the operator chain
walked by statement affected-row collection. A writer therefore accumulates the
logical weight separately from its physical block row counts and emits a small
internal affected-row control record at terminal flush. Independent writer
records are additive; partition-local writers sharing one partition wrapper
drain that wrapper's count once. The coordinator consumes these records before
table resolution or batch decoding, and storage records retain their existing
physical row-count contract. A pure no-op can consequently report
`CLIENT_FOUND_ROWS` without manufacturing a storage write.

Self-referencing FKs retain their existing statement-level post-write check in
this change. Their parent domain can include rows created by the same statement,
so moving them into the row-scoped parent scan requires a separately specified
statement-local parent-key source; treating only the pre-statement table as the
parent domain would reject valid self-reference inserts. The ordered action
work must not claim row-scoped self-FK semantics until that source exists.

## Ownership and unhappy paths

- DEDUP owns its stable expression-result vectors until `Free`; swapping a
  logical row image transfers vector references but never drops the owned pool.
- A compiled materialized source is closed once per pipeline attempt by the
  attempt owner. A receiver-less SINK rejected during compilation releases only
  its newly compiled producer scopes; it does not assume ownership of a source
  registered elsewhere.
- Errors from action validation abort the statement before physical writes are
  committed. The final-action barrier cannot suppress a prior action error.
- ALTER/DROP INDEX owns hidden child-relation deletion under the parent DDL
  lifecycle. It must not recursively enter SQL DDL and acquire child metadata
  locks in the inverse order of concurrent DML.
- DEDUP action replay is resumable. Each `Call` emits at most one ordinary
  result batch, bounded by `DefaultBatchSize` and a soft byte budget. Because
  expressions are materialized before their output size is known, a batch may
  cross the byte budget only with its final admitted row; this also guarantees
  progress for an intrinsically oversized row. Probe-row,
  action, and unmatched-group cursors are reset on error, reuse, and spill
  bucket transitions.
- Target-arbitration hash entries, accepted identities, and group-to-identity
  ordinals are owned by the statement allocation account. Exact arbitration
  remains linear retained state, but exceeding the statement budget fails
  through resource admission instead of escaping accounting into the Go heap.
- WriteS3 logical counts have one scalar pending owner per writer operator (or
  partition wrapper). A successful terminal flush transfers and clears it once;
  reset/error discards it with the failed attempt. The final flush operator is
  the only component that publishes the transferred count to statement state.
- No goroutine, retry loop, or unaccounted retained history is introduced.

## Performance model

Tables whose ODKU targets cannot affect an action-level constraint retain the
one-row-per-key-group fast path, even when the table has unrelated CHECK or FK
metadata. Constraint-bearing statements pay for action rows only for CHECK/FK
dependencies intersecting the generated-column closure of the assigned
columns, or for a nullable expression targeting NOT NULL. Unaffected
constraints validate an inserted group once. Fixed scalar comparisons use
typed vector reads with zero allocation; varlen values compare their existing
bytes, and JSON decoding is limited to JSON columns.

Required performance evidence covers no-conflict, distinct conflict, hot-key,
pure no-op, CHECK/FK/NOT NULL action validation, and a wide varlen row. Compare
the same binary/mode/data on the same machine and report medians rather than an
individual run.

Target arbitration is linear in input rows times usable UNIQUE constraints.
Its retained state contains one copy of each accepted INSERT identity, one hash
entry per published non-NULL key, and one 8-byte identity ordinal per hash
entry. It deliberately does not copy a possibly-wide primary key into every
UNIQUE-key map. All three components use the statement allocation account; the
account capacity is the admission bound for a statement whose exact conflict
set does not fit in memory.

The WriteS3 count protocol adds at most one five-column control row per writer
operator and no payload serialization, S3 I/O, or per-input-row allocation. It
does not change ordinary INSERT/UPDATE plans or ODKU's per-row hot path.

## Validation matrix

| Contract | White-box proof | SQL-visible proof |
|---|---|---|
| ordered and repeated assignments | DEDUP replay tests | dependent/repeated SET cases |
| action validation precedes final filtering | plan barrier/metadata tests | invalid-then-valid CHECK/FK/NOT NULL rollback |
| constraint-sensitive action emission | dependency-closure plan tests and hot-group compact/action operator tests | unrelated-column ODKU on CHECK/FK tables |
| no-op does not write | change/count marker tests | ROW_COUNT, timestamp, base and forced-index state |
| type-aware equality | scalar/vector comparator tests and allocation oracle | CHAR/JSON/scaled FLOAT no-op cases |
| distributed compatibility | encode/decode and version-fence tests | mixed-version rejection coverage |
| DDL child ownership | deterministic barrier/fake-engine test | concurrent DML versus ALTER DROP INDEX harness |
| statement-local target selection | ordered multi-key arbiter and reset tests | repeated new PK/UNIQUE, fake/composite PK, nullable key, conflicting-target priority |
| bounded action replay | row/byte boundary, probe/finalize resume, reset tests | existing hot-key and wide-row coverage |
| arbitration memory admission | allocation-account provenance, capacity failure, reset tests | large-data validation belongs to the performance harness |
| resolved target locking | typed LOCK_OP input for explicit and synthetic PK tables | concurrent secondary-UNIQUE conflict versus direct target-row UPDATE |
| WriteS3 affected-row ownership | no-op control record, multi-writer sum, partition drain-once, reset tests | large `INSERT ... SELECT ... ODKU` in single-/multi-scope execution |

Every failure case also checks durable table/index state after rollback. Tests
use barriers or direct typed state; sleeps and probabilistic retries are not
correctness oracles.
