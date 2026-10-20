# Transactional event-ledger sink profile

Status: design-phase PASS for issue #29857; implementation PR #30561.
@aptend approved the design privately. The PR author records that manual
decision here on 2026-10-20 for the reviewed design revision
`00c4f773ba188c9d8410fd4b64d0f71e502a2c17`. This records the design
decision separately from implementation approval.

The first implementation will be a runnable Kafka Connect sink and a NiFi
ingress route through Kafka to that sink. A direct NiFi-to-MatrixOne processor
is outside this first implementation. The generic JDBC sink and NiFi `PutSQL`
do not provide this contract.

## Implementation decision

The incident is an integration-protocol gap: an upstream acknowledgement can
be lost after MatrixOne commits. A target-table primary key does not protect a
non-idempotent `UPDATE`. Generic JDBC/PutSQL cannot commit an event ledger,
effect, and database checkpoint together or fence a stale owner. A new
server-side Kafka transaction coordinator would cross a larger MatrixOne
boundary. A custom Connect `SinkTask` with application SQL tables is the
smallest deployable owner. NiFi may publish an immutable envelope into the
same Kafka topic; PublishKafka success acknowledges only Kafka ingress, while
the Connect sink owns MatrixOne progress. This route does not certify direct
NiFi PutSQL effects.

Version 1 of the runnable adapter accepts only one operation: increment a
pre-created scoped `BIGINT` counter. Its Kafka value is strict UTF-8 JSON with
exactly `version` (integer 1), `event_id` (nonempty string, <=256 UTF-8 bytes),
`operation` (literal `increment`), `delta` (signed int64), and `payload`
(string, <=1 MiB). Duplicate JSON keys, unknown fields, tombstones, missing
IDs, unsupported operations, deletes, row-PK changes, and schema evolution
fail closed before checkpoint advancement. The producer must reuse the same
stable business-operation ID and values on retry. The adapter computes the
digest, not the producer. The configured scope is never taken from the event.
Identity columns use exact UTF-8 bytes bound with JDBC `setBytes`, because
MatrixOne's default `utf8mb4_general_ci` can fold distinct text values such as
`A` and `a`. Validation covers case, trailing spaces, and Unicode variants.
The configured scope is nonempty and at most 128 UTF-8 bytes; topic is at
most 249 UTF-8 bytes. The JSON parser decodes `delta` directly as an exact
signed int64 integer token; fraction, exponent, overflow, and float
intermediates are forbidden. Escaped strings decode to Unicode scalar values;
unpaired surrogates and invalid UTF-8 fail. No Unicode normalization occurs.
The fixed frame is independent of JSON key order and whitespace. Checked-in
vectors cover negative and int64-limit deltas, escaped Unicode, and
case/trailing-space-distinct IDs.

`mo-event-frame-v1` hashes a deterministic binary frame: ASCII `MOE1`, then
scope, event ID, operation, and payload as unsigned 32-bit big-endian byte
length plus exact UTF-8 bytes, then delta as signed 64-bit big-endian. It is
SHA-256 over that frame. Test vectors fix interoperability. A new envelope or
frame version requires an explicit mixed-version migration; old and new
digests must never silently compare equal.

The Connect task describes each Kafka topic to obtain its broker topic ID,
then claims each pre-created checkpoint row under `FOR UPDATE`. A new row may
bind its null topic ID only while `last_offset=-1` and `owner_epoch=0`; a
progressed row without topic ID fails closed. A previously bound ID must match
the current broker ID. This detects a deleted/recreated topic with the same
name even when its earliest offset is 0. The task checks the bound ID again
before each nonempty `put` along with retained beginning offsets. A topic
ID is stored as the exact 22 ASCII bytes of Kafka `Uuid.toString()` in
`VARBINARY(22)`; a missing, reserved, or malformed broker ID is rejected.
The value is compared byte-exactly, with no collation or normalization. A topic
recreation requires an explicit new scope/checkpoint and replay decision;
there is no automatic rebind or safe inference from offset numbers. Existing
deployments with progressed checkpoints require an operator-verified migration
that records the original topic ID; if lineage cannot be proven, start a new
scope and replay from a trusted source. After identity validation, the task
increments `owner_epoch`, commits, and calls `SinkTaskContext.offset` with
`last_offset+1` on assignment. A missing row is an administrative error.
`put` processes each partition serially, in the original Kafka coordinates,
and computes its own delivered-record cursor from the assignment checkpoint;
no source field can attest its own predecessor. The task uses
`SinkRecord.originalTopic/Partition/Offset` so a single-message transform
cannot change its recovery coordinates. A numeric offset gap is valid when
Kafka transactions or control records occupy the missing positions. The
durable cursor advances only after a known MatrixOne commit; a transaction
uses a separate staged cursor and discards it on rollback. `preCommit`
rechecks the owner epoch and offers only next offsets durably recorded in MO.
Assignment and each `put` callback query broker beginning offsets and reject
a checkpoint seek below them. Kafka Connect initializes a fresh consumer
position before calling `SinkTask.open`; a real worker test showed
`auto.offset.reset=none` stops the task before this adapter can seek to its
MO checkpoint. The required override is therefore `auto.offset.reset=earliest`.
The adapter rejects a missing or different override and requires
`isolation.level=read_committed` to exclude aborted records. The plugin's
configuration rejects Connect's `errors.tolerance=all` and any SMT chain,
since filtering an original event before `put` would let a later checkpoint
skip its business effect. The plugin's
preflight on assignment and again before every delivered batch
detects a retention-driven reset before database effects. Each beginning-offset
query has a 10-second deadline and fails closed on timeout or broker error.
The adapter also queries `cleanup.policy` on assignment and each nonempty
`put`, requiring exactly `delete`; a compacted topic can have invisible event
holes while its beginning offset still looks safe. The three broker RPCs
(topic ID, policy, beginning offset) on every nonempty `put` callback are a measurable throughput
cost; operators should batch records and measure it under their target load.
The data topic must use delete retention
without compaction and retain records past the maximum outage/replay window.
Control and aborted offsets inside the retained range are valid holes.

Each bounded partition batch uses one pinned JDBC transaction. It locks and
verifies the checkpoint epoch and last delivered cursor; checks each ledger
ID/digest; inserts a new ledger row and applies a parameterized counter update
once, or skips a matching replay; then updates the checkpoint. Any statement
failure rolls back the entire batch. An ambiguous COMMIT result closes that
connection and reads every batch ID/digest plus checkpoint on a new connection;
it never blindly repeats business DML. Only known transaction-abort and unique
conflict errors have bounded fresh-transaction retries. `close` and `stop`
release all connections and discard epoch tokens; no SQL transaction spans
callbacks. Ledger rows remain permanent by default, subject to a configured
finite admission budget that pauses the task at capacity rather than deleting
old deduplication evidence. Backup and restore include the ledger, checkpoint,
and business table at one consistent recovery point.

The implementation proof must register and execute the real Connect task
against Kafka and MatrixOne. A fresh worker group with no broker offset must
start with `earliest`, invoke `open`, and seek from the MO checkpoint. Separate
retention tests must delete the expected next offset before assignment and
between assignment and `put`; both must stop before business SQL. A
topic-deletion/recreation test must reuse a name with fresh offsets and prove
the stored topic ID rejects it before any business SQL. A compacted topic must
be rejected before any business SQL. An aborted
transaction and its control offset within retained data must be accepted as
a legal delivered-offset hole. Required fault cells are before-COMMIT failure,
COMMIT OK loss, after-MO-COMMIT/before-broker-ACK restart, duplicate and
conflicting ID, stale epoch after rebalance, read-committed offset holes, and
whole-batch rollback. Independent audits compare every input event's digest
and effect plus all database checkpoints and broker committed offsets. A NiFi
route test sends the same immutable envelope twice and observes one MO effect.
The real adapter and database are the validation target; a state-only model
cannot certify these failure paths.

## Problem and boundary

Kafka Connect and NiFi acknowledge input independently from a database
transaction. If the MatrixOne `COMMIT` succeeds but its response is lost, a
sink that retries a non-idempotent `UPDATE` can apply the business effect twice.
The Kafka broker offset and the MatrixOne transaction therefore have two
different commit points. This is an integration-protocol gap, not evidence of
a MatrixOne ACID failure.

The profile makes the MatrixOne checkpoint table the recovery authority. A
stable business event identity and its canonical payload digest make a replay
safe, while the business effect, event ledger, and checkpoint advance are
published by one MatrixOne transaction.

The profile does not provide a cross-system two-phase commit or claim Kafka
committed offsets as proof of MatrixOne commit. The custom Connect sink is the
implementation owner; NiFi reaches it through Kafka in the first delivery.

## Contract

`scope` is connector configuration. `topic`, `partition`, and `offset` come
from the original Connect record metadata. The Kafka value has the exact
version-1 JSON fields and frame digest described above. The source supplies a
stable operation ID; the sink computes `payload_digest` and stores the literal
digest version `mo-event-frame-v1`. It rejects unsupported versions rather than
comparing different encodings as equal.

MatrixOne owns two durable state families:

```sql
CREATE TABLE mo_sink_ledger (
    scope              VARBINARY(128) NOT NULL,
    event_id           VARBINARY(256) NOT NULL,
    digest_version     VARCHAR(64) NOT NULL,
    payload_digest     CHAR(64) NOT NULL,
    first_topic        VARBINARY(249) NOT NULL,
    first_partition    INT NOT NULL,
    first_offset       BIGINT NOT NULL,
    PRIMARY KEY (scope, event_id)
);

CREATE TABLE mo_sink_checkpoint (
    scope              VARBINARY(128) NOT NULL,
    topic              VARBINARY(249) NOT NULL,
    partition_id       INT NOT NULL,
    topic_id           VARBINARY(22) NULL,
    last_offset        BIGINT NOT NULL,
    owner_epoch        BIGINT NOT NULL,
    PRIMARY KEY (scope, topic, partition_id)
);

CREATE TABLE mo_sink_totals (
    scope VARBINARY(128) PRIMARY KEY,
    n BIGINT NOT NULL
);

CREATE TABLE mo_sink_budget (
    scope VARBINARY(128) PRIMARY KEY,
    used_rows BIGINT NOT NULL,
    used_bytes BIGINT NOT NULL,
    max_rows BIGINT NOT NULL,
    max_bytes BIGINT NOT NULL
);
```

An administrator creates a checkpoint row `(scope, topic, partition, -1, 0)`
for each assigned partition, a totals row `(scope, 0)`, and a budget row with
zero use and positive finite limits. The version-1
business effect is `UPDATE mo_sink_totals SET n=n+? WHERE scope=?`. The adapter
first locks and verifies the totals row; MatrixOne reports zero changed rows
for a valid `delta=0`, which is accepted after that existence check. The
adapter may add audit columns but preserves ownership
and uniqueness constraints. No table is implicitly created in the hot path.
The transaction locks the per-scope budget row after its partition checkpoint
and before ledger lookups. For each *new* ledger row it reserves one logical
row and the sum of the stored UTF-8/binary column bytes plus fixed-width
integer bytes. Matching duplicates reserve nothing. It rejects an insertion
that would exceed `max_rows` or `max_bytes`, and rolls back all reservations
with the batch. This serializes new IDs across partitions within a scope;
that throughput tradeoff is intentional for the first profile. Logical bytes
are an admission bound, not a precise physical disk accounting measure;
operators must separately monitor physical storage and headroom.

The following invariants are normative:

1. For an accepted record, ledger insertion or replay recognition, business
   DML, and checkpoint advancement commit or roll back together.
2. `mo_sink_checkpoint` is the only recovery authority. A broker offset is a
   downstream acknowledgement hint, not a substitute for that row.
3. Only the current `owner_epoch` for a `(scope, topic, partition)` may write.
   A rebalanced or resumed old owner must fail before changing any state.
4. The same event ID with the same digest version and digest has no second
   business effect. A version or digest mismatch fails closed and does not
   advance the checkpoint.
5. Per-partition order is established by the task's serial delivery cursor,
   initialized from the MO checkpoint. The next offset need not be
   `last_offset + 1`: committed Kafka data can contain gaps from aborted
   transactions or control records. A source-supplied predecessor is ignored.
6. The Connect sink acknowledges a Kafka consumed offset only after MatrixOne
   commit is known to have succeeded. An unknown commit result is resolved by
   reconnecting and reading ledger/checkpoint, never by assuming rollback or
   blindly replaying business DML. NiFi PublishKafka acknowledges only durable
   Kafka ingress; MO completion is asynchronous and audited separately.

## State transitions

Assignment claims a partition in a short MatrixOne transaction: lock the
checkpoint row, increment `owner_epoch`, and commit. The adapter then consumes
from `last_offset + 1` in the checkpoint, not from an untrusted broker
position. If an offset at or below the checkpoint is deliberately redelivered,
it is a special replay cell: it must already exist in the ledger with the same
digest version and digest, and it is a no-op.

For one record or a bounded micro-batch, the adapter performs:

```text
BEGIN
  SELECT last_offset, owner_epoch, topic_id
    FROM mo_sink_checkpoint
   WHERE scope = ? AND topic = ? AND partition_id = ?
   FOR UPDATE
  require owner_epoch == assignment token, last_offset == local cursor,
          and topic_id == captured broker topic ID
  base = last_offset; staged = base
  SELECT budget row FOR UPDATE
  for each Kafka-delivered record, in partition order:
      require record.offset > staged, except exact base-offset replay
      SELECT digest_version,payload_digest FROM ledger by exact byte ID
      if new: reserve budget; INSERT ledger; SELECT totals row FOR UPDATE;
              require it exists; UPDATE business total; permit zero changed
              rows only for delta=0
      if existing: require same version and digest; skip business UPDATE
      staged = record.offset when greater than staged
  if staged > base:
      UPDATE mo_sink_checkpoint SET last_offset = staged
       WHERE scope = ? AND topic = ? AND partition_id = ?
COMMIT
promote local cursor = staged only after known COMMIT (or verified recovery)
Connect offset may advance to staged+1 only after that promotion
```

The first owner of the epoch and checkpoint is the adapter's assignment
coordinator; the first owner of ledger rows and business effects is the
MatrixOne transaction. `staged` is discarded on rollback. No in-memory
progress is authoritative. Mixed exact-base replay followed by new ordered
records is allowed; any offset below base or repeated offset above base fails.
A bounded micro-batch must roll back completely if any record fails.

An ambiguous COMMIT result is resolved on a fresh connection. Require the
stored topic ID to still match the owner's captured ID. If the epoch is
unchanged and the checkpoint equals the batch final `staged` offset, require
every batch ledger ID and digest to match; only then classify the batch as
committed. This also handles a batch of duplicate IDs. If the epoch is
unchanged and the checkpoint remains at `base`, classify the batch as not
published and retry under the normal bounded policy; pre-existing ledger IDs
alone do not prove this batch committed. If `staged == base` (exact replay
only), matching ledger rows and unchanged checkpoint suffice to acknowledge
the no-op. An intermediate checkpoint, mismatched ledger, changed epoch,
unavailable database, or other inconsistent state is indeterminate: stop this
task and withhold offset acknowledgement for owner handoff/inspection.
`preCommit` rechecks the epoch and checkpoint before returning any offset, so
an old owner never offers an offset after a new claim.

### Failure and recovery matrix

| Boundary | Required result |
| --- | --- |
| Process dies before MatrixOne commit | ledger, business effect, and checkpoint are absent/unchanged; replay is allowed. |
| MatrixOne commit succeeds, acknowledgement is lost | Reconnect and inspect the checkpoint and digest; replay is a no-op and may then be acknowledged. |
| Acknowledgement is sent before MatrixOne commit | This is an invalid adapter ordering. The profile intentionally exposes possible data loss. |
| New owner takes a partition | It commits a higher epoch. An old owner is rejected by the epoch check and cannot advance the checkpoint. |
| Same event ID, different digest | Stop or quarantine under a separately defined transaction; do not silently deduplicate or advance the checkpoint. |
| Business statement fails in a micro-batch | Roll back the whole batch; retry only with an explicit bounded policy for known transaction-conflict errors. |
| Broker offset has a numeric gap | Use the task's serial delivery cursor, not numeric adjacency; do not skip a record because `offset != last_offset + 1`. |
| Replay at the checkpoint | Permit only the existing ledger row for exactly `last_offset` with the same digest version and digest; it is a no-op replay cell. Older offsets and new events at/below the checkpoint fail closed. |
| Ledger cleanup or late replay | Rows are retained permanently by default. Any audited compaction must be separately versioned and cover the maximum replay/late-arrival window. |

## Connector boundaries

Kafka Connect uses the custom `SinkTask`. It resets the consumer on assignment
with `SinkTaskContext.offset(topicPartition, databaseLastOffset+1)`, processes
original record coordinates serially per partition, and returns only database
committed positions from `preCommit`. `read_committed` is required when the
source uses Kafka transactions, but source EOS does not make the database
transaction atomic.

The first NiFi route uses PublishKafka with stable, immutable event envelopes;
the Connect task above owns assignment epochs and MO transaction recovery.
NiFi success means Kafka accepted the envelope, not that MO committed it.
The source sets a stable operation ID before the first PublishKafka attempt;
one FlowFile contains one immutable envelope. PublishKafka requires broker
replicated acknowledgements, retries failed sends without regenerating the
ID, and routes irrecoverable records visibly. When transactional Kafka
publishing is configured, Connect consumes with `read_committed`. Operations
must inspect connector lag and the MO ledger to verify downstream completion.
`PutSQL` alone is insufficient for the non-idempotent effect. A direct NiFi
processor requires its own implementation and qualification.

The standard JDBC sink and generic `PutSQL` remain useful for idempotent
snapshot loads. They are outside this profile for non-idempotent event
effects. Adapters must publish their exact connector/processor version,
supported delete and schema-change operations, retry whitelist, and
quarantine behavior.

## Compatibility, limits, and operations

The profile adds application tables only; it changes no MatrixOne catalog,
wire, or Kafka external-table format. Existing Kafka external-table clients
continue to use the explicit checkpoint-table contract in
[`kafka_exttab.md`](../cn/kafka_exttab.md). A deployment can roll back an
adapter without rolling back MatrixOne: leave ledger/checkpoint rows intact
and resume from them. Backup and restore must include both profile tables and
the business tables in the same recovery plan.

The intended hot path is one indexed ledger lookup/insert and one indexed
checkpoint row lock per record, or one bounded micro-batch transaction. Ledger
rows are retained permanently by default; the profile has no automatic TTL,
because deleting a row can make a late replay execute a second business effect.
Each deployment must set a finite ledger capacity budget (rows and bytes),
alert before the budget, and pause/fail closed at the admission threshold
rather than delete rows automatically. Any audited compaction policy is a
separate versioned design. Adapters must also bound retries, batch size,
connection lifetime, and failure queues, and expose counters for committed
effects, duplicate replays, digest conflicts, epoch fencing failures, unknown
commits, and quarantined records.

The adapter database account must be restricted to the profile tables and the
declared business tables. `scope` must prevent tenants from sharing an event
identity accidentally. Event payloads and digests may contain sensitive data;
logs should record IDs and hashes, not raw payloads.

## Acceptance and evidence

The implementation must register and run a real task with MatrixOne and Kafka,
then prove:

* transaction-before-ack and commit-ACK-loss recovery with a packet-aware
  test proxy;
* process failure before and after MatrixOne commit;
* rebalance and zombie-owner fencing;
* duplicate delivery with equal digest and conflict delivery with a different
  digest;
* bounded micro-batch rollback;
* transactional Kafka offset holes using the task's serial delivery cursor;
* per-event ledger, digest, effect, checkpoint, and broker-offset audit output.

Final row counts or an unchanged snapshot are not sufficient evidence of
exactly-once effects.
