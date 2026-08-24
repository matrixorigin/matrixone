# Kafka external table (`ENGINE = KAFKA`)

Implements issue #27518: read a Kafka topic partition through an external
table, with per-message metadata columns, WHERE-driven read controls, and
`LAST_KAFKA_MESSAGE_ID()` for exactly-once chaining.

## 1. DDL

```sql
create external table kt (
    a int,
    b varchar(100)
) engine = kafka with (
    'brokers'    = 'host1:9092,host2:9092', -- required, comma-separated host:port
    'topic'      = 'mytopic',               -- required
    'partition'  = '0',                     -- optional, default 0
    'autocommit' = 'false',                 -- optional, default false
    'group'      = 'g1',                    -- optional, default mo_kafka_<db>_<table>
    'format'     = 'csv',                   -- csv | jsonl, default csv
    'separator'  = ','                      -- csv only, one character, default ','
);
```

Options are validated at CREATE (`pkg/sql/kafka.ParseTableOptions`) and
persisted in the planner-owned `rel_createsql` envelope (`/* MO_KAFKA: ... */`)
plus the durable `features.KafkaExternal` bit — envelope and bit must agree at
recognition time (`IsKafkaTableDef`), same anti-forgery rule as
datastream/foreign tables. `SHOW CREATE TABLE` round-trips every option
(nothing is secret in v1). `ALTER TABLE` is rejected; drop and recreate.

## 2. Message format

Each Kafka message **value** must parse to exactly one record:

* `format=csv`: one CSV record with the configured separator, plain quoting,
  no backslash escaping. The field count must equal the declared column count.
* `format=jsonl`: one JSON object; every declared column name must be a key.

A message that parses to zero or multiple records fails the query ("did not
parse to exactly one record"), as does a wrong field count.

## 3. Synthetic columns

Hidden from `SELECT *`, selectable by name, ColId-scoped
(`catalog.IsKafkaHiddenCol`) so a pre-existing real column with the same name
in an ordinary table keeps working; the names are reserved for new schemas.

| column | type | meaning |
|---|---|---|
| `__mo_message_id` | bigint | Kafka offset of the message |
| `__mo_message_ts` | timestamp(3) | Kafka message timestamp |
| `__mo_message_key` | varchar | message key (NULL when absent) |
| `__mo_message_value` | varchar | raw message value |
| `__mo_read_start_id` | bigint | read control (below); reads back the effective value |
| `__mo_read_size` | bigint | read control; 0 = unlimited |
| `__mo_read_timeout` | bigint | read control, seconds; 0 = block forever |

## 4. Read controls

Top-level `<control> = <constant>` conjuncts are resolved at compile time
(`external.DeriveKafkaReadControl`) and **consumed** — they position the read
instead of filtering rows:

```sql
select * from kt
where __mo_read_start_id = 1000
  and __mo_read_size = 1000000
  and __mo_read_timeout = 10;
```

* `__mo_read_start_id` is the **last consumed offset**: reading begins at
  `start_id + 1`. With `autocommit=false` it is **required**, and the read
  offset is committed at that position before reading (committing the same
  start twice is idempotent, so a retried read returns the same data);
  `-1` means "from the earliest". With `autocommit=true`, `0` (the default)
  means earliest-inclusive and `-1` means latest.
* `__mo_read_size` caps the message count (default unlimited).
* `__mo_read_timeout` ends the read when no new message arrives within that
  many seconds (default 10; 0 blocks until cancelled).

Contradictory duplicate controls are an error. Any other use of a control
column (ranges, ORs) stays an ordinary row filter over the effective value.

## 5. `LAST_KAFKA_MESSAGE_ID()`

Session builtin (`id 576`): the offset of the last message a **completed**
Kafka scan returned in this session, NULL before any scan. With
`autocommit=false`, feeding it back as the next `__mo_read_start_id` gives
exactly-once consumption:

```sql
select * from kt where __mo_read_start_id = 1000 and __mo_read_size = 100000;
select last_kafka_message_id();      -- e.g. 4711
select * from kt where __mo_read_start_id = 4711 and ...;  -- continues after
```

An aborted scan (error/cancel) updates neither the session id nor (with
`autocommit=true`) the committed offset. With `autocommit=true` a completed
scan commits `last+1` (Kafka next-to-read convention).

## 6. Execution

`plan.KafkaScan` (ExternType `KAFKA_TB = 7`) rides the shared external-scan
pipeline: `compileKafkaScan` pins the scope to the session CN with Mcpu=1
(session state + ordered partition read) and participates in the shuffle
receiver graph like foreign scans. `KafkaReader`
(`pkg/sql/colexec/external/reader_kafka.go`, franz-go client) streams message
values as lines into the shared CSV/jsonline machinery; a per-message metadata
FIFO pairs each converted record with its message (order-based, re-checked at
EOF), and `getFieldFromLine` synthesizes the metadata columns.

v1 limits: one partition per table (create several tables for several
partitions), plaintext brokers (no SASL/TLS yet), no discovery of committed
group progress as an implicit start (state your start explicitly or use
autocommit earliest/latest).

## 7. Tests

* `pkg/sql/kafka`: option/envelope round-trip.
* `pkg/sql/colexec/external`: `reader_kafka_test.go` runs the full reader
  against an in-process `kfake` cluster (csv, jsonl, metadata columns,
  start/size caps, commit and session-id semantics, malformed messages);
  `kafka_read_control_test.go` covers the control derivation.
* `pkg/sql/plan` / `compile` / `parsers` / `function`: DDL build, SHOW
  CREATE, recognition, hiding, ALTER guard, compile dispatch, shuffle
  receiver pinning, builtin.
* BVT `test/distributed/cases/function/kafka_exttab.sql`: DDL/SHOW
  CREATE/guard/control-validation paths that need no broker.
