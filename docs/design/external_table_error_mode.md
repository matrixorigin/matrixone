# External table error mode

Issue: [#27517](https://github.com/matrixorigin/matrixone/issues/27517)

A scan of an external table fails the whole statement on the first record it
cannot parse. One bad line in a million-line file means no result at all, and
no way to find out which line it was without reading the file by hand.

Error mode makes a bad record a *row* instead of a *failure*, and it is
switched on by asking for it — by selecting one of two columns the query would
otherwise not mention. A query that does not mention them behaves exactly as
it did before, down to the error message.

## Syntax

Three columns exist on every external table (`ENGINE=EXTERNAL`, `KAFKA`,
`DATASTREAM`, and foreign tables). They are hidden: `select *`, `desc` and
`show create table` do not list them, and they can only be named explicitly.

| Column | Type | Meaning |
|---|---|---|
| `__mo_file_line` | `bigint` | the physical line the record starts on (`NULL` for Kafka, which has no lines) |
| `__mo_error_message` | `varchar` | why the record failed, `NULL` if it parsed |
| `__mo_error_text` | `varchar` | the record as written in the source, `NULL` if it parsed |

```sql
create external table t (a int, s varchar(20))
  infile{'filepath'='/data/rows.csv'};

-- unchanged: fails on the first bad record
select * from t;

-- reports them instead
select a, s, __mo_file_line, __mo_error_message, __mo_error_text from t;

-- the two halves of a load
insert into good select a, s from t where __mo_error_message is null;
insert into rejects
  select __mo_file_line, __mo_error_message, __mo_error_text from t
  where __mo_error_message is not null;
```

On a failed row every user column is `NULL` — including a column that happened
to convert before the failure, because the record is not trustworthy — while
every column the scan *synthesizes* keeps its value: `__mo_filepath`,
`__mo_file_line`, and the Kafka metadata columns. Those are what let a user
find the record again.

## The switch is the projection

`__mo_error_message` and `__mo_error_text` turn tolerance on. `__mo_file_line`
does **not**: it is position metadata, and a query that only wants line numbers
must still fail on a bad record, or `select a, __mo_file_line from t` would
silently start returning half a file.

The decision is made once per scan, in `resolveExternalErrorMode`
(`pkg/sql/colexec/external/external.go`), from the attribute list the operator
was given. Column pruning has already run by then, so an unmentioned column is
simply absent and the scan takes the historical path with no per-row test to
pay for. The check is keyed on the reserved column ids, not on the names, so a
table that predates the reservation and has a user column called
`__mo_error_message` cannot switch it on.

### Pruning has to be taught one thing

Whether a record failed is a property of the record, not of the projection.
Pruning breaks that: with the user columns pruned away nothing is converted, so
nothing fails, and

```sql
select __mo_error_message from t;                         -- which records failed?
select __mo_file_line, __mo_error_text from t where __mo_error_message is not null;
```

— the two queries the feature exists for — answer "none".

So when a scan keeps one of the two error columns, `remapAllColRefsForConsumer`
(`pkg/sql/plan/query_builder.go`) keeps every record-backed column of that
external scan even if the query never references it. The scan's own
`ProjectList` still projects only the referenced ones, so the extra columns
occupy a batch vector and go no further. Synthetic columns
(`catalog.IsReservedExternalColName`) and hidden columns are pruned as before —
they are not read from the record, so they cannot fail.

The cost of this is paid only by a query that asked for the error columns.

## Where a record fails, and where that is handled

Two places produce the error columns, one per text format, shared by every
engine that reads text — which is all of them except Parquet and Iceberg, whose
readers hand back typed values rather than text.

```
                    ┌──────────────── CsvReader.makeBatchRows ─────────────────┐
file / datastream ─►│ csvparser.Read ─► [JSONLINE: transJson2Lines] ─► getOneRowData │
                    └──────────────────────────────────────────────────────────┘
kafka ────────────► KafkaReader.ReadBatch ─► parseOneMessage ─► getOneRowData
```

`getOneRowData` (`external.go`) is the single conversion site. Under tolerance
it snapshots every vector's length, calls `materializeOneRow`, and on failure
rolls the vectors back to the snapshot — columns are appended one at a time, so
a failure part-way leaves them at unequal lengths — then emits the replacement
row through `appendErrorRow`.

`appendErrorRow` fills the synthesized columns through the ordinary column path
(`getColData`) rather than by hand, so they are typed as the catalog declares
them and any future synthetic column is carried automatically.

The format-specific sites handle a record that never became fields at all:

- **CSV** — the parser yields fields for any line, so every failure is a
  conversion failure and `getOneRowData` covers it.
- **JSONLINE** — a line that is not a JSON object fails before conversion, in
  `transJson2Lines`; `makeBatchRows` reports it and moves to the next line.
- **Kafka** — a message value that is not one CSV record or one JSON object
  fails in `parseOneMessage`; `ReadBatch` reports it and moves to the next
  message.

### Line numbers

`__mo_file_line` is the physical line the record *starts* on, so a CSV record
with a quoted field spanning lines 5-7 reports 5 and the next record reports 8.
The counter lives in the parser (`pkg/sql/util/csvparser`): `lineNo` counts
newline bytes as they are consumed, `readRecord` snapshots it at the record's
first content byte, and `RecordLine()` returns it 1-based. Taking the snapshot
at the first content byte rather than at the start of the scan is what makes a
record following a blank line report its own line.

For a datastream (jstfu) scan the same counter is the line count of the run,
since the reader is the same one reading a gRPC stream instead of a file. For
Kafka it is `NULL`: a message has no line, and `__mo_message_id` — the offset —
is what identifies it.

## Three defects this uncovered in the JSONLINE reader

All three were latent before error mode and are fixed here.

1. **The synthesized columns were matched against JSON keys.**
   `transJsonObject2Lines` counted every attribute as a key the object must
   supply, so with the error columns projected *every* record — valid ones
   included — failed with "the table column is larger than input data column".

2. **A malformed line was glued onto the next one.** A failed parse was held
   over in `prevStr` to be completed by the following line. `bytejson` reports
   a bare word (`this is not json`) as an unexpected EOF exactly as it reports
   a truncated object, so a malformed line consumed the line after it. Text is
   now held over only when the parser ran off the end *and* the text opens the
   expected value (`{` or `[`); anything else is a malformed record and says
   so. Text held over that the file never completes is reported at EOF rather
   than dropped.

3. **The batch tail was trimmed by one row on every held-over record.** That
   compensated for rows being written into pre-allocated slots, which is how
   the reader worked in 2023 (`support load local`, #7493); rows are appended
   now, so the trim silently discarded a complete row.

## What is not covered

- **Parquet and Iceberg.** Their readers decode typed columnar values, not
  text. There is no "line" and no per-record text to report, and a decode
  failure is a corrupt file rather than a bad record. The columns exist on
  those tables (they share the external table shape) but no record ever fails
  into them.
- **Constraint violations at the destination.** Error mode reports what the
  *scan* could not parse. An `insert ... select` that fails a primary key or a
  NOT NULL constraint fails the statement as usual.
- **A record too damaged to delimit.** Tolerance works per record, and a record
  is whatever the CSV parser delimits. An unbalanced quote swallows the rest of
  the file into one record; that record is reported as one failure.

## Testing

| Level | What it proves |
|---|---|
| `pkg/sql/util/csvparser` `TestRecordLine` | line counting: one record per line, a multi-line quoted record reporting its first line, exactness across a small read block, and blank lines still counted |
| `pkg/sql/colexec/external` `error_mode_test.go` | the switch (error columns tolerate, `__mo_file_line` alone does not, a lookalike user column does not), a pruned scan failing exactly as before, and CSV / JSONLINE / Kafka each reporting a bad record with its position, message and source text while the records around it are unaffected |
| `pkg/sql/plan` `TestExternalScanTolerates` | the pruning rule is driven by the two error columns only, keyed on column id |
| BVT `table/external_table_error_mode` | end to end over CSV and JSONLINE: hidden from `select *` / `desc` / `show create table`, the same bad record found regardless of which user columns are projected, the good/rejects split loaded into real tables, multi-line records, blank lines, an unterminated object, and the reserved names refused by DDL |
