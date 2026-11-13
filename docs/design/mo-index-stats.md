# Read-only index diagnostics

Chinese design and metric reference: [索引统计设计](../cn/mo_index_stats.md).

`mo_index_stats(database, table, index)` is a built-in table function. Its three
arguments are non-empty string literals. Currently it supports local ordinary
tables with declared primary keys, unique indexes, regular BTREE secondary
indexes, IVF_FLAT, classic fulltext, or fulltext2 indexes. Other index types
return a not-supported error;
unknown indexes return an index-not-found error. The caller must have SELECT
permission on the source table.

```sql
SELECT * FROM mo_index_stats('app', 'documents', 'idx_embedding') AS s;

-- Summary only.
SELECT metrics
FROM mo_index_stats('app', 'documents', 'idx_embedding') AS s
WHERE row_type = 'index';

-- Ten centroids with the largest center mismatch, then the largest bucket.
SELECT object_id, metrics
FROM mo_index_stats('app', 'documents', 'idx_embedding') AS s
WHERE row_type = 'centroid'
  AND JSON_EXTRACT(metrics, '$.center_mismatch_ratio') IS NOT NULL
ORDER BY CAST(JSON_EXTRACT(metrics, '$.center_mismatch_ratio') AS DOUBLE) DESC,
         CAST(JSON_EXTRACT(metrics, '$.vector_count') AS BIGINT) DESC
LIMIT 10;
```

IVF returns one `index` summary row followed by `centroid` detail rows. Key indexes
and fulltext return exactly one `index` row; fulltext never enumerates vocabulary
terms. All summary rows have a NULL `object_id`. For IVF, only the current metadata version is read,
and empty buckets are included. Relational reads, physical-object visibility,
and async job reads use the statement's transaction snapshot. The engine's
size estimate can include asynchronously published statistics. Nothing is
persisted, and CREATE INDEX, REINDEX, and DML acquire no new diagnostic writes.

The public schema contains only algorithm-independent columns. Each algorithm
defines its own metric payload and detail row kinds. Adding diagnostics for
other indexes therefore does not require adding IVF-specific columns or filling
unrelated columns with NULLs. Spatial and other vector index types remain unsupported.

| Column | Meaning |
| --- | --- |
| index_type | Index family: `primary`, `unique`, `secondary`, `ivfflat` or `fulltext` |
| row_type | `index` summary or `centroid` detail; future algorithms can define their own detail kinds |
| object_id | Detail object's identifier as a string; NULL for the index summary |
| metrics | Native SQL JSON object, containing only metrics applicable to this row |

The IVF summary payload contains `index_version`, `vector_count`,
`centroid_count`, `empty_centroid_count`, `min_bucket_size`, `max_bucket_size`,
`avg_bucket_size`, and `bucket_size_cv`. Available L2 geometry adds only
`center_mismatch_ratio`. Raw residual distances and absolute center shifts are
not exposed: their magnitude depends on the embedding model and scale.

The summary also reports `empty_centroid_ratio` (empty / actual centroids) and
`max_bucket_size_multiple` (largest bucket / mean bucket). A centroid payload
contains `vector_count`, `bucket_size_multiple` (its size / the same global
mean), and available `center_mismatch_ratio`. Thus 1 means an average-sized
bucket, 10 means ten times the average, and zero means empty. Multiples are
omitted when the mean is zero; the empty ratio is omitted when there are no
centroids. These size measures are available independently of the vector metric.

Index-wide fields such as centroid counts, ranges, CV and empty ratio are never
repeated on centroid rows. Unavailable mismatch is absent; a valid zero remains
present. JSON numbers stay numeric for filtering and ordering.

`bucket_size_cv` is the population standard deviation of all bucket sizes divided
by their mean. `status` appears only when measurement is unavailable or requires
an explanation; normal and empty rows omit it.

For a bucket with center c, current vectors x, and mean m, center mismatch is
`count * ||m-c||² / sum(||x-c||²)`. Zero residual loss gives zero mismatch. The
summary divides the sum of the numerators by the sum of the denominators;
it does not average bucket ratios. Uniformly scaling all vectors and centers
leaves this ratio unchanged. It is a fixed-partition L2 diagnostic, not a
measurement of recall loss, predicted latency improvement, or a universal
threshold across embedding models and workloads.

The implementation scans all current entries, without sampling, and retains
only running statistics per centroid and dimension. Center geometry currently
supports `vector_l2_ops`. Other distances still return bucket counts, with absent
geometric metrics and `center_stats_unsupported_distance` status. Synchronous
quantized indexes obtain original vectors from the source table. Asynchronous
quantized indexes return counts with `center_stats_unavailable_async_quantized`,
because current source vectors may be ahead of CDC's indexed state. Unquantized
asynchronous indexes describe the index's consumed data, not necessarily all
current source-table data.

Empty buckets omit geometric metrics; a NULL placeholder center has `untrained`
status. Missing source vectors produce
`incomplete_vectors` rather than a misleading geometric score. This is a scan
and computation operation, not a constant-time metadata lookup. Output filters
and LIMIT do not eliminate the diagnostic scan. No historical baseline or
automatic rebuild threshold is provided. A newly registered async index that
has not initialized its metadata returns an index summary with a NULL
`index_version` and `index_not_initialized` status.

## Primary, unique and secondary key statistics

These families return one summary row with `columns`, `method`, `entry_count`,
`cardinality` and `storage`. Prefix lengths are shown as a typed map when set,
and additional stored options are optional.
Logical columns exclude the storage-only PK alias; functional keys show their
expression instead of the generated owner column.

Counts use exact snapshot queries. Primary cardinality equals its source row
count by constraint. Unique and regular secondary entries are counted from their
hidden table. Cardinality groups logical keys on the corresponding source rows:
the join uses the stored PK reference, so the index format's NULL inclusion policy
is preserved. Prefix projection uses SUBSTRING; grouping the typed logical key
expressions directly preserves each column's SQL comparison semantics and NULLs,
independently of the index's physical serialization. A joined-count mismatch produces an error instead
of a silently inconsistent cardinality. NULL tuples that are present count once.
The storage-only secondary PK suffix is excluded from logical key cardinality.

Primary keys share source objects: size counts only the declared primary columns,
while object count covers the shared source relation. This does not charge an
entire source table as independent primary-index bytes. No implicit fake PK is
advertised as PRIMARY. These families have no async object or reorganization advice.

## Fulltext summary

Classic fulltext and fulltext2 return `index_type='fulltext'`, `row_type='index'`,
NULL `object_id`, and one JSON object:

| Metric | Meaning |
| --- | --- |
| version | Implementation version: 1 for classic, 2 for fulltext2 |
| columns | Indexed source columns, in declaration order |
| parser | Effective tokenizer; the default/default alias is `ngram` |
| maintenance_mode | `sync` or `async`; fulltext2 is always async |
| document_count | Number of live documents recorded in the index |
| term_count | Distinct indexed terms with positive live document frequency |
| avg_document_frequency | Mean document frequency over all such terms |
| min_document_frequency | Minimum document frequency, or zero with no terms |
| max_document_frequency | Maximum document frequency, or zero with no terms |
| storage | Same three-field aggregate storage contract as IVF |
| async | Same async maintenance contract as IVF; omitted for sync indexes |

A document containing a term several times contributes only one to that term's
DF. These are global vocabulary aggregates, not one row or avg/min/max per term.
Empty vocabularies return zero for all three frequency aggregates.

Classic fulltext aggregates `(word, doc_id)` pairs in the index's hidden table,
excluding the internal `__DocLen` marker from term statistics. Its document count
comes from marker records: classic indexes do not store documents that produce
no tokens. The SQL engine performs the grouping; diagnostics never return the
vocabulary to the client. The average uses floating-point input to avoid the
fixed-scale rounding of an integer AVG followed by a cast.

Fulltext2 reads a private index at the query's snapshot, using the existing load
budget checks and releasing its segment mappings after scanning. The engine's
liveness resolution removes superseded copies and delete-shadowed documents.
Statistics stream and merge sorted segment dictionaries in O(segment count)
additional vocabulary memory, counting only live postings. A term present in
several segments is counted once globally; a term appearing only in dead copies
is excluded. No compaction, retokenization, diagnostic persistence, or shared
query-cache insertion is performed. Statistics describe consumed index contents;
async freshness independently reports whether they cover source changes.

```sql
SELECT JSON_PRETTY(metrics)
FROM mo_index_stats('app', 'documents', 'ft_body') AS s;
```

## Index configuration

Only the summary includes `lists` (configured bucket count), `op_type`,
`quantization` (`none` when disabled), `maintenance_mode` (`sync` or `async`),
and non-duplicated `index_options` (additional catalog options such as training
parameters, whose values are strings). Options already represented by these
typed fields are excluded from `index_options`, which is omitted when empty.
`lists` can differ from the actual `centroid_count` of an untrained index.
`avg_bucket_size` divides current-version vector count by all actual centroids,
including empty ones; an index with no centroids returns zero.

## Storage

The summary's `storage` object contains `size_bytes`, `table_count`, and
`object_count`. It reports aggregate values only, without
hidden-table names, per-table details or data/tombstone breakdowns. It includes
every physical table belonging to the index (metadata, centroids, entries and
the rejected-row table when present), including retained old versions. It
excludes the source table except for primary keys, which share source objects
and count only the declared primary columns in `size_bytes`.

The `size_bytes` contract is an estimate of uncompressed data volume, using the
same engine relation size facility as existing table-size queries: published
column `OriginSize` statistics plus in-memory/workspace vector sizes. It is
neither compressed disk usage nor actual process memory usage. This meaning is
fixed by the metric definition instead of an always-true estimate flag. Object counts are
visible persisted data and tombstone objects, deduplicated across columns;
unflushed vectors occupy memory and can exist with zero persisted objects.
Storage does not claim the size of only the active IVF generation. Relation.Size
reuses published engine statistics when available. A cold cache or incompatible
table version may trigger a synchronous statistics build and wait, adding CPU and
I/O cost. Diagnostics do not request a flush or persist a diagnostic baseline.

## Async maintenance

Synchronous indexes omit the `async` object. An async summary reads the latest
non-dropped `index_<canonical-index-name>` job for the source account and table
from the system-owned `mo_iscp_log`, at the same read snapshot. It exposes
`task_state` (SHOW INDEX STATUS's running/retrying/stopped distinction, plus
canceled), UTC `synced_at`, nullable `caught_up`, and nullable `lag_seconds`.
An actual error adds a nonzero `error_code` and available `error_message`.
Internal job IDs, numeric states and raw timestamps are omitted; watermark
comparisons still use both physical and logical timestamp components.

`caught_up` compares the full physical/logical watermark to the engine's source
coverage bar at the read snapshot. The bar includes the partition-state
retention boundary, so a false result conservatively means coverage is not yet
proved. It is **not** a comparison against wall-clock time: an idle table can be
caught up with an old watermark. The bar is unavailable with transaction-local
source writes. `lag_seconds` is the nonnegative physical gap between the source
bar and the durable watermark, not pending row count, the age of the oldest
pending change or a prediction of catch-up duration. Logical-only lag can have
zero seconds while `caught_up` is false. Durable watermarks may lag the
consumer's in-memory progress until the existing ISCP flush occurs.

When freshness cannot be established, `async.status` is `task_missing`, `initializing`,
`checkpoint_unavailable`, `job_status_unavailable`, or
`source_commit_ts_unavailable`. Unknown freshness has JSON null `caught_up` and
`lag_seconds`. Initialization reports `caught_up: false` without a fabricated
lag. Sentinel maximum timestamps are never accepted as proof of coverage.
Pending row counts are omitted because the job log does not provide them.
Successful freshness measurements omit `async.status`. These exceptional states
describe maintenance independently of geometric measurement's `status`.

## Interpreting measurements

The payload contains measured values and specific reasons for unavailable
measurements. Generic recommendations, fixed disclaimers and textual restatements
of the numeric metrics are omitted. Rebuild decisions require validating a fixed
query set against exact top-k and measuring latency. Positive mismatch or unequal
bucket sizes alone do not establish a recall or performance regression; no
unvalidated rebuild threshold is supplied.
