# Exact JSON parsing and Parquet VARIANT interoperability

- Status: implemented for review; automatic persisted path subcolumns and local instance tests are covered in the [storage extension](20261013-automatic-json-subcolumns.md).
- Design approval: [combined R1 at immutable revision b4fcf49c](https://github.com/dengn/matrixone/blob/b4fcf49c069b9ade11ae39f47e325360628a650f/docs/design/20261014-exact-json-variant-design-review.md), [APPROVED decision](https://github.com/matrixorigin/matrixone/pull/30142#pullrequestreview-5512860139). Design approval is separate from implementation acceptance and production release gates.
- Feature issues: [exact JSON #30057](https://github.com/matrixorigin/matrixone/issues/30057), [Parquet VARIANT #30058](https://github.com/matrixorigin/matrixone/issues/30058).
- Original development baseline: `dd38098853ea197699ed3fac3e69dc53975ac061`.
- Integration base: `3e50e1b32d8da61bbbf70c89989fb08b671809de`.
- Reference: Apache Parquet VARIANT v1 encoding/shredding and DuckDB 2.0 development branch commit `c65ca930b9fe39b2e8d8ed8821018dca650965f4`.

## Problem and requirements

Agent telemetry includes large token counters and identifiers, fractional costs, tool inputs/outputs, nested events, and attributes whose types change between rows. Converting numeric JSON text through DOUBLE can change costs or identifiers. Treating a Parquet VARIANT group as an ordinary object exposes metadata rather than the document; reading only its residual `value` silently loses shredded fields.

The implementation must:

1. Provide an explicit text-to-JSON entry point that preserves valid numeric text without intermediate floating-point conversion.
2. Preserve existing JSON parsing and MySQL JSON comparison behavior for existing callers.
3. Read standard unshredded and supported shredded Parquet VARIANT values into JSON/text destinations.
4. Preserve missing object fields, JSON null, SQL NULL, case-sensitive names, and supported typed scalar identities.
5. Offer explicit VARIANT Parquet export while preserving the default string representation of JSON in `FORMAT 'parquet'`.
6. Reject unsupported logical identities, malformed representations, range/precision violations, and resource-limit violations; honor context cancellation.
7. Exercise real files, independent wire-format vectors, deterministic properties, invalid inputs, race checks, and fuzzing. State unverified integration boundaries explicitly.

## Exact parsing API and representation

```sql
SELECT JSON_PARSE_EXACT('{"cost":9007199254740993.001}');
SELECT CAST(JSON_EXTRACT(
    JSON_PARSE_EXACT('{"cost":9007199254740993.001}'), '$.cost'
) AS DECIMAL(38,3));
```

`JSON_PARSE_EXACT(text)` returns the existing JSON SQL type. SQL NULL input yields SQL NULL. Invalid JSON is an error. Constants, prepared parameters, and executor row selection follow the normal unary function contract; an unselected malformed row must not be evaluated.

`bytejson.ParseFromByteSliceExact` and `ParseFromStringExact` enable exact numeric parsing in the existing writer. Fitting integer text keeps INT64/UINT64. Fractional, exponent, and larger integer text uses the existing `TpCodeDecimal` payload, retaining the original number spelling. No new persisted JSON type code or automatic schema migration is introduced. Exact lexical preservation does not imply arbitrary-precision arithmetic or unlimited DECIMAL casts: downstream typed conversion retains its own range/precision contract.

Regular parsing retains its previous conversion behavior. Programmatic `json.Number` construction also uses the number parser to correct exponent/decimal classification and reject invalid/nonfinite numeric text. Native FLOAT/DOUBLE values retain floating-point semantics.

The SQL function ID is appended after the IDs allocated by current main; existing IDs are preserved. The registry and pinned ID test must be updated together.

### Remote execution and durable admission

Main reserves v113 for DOUBLE results of DECIMAL statistical aggregates.
Those workers do not implement exact JSON or standard Parquet VARIANT reads;
the independent feature floor is therefore v114 after main synchronization.

MORPC v114 identifies two independent contracts: bound `JSON_PARSE_EXACT`
identity 598, and existing JSON-to-DECIMAL64/128 CAST semantics. Executors
sharing the JSON scalar cast path are covered. A JSON column cast needs this
floor even without the parser in its query. FLOAT conversion and
JSON-to-DECIMAL256 retain their unchanged contracts.

Placement takes the maximum required capability and collapses to the coordinator
when a worker is old or unknown. The sender checks actual identity/address again
before emitting a payload; the receiver recomputes requirements after decoding.
Parent cancellation and a coordinator floor lowered during the probe cannot
authorize a send. Observations do not survive another execution/send. Durable
expression admission also requires v114; view requirements are captured before
constant folding.

Readers discover Parquet logical annotations after placement, so all Parquet
file scans conservatively use the same v114 reader floor and stay on the
coordinator when workers are unsupported. CSV/JSON/Arrow retain their own
contracts; Kafka, MongoDB, Iceberg, foreign and datastream metadata retain their
existing owners. Frontend export is coordinator owned; input expressions obey
the expression gates.

Protocol tests cover bound identities, wire round trips, controls, worker
placement/replacement, cancellation, failed probes and persisted owners/views.
A real two-CN SQL test forces multi-CN placement, advertises v113 on a current
worker and requires its address to be absent from exact-query execution; with
v114 restored, that address must occur in distributed execution. Unchanged
native worker-wire tests at pre-feature main reproduce missing function 598 and
decimal rounding, with `0.125` as a control. The advertisement test does not
deploy an older CN binary.

A separate older-binary startup attempt against a newly initialized v113
cluster was rejected by persisted-expression admission. This does not certify
a real rolling upgrade: that topology must start from older compatible cluster
and admission state. R1 write-cost budgets, real old/new rolling upgrade,
restore/rewrite and production rollout criteria remain release gates, not
results inferred from component tests or skipped CI.

## Parquet codec and type mapping

`pkg/util/parquetvariant` owns the v1 wire codec independently of the file reader and exporter. Metadata holds one dictionary shared by all residual fragments in a row. Encoding uses compact integer, dictionary, field-ID, offset, and count widths supported by the specification.

| Source VARIANT / typed leaf | MO JSON mapping and limits |
| --- | --- |
| null / boolean | JSON literal |
| signed integers | INT64; typed INT8/16 values are range checked |
| UINT64 above INT64_MAX on export | decimal16 preserving the mathematical value |
| FLOAT / DOUBLE | FLOAT64; FLOAT32 widens exactly; nonfinite values are rejected |
| decimal4/8/16 | exact DECIMAL text; maximum precision/scale 38 |
| string | UTF-8 string |
| binary | opaque binary, persisted with the existing compatible binary representation |
| date | DATE, years 1–9999 |
| timestamp NTZ MICROS | DATETIME, microsecond precision, years 1–9999 |
| time NTZ MICROS | TIME within one calendar day |
| object / array | binary JSON object / array |
| UTC or nanosecond timestamp, UUID, unsupported annotations | explicit error; no conversion to an ordinary string |

Shredded DECIMAL physical payloads are signed big-endian; residual decimal wire payloads are little-endian. Conversion checks coefficient precision, scale, payload width, and the maximum precision that a fixed-length physical column can represent. Integer widths and original numeric lexical spellings are not guaranteed across Parquet round trips; supported logical values are preserved.

Wire validation checks dictionary version, UTF-8, offsets, lengths, field IDs, sorted/unique object names, complete payload coverage, and exact primitive sizes. Nonmonotonic object payload offsets are legal; aliases, overlaps, gaps, and trailing bytes are rejected.

## Shredded reconstruction

The file reader validates selected schemas before materializing rows, including schemas whose values happen to be SQL NULL. Fields are identified by name rather than physical order.

A row-local `Document` shares the metadata dictionary, context, and admission budget between typed fields and residual fragments. Recursive reconstruction returns both a JSON value and a presence bit:

| `value` | `typed_value` | Required behavior |
| --- | --- | --- |
| NULL | NULL | missing object field; root becomes JSON null; missing array element is rejected |
| populated | NULL | decode residual; an object with a typed object schema requires that typed object group to be defined |
| NULL | populated | reconstruct typed scalar, object, or array |
| populated | populated | valid only for a partial object; residual must be an object and is merged with typed fields |

A residual object must not contain any field represented by its typed schema, even when that typed field is missing in this row. Conflicts are errors rather than a precedence rule. Explicit residual null is a present JSON null. An absent optional outer VARIANT group is SQL NULL. The NULL shortcut checks every descendant so malformed populated leaves cannot hide behind a null first leaf.

Typed objects use required named field groups containing optional `value` and/or `typed_value`. Arrays require standard three-level LIST with a required `element` field group. Reconstruction validates definition levels, the initial repetition boundary, outer element counts across leaves, empty-array placeholders, and nested repeated descendants. Empty arrays, empty objects, null elements, nested arrays, and arrays of partial objects remain distinct.

The selected VARIANT group uses the existing nested row reader. Unrelated selected scalar columns retain their existing page-based mapping. This implementation materializes the complete selected VARIANT; it does not provide JSON-path physical page pruning.

## Export integration and compatibility

```sql
SELECT doc INTO OUTFILE '/path/agent.parquet' FORMAT 'parquet_variant'
FROM agent_events;
```

`ParquetWriterOptions{JSONAsVariant: true}` writes JSON result columns as optional logically annotated VARIANT groups with required BINARY metadata/value. Other column domains retain main's existing mapping. Plain `FORMAT 'parquet'` keeps JSON as string. The parser source and generated parser both include the explicit format, and suffix validation accepts `.parquet` for this format.

The options must propagate through initial writer creation and writer recreation during split output. VARIANT binary lengths participate in the row-group buffering estimate. File rotation, bounded row groups, YEAR/TIMESTAMP mappings, and unsigned scalar handling from main are retained.

parquet-go v0.25.1's GenericWriter map/any `Write` path invokes a second row-counting writer. Export deconstructs maps into physical rows and calls `WriteRows` directly, preventing duplicated file counts and boundary panics. Constant SQL NULL uses the vector's row-aware NULL API.

## Resource and cancellation contract

- Input and conservative per-document expansion admission are bounded at 64 MiB.
- Dictionary expansion, leaf/group storage, container tables, field names, typed data, residual fragments, and copied nested child payloads share the budget. Fragment decoding cannot reset it.
- Logical nesting is limited to 100 containers across the combined typed/residual document. parquet-go additionally limits physical schema depth to 127, which can reject a deeply shredded schema earlier.
- Schema traversal has a bounded node count. Context cancellation is checked during schema/dictionary/row traversal and recursive decoding and returns the caller's cancellation cause.
- The budget is an admission bound, not a process RSS guarantee, and does not promise acceptance of every document with 64 MiB of encoded bytes.

## Test cases and acceptance criteria

| ID | Input / path | Expected result | Executable coverage |
| --- | --- | --- | --- |
| J01 | INT64/UINT64 boundaries, values beyond 2^53 | exact integers and original numeric values | `bytejson/exact_test.go` |
| J02 | fractional/exponent/oversized integer text, invalid/nonfinite JSON, depth 100/101 | retained valid text; invalid/deep input rejected | `bytejson/exact_test.go` |
| J03 | NULL, constants, prepared input, masked invalid rows, typed DECIMAL projection, generated column planning | normal SQL function and conversion contracts | `func_json_parse_exact_test.go`, `json_parse_exact_test.go`, `exact_ingestion_test.go` |
| J04 | 10,000 integer/DOUBLE/DECIMAL comparisons | existing common numeric domain and canonical keys retained | `variant_semantics_test.go` |
| V01 | independently specified v1 primitive/container bytes, reordered offsets, malformed tables | exact mapping or explicit rejection | `parquetvariant/decode_test.go` |
| V02 | compact widths, integer/decimal boundaries, 255/256 count boundary, 16 MiB child | valid standard encoding and logical round trip | `parquetvariant/encode_test.go` |
| V03 | 10,000 mixed shredded file rows in 79 row groups and small pages | JSON/text equality, scalar alignment, missing/null distinctions | `TestParquetVariantShreddedTenThousandFileRows` |
| V04 | 10,000 malformed rows | every case rejected; no panic or silent fallback | `TestParquetVariantShreddedTenThousandInvalidRows` |
| V05 | nested arrays, 1,000 arrays-of-objects file rows, Unicode and A/a keys, partial objects | preserve complete document and element boundaries | `parquet_variant_shredded_test.go` |
| V06 | typed DECIMAL(38,38), +/-maximum precision, impossible physical precision, INT8/16 overflow | exact coefficient/scale or explicit error | `TestParquetVariantShreddedTypedScalars`, `TestParquetVariantShreddedSchemaContracts` |
| V07 | duplicate/missing leaves, conflicting representations, NULL shortcut corruption, unsupported schema | reject; target vector remains unappended for the tested row failures | `parquet_variant_test.go`, `parquet_variant_shredded_test.go` |
| V08 | combined depth, cancellation cause, small nested input with excessive copy amplification | admission failure/cancellation, not unbounded assembly | `document_test.go`, `TestParquetVariantShreddedCombinedDepthAndCancellation` |
| V09 | 10,000 exported rows, plain/VARIANT formats, 79 groups, SQL NULL and exact costs | correct declared counts, group counts and values | `TestParquetVariantExportTenThousandRowsAcrossGroups` |
| V10 | split output writer recreation and mixed scalar/JSON columns | every output file keeps chosen annotation and scalar domains | `TestParquetVariantSplitExportKeepsSchemaAndValues` and frontend export regressions |
| F01 | arbitrary wire bytes and mutated shredded rows including arrays | no panic; accepted output valid; supported round trip; valid generated rows accepted | `FuzzDecodeVariant`, `FuzzParquetVariantShreddedRows` |

The 10,000-case counts refer to per-row input/expected-value checks, not 10,000 distinct SQL query shapes or a throughput benchmark.

## Validation and reproduction

On the original development baseline, bytejson/codec tests, Parquet regressions, export/format tests, planner/type-projection tests and JSON function regressions passed. Targeted race tests passed. Final 30-second runs executed 161,197 wire fuzz inputs and 15,613 shredded row inputs without a failure. Codec statement coverage was 90.6%. Integration-base package, function, parser, planner and Parquet regression reruns passed, including split/empty VARIANT output and the existing bounded split tests. Integration-base race status is recorded in the PR; earlier fuzz counts are not results of a newly deployed server.

With the normal native dependencies built:

```bash
export CGO_CFLAGS="-I$PWD/cgo -I$PWD/thirdparties/install/include"
export CGO_LDFLAGS="-L$PWD/cgo -L$PWD/thirdparties/install/lib"
export LD_LIBRARY_PATH="$PWD/cgo:$PWD/thirdparties/install/lib:${LD_LIBRARY_PATH:-}"
go test ./pkg/container/bytejson ./pkg/util/parquetvariant -count=1
go test ./pkg/sql/colexec/external -run '^TestParquet' -count=1
go test ./pkg/frontend -run 'Test.*Parquet|Test_getExportFormat|Test_validateExportFormat' -count=1
go test ./pkg/sql/plan/function -run 'Test.*[Jj][Ss][Oo][Nn]|Test_funids|Test.*Function' \
  -skip 'TestJsonSchemaExternalReferenceDoesNotPerformNetworkIO' -count=1
go test ./pkg/sql/jsonvalue ./pkg/sql/plan ./pkg/sql/parsers/dialect/mysql \
  -run 'TestExactIngestionTypedProjection|TestJsonParseExactSQLPlanning|Test.*ExportFormat' -count=1
go test -race ./pkg/container/bytejson ./pkg/util/parquetvariant ./pkg/sql/colexec/external ./pkg/frontend \
  -run 'TestDocument|^TestParquetVariant|^TestDecodeVariant|^TestEncodeVariant|TestParseExact|TestJSONCommon' -count=1
go test ./pkg/util/parquetvariant -run '^$' -fuzz '^FuzzDecodeVariant$' -fuzztime=30s -parallel=1
go test ./pkg/sql/colexec/external -run '^$' -fuzz '^FuzzParquetVariantShreddedRows$' -fuzztime=30s -parallel=1
```

The previously excluded JSON Schema local HTTP test now also passes with explicitly enabled socket access. Finite fuzz duration and coverage percentages do not certify all inputs.

## Follow-up integration and scope

Server BVT should execute the SQL examples, insert exact JSON into a table, verify exact DECIMAL costs and generated projections, export/reload plain and VARIANT files with SQL NULL/JSON null, and repeat with split output, prepared statements, rollback and multiple CNs. Independent DuckDB/Doris fixtures should test supported scalar identities, partial objects, arrays and intentionally unsupported types with pinned engine versions.

Local server and independent DuckDB/Doris runs are now recorded in the [storage extension](20261013-automatic-json-subcolumns.md). Multi-CN testing and engine benchmarks remain unexecuted; Doris native VARIANT compatibility failed content-level checks. DuckDB's referenced 2.0 commit is a development version, not an assumed stable 2.0 release.

Bounded automatic native JSON path fragments are now implemented. Checkpoint shredding, native scalar path vectors and predicate statistics, extended UTC/nanosecond/UUID JSON identities, and Iceberg VARIANT support are separate storage/type projects. Their transaction, backfill, compaction, replication and recovery contracts are outlined in the [source research and remaining design](../rfcs/20261012_json_variant_interop.md). The storage extension states exactly which capabilities and tests are implemented.

## References

- [Parquet VARIANT encoding](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md)
- [Parquet VARIANT shredding](https://github.com/apache/parquet-format/blob/master/VariantShredding.md)
- [DuckDB 2.0 development iterator](https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/common/types/variant/parquet_variant_iterator.cpp)
- [DuckDB 2.0 development native storage](https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/storage/table/variant_column_data.cpp)
- [Doris 4.0 ColumnVariant](https://github.com/apache/doris/blob/branch-4.0/be/src/vec/columns/column_variant.h)
