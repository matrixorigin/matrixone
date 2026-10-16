# Automatic persisted JSON path subcolumns

Status: implemented with conservative object-key projection; tested locally. Feature request: [#30087](https://github.com/matrixorigin/matrixone/issues/30087); implementation: [PR #30071](https://github.com/matrixorigin/matrixone/pull/30071). This extends [exact JSON and Parquet VARIANT](20261013-exact-json-parquet-variant.md). Native scalar zone maps, path predicates in storage, online historical backfill and complex/distributed query proofs are future work.

The combined R1 design was [approved at b4fcf49c](https://github.com/matrixorigin/matrixone/pull/30142#pullrequestreview-5512860139). Default-on write-cost budgets, rollback limits and release validation remain conditional gates. The additive projection hint permits older readers to fall back to full source; actual exact parsing/JSON-to-DECIMAL expressions independently require MORPC v119 under the [execution admission contract](20261013-exact-json-parquet-variant.md#remote-execution-and-durable-admission).

## Requirements

Agent observations contain a few frequently accessed attributes and a large tail of tool arguments, responses and evolving attributes. Reading trace IDs, latency and costs should avoid fetching the whole persisted JSON column. Ingestion must not require manually defined generated columns. SQL NULL, explicit JSON null, missing fields, exact numeric tags, quoted/case-sensitive keys and scalar/container type drift must remain distinguishable. Source and subcolumns must share publication, visibility and reclamation. Unsupported queries and old objects must remain correct through full-column reads.

## Physical design

Normal object writers automatically discover scalar object-key paths in the first 1,024 rows of each JSON source block. Discovery is bounded to 256 candidates, depth 16, 512-byte keys and 32 selected paths per block; selection orders frequency descending and canonical path ascending. Arrays are preserved as fragment values if a selected scalar path subsequently drifts to an array, but array element paths are not discovered. All-null or object-only paths may remain in the source column.

Canonical quoted keys use JSON string encoding, including `\u0000`/`\u0001` and other control characters. Go-only `\x` or `\a` escapes are not emitted. If a discovered candidate cannot be represented by the path parser, that candidate is skipped while the valid source document is still persisted. Manifest key arrays remain unchanged, so existing objects do not require a rewrite for this encoding repair.

The source JSON is always written unchanged to `SchemaData`. A reserved additive ObjectMeta V3 sub-meta section (`CkpMetaEnd + 1`, section index 29) stores one auxiliary block per source block, in the same immutable object:

1. A constant VARCHAR manifest describes version, source block ordinal, row count, source physical sequence numbers, root-null columns and exact key arrays for fragment columns.
2. A BOOL vector records source SQL NULL separately for each materialized JSON source.
3. Independently compressed JSON fragment vectors store each selected path. Vector NULL means missing. A populated JSON null retains its explicit tag. Exact DECIMAL text and drift to object/array retain ByteJson identities.

Manifest bytes and conservative serialized fragment accounting share an 8 MiB per-source-block budget. Keys/columns/source counts are bounded. Long tails and rejected over-budget paths use the original column. The layout adds no new JSON type, compression method or user-visible schema column. These are independently stored JSON fragments, not native SQL scalar vectors with their own zone maps.

Auxiliary block IDs follow all source IDs. Auxiliary blocks are excluded from the writer's returned data-block list and data block statistics. Object byte statistics include their bytes. Checkpoint, query-result, GC and ETL writers do not automatically shred. Object-level source column statistics are not copied into auxiliary column metadata.

## Publication, mutations and recovery

A single object write publishes both representations. Existing flush/merge/compaction writers rebuild fragments from the batch they actually publish. Delete masks, append commit timestamps, abort flags and snapshots retain their source-row coordinates. A compaction or update creates a new object identity and new manifest; readers do not consult a separately versioned sidecar. GC of the source object reclaims all auxiliary bytes. Existing objects need no migration and use the original column until rewritten. No independent backfill task is implemented.

The reader verifies optional section/directory bounds, source block identity/row count, vector types, dense auxiliary column indexes, source JSON identities, manifest version and trailing bytes, unique keys and bounded counts. Malformed auxiliary data errors rather than silently replacing stored data. Missing sections or unavailable selected paths fall back to a complete source-column read.

## Query proof and read path

Before column remapping, a single-step unary TABLE_SCAN/PROJECT/FILTER/SORT/AGG plan is inspected. Every reference to a JSON source must occur exclusively as the first direct-column argument of a two-argument JSON_EXTRACT, JSON_EXTRACT_STRING or JSON_EXTRACT_FLOAT64 call with a literal object-key path. A full-document consumer, root/array/wildcard/dynamic path, unsupported expression or graph shape declines that column's projection. Forwarded bare JSON aliases conservatively decline. Joins, CTE/multi-step plans and windows currently use the source JSON.

The proof records source physical sequence numbers in TABLE_SCAN's extra options and is carried in an additive pipeline TableScan field. Older receivers ignore that field and read the source column. Reader summaries disable partial-document projection. `LoadColumnsData` and the modern destination/selected-row loader pass the proof through the read context.

The encoder applies the receiver's admission checks before attaching extra options: at most 64 projected JSON columns, at most 32 paths per column and at most 65,536 encoded bytes, including the version prefix and JSON escaping. Exact byte-boundary contracts are accepted. A larger or otherwise unsupported optimization returns an empty contract and the scan reads full JSON; valid wide/long-path SQL must not fail during `TableScan.Prepare` because of an optional projection. The receiver continues rejecting malformed or oversized nonempty wire contracts.

For an eligible persisted column, the reader fetches the manifest, source-null flag and only requested fragment extents. It constructs private minimal JSON documents so existing JSON functions continue to execute without a new scalar evaluator. Ancestors are inserted last to preserve complete selected container values. Partial documents are never stored in the file cache under the raw JSON extent. Other columns and visibility metadata retain their ordinary read paths. In-memory rows retain complete JSON.

Fixed-width root-null vectors borrow their IOVector's backing allocation. The complete input IOVector stays alive through materialization and decoded-vector cleanup, including cancellation and failure exits. Only independently owned output bytes escape. This lifetime also applies when the cache bypasses or declines a read, or drops its own reference during eviction.

## Tests and evidence

| Test | Standard and result |
| --- | --- |
| `TestJSONSubcolumnsTenThousandRows` | Two source blocks, 10,000 rows, missing/JSON null/SQL NULL, root scalars/arrays, scalar/container drift, exact DECIMAL, quoted/case-sensitive keys. Fragment query bytes match the source query and recorded file reads do not touch its JSON extent. Full consumers after projection and unknown paths receive original bytes. |
| `TestJSONSubcolumnsLegacyAndContract` | Old objects fall back; malformed/root/wildcard/array contracts reject. |
| `TestJSONSubcolumnsDiscoveryBudgetAndCorruption` | Bounded candidate/path/serialized sizes, corrupted section identities/offsets reject without panic, ancestor/descendant extraction remains correct. |
| `TestJSONSubcolumnsVisibilityAndSelection` | 1,000 appendable rows retain commit timestamp/abort masks; selected row order and SQL NULL are preserved. |
| `TestJSONSubcolumnProjectionProof` | Safe fixed paths accepted; complete/raw/unsupported consumption declines, including raw scan filters. |
| `TestJSONSubcolumnsPoisonOnRelease` | A real object read uses an allocator wrapper that overwrites buffers on final release. SQL NULL flags, fragment tags/bytes and allocation/release counts remain correct. The pre-fix code turned row 0 from non-NULL into SQL NULL. |
| `TestJSONSubcolumnsReleaseOnError` | Cancellation during materialization and a failed fragment decode release every input allocation and expose no partially owned result. |
| `TestJSONSubcolumnsControlCharacterKeys` | 73 distinct empty/quoted/Unicode/C0/DEL/C1 key cases at root and nested positions; real object writes and projected reads preserve exact numbers, missing/JSON null/SQL NULL and scalar/container drift without reading the source extent. |
| `TestJSONSubcolumnProjectionAdmission` | 1/63/64/65/4096 columns; encoded lengths 65,535/65,536/65,537 and escaping amplification. Admitted contracts decode; over-limit plans retain full-document reads. |
| `review_json_subcolumns.py` | Real LOG/TN/CN SQL, three forced table flushes, 73 key cases plus full-source verification, 63/64/65-column queries and all encoded-size boundaries. Run with the newly built service and retain its JSON summary. |
| `TestCastExactJSONDecimal` | Real SQL testing exposed a DOUBLE conversion in JSON CAST; exact integer/DECIMAL to DECIMAL64/128 now parses its exact text and retains legacy FLOAT conversion. |
| `mo_local.py` | Real built local LOG/TN/CN service: 10,000 input rows, forced flush, rollback, concurrent update with observed READ-COMMITTED semantics, deletion, complete/path/DECIMAL checks on 9,677 surviving rows. The same 9,677 values also pass after a complete service restart with the final binary. |
| race | Targeted objectio/ioutil/planner tests pass. |
| fuzz | 30 seconds, 26,154 input executions, no failure. Finite fuzzing is not exhaustive certification. |

Full objectio/ioutil package tests and selected existing Parquet regression tests passed. Multi-CN transport is implemented but not exercised as a distributed cluster. A targeted compaction/rewrite path is provided by the normal writer, but a full historical backfill/production compaction workload has not been certified. No throughput or latency advantage is claimed from these correctness checks.

## Independent engines

See [reproduction instructions](../../test/variant_interop/README.md). The executed DuckDB Python build is `2.0.0.dev2610011535`, reporting `v2.0.0-alpha43763`, commit `96063b9e39`; it differs from the previously researched October 12 development source. Default automatic shredding and explicit STRUCT shredding each produced 10,000-row native VARIANT files consumed by MO. Both independent files were also loaded through the final MO SQL service, flushed, and queried for every full document and exact path DECIMAL. MO SQL OUTFILE and the frontend exporter's unshredded native VARIANT exports were consumed by DuckDB with exact DECIMAL(38,3), booleans, nested lists and explicit null preserved. A lost converted-only LIST annotation in parquet-go v0.25.1 was repaired by retaining the actual footer annotations per reader, without inferring types from field names.

Doris image `apache/doris:4.0.3-all-slim`, digest `sha256:3237bfb73471da7482c6575cecc1632c96dec09509cd2e60bcb798e2c5342823`, reports BE `doris-4.0.3-rc03-e9096296b8`. All three native VARIANT imports reported 10,000 loaded / zero filtered, but document-level checks failed: metadata/value/typed_value became an ordinary object and trace/cost extraction was NULL for every row. This is an observed incompatibility, not a pass.

Doris JSON ingestion preserved 10,000 decimal *strings*, but dropped 910 explicit null fields and returned every boolean as an integer. Its native VARIANT Parquet export is UTF8 JSON text. The final MO SQL service and package reader each read that actual 10,000-row export and matched what Doris returned, including its already altered values. This proves the JSON-text export bridge, not lossless standard VARIANT compatibility or numeric DECIMAL preservation inside Doris.
