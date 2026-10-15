# Exact JSON, Parquet VARIANT and automatic path storage: design review R1

Status: **proposed; design approval pending**. This document starts a distinct
design review phase. It does not approve the existing implementation.

Scope: the complete feature in [implementation PR #30071](https://github.com/matrixorigin/matrixone/pull/30071),
including [#30057](https://github.com/matrixorigin/matrixone/issues/30057),
[#30058](https://github.com/matrixorigin/matrixone/issues/30058) and
[#30087](https://github.com/matrixorigin/matrixone/issues/30087).
Implementation snapshot examined: `72b22e7d572bac898434fd175fb6bf060261144c`.
Design-only base: `fcad62a8ff06000e682824802f1155cbeea02a2f`.
Revision: R1, 2026-10-14. Approval must name the design commit, reviewer and
decision; no approval is inferred from implementation instructions or CI.

The following immutable specifications are incorporated into this proposal:

- [Exact API, codec, type mapping and file test matrix](https://github.com/dengn/matrixone/blob/72b22e7d572bac898434fd175fb6bf060261144c/docs/design/20261013-exact-json-parquet-variant.md).
- [Object layout, discovery, projection proof, visibility and storage test matrix](https://github.com/dengn/matrixone/blob/72b22e7d572bac898434fd175fb6bf060261144c/docs/design/20261013-automatic-json-subcolumns.md).

Their statements that code is implemented or tested describe historical work,
not design acceptance. This R1 document supersedes their missing execution
admission, rollout and approval contracts. Implementation differences below
must be closed after design acceptance and before implementation approval.

## 1. Problem, users and invariants

Agent observations contain token counters above 2^53, fractional costs,
heterogeneous tool inputs, nested event arrays and long-tail attributes.
Ordinary floating-point parsing can round `9007199254740993.001`. A reader
that treats standard Parquet VARIANT as an ordinary metadata/value object,
or reads only its residual payload, returns a different document. Repeated
access to a few paths currently reads the complete persisted JSON column.

The required invariants are:

1. Explicit exact parsing and integer/DECIMAL-to-DECIMAL conversion never
   introduce DOUBLE rounding. Precision/range limits of the destination remain
   errors; lexical exactness does not imply arbitrary-precision arithmetic.
2. Missing fields, JSON null, SQL NULL, case-sensitive names and supported
   logical identities survive import, storage and extraction.
3. Every worker evaluating a new function or changed semantic contract supports
   that contract. Placement alone is insufficient: the actual send destination
   must be checked before any execution payload is sent.
4. The complete JSON document is authoritative. Optional path storage cannot
   prevent a valid source write merely because discovery or projection is
   unsupported, and cannot expose partial JSON to a full-document consumer.
5. Source rows and path fragments share immutable object identity, transaction
   visibility, row coordinates, publication, rewrite and reclamation.
6. Input expansion, discovery, persisted fragments, wire contracts and temporary
   ownership have explicit bounds. Corrupt data is rejected, not silently
   reported as a valid optimized result.

Non-goals: native SQL path vectors/zone maps, arbitrary path predicates,
array-index pruning, a new SQL VARIANT type, online historical backfill,
Iceberg VARIANT, UUID/time-zone/nanosecond identity extensions, and performance
equivalence to DuckDB or Doris. A ClickHouse benchmark is not this design's
acceptance evidence.

## 2. Standards, precedent and alternatives

[RFC 8259](https://www.rfc-editor.org/rfc/rfc8259) defines JSON syntax and permits
implementation number limits. This proposal preserves valid number text at an
explicit entry point while retaining MO's existing typed limits and comparison
domain. Quoted JSON-path keys use JSON escaping, not Go string escaping.
[Parquet VARIANT v1](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md)
and [shredding](https://github.com/apache/parquet-format/blob/master/VariantShredding.md)
define the file interchange contract; physical MO path storage is independent.

DuckDB development source at `c65ca930b9fe39b2e8d8ed8821018dca650965f4`
is precedent for typed/residual reconstruction and bounded path selection.
The actual independent engine used was `96063b9e39`,
`2.0.0.dev2610011535` / `v2.0.0-alpha43763`; neither is claimed to be a stable
DuckDB 2.0 release. Doris 4.0.3 uses a different native VARIANT representation;
its successful load counts do not prove standard VARIANT content compatibility.

| Alternative | Correctness and cost | Decision |
| --- | --- | --- |
| Status quo: ordinary JSON and full scans | Simple compatibility; exact-number and file reconstruction problems remain; full-document I/O continues | Retain as default ordinary API and fallback, insufficient for explicit exact ingestion |
| User-defined generated scalar columns | Existing indexing and typed bounds; manual schema, evolution and cast/type-drift policy | Recommend for indexed hot predicates; does not replace automatic document preservation |
| A new native VARIANT type and typed/residual storage | Rich logical identities and statistics; changes SQL type, comparison, catalog, wire, migration and ownership contracts | Defer; substantially wider project |
| Tagged JSON path fragments inside the source object | Reuses current JSON functions and tags; duplicate storage and private document assembly; no native scalar statistics | Select for this feature with budgets, conservative proof and rollout gates |

For mixed versions, a new private CAST overload could distinguish exact
conversion, but expands registry/binder identities. Testing only the new parser
misses stored JSON that reaches the existing CAST. A shared semantic capability
floor on the existing CAST is narrower and covers both origins. Reuse the
existing MORPC expression admission system; do not add a second capability
cache, probe transport or worker registry.

## 3. Contracts and first owners

| Contract/state | First owner and consumers | Bound / terminal action |
| --- | --- | --- |
| `JSON_PARSE_EXACT(text)` | Function registry/binder; bytejson parser; vector executor | Existing JSON type; SQL NULL passes through; invalid syntax errors; masked rows are not evaluated |
| JSON INT64/UINT64/DECIMAL to DECIMAL64/128 | CAST executor; destination decimal parser | Parse exact scalar text with requested precision/scale; ordinary FLOAT retains legacy float conversion |
| Standard VARIANT import | External reader and row-local `Document`; codec | 64 MiB conservative expansion admission, 100 logical containers, 65,536 schema nodes; cancellation cause propagates |
| Explicit VARIANT export | Frontend writer and each rotated writer | `FORMAT 'parquet_variant'`; ordinary Parquet JSON stays UTF8 text; existing bounded row groups/rotation remain |
| Optional persisted paths | Normal ObjectIO writer; flush/merge/compaction consumers | First 1,024 rows, 256 candidates, depth 16, key <=512 bytes, <=32 selected paths; manifest/fragments <=8 MiB per source block |
| Projection proof | Planner before column remapping; compiler, pipeline, scan and loaders | <=64 projected columns, <=32 paths each, <=65,536 encoded bytes including prefix/escaping; decline oversized producer contracts |
| Cached input backing | Read IOVector; decoded borrowed vectors | Free vector views before releasing backing on success/error/cancellation; only owned outputs escape |
| Execution admission | Expression walker; placement, actual sender, receiver and persisted-expression admission | One fresh endpoint observation per validation; existing five-second timeout; release capability responses, respect parent cancellation |

Source JSON remains unchanged in `SchemaData`. ObjectMeta V3 additive section
29 (`CkpMetaEnd + 1`) holds auxiliary blocks after the source block IDs.
Versioned manifests identify source block, row count, physical sequence numbers,
root-null BOOL and dense fragment columns with exact key arrays. Auxiliary
blocks do not become source data blocks or source column statistics. Serialized
bytes contribute to object size. Checkpoint/query-result/GC/ETL writers do not
automatically discover paths. No separate sidecar or background worker exists.

The reader validates section bounds, manifest version, identities, dense indexes,
types, key uniqueness and lengths. Missing optional sections/paths use complete
source reads; corrupt present metadata is an error. Discovery/parser rejection
of one candidate declines that candidate. Allocation/I/O failures remain errors
and follow ordinary transactional publication; they are not silently swallowed.

## 4. Mixed-version execution admission: required correction

The inspected implementation allocates function ID 598, but the older registry
does not recognize it. JSON-to-DECIMAL CAST keeps its identity while its exact
integer/decimal semantics change. Example failures are a remotely evaluated
`JSON_EXTRACT_STRING(JSON_PARSE_EXACT(s),'$.x')='v'` on a VARCHAR column and
`CAST(JSON_EXTRACT(doc,'$.cost') AS DECIMAL(38,3))` on previously stored JSON.
The latter must be fenced even without `JSON_PARSE_EXACT` anywhere in the plan.

Introduce two independent expression requirements:

- `ExactJSONParsing`: identify the bound `JSON_PARSE_EXACT` registry identity,
  including nested filters, projects, prepared plans and serialization. Name
  matching must follow existing plan-walker conventions without allowing an
  unrelated renamed function to evade the bound identity check.
- `ExactJSONDecimalCast`: identify CAST with a JSON source and DECIMAL64 or
  DECIMAL128 destination. Require this floor regardless of which parser/file
  created the source and regardless of its current row values. A FLOAT source,
  JSON-to-FLOAT cast, and ordinary string parser are controls, not new contracts.

Both requirements use one newly allocated MORPC version `V_exact`. Main at the
design-only base already allocates v112 for WEEK/DAYOFYEAR; **do not reuse v112**.
v113 is the next candidate, subject to rechecking main at implementation time.
Pin function IDs and the version allocation with registry/protocol tests.

The same analysis must feed all consumers:

1. `RequiredRemoteExpressionFeatures` and `RemoteExpressionFeatures.Any()`.
2. `remoteExpressionProtocolVersion`: max of all independently required floors.
3. `constrainRemoteExpressionWorkers`: on unsupported/unknown workers, fall back
   from multi-CN execution to the existing coordinator-only scheduling path.
4. `validateRemoteExpressionFeatures`: sender/receiver source floor validation.
5. `validateRemoteExpressionDestination`: check current coordinator rollout
   floor and actual worker identity/address before send; missing/unsupported
   capability produces a clear not-supported error. Never dispatch and retry
   after an unknown-function error or potentially rounded results.
6. `RequiredPersistedExpressionProtocolVersion` and existing authoring/read
   admission: views, defaults/generated/index expressions that retain these
   contracts cannot be authored below the cluster's supported floor. Capture
   the requirement before constant folding can erase its function tree.

Folded ordinary values require only the capabilities needed to consume their
remaining representation. A capability observation does not survive a later
execution/send. Worker replacement, a changed endpoint, lowered coordinator
floor, unavailable query client, nil response, timeout or stale placement cannot
authorize dispatch. Parent cancellation returns its cause; transient probe
failure yields coordinator fallback during placement or rejection before send.

The additive `TableScan.json_projection` hint has a different rule: old workers
ignore it and read the authoritative source. The hint itself need not impose
`V_exact`; actual expressions using either exact contract do. New receivers
validate nonempty hints; unsupported planner shapes never attach them.

Parquet reader semantics also depend on the executing binary. Before certifying
mixed-version file ingestion, either validate every external reader's support
for standard VARIANT or conservatively keep Parquet external scans on upgraded
coordinators during a rolling upgrade. The selected rollout policy for R1 is
coordinator-only Parquet external scans while worker support is unknown or old;
this requires an admission test and placement correction if current compilation
can dispatch them. Metadata/value field names are not a schema capability test.
Frontend export itself is coordinator owned; distributed expressions producing
its rows still obey the expression gates.

## 5. Visibility, lifecycle and compatibility

The existing object synchronization/publication boundary publishes complete
source and optional fragments together. Failed preparation publishes neither;
existing retry restarts the object write. Update/compaction rebuild from the
published batch, never reuse fragments from another object generation. Commit,
abort, delete and selected-row masks use unchanged source coordinates. Object
GC reclaims both representations; reader backing remains retained until views
and materialization finish. Context cancellation releases input and partial
owned output according to the read-path owner.

| Operation | Required behavior / release condition |
| --- | --- |
| New reader, legacy object | Full source read without migration |
| Legacy reader, new object | Ignore additive section and read complete source; prove with the pinned old binary before release |
| Mixed workers, new expressions | Coordinator-only fallback or pre-dispatch rejection; never unknown function or silent decimal rounding |
| Restart/restore | Manifest and source share identity; no process-local path catalog is required |
| Rewrite by an old writer | May omit optional fragments; source document remains valid and later new readers fall back |
| Downgrade | Stop new exact-contract authoring; inventory/remove/rewrite persisted definitions requiring `V_exact` before lowering admission; do not merely lower the version variable |
| Backup and restore | Copy the complete object and ordinary catalog admission metadata; test restore with mixed old/new objects and unchanged full-document values |

Existing ByteJson tags are reused, but that alone does not prove old binaries
retain changed CAST semantics. Exact-contract persisted definitions impose an
explicit downgrade limit. Full source bytes, a backup and a rollback plan are
required; derived subcolumns do not require a one-way migration.

Files and rows are accessed through existing authorization/tenant-scoped file
services. No new external JSON/schema lookup is introduced. Manifests/fragments
inherit source access and GC; private partial documents must not be cached under
the authoritative raw extent. Malformed file/manifest budgets contain resource
amplification. No raw documents or path keys are added as metric labels.

## 6. Default-on write costs and rollout decision

The proposal retains automatic discovery for normal writers, as in the current
implementation. This is a deliberate storage/ingestion tradeoff, **conditional
on the release checks below**, not a proven cost-free default. Existing writers
of non-JSON blocks and excluded writer kinds must take the cheap existing path.

For B source rows and P selected paths, discovery is bounded to 1,024 sampled
rows and 256 candidates; materialization still visits source rows and selected
paths, and can cost O(B * P * path traversal), P <=32. Each fragment duplicates
bytes already in source JSON, adds compression and vector allocation, and can
increase flush/compaction CPU. The 8 MiB serialized budget is per source block,
not per object, query, concurrent flush, heap or process. Large source blocks
and multiple concurrent writers still need measured peak memory. Reading a
path incurs manifest/root-null I/O and private document assembly; a tiny source
or broad projection can be slower. No latency or throughput benefit is assumed.

Before enabling this binary on production writers, compare otherwise identical
normal writer runs with discovery enabled and disabled at the existing writer
boundary. Use scalar telemetry, large tool payloads, 256-candidate long tails,
32 selected paths, type drift, over-budget blocks and non-JSON controls. Retain
source/auxiliary compressed bytes, elapsed CPU/flush time, allocation bytes and
peak memory under fixed concurrency. Proposed release budgets are <=15% ingest
CPU increase, <=10% p99 flush latency increase and <=25% auxiliary/source bytes
for representative telemetry; disclose worst-case bounded adversarial results
separately. These thresholds are acceptance decisions for this review, not
measurements. If they fail, revise the default/selection policy and re-review
before release rather than relax the measurements or assertions.

Rollout order:

1. Approve this exact design revision. Keep implementation acceptance blocked.
2. Close deviations and run deterministic admission/storage/file regressions.
3. Execute old/new binary, restart/restore/rewrite and cost gates. Skipped upgrade
   CI and mock worker versions are insufficient substitutes for old/new binaries.
4. Upgrade a canary deployment with Parquet reads restricted as above; gate
   exact persisted authoring through the existing cluster protocol floor.
5. Expand deployment only after canary flush errors, object size, CPU/memory,
   query correctness and fallback rates meet the budgets; no new background job.
6. On regression stop new deployments and restore the previously verified binary
   only after the downgrade inventory check. Retain source objects and backup.

There is no shipped live SQL switch disabling discovery in the inspected code.
Do not advertise one. The current fallback/removal plan is controlled binary
rollback after compatibility checks; adding a live switch would require a
defined configuration/ownership contract and a reviewed design revision.

## 7. Acceptance tests and evidence

Use existing `expressionVersionClient`, scheduler and protocol fixtures; keep
tests deterministic, isolated and resource cleanup lexical. The smallest new
mixed-version data fixture needs rows for exact integer/decimal, SQL NULL,
JSON null and an ordinary control, not another 10,000-row generation loop.

| ID | Input or transition | Required independent oracle |
| --- | --- | --- |
| M01 | Bound ID 598 with nested filter/project; ordinary parser control; wire round trip | Exact parser requirement retained by the actual plan walker, controls unflagged |
| M02 | JSON column to DECIMAL64/128 without parser call; JSON-to-FLOAT and string-to-DECIMAL controls | Exact cast requirement set only for affected contract; below-floor remote cannot evaluate |
| M03 | v111, v112, `V_exact-1`, `V_exact`, newer/unknown workers; combined requirements | Correct max floor; old/unknown placement falls back; new worker remains eligible |
| M04 | Worker changes after placement, mismatched ID/address, nil/error response, missing client, floor lowered during probe | Actual sender rejects before creating/sending an execution payload; balanced response releases |
| M05 | Cancel before/during probe and existing timeout | Bounded termination; no payload sent; parent cause preserved |
| M06 | Receiver deserialization at below/equal floor | Independent source validation; unsupported error before executor construction |
| M07 | View/generated/default/index exact expressions, including constants folded by binder | Durable requirement observed before folding; authoring/read rejection at inadequate cluster floor |
| M08 | Actual upgraded coordinator plus pinned pre-PR CN, row-dependent example and stored JSON cast | Coordinator result is exact or explicit rejection before dispatch; old worker invocation counter/log proves neither contract ran there |
| M09 | Actual upgraded worker replaces old worker on same fixture | Distributed execution succeeds with exact DECIMAL(38,3) string `9007199254740993.001`; ordinary query still works in mixed cluster |
| M10 | Standard VARIANT file plus ordinary Parquet control during worker upgrade | VARIANT reader cannot execute on unsupported worker; no metadata/value ordinary-object result; supported file values preserved |
| S01 | Source and projected reads on real object extents; null/missing/drift/control keys | Same tags/bytes and row coordinates; projected read avoids raw extent; full consumer still gets complete source |
| S02 | Cache bypass/last reference release, decode error and cancellation | Poisoning allocator proves valid root-null backing until use, balanced releases, no partial output |
| S03 | 64/65 columns, 65,536/65,537 encoded bytes and escaping growth | Over-limit optimization declines, valid SQL still executes; malformed nonempty receiver contracts reject |
| S04 | Old/new objects, flush/update/delete/compaction, backup/restore and restart | Visibility/full document values preserved; legacy binary ignores auxiliary section; no orphan sidecar |
| P01 | Standard independent primitive bytes, typed/residual objects/lists, SQL NULL vs JSON null | Supported logical values exact; overlap, conflicts, unsupported annotations and malformed levels reject |
| P02 | Explicit/plain export, NULL, empty/split output, independent DuckDB read | Correct annotation/counts/value identities; ordinary format remains compatible |
| C01 | Enabled/disabled discovery with fixed data/concurrency, controls and adversarial bounds | Retained measurements satisfy section 6; serialized budget is not reported as a heap bound |

Historical evidence on `72b22e7d`: 13 successful, 15 skipped and two neutral CI
checks, including UT/SCA/BVT/Connector; both upgrade checks skipped. Prior
ObjectIO/planner/race/SQL/fuzz checks and the 9,677-row restart dataset cover
their stated local contracts. No real mixed-version run or write-cost acceptance
run is certified. Originals of local execution logs must accompany new claims;
extracted summaries and hashes identify logs but are not execution receipts.

DuckDB native files and MO exports passed the reported content tests. Doris
native VARIANT import failed content checks despite 10,000 loaded rows. Its
UTF8 JSON export bridge matched actual exported values after Doris had dropped
explicit null fields and changed booleans to integers. Lossless Doris native
VARIANT interchange is excluded; this limitation must remain visible to users.

## 8. Implementation deviations and design decision record

The current snapshot is not ready for approval. After design acceptance:

- Add both exact-JSON execution requirements to all runtime and durable
  admission consumers; allocate a fresh protocol version against current main.
- Prove/correct Parquet external-scan mixed-worker placement before claiming
  mixed-version file ingestion.
- Execute real old/new binary compatibility, rewrite/restore and cost acceptance
  gates; retain original commands, source/binary/toolchain IDs and exit status.
- Reconcile the implementation with this approved revision, then perform the
  remaining full implementation review. Earlier reviews covered previous fixes,
  not complete acceptance of this combined feature.

Decision log proposed for approval: explicit exact API with existing JSON tags;
standard VARIANT with strict supported identities; complete authoritative source
plus same-object optional fragments; semantic fencing of existing CAST;
coordinator fallback before dispatch; automatic normal-writer discovery subject
to measured release budgets; no lossless Doris-native claim or runtime disable
switch claim. No unresolved architectural choice is deferred as an implicit
implementation decision. Failed acceptance evidence requires design revision.

```text
Change scope: combined PR #30071 and issues #30057/#30058/#30087
Trigger: public SQL/format, persisted object metadata, distributed execution,
         multiple ownership boundaries and default-on write hot path
Design: R1; exact commit to be linked by the design-only PR
Blocking findings: distinct design approval absent
Decision log: proposed decisions above, awaiting reviewer acceptance
Decision: REQUEST_CHANGES / approval pending
Implementation deviations: exact execution/durable admission, mixed-version
                          Parquet placement proof, release compatibility/cost evidence
Approval record: pending; reviewer, immutable revision, decision and date required
```
