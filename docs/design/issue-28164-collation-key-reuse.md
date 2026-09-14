# Collation-aware keys using existing indexes (#28164)

Status: replacement design proposal; implementation and product acceptance pending.

## 1. Scope and supersession

The preceding commit reverts design-only PR #28423 (merge
`bd8801ab3b4669b278611a1e13244da4c5bdabdd`). That revert removes the independent
sidecar/locator/registry/activation design as the implementation baseline. It
does not revert production code, close #28164, or close the existing Draft PRs.
This document proposes the replacement on upstream/main
`bb358ea76aeb44a6eed5079616e7e75ba35e75b9`.

The observable goal is consistent collation-aware uniqueness and query results:
an INSERT of a collation-equivalent non-NULL PK/UNIQUE value conflicts; ODKU
selects the correct existing row; indexed and unindexed queries return identical
original values. Physical lookup and filtering use the same transformed domain
as physical storage. No independent sidecar or transaction state machine is added.

This delivery changes documents only. Library integration, tuple changes, SQL
semantics, migration, performance and distributed tests are implementation work.

## 2. Verified baseline (do not reuse the earlier checkout's assumptions)

The earlier discussion inspected `816c913bf02c`, which lacked the newer metadata
path. The current baseline already has:

- `proto/plan.proto`: `Type.charset` (field 8), `TableDef.default_charset` (39).
- `pkg/container/types/types.go`: persisted semantic classes Legacy=0, Binary=1,
  UTF8MB4Bin=2 (text PAD SPACE), UTF8=3 (general-ci).
- `pkg/sql/plan/build_util.go`: column/table attribute resolution and explicit
  compatibility aliases. The spelling `utf8mb4_0900_ai_ci` currently maps to the
  general-ci class; it is NOT native MySQL UCA 9.0 NO PAD semantics.
- `pkg/sql/colexec/aggexec/utf8mb4_general_ci.go`: a TiDB-derived weight table,
  original source commit `6cbbd222c786948379edc50ef8a8c37e485957c0`, with MO-specific
  malformed-byte handling. Existing MIN/MAX comparison is not by itself proof
  that every scalar/hash/index consumer uses the same semantics.
- `build_ddl.go`: hidden UNIQUE index key -> main-table physical PK; composite
  PK user columns -> hidden `__mo_cpkey_col`.
- `multi_update`: existing main/index maintenance through the transaction's
  relation handles. This is a reuse point, not proof of new-code atomicity.

Reuse this metadata and these structures. Do not allocate new protobuf numbers,
copy a second weight table, or redefine zero-valued old metadata casually.

## 3. Representation and non-negotiable invariants

For a resolved comparison domain D, let C_D compare original values, W_D produce
comparison weights and E encode a tuple field. Domain includes effective
collation, type, index prefix and padding policy, resolved from schema/expressions.

1. For non-NULL full unique keys, C_D(a,b)=0 iff K_D(a)=K_D(b).
2. For any physical byte-ordered lookup, sign(bytes.Compare(K_D(a),K_D(b)))
   equals the comparator's sign in that same domain.
3. Equivalent values use the same index probe and lock identity; hash build and
   probe use the same resolved domain. Hash collisions still require equality.
4. Filters may retain extra candidates but must never discard a true match.
5. Original user values are immutable inputs to key construction. Query output,
   FK presentation and FULLTEXT maintenance do not receive opaque weights.
6. No collation ID, registry digest or domain descriptor is embedded per key.
   Existing tuple type/escape framing remains. Key-format identity belongs to
   the relation/index metadata, not every row.
7. Binary/legacy pass-through keeps existing key bytes and avoids a weight
   allocation. Nonbinary `_bin` is NOT automatically a pass-through domain.

Prefix UNIQUE compares indexed prefixes, not full SQL values. NULL UNIQUE parts
retain existing exclusion rules; a PK rejects NULL. Mixed numeric/text tuples
retain the existing numeric codecs; no new all-type key format is introduced.

## 4. strxfrm-like implementation research and selection

Inspected candidates (pinned source, not moving branch names):

| Candidate | Relevant API | Decision |
| --- | --- | --- |
| Existing MO TiDB-derived general-ci weights; TiDB v8.5.3 `dc2548aac79a712265e831cff2a3a896bc0a5a38` | `Collator.Compare`, `Key`, `KeyWithoutTrimRightSpace`, `CanUseRawMemAsKey` | Primary implementation route for the existing supported general-ci class: extract the already present weight owner into a dependency-light shared package and add an append-to-buffer key adapter. Do not import all of TiDB. |
| Vitess v22.0.1 `aafd40357555438f9df7b7e28afe7e0828502896` | `colldata.Collation.Collate`, `WeightString`, `WeightStringLen` | MySQL-oriented reference and alternative if the existing mapping fails the agreed compatibility oracle. Its padding/length options require an explicit adapter; do not call with an arbitrary buffer capacity. |
| `golang.org/x/text/collate` (MO already depends on x/text v0.35.0) | `Key`, `KeyFromString`, `Compare` | Useful Unicode API, but locale options do not establish MySQL named-collation compatibility. Not the production selection for this issue. |
| MySQL `MY_COLLATION_HANDLER::strnxfrm` | charset-specific transformation | Semantics reference. A direct C integration/port needs dependency, license and deployment evaluation; it is not needed merely because the analogous C API exists. |

The TiDB snapshot's unknown-collation fallback and malformed-input behavior must
not be copied blindly. Resolve only supported domains; preserve MO's existing
malformed legacy-value contract until a separately tested policy changes it.
Do not use process-global libc locale state for persisted keys.

Implementation route is selected; the final byte format is deliberately NOT
frozen until the independent compatibility and performance spike below passes.
If that spike fails, revise the adapter/selection before connecting writers.
Do not compensate for a comparator mismatch by creating another storage layer.

Sources:

- [TiDB interface](https://github.com/pingcap/tidb/blob/dc2548aac79a712265e831cff2a3a896bc0a5a38/pkg/util/collate/collate.go)
- [TiDB general-ci](https://github.com/pingcap/tidb/blob/dc2548aac79a712265e831cff2a3a896bc0a5a38/pkg/util/collate/general_ci.go)
- [TiDB binary/PAD variants](https://github.com/pingcap/tidb/blob/dc2548aac79a712265e831cff2a3a896bc0a5a38/pkg/util/collate/bin.go)
- [Vitess weight-string contract](https://github.com/vitessio/vitess/blob/aafd40357555438f9df7b7e28afe7e0828502896/go/mysql/collations/colldata/collation.go)
- [Go collation API](https://pkg.go.dev/golang.org/x/text/collate)
- [MySQL strnxfrm contract](https://dev.mysql.com/doc/dev/mysql-server/8.0.45/structMY__COLLATION__HANDLER.html)

### Required selection spike

Compare both equality and order against a pinned MySQL server using explicit
collations, plus the current MO comparator as a separate regression oracle.
Record any differences rather than claiming MySQL agreement from MO self-tests.
Include embedded NUL/control characters versus PAD SPACE, trailing spaces,
combining marks, accent/case variants, supplementary characters, long strings
and prefix boundaries. MySQL's real 0900 semantics and MO's existing alias must
be reported separately. Native 0900 support is not silently introduced here.

PAD SPACE deserves a dedicated decision: trimming trailing spaces establishes
some equality cases but is not a universal proof of ordering for shorter strings
versus extensions with weights below SPACE. Evaluate a library-correct compact
key against a fixed-domain padding reference; do not choose per-row padding
length, truncate overlong query constants, or pad to buffer capacity. Freeze the
chosen adapter with normative hex vectors and maximum-size behavior after this
comparison. Full-width padding is not accepted without measured space/CPU cost.

### Local research evidence already obtained

An isolated standard-library-only Go probe copied the current MO weight source
without changing it, emitted each weight as big-endian uint32, trimmed trailing
ASCII spaces like the current comparator, and used tuple-style zero escaping.
28,224 pair comparisons agreed with that MO comparator; key escape/decode round
trips passed. Mutants demonstrated that a raw-value suffix breaks unique identity
and a leading length can break ordering. This is a candidate experiment, not a
production codec choice, native MySQL compatibility, or a full tuple-parser test.
The raw probe and log are retained in the delivery's ignored research artifact.

## 5. Tuple serialization and deserialization

### Encoding

Keep generic value serialization and persisted legacy tuples unchanged. Add a
schema-aware physical-index encoding entry point used by actual index producers.
Resolve a small immutable per-part plan once per schema/plan, not per row.
Conceptual API names below are proposals, not new public SQL functions:

```text
ResolveKeyPart(column, prefix, keyFormat) -> immutable part plan
AppendIndexTuple(dst, partPlans, typedValues) -> owned tuple bytes or error

for each part:
    apply existing SQL storage coercion and prefix semantics to the original value
    preserve PK/UNIQUE NULL policy
    if part is proven byte-compatible: use existing raw tuple encoder
    else: obtain W_D(value) and pass those bytes to existing escaped field encoder
```

CHAR storage normalization is separate from comparison padding. Prefix lengths
are measured in the declared units before the weight transformation; do not
truncate a generated multi-weight sequence to simulate a character prefix.
Input coercion errors occur before publication, without partial index mutation.

`Packer.encodeBytes` already escapes 00 as 00 FF and ends the field with 00.
Use the existing framing after proving it preserves the selected weight order
for all emitted bytes. Do not prefix weights with payload lengths, or insert raw
values between composite comparison parts. Check packer overflow and size limits;
never accept truncated weight output as a complete key.

Raw binary types and legacy keys retain their old bytes exactly. Text `_bin`
PAD SPACE may require padding-aware transformation despite its name; only a
proven raw-compatible domain uses the no-transform fast path. Resolve this branch
once, reuse bounded scratch buffers, and copy borrowed weights before their owner
is reset. No per-row registry lookup, locale mutation or C call is required.
Hidden weight columns and internal weight expressions use the existing binary
semantic class even if their storage container is varchar-shaped. Their original
source columns retain their declared text class. Never apply collation again to
the internal key's bytes or expose that binary class as the user's column type.

### Decoding: two distinct outputs

`tuple.decodeBytes` reverses framing and returns the weight bytes; it cannot
recover the original text. `K("Alpha")=K("alpha")` makes such an inverse
mathematically impossible. The schema-aware index decoder must mark transformed
parts as opaque key data, not ordinary VARCHAR values. Bounds checks reject
truncated escapes, missing terminators, invalid part counts and oversized input.
Caller-owned versus borrowed decode buffers must be explicit.

Default for this design: keep originals in the existing main-row columns and
fetch the main row when a query needs a transformed column's original value.
Disable only the invalid covering/serial_extract substitution for those columns.
An index can still cover unrelated original-valued columns/PKs when valid.
An additional original-value payload is a future measured tradeoff, not required
for the initial design. Never append original bytes to the UNIQUE comparison key:
that makes collation-equivalent strings different physical keys.

Required contracts: Decode(Encode(raw))=raw on unchanged raw tuples;
DecodeKey(EncodeKey(value))=W_D(value) on transformed fields; fetching by the
decoded locator returns the exact stored original. Do not advertise key decoding
as original-value round-trip.

## 6. Existing storage and DML integration

UNIQUE secondary indexes keep their two-column structure: transformed tuple key
and actual main-table physical PK. Nonunique secondary indexes transform indexed
text parts and retain the existing PK tie-breaker; the tie-breaker is not included
in UNIQUE conflict identity. Integer and unrelated indexes keep their old paths.

A transformed single-column string PK keeps its user column and adds a hidden
physical key through the existing composite-PK construction mechanism. Audit
all `len(Pkey.Names)` branches: logical arity must not substitute for physical
key representation. Secondary-index backfill and ordinary writes must copy or
generate the exact same hidden PK, rather than reconstructing legacy serial bytes.

| Path/owner | Required change and terminal assertion |
| --- | --- |
| `build_ddl.go`, `preinsert.go` | Hidden-key representation and schema-aware expression; user columns and SQL DDL presentation preserved. |
| `bind_insert.go`, `build_insert.go` | Physical write key, existing-row probe, same-batch duplicate arbitration and ODKU candidate selection agree. Preserve current restrictions on changing PK/UNIQUE through ODKU. |
| `bind_update.go`, `bind_delete.go`, `bind_replace.go` | Old/new keys use one contract; no stale index entries; self-updates with equal keys do not conflict with themselves. |
| `compile/util.go`, `ddl_index_algo.go` | CREATE/ALTER UNIQUE duplicate precheck and backfill group by the exact final key; `SkipPkDedup` is valid only after equivalent precheck. |
| `multi_update`, bulk/LOAD/object-storage and partition writers | Reuse current transaction and maintenance paths; prove every physical writer supplies the same key and does not bypass validation. |
| FK probes and `lockop`/commit validation | Same resolved parent/child domain and same physical lock identity; main/index commit or rollback together, including two-CN races. |

No new transaction coordinator is introduced. Existing transaction acquisition
alone does not prove correctness: inject failure between main/index operations
and prove rollback, retry and recovery against real storage.

## 7. Query planning: always probe the stored representation

For all PK, UNIQUE and ordinary secondary-index lookups, transform the search
operand to the exact stored key domain, including IN lists, joins, prepared
parameters and DML conflict probes. A stored-key expression must not be transformed
twice. Tag internal plan expressions with representation information; do not infer
that arbitrary VARCHAR is already a weight string.

The optimizer can use an index only when the expression's resolved comparison
domain matches that index's domain, or a proven conversion preserves the needed
predicate. Explicit COLLATE, mixed collations/coercibility, casts and over-width
constants cannot be blindly forced into an index's narrower domain. Otherwise
use the correct original-value residual/scan. Never truncate a probe to fit a key.

Range bounds must have the same ordering as stored keys. Composite partial bounds
need proper tuple prefix/end boundaries; inclusive/exclusive bounds cannot be
implemented by casually appending bytes. LIKE/pattern matching is not generally
equivalent to weight-prefix matching: retain the true residual and disable an
unproved prefix rewrite, especially for expansions/contractions and ignorable
characters. Preserve NULL three-valued logic for prepared and residual predicates.

`apply_indices.go` currently substitutes index keys or serial-extracted parts for
user columns. Gate that substitution by recoverability. `runtime_filter.go`,
shuffle, index joins and PK-specific rewrites must use physical representation,
not just logical PK arity. Scalar comparisons, hash join, grouping/DISTINCT and
MIN/MAX must resolve the same effective domains; physical fixed-width keys from
different schemas must not be blindly reused as cross-domain SQL hash keys.

## 8. Zonemap, Bloom filter and performance

Transformed physical keys can use existing binary filter machinery only if key
generation and probe conversion agree. Bind Bloom build/probe and lock hashing
to the same bytes. Pin any persistent hash format under existing storage rules.

Original text columns have separate zonemaps: their summaries are not transformed
merely because an index is. Audit original-column predicates before pushing them
to bytewise summaries. Rebuild compatible summaries or decline unsafe pruning;
never reinterpret old min/max using a new order. In `tae/index/zm.go`, long string
bounds truncate to 30 bytes and adjust upper bounds: test this with generated
weights, embedded zero, long common prefixes and maximum-prefix overflow.

`readutil/expr_filter.go` also uses order for object/block exclusion, binary seek
and early stop. Validate these, not only the row comparator. A filter PASS requires
persisted objects, observed filter execution, and identical original results to
a verified unpruned reference. Always-reading-everything is safe fallback, not
completion of the promised optimized path.

Measure raw binary/legacy paths for unchanged bytes, allocations and CPU; measure
transformed paths for key expansion, encode/probe CPU, memory, index size, write
cost and end-to-end lookup/range cost. Record selectivity/blocks read to distinguish
correct but ineffective pruning from useful filtering. Set acceptable budgets with
reviewers before implementation acceptance; no unmeasured speedup claim.

## 9. Metadata, old data and rollout

Column metadata supplies effective collation. Reuse `Type.Charset` and
`TableDef.DefaultCharset` with their documented meanings; existing aliases are
explicit compatibility policy, not exact external names reconstructed from an ID.
Copy through plan/type conversion, catalog/cache, schema clone, ALTER, prepared
plan invalidation and backup/restore. If native additional collations are later
added, extend metadata rather than embedding their IDs per physical key.

Collation semantics alone cannot distinguish a pre-change raw index from a new
weight index having the same declared collation. Introduce the minimum durable
relation/index key-format marker (absent=legacy, new=collation tuple), reusing
existing schema-property transport where safe; verify the actual persistence
owner before selecting a protobuf field. This marker is not per-key metadata.

Initial rollout policy: old relations/indexes stay on their old physical formats;
new format creation is admitted only in a cluster whose readers and writers all
support it. Use an existing enforceable cluster upgrade/capability gate; identify
its concrete owner in the integration stage. If no such gate can protect old
nodes, require a completed homogeneous upgrade before enabling creation. A table
flag ignored by old nodes is not protection. Do not recreate the old bespoke
activation state machine just to check a format version.

No live in-place reinterpretation. First release supports new tables; old tables
migrate explicitly via a new-format table/index rebuild using existing DDL
ownership and transaction publication. Precheck collisions, fence/drain writers
under that DDL mechanism, validate reconstructed FK/index references, and publish
only the complete result. Coordinator failure, cancellation and uncertain commit
must use existing DDL recovery ownership; if that path cannot be proven, in-place
migration remains unsupported and copy/reload is the documented route. Rollback
after new-format writes requires compatible binaries or explicit data conversion;
turning a switch off must not reinterpret bytes. Snapshots/restore carry the format.

## 10. Delivery stages and acceptance

| Stage | Concrete deliverable | Acceptance before dependent work |
| --- | --- | --- |
| D0 (this PR) | Revert old design; publish this proposal and source research | Documentation review, explicit decisions/gaps; no product-fix claim. |
| D1 | Shared comparator/weight adapter spike, binary fast path, golden vectors | Independent MySQL/MO differential results; PAD/control-character and alias policy settled; bounded cost; freeze key bytes only then. |
| D2 | Schema-aware tuple encoder/decoder and minimal format metadata | Raw byte compatibility, key round-trip, order, malformed-input fuzz, copy/persistence checks; no enabled tables. |
| D3 | Existing UNIQUE/secondary indexes, hidden string PK and all DML/DDL producers | Public SQL/BVT and failure/interleaving tests; correct original-value retrieval and backfill. |
| D4 | All compatible lookup/optimizer/filter consumers | Forced/observed index vs reference scan results, persisted ZM/Bloom/seek coverage; SQL hash/group agreement. |
| D5 | Upgrade admission, explicit rebuild/copy route, distributed QA and performance | Exact binary/head evidence, mixed-version rejection, restart/restore, no lost uniqueness, measured budgets. Only then enable. |

D1-D4 may be developed on disabled branches, but code for partial consumers is
not a complete fix. No stage automatically makes existing Draft PRs Ready.

Required test families (extend canonical `test/distributed/cases/charset_collation/`
and the applicable INSERT/ODKU cases, with intentional `.sql` and `.result`):

- Case/accent, composed/decomposed text, punctuation, controls/NUL, supplementary
  characters; PAD SPACE/NO PAD and actual binary vs text `_bin` controls.
- Scalar/vector/constant/prepared expressions, operand reversal, explicit mixed
  domains, NULL vs empty, single/composite/prefix keys, numeric/text combinations.
- INSERT, IGNORE, ODKU, UPDATE, REPLACE, DELETE; same-batch and two-transaction
  duplicates; update-to-self vs another row; rollback without phantom indexes.
- CREATE UNIQUE after existing collation-equivalent rows; concurrent write during
  rebuild; failed publication does not expose half-built data.
- Covering index must return `Alpha`, not its weights; changed raw value with an
  equal logical key still updates originals and applicable FULLTEXT state.
- Point/range/IN/join probes, composite boundary inclusivity, LIKE residuals;
  index enabled/disabled reference with the same correct comparison semantics.
- Persisted multi-block/object data, 30-byte truncation, Bloom execution, seek and
  early termination; verify observer counters/plans and include an intentionally
  unconverted-probe mutant that fails the test.
- Crash/restart, restore, compaction, batch/object ingestion, partition and FK
  paths, mixed-version reader/writer rejection, migration collision handling.

Use barriers rather than sleeps for races; prove the observer with a counterexample.
Unit/property tests establish local contracts; production BVT and distributed
failure tests establish the terminal. Do not count one as the other.

## 11. Existing PRs and review decisions

At preparation time #28472, #28494, #28505, #28507, #28512, #28518, #28519 and
#28520 are open Drafts. This PR does not mutate them. Reassess their content
against D1-D5 before reuse: sidecar/locator/registry protocols are not prerequisites;
metadata work must not duplicate current main; general-ci source should have one
owner. FULLTEXT stored-value identity can be reviewed independently with real SQL
coverage. #28519's formal CHANGES_REQUESTED remains the reviewer's decision.

| fengttt concern | Response / remaining review obligation |
| --- | --- |
| Why sidecar rather than normalization/comparison? | Reuse existing index/hidden-PK and transaction ownership; eliminate independent sidecar. |
| Use a real transformation library | Pinned TiDB/Vitess research and existing MO lineage; D1 validates the selected adapter independently. |
| No collation metadata in every key | Domain comes from schema; only existing tuple framing in keys; format version is relation-local. |
| Query transformed values, keep order and filters working | D3/D4 producer/consumer closure, matching-domain bounds and observable persisted filter tests. |
| Redesign before writing more code | D0 replaces the premise; D1 semantic/format decisions precede writer integration. |

Review must explicitly accept the supported-domain/alias policy, final PAD/order
adapter, original-value retrieval tradeoff and rollout boundary. Source research
and the local candidate probe are complete for this document; design approval,
native MySQL differential testing, production implementation, BVT, upgrade and QA
are pending. No timing or exact-head CI statement in an older PR is reused here.
