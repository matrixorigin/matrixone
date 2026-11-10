# SDK quality contracts: second extension set

Revision: 1, 2026-10-26. Status: accepted for implementation after the separate
`gpt-6.1-sol` / `xhigh` design gate records this exact revision's hash.
Parent designs: [core contracts](sdk-quality-contracts.md) and
[first extensions, revision 2](sdk-quality-extensions.md).

This addendum covers sixteen further issues assigned to the SDK repair task. It
changes Python SDK behavior and tests; it does not add a kernel feature, a general
query AST, composite Pinecone IDs, async analytics, or new automatic retries.
The first extension set's reviewed physical-connection checkout owner and scoped
fulltext setting lifetime remain authoritative.

## 1. Evidence and issue disposition

The implementation input is base `b466f4ae9b74813a4ff93eaf43a6c81e6cbb8b0b` plus
the already implemented and reviewed first scope. Design commits `559fd3ba5f`,
`e78ca18589` and `e0bf35f4d2` precede their implementations. External task record
`~/.codex/issue-to-pr/matrixone/sdk-quality-20261026` stores issue snapshots,
source fingerprints, baseline probes and exact server/dependency identities.

Evidence reused without repeating discovery:

* `new16-baseline.log` / `probe-new16.py`: actual current service reproduces
  missing JOIN ON, l2_sq mapped to L2, duplicate distance overwrite, textual
  vectors, metadata ID leak, partially committed upsert and no-PK wide deletion.
  The reported binary-expression and subquery cases already return correct rows
  through the original shared compiler; they need regression coverage, not a
  second implementation.
* `new16-managers-baseline.log`: current service confirms PITR write acknowledgment
  followed by failed mixed-case lookup; Stage no-op create returns proposed state,
  missing ALTER IF EXISTS raises; changed CSV types still expose old relation SQL.
* `new16-metadata-baseline.log`: wildcard emits the same object's sizes
  1909/792 bytes twice, named v emits column sizes 46/48 and nonempty ZoneMap bytes.
  Object statistics and named-column statistics must have separate owners.
* `fulltext-token-discriminator.log` and `fulltext-dollar-discriminator.log`:
  release 4.2.5 and current both accept doubled-quote apostrophes and dollar STRING
  terms with backslashes, reject CONVERT(hex) in AGAINST, and reject dollar-string
  bodies containing another dollar. This is MatrixOne's lexer behavior, not
  PostgreSQL's delimiter-matching behavior.

| Issue | Causal boundary in current code | Disposition |
| --- | --- | --- |
| #31227 | query/query_async assign raw driver vector text to values | Shared numeric response decoder, sync/async flags matrix. |
| #31238 | engine connect_args omit local_infile; LOCAL dispatch enters an unnegotiated protocol | Explicit connection opt-in and fail-before-I/O LOCAL admission. |
| #31240 | CSV source check validates names/records as strings but never compares fresh inferred types | Capture observed source schema independently of declared scan schema; re-infer before SQL/query/import. |
| #31242 | brief sizes and detail rows aggregate wildcard per-column duplicates as separate objects | Shared object grouping for wildcard brief/detail, preserve column null-count meaning. |
| #31243 | Stage create returns request object; alter unconditionally requires post-write row | Return observed catalog state; missing successful IF EXISTS yields None through a typed optional lookup path. |
| #31244 | PITR lookup compares original case as a string; write/readback share one failure message | Server LOWER lookup and explicit acknowledgment/readback error boundaries for all create scopes and alter. |
| #31245 | subquery historically discarded deferred parameters | Reported path already fixed by safe fragment rendering. Render any remaining SQL/params at the subquery string boundary and test derived JOINs. |
| #31246 | scan always selects metadata_scan(table,'*'), whose fast path has no ZoneMap | Add source-column selection distinct from output projection; explicit statistics requests use named-column scans. |
| #31247 | vector queries emit duplicate distance labels then dictionary conversion overwrites stored values | Validate result labels before conversion and raise QueryError on ambiguity; no new distance_alias API required. |
| #31248 | old literal_binds could not encode non-UTF8 binary | Already fixed by typed binds plus safe binary literals. Verify binary/NULL/Decimal boundaries with public sync/async paths. |
| #31249 | unknown distance types fall back to L2 in several SQL producers | One strict metric-to-function mapping including l2_sq; all producers share it. |
| #31251 | optional JOIN ON emits a bare join, even with FK metadata | SQLAlchemy-owned unique FK inference; missing/ambiguous relationship rejects. |
| #31252 | response metadata loop includes every non-distance/non-vector result field | Schema-derived metadata whitelist excludes the ID, shared with #31227. |
| #31253 | per-record local validation interleaves with separately committed writes | Whole-batch validation and SQL preparation before mutation; one owned transaction for all DML. |
| #31254 | all fulltext SQL producers interpolate unescaped terms | One grammar-aware fulltext STRING renderer across basic/advanced/filter/query helpers. |
| #31255 | first-PK or id fallback does not prove unique vector identity | Exactly one real primary-key column required for all identity-bearing operations; no fallback. |

Source inspection supports these causes. New local current-server probes cover
most individual triggers, not every acceptance cell. Both-server post-fix proof,
LOCAL negotiation, CSV import non-mutation, named-column statistics and cancellation
controls below remain implementation acceptance work. No design PASS is a claim
that these pending tests have already passed.

## 2. Invariants and ownership

1. No SDK response replaces one stored field with another computed field or
   reports a string where a numeric vector list is promised.
2. One Pinecone ID names at most one row. A name like id is not evidence of a
   primary key. No mutation is admitted on an unsupported identity schema.
3. A locally rejectable batch input causes zero writes; server rejection or
   cancellation before commit rolls back the one owned transaction. Lost COMMIT
   acknowledgment remains an uncertain outcome, with no automatic replay.
4. SQL text boundaries consume their own values once. Text-only APIs keep their
   return type; binary/Decimal/quoted terms retain value semantics.
5. JOIN inference never silently requests a Cartesian product. Explicit ON is
   authoritative; a deliberate Cartesian product can be expressed with ON 1=1.
6. Observed catalog/source state is reported as observed state. Successful SQL
   plus failed readback is not described as an unexecuted CREATE/ALTER.
7. Object-level sizes are counted once per object; column ZoneMap/size results
   retain their own column identity and cannot be folded into object totals.
8. LOCAL negotiation is a connection-level decision. A LOAD call never mutates a
   pooled connection's capability flags or opens a hidden replacement engine.

Pure helpers own no resources. Engine/Session contexts own admission, commit,
rollback and release. New async work is awaited by its caller; no tasks, queues,
retries, global caches or cleanup daemons are introduced.

## 3. Vector values, result labels and row identity

### Response decoder

Extract one response materializer used by query and query_async. Its inputs are
an operation's observed single PK, metadata column names, vector column name,
include flags and checked vector result rows. Metadata uses that explicit
whitelist, preserving ordinary metadata names and values. It excludes the PK and
the selected vector column by schema identity, not by assuming the PK is id.
A stored metadata field named distance cannot silently disappear: ambiguous
projection labels fail as specified below.

When include_values=True, accept an existing numeric list/tuple or parse the
server's JSON-like vector text with a non-executing parser. Produce a fresh
List[float]. Reject malformed text, nested/non-numeric/bool/nonfinite components,
and NULL values that cannot satisfy the promised vector representation, with
QueryError identifying response decoding. Never use eval, silently return an
empty vector, or turn malformed data into None. Known dimension mismatches are
errors. include_values=False continues to produce None and avoids decoding;
include_metadata=False returns {}. PK values remain stringified in match.id.

### Duplicate distance labels

VectorManager's similarity, range and rank methods share one result-to-dictionary
helper. Read result.keys() before constructing any dictionary, including for a
zero-row result. Reject duplicate case-insensitive labels with QueryError and
an actionable instruction to alias a stored projection. The diagnostic names
conflicting labels without dumping row payloads. Cover Client/AsyncClient buffered
results and Session CursorResults. Preserve the original result/connection owner.

Default SELECT * and explicit stored distance projections therefore fail clearly
when they collide with the computed distance. Callers can select
`distance AS stored_distance`; their stored Decimal and computed distance are
then both retained. Pinecone include_metadata on such a schema may also reject;
this is a documented correctness boundary until a separately designed alias API
exists. include_metadata=False can succeed when its selected ID/vector fields do
not collide. Do not silently rename or overwrite public fields.

### Primary key and batch write contract

The shared DESCRIBE interpretation must require exactly one PRI column. Reject
both composite and missing PK schemas with ValueError before query/delete/upsert/
batch_insert can publish IDs or mutate rows. Remove the ordinary-column id fallback.
Empty input remains a no-op where already documented; it does not imply schema
certification. Fresh operation observations preserve the prior no-stale-cache rule.

For nonempty upsert, copy/validate every input mapping and build every DML statement
before the first mutating SQL. Check required PK/vector fields, supported scalar
values/identifiers, vector numeric shape and known dimensions across the whole
batch. Use current schema to reject unknown input fields. Retain per-row optional
metadata semantics; sparse rows need not share identical field sets. Do not mutate
caller dictionaries/lists. HNSW mutation restrictions remain explicit.

Use existing sql_identifier and safe parameter/literal rendering for all column
names, scalar values and vectors; no ad hoc quote formatting. Store prepared
statements/values as batch-local data, not an alternate query framework. Then
execute all upserts sequentially through one client.session()/AsyncClient.session()
and return upserted_count only after successful commit. This reuses the explicit
backend BEGIN and BaseException rollback/close owner already reviewed. Do not
commit through Client.execute per row, acquire one session per row, or implement
application-side compensating deletes. Repeated IDs preserve sequential upsert
semantics within that transaction.

The documented handle client is Client/AsyncClient. Do not silently commit a
caller-owned Session supplied through unsupported duck typing; either explicitly
reuse its ownership without a nested commit or reject before mutation. No support
for composite ID serialization is added. Concurrent DDL between observation and
DML remains an honest server error and transaction rollback.

For async upsert, one existing query_timeout budget must cover awaited preflight,
admission, writes and commit (asyncio.wait_for is available on Python 3.8). Do not
reset a full budget per row. Cancellation reaches Session cleanup; cleanup can
extend the foreground deadline while the driver releases ownership. Sync driver
read/write timeouts remain per-I/O rather than a new whole-batch wall-clock promise.
Pure local validation is O(total input size); input is already an in-memory batch.

### Strict metrics

One pure mapping owns l2 -> l2_distance, l2_sq -> l2_distance_sq,
cosine -> cosine_distance and inner_product -> inner_product. Similarity and rank
producers, sync and async, share it. Unknown names raise a clear existing public
validation/query error before SQL dispatch; never default to L2. Range remains
its documented L2 operation unless an explicit metric parameter is separately
added. Scores are raw native distances, including negative inner product; this
addendum introduces no sign inversion or Pinecone score normalization.

## 4. ORM joins, subqueries and binary predicates

For JOIN without ON, use SQLAlchemy's join-condition/FK metadata machinery over
the mapped root and known joined selectable graph. Support one uniquely identified
constraint, including its composite column pairs; compile that resulting typed
expression through the existing shared compiler. Root alias changes must refresh
inferred predicates from the original expression, just like explicit ON.

No relationship or multiple applicable relationships requires an explicit ON and
raises ValueError at build time. Raw string targets or an unknown source graph
without sufficient metadata also require ON. Do not hand-match same-named columns,
inspect live server FKs as a fallback, or emit a conditionless join. The same rule
applies to inner/left/right/full helpers; explicit ON behavior is retained. Chained
inference must either find one relationship in its known graph or reject clearly.

subquery remains a string API. At that boundary, render the pair from _build_sql
with the existing checked fragment owner before wrapping/adding a safely quoted
alias. This is idempotent for the already literal-rendered reported path and
preserves any legitimate legacy deferred parameters. Add public derived-table
JOIN controls with Decimal and non-UTF8 binary, repeated builds and quoted values.

The original typed bind processors and X'hex' binary rendering remain the sole
owner for #31248. Do not reintroduce literal_binds=True or add a binary-specific
predicate branch. Test actual matching bytes and rows rather than a SQL substring.

## 5. Fulltext grammar STRING values

mysql_sql.y search_pattern accepts STRING, VALUE_ARG or identifier, not a general
expression. The usual sql_literal backslash representation CONVERT(hex) is
therefore invalid here. The text-returning public build_sql/compile/as_sql APIs
must remain usable, so this scope selects one pure fulltext-specific term token
renderer and reuses it in all basic/advanced/filter/query SQL producers, including
score projections and logical_in subqueries. The term is encoded exactly once.

* Ordinary text without NUL/backslash uses the shared doubled-single-quote STRING
  renderer. Apostrophes, Unicode, newlines, dollar text and SQL-looking content
  remain one string value independent of NO_BACKSLASH_ESCAPES.
* Text containing backslashes but no dollar uses a fixed valid MatrixOne dollar
  STRING token, with the body preserved verbatim. Both supported server targets
  have a native discriminator for this form.
* NUL or the combination of backslash and dollar rejects with ValueError before
  query I/O. MatrixOne terminates the body at its first dollar and then validates
  the tag; changing to a supposedly collision-free tag does not solve this.

Do not escape only apostrophes at selected call sites, inject user text into SQL,
use CONVERT in STRING grammar, switch session sql_mode, or assume PostgreSQL dollar
semantics. Additional raw WHERE predicates retain their caller-authored SQL role
and use the existing conjunction owner when chained. Explicit fulltext algorithms
keep the same one-connection save/SET/query/restore/discard lifetime; term rendering
must not create another connection, reset the async budget, or escape Session ownership.
The remaining grammar limitation must be stated in docstrings and tested.

## 6. LOCAL file negotiation

Add keyword-only local_infile: bool = False to both connect APIs and pass the
validated boolean into the real PyMySQL/aiomysql connect_args. Keep the option in
connection-generation state so reconnect cannot inherit stale authorization.
Document an enabled example for from_local_file. Disabling remains the default;
the new supported behavior is an explicit opt-in plus an immediate clear error
instead of a long protocol timeout when disabled.

Both from_local_file and from_file(local=True) must check this configured client
capability before dispatch. The same applies through Session load managers, whose
client owns the actual engine. Use one guard, not two divergent wrappers. It must
not open the path, check server filesystem access, mutate driver private flags,
recreate a pool, or retry a rejected load as server-side INFILE.

For externally adopted engines, the SDK cannot infer closure-captured creator
arguments reliably. Treat negotiation as unknown and require an explicit adoption
assertion (a validated from_engine local_infile keyword distinct from unrelated
Client constructor options), or reject LOCAL with guidance to use an explicitly
configured SDK connection. Do not claim that an assertion enables the driver.
Server/version banners do not prove LOCAL negotiation. General capabilities need
no new server probe for this client option.

Enabled success, server refusal, missing local file, timeout and cancellation use
the existing execution/error/connection owner. A timed-out load may have uncertain
remote effects and cannot be retried automatically. Read recovery and pool release
must be tested. This adds no path sandbox and makes no claim that arbitrary
external engine drivers support LOCAL.

## 7. Catalog readback and metadata statistics

### Stage and PITR

Introduce a shared-shape optional Stage lookup that distinguishes an empty result
from execution/decoding errors. Public get stays strict. CREATE IF NOT EXISTS
returns observed catalog state, never a Stage built from the ignored proposal.
Prefer authoritative readback for normal CREATE too. Preserve the returned Stage's
client helper association and observable timestamps/status. Credentials not exposed
by the catalog remain unknown; never attach newly proposed credentials to an old
Stage when CREATE may have been a no-op.

After successful ALTER IF EXISTS, an empty lookup returns None; update both return
types/docstrings to Optional[Stage]. Without IF EXISTS, missing still raises the
existing not-found error. A broad catch ValueError must not turn driver/parsing
failures into successful no-ops. Concurrent external DROP can produce None on the
optional path; there is no fabricated claim that an alteration occurred. Other
post-write readback errors explicitly state SQL acknowledgment and preserve cause.

PITR get uses server LOWER(name) in its SHOW WHERE predicate with safe literal/
parameter encoding, accounting for supported quoted-identifier input in the same
way as snapshot lookup. Do not rely on Python's possibly different Unicode case
tables. All create scopes and alter separate write acknowledgment from readback
failure. Keep PitrError and original causes; never retry CREATE on a failed get.
No general PITR restoration implementation or retention-policy redesign is needed.

### Wildcard object aggregation (#31242)

brief/detail retain their own metadata_scan(target,'*') producer. Group its rows
by object_name. Count common row count, origin_size and compress_size once for
each object; emit one object detail item. Validate common repeated fields instead
of arbitrarily selecting inconsistent values. Null counts are per-column values:
aggregate them as null cells and document that the result can exceed row count.
Preserve raw bytes until formatting final sizes. Keep main/tombstone/index result
names separate. Do not feed named-column sizes into this object aggregator or
sum the repeated wildcard object size once per column.

### Column statistics (#31246)

Add source_column: Optional[str] = None to scan and its shared producer, distinct
from existing columns (output projection). Preserve old positional arguments by
adding it at the end/keyword-only. An explicit named source scans only that column;
explicit '*' requests the fast wildcard object view and may have NULL ZoneMaps.

When source_column is None, a projection explicitly requesting min/max/sum uses
named-column scans; structured all-fields projections '*' / [] also request those
statistics. Legacy columns=None raw scans and distinct_object_name remain the
wildcard fast path. This keeps ordinary raw scan/object-size semantics stable
while fixing the reported explicit column-statistics requests. Document the
selection rule and min/max/sum as native encoded values, not decoded Python sums.

Discover the target's distinct col_name values through its wildcard metadata
rows, preserving index/tombstone target selection. Then issue named-column scans,
preferably one UNION ALL for the finite discovered list, through the same executor.
Quote table-reference strings and each source column as SQL values; no string
interpolation of untrusted names. Empty discovery yields a valid empty result.
The raw/structured output types stay compatible. Sync/async share the planner and
processor; no gather on a Session. Costs are O(observed columns) scans/SQL terms
plus existing buffered results. No indefinite metadata cache is introduced.

An actual absent ZoneMap, empty table or all-NULL field can still return None.
Do not invent statistics, merge object/column size meanings, or treat these as
exact transactional table MIN/MAX/SUM across deleted rows. Test encoded values
against native named-column metadata_scan and decode known integer bytes only in
the test oracle. Observation and subsequent DDL are not an atomic schema snapshot;
a race is an honest error and never transparently retried.

## 8. CSV source schema observation

At relation creation, capture the observed inferred source schema for every CSV
independently of the declared/merged logical scan schema, including when an explicit
schema intentionally casts types. For each existing source-check entry, re-run
bounded _infer_schema with the same format/delimiter/filesystem and compare names,
order and types, ignoring metadata, against that observed baseline. Also retain
full streamed CSV record validation; the new type check does not replace width,
quoting, UTF-8 or header validation.

A mismatch raises the same recognizable source-schema-changed ValueError before
relation.sql is exposed, query creates a stream, or import starts a transaction.
Same-schema changed values remain visible. An explicit schema's intentional
conversion is not itself drift: compare observed source to observed source, not
fresh inference to the caller's target type. Preserve supported null conventions,
BOM, quoted newlines, custom delimiters, remote files and multi-file checks.

Inference still observes a bounded initial block. A type-only change beyond that
block may remain unobserved, and a file can change between preflight and scan;
there is no immutable file snapshot or full-file type-inference promise. Full
record validation is bounded in memory but consumes file I/O. Reuse context-managed
Arrow source/reader owners so failed inference or record validation releases file
handles; relations still own cleanup of their already-created external tables.
No async analytics entry point is introduced. Analytics retains its separate
Python/PyArrow support floor; core Python 3.8/3.9 imports do not require Arrow.

## 9. Acceptance and compatibility matrix

| Risk | Required proof |
| --- | --- |
| Vector shape/identity | Sync/async include_values × include_metadata matrix; numeric exact values, PK excluded, custom PK name, existing list and textual driver forms; malformed response rejection; no/composite PK query/delete/upsert/batch_insert reject with source rows unchanged. |
| Response collision and metrics | Default *, explicit stored distance, range and rank produce QueryError rather than overwrite; explicitly aliased stored Decimal retains both values; l2_sq native 0/4/25 versus L2 0/2/5, cosine/IP controls, unknown metric no dispatch. Include zero-row duplicate-label result. |
| Batch atomicity | Invalid final record after valid records causes zero writes; server rejection on a later statement rolls back a prior insert and update; successful retry commits all; cancellation ordered after first DML and before commit leaves original rows, zero checkouts and reusable connection. Lost COMMIT is documented uncertain, not asserted rollback. |
| JOIN and text boundary | Unique FK exact rows, explicit ON control, empty child, no FK, two FK ambiguity, root alias before/after, chained graph either correct inference or explicit rejection; Decimal/binary derived joins and repeated builds; binary ASCII/NUL/non-UTF8 exact HEX. |
| Fulltext grammar/lifetime | Basic/advanced/filter/query helper apostrophe + Unicode, literal $, backslash, sql_mode default/NO_BACKSLASH_ESCAPES on same owner; backslash+$ and NUL reject before query dispatch; compare exact IDs/scores to native STRING query; explicit algorithm restoration still passes. |
| LOCAL protocol | Disabled rejects before executor call; both true/false reach each driver connect_args; sync/async enabled native local-file exact rows on both servers, file missing and protocol refusal recovery, cancellation/timeout release. Existing-engine unknown/asserted behavior documented and tested. |
| Catalog readback | Existing Stage repeated create with different URL/comment returns old actual state; missing IF EXISTS alter returns None; normal alter/get strict; injected lookup execution/decode error not swallowed; PITR mixed-case create/get/alter and invalid range unchanged, delete, query reuse; explicit acknowledged-readback failure test. |
| Object and column metadata | One/multiple columns, persisted nonempty objects, ADD COLUMN and CLONE; object details cardinality and byte totals match deduplicated native wildcard; named v sizes stay separate; projected min/max/sum bytes match native named scan, enum/string projections, empty/all-NULL controls, index/tombstone where supported. |
| File schema/lifecycle | Same-header int->double drift rejected by sql/query/import before target mutation; same-schema updates visible; explicit conversion baseline works until observed source changes; header/late-width/remote/multi-file controls; cleanup and Arrow stream suite preserved. |

Required server targets remain release 4.2.5 and actual current-source with observed
version()/git_version(), never a development-banner blanket exemption. Verify the
new cells against both; unknown/older servers retain explicit native errors and no
silent metric/load/fulltext fallback. No capability record becomes supported from
this design alone. Additional LOCAL and fulltext lexical evidence must include
actual driver, server and mode identities.

Reuse the existing Python 3.8/3.9 installed-core smoke and Python 3.10/3.12 plus
SQLAlchemy 1.4.54/2.0.43/current-2.0/2.1 contract matrix for changed owners. Keep
analytics tests separate with their installed optional dependencies and native
oracles. Add new server cases to the fixed required selection, not only to an
optional file that the zero-skip runner never executes. Run appropriate formatting,
fatal lint/type checks and affected deterministic cancellation/lifecycle tests.
Go race/CGo tests are N/A for this Python-only delta.

Measurable completion: every sixteen-issue row has current public-interface proof
or an explicit already-fixed regression; all required selections are nonempty,
zero unexpected skips/failures; exact values and unchanged-state oracles hold;
all new connection/file/transaction owners are released on tested errors/stops;
original accepted scope's tests remain passing. Final separately selected overall
review must reconcile this addendum with the complete PR and actual content hashes.

## 10. Alternatives and deliberate boundaries

A new universal predicate/response/capability framework would duplicate established
owners. Ad hoc binary/fulltext escaping, implicit L2 fallback, first-PK selection,
per-row commits and request-derived Stage state all violate demonstrated invariants.
A configurable distance-alias API and composite ID encoding are possible future
work; explicit ambiguity errors solve this scope without inventing their contracts.

Enabling LOCAL for every connection by default or changing a live pool mid-load
would make negotiation implicit; explicit connect-time opt-in is selected. Full
file inference and immutable source copies would change resource/consistency
contracts; bounded re-observation is selected. Always replacing wildcard metadata
with named-column scans changes object-size semantics; selection and aggregation
therefore remain separate. Reimplementing all SQL with a parser or changing server
STRING grammar is unnecessary. The verified fulltext dollar limitation is an
explicit fail-before-I/O boundary, with no speculative tag-escaping fallback.

No pending experiment blocks this implementable design. Native post-fix matrices,
error/cancel cleanup, static checks and final review remain required before PR
completion. A new observed incompatibility invalidates only its affected design
cell and must be recorded as a revision before dependent implementation continues.
