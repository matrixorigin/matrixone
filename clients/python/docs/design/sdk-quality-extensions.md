# Python SDK quality: related API and lifecycle corrections

Revision 2 — 2026-10-26. Accepted design supplement to
[sdk-quality-contracts.md](sdk-quality-contracts.md). Owner: Python SDK.
Implementation: the same `codex/python-sdk-quality-20261026` PR; URL added when
published. Base remains `b466f4ae9b74813a4ff93eaf43a6c81e6cbb8b0b`; the first
design was committed as `559fd3ba5f233b68843fdeda9055fa5f4568f312` before its
implementation. This supplement must also be committed before the extension
implementation. Exact reviewed hashes/decision are recorded externally.
Revision 1 was committed as `e78ca18589`; revision 2 narrows LOAD grammar-only
string tokens after a native parser discriminator and does not alter the other
extension decisions.

## 1. Evidence, scope and first owners

Twenty subsequently reported issues expose the same broad failure class:
accepted API inputs lose their meaning between a manager, its SQL builder,
the execution connection and the server. The correction should remove duplicate
authorities instead of adding per-issue dispatch rules or a new framework.

The downloaded issue snapshots provide exact native-SQL controls at
`5fe08a3307e246e1ab9951ab690b50557af0f509`. Those are historical evidence, not
proof that every current environment behaves identically. Source inspection
below identifies the causal owner. Main-agent current-source and 4.2.5 local
discriminators are reusable: `extensions-{current,release}-baseline.log` prove
branch gating, missing dialect state, snapshot/publication case, ACCOUNT ALL and
CLONE versus explicit BEGIN; `io-{current,release}-baseline.log` prove gzip
compression omission, JSONLINE SET omission and inline CSV skiprows omission.
Their scripts and logs remain outside tracked source. Do not rerun them solely
because a different review phase starts.
Subsequent `vector-{current,release}-baseline.log` locally distinguish wrong
metric, dropped range operator, wrong vector-column index and stale ADD-column
metadata. `fulltext-current-baseline.log` distinguishes the ignored algorithm;
`hooks-current-baseline.log` proves duplicate callbacks on one physical ID and
the unawaited async callback on a forced second physical connection. Reuse these
as baseline evidence; they do not replace post-change contracts.

| Issues | Causal owner observed in source | Required correction / disposition |
| --- | --- | --- |
| #31214 | `branch.py` version decorators reject before dispatch; banner is not actual support. | Remove branch's version-only veto; attempt the requested operation once, preserve actionable server rejection, never infer branch support from a synthetic version. |
| #31230 | Simplified sync/async `create(database=True)` drops snapshot; database builder has no snapshot parameter. | Carry optional snapshot through the same shared database-branch builder; no live fallback. |
| #31231 | Sync snapshot CREATE succeeds then exact-name catalog readback misses the server's lowercase identity; async already normalizes. | Share normalization and readback policy across sync/async create/get/exists/delete. Separate CREATE acknowledgement from metadata-readback failure. |
| #31233 | Publication builders quote ALL as an account name. | Recognize case-insensitive `account='all'` as the explicit wildcard grammar token; quote ordinary account identifiers. |
| #31234 | Sync/async get compares SHOW name to caller spelling exactly. | Share server-consistent normalization of requested and returned publication names, retaining exact canonical catalog names in results. |
| #31220, #31226, #31229 | LOAD branches drop common options; inline convenience APIs drop mappings before the builder. | One common LOAD tail/validation owner, separate source descriptors for plain/brace/inline forms; every nondefault accepted option is emitted or explicitly rejected. |
| #31215, #31223 | Search never forwards observed metric; DDL parser matches a column against the entire table rather than the current index. | Resolve the actual requested column/index and map its metric once into the similarity owner. |
| #31217 | Per-field operator if/elif consumes one member; logical if/elif can also drop sibling entries. | Compile every entry with explicit conjunction/disjunction and ordered bound values; reject any unknown operator before execution. |
| #31228 | `_metadata_columns`, `_id_column`, `_index_info` persist across external ALTER indefinitely. | Use an operation-scoped current schema/index observation; remove indefinite unversioned metadata caching. |
| #31224 | IVF stats resolve omitted database from connect-time `_connection_params`. | Resolve current database from the owning execution connection, then fully qualify every catalog/index-table lookup. |
| #31219 | Fulltext `.with_algorithm` records a field never used at execution. | Save/select/query/restore algorithm on the same owned connection; no pool-wide setting change. |
| #31216, #31218 | Hooks use physical/wrapper object IDs in two events and call coroutine callbacks synchronously. | One physical-connection initialization event, await completion on async adapters, no global ID ledger or duplicate immediate path. |
| #31221 | AsyncSession docs promise same-session concurrent gather although SQLAlchemy owns one mutable transaction. | Sequential work per session; separate sessions for concurrent transactions. Correct all examples; no serialization worker or hidden lock. |
| #31225 | New MatrixOne dialect object replaces the engine's live driver dialect without its initialized state. | Preserve one initialized driver-dialect owner and add MatrixOne compilation/reflection traits without erasing its runtime state or driver behavior. |
| #31235 | SQLAlchemy logical begin does not send the backend BEGIN needed for CLONE rollback. | Explicit server BEGIN on the context's actual Session connection before publishing the context; preserve one transaction owner and terminal cleanup. |
| #31236 | Metadata distinct projection selects object_name and every row column. | Distinct mode projects only object_name and preserves an explicit result-shape contract; do not claim to fix the independent server aggregate anomaly. |

These are intended fixes in this PR, subject to the required post-change proofs.
The first design's deferred async analytics, telemetry and broad driver-binding
migration remain deferred. No server kernel or catalog-format change is included.

## 2. Governing invariants

1. An accepted option/predicate/metric/snapshot reaches its first authoritative
   SQL owner exactly once; it is never silently dropped or replaced by a default.
2. Table, column, database, index and catalog-name identity comes from the
   operation's actual source/connection, not unrelated DDL text, another pool
   checkout, a stale handle cache or the version banner.
3. Physical connection initialization finishes before user SQL is admitted.
   Reuse does not initialize again; reconnect/new physical connection does.
4. One Session context has one backend transaction and one physical connection
   after admission. Logical completion, cancellation, rollback and unknown
   COMMIT outcome remain distinct.
5. Temporary session settings restore before pool reuse. Failed restoration
   discards/invalidate the connection; it never publishes reusable dirty state.
6. Existing public result/error types and optional dependencies remain compatible
   unless an objectively invalid input must now fail explicitly. Extra features
   are not inferred merely because compilation succeeds.

## 3. Stateless manager and SQL contracts

### Branch support and historical sources

Remove `requires_version(min_version='3.0.5')` as an admission guard from the
branch manager family. Keep legacy version information descriptive. No safe
generic probe can prove every branch operation without accessing actual objects;
the requested server operation is authoritative. Unknown support dispatches once;
syntax/privilege/transaction errors retain BranchError and original cause. A
failed branch write is never retried automatically, and no synthetic
`set_backend_version('999.0.0')` workaround is needed by normal callers.

Add `snapshot_name=None` at the end of database branch methods/builders; forward
`create(..., snapshot=..., database=True)` for both clients and Session executors.
Emit the suffix immediately after the source database, using the shared safe
literal/identifier authorities and the existing statement-builder syntax as a
native reference. Table branches and no-snapshot controls retain behavior. An
invalid snapshot produces a server error, never a live branch. MERGE/PICK retain
their existing explicit-transaction restriction; this correction must not route
them through a Session that would commit unrelated work.

### Snapshot/publication names and post-acknowledgement errors

SDK name arguments are identifier values, not caller-written quoted SQL tokens.
Reuse server-consistent lowercase normalization already applied by async
snapshots; do not use Unicode casefold, which can merge distinct names more
aggressively than the server. Normalize both requested and catalog-returned names
for publication comparison. Preserve the catalog's canonical spelling in objects.
Quote normal identifiers; do not lowercase arbitrary string data, account
credentials, file paths or case-sensitive metadata values.

After CREATE is acknowledged, a failed readback must report that creation was
acknowledged and metadata retrieval failed. Keep SnapshotError/PubSubError and
the cause; a message/compatible additive field can distinguish this phase. Never
retry CREATE or delete the new object automatically to compensate for readback.
Duplicate retry remains the server's duplicate-object error. This is a narrow
phase distinction, not the separate telemetry/error-catalog redesign.

Publication wildcard selection is explicit: stripped case-insensitive `'all'`
means SQL `ACCOUNT ALL`; every other string is one safely quoted account
identifier. It does not parse arbitrary account SQL or broaden ordinary accounts
to all. Database/table creation and ALTER account-selection builders should use
the same keyword owner when that public parameter has the same meaning.

### Metadata distinct projection

`distinct_object_name=True` returns exactly one `object_name` column, with the
same set as native `SELECT DISTINCT object_name FROM metadata_scan(...)`. The
default scan continues returning the complete metadata rows. The distinct mode
must bypass full MetadataRow construction, since those required fields do not
exist in the projection. Raw results retain the existing cursor-like API.
Explicit `columns=['object_name']` may yield one-key dictionaries; requests for
full rows/other columns combined with distinct mode fail before dispatch with an
actionable ValueError rather than guessing arbitrary representative rows. Do not
silently pick an object record using MIN/ANY_VALUE to preserve an accidental old
shape. The native scan requires persisted objects: its public test must trigger
or observe an owned checkpoint condition with a bounded deadline, not sleep or
accept an empty set as success.

## 4. LOAD: one option-preserving builder boundary

Normalize convenience aliases once: skiprows → ignore_lines, names → columns,
escapechar → fields_escaped_by. Both sync and async file/inline/stage entry points
then call the same validators and shared tail renderer. Source syntax selection
owns only `INFILE 'path'`, brace parameters, or `INLINE FORMAT/DATA`; it cannot
discard tail options. Explicit CSV compression selects brace syntax containing
filepath/compression even for a nonstandard suffix. Unspecified compression
retains existing filename/server behavior. Formats/codecs are normalized and
validated without inventing server support for every declared codec.

Preserve native grammar order for CHARACTER SET, FIELDS, LINES, IGNORE, column
list, SET and PARALLEL. Safe identifier/value rendering applies at each typed
boundary; SET expression strings remain documented caller-authored SQL, not
quoted data. Reject unknown keyword arguments and unsupported nondefault
combinations instead of swallowing **kwargs. Reject negative/bool skip counts
and invalid option shapes before dispatch; no input row/file is opened merely
for syntax validation.

LOAD source/options occupy grammar positions requiring a STRING token, rather
than an arbitrary SQL expression. The general safe literal renderer's
`CONVERT(X'...' USING utf8mb4)` representation cannot be substituted there.
Native dollar-quoted SELECT literals work on both targets, but the observed
current-source `LOAD DATA INLINE ... DATA=$tag$...$tag$` discriminator fails with
1064 (`manager-current-1.log`); do not infer grammar-token compatibility from
SELECT expression support.

Use a small grammar-specific string-token renderer: require str, reject NUL
and any backslash, and double single quotes inside one ordinary single-quoted
token. Plain quotes, literal newlines, Unicode and dollar text retain their
bytes. With backslashes excluded, this representation is independent of
NO_BACKSLASH_ESCAPES and ANSI_QUOTES. Apply it consistently to LOAD paths,
inline DATA, separators/enclosure/escape/line options and brace values. Never
fall back to an expression, dollar quote, removed bytes or an assumed SQL mode.
NUL/backslash inputs raise an actionable ValueError before executor invocation
or connection checkout. Caller-authored SET expressions retain their separate
raw-SQL contract; they are not grammar-token data values.

For inline **data** requiring NUL/backslash bytes, suggest writing a caller-owned
file and using supported non-inline LOAD, preserving those file bytes and normal
CSV decoding conventions. This does not solve an unsupported path or option
token: a backslash-containing path/escapechar itself remains explicitly rejected;
use a suitable path/stage or a different supported convention. The SDK does not
create a temporary file, change session SQL mode, or add a second I/O/cleanup
owner merely to support this neighboring input. This is a deliberate bounded
limitation, not a claim of complete arbitrary-byte inline support.

Supported minimum cells, each proved on both MatrixOne targets:

| Source/format | Required options |
| --- | --- |
| CSV file and read_csv delegation | explicit gzip on `.payload`, delimiter/quote/escape, skip count and columns; existing supported SET behavior retained. |
| JSONLINE file | object/array selector, compression, IGNORE lines, mapped/reordered columns, server-supported NULLIF SET transformation. |
| Parquet file | explicit column subset/reordering/default fill, existing compression semantics. Do not claim IGNORE/SET behavior from JSONLINE evidence. |
| Inline CSV | skiprows/IGNORE, names/column order/default fill, escapechar, delimiter/quote; complete values for newline/quote/Unicode/dollar text. NUL/backslash grammar-token inputs fail before I/O. |

Inline CSV SET was observed ineffective even through direct native SQL in the
issue; it must be rejected/documented rather than claimed as repaired. Encoding,
compression, parallel and format-specific options without a proved native inline
meaning receive explicit rejection when nondefault. Plain and brace builders
must not route accepted options into a branch that ignores them.

Existing LOAD transaction/commit behavior remains the executor's contract. This
work does not promise atomicity for every server LOAD mode, automatic file
upload, client-local path visibility or rollback of caller-owned output files.
Tests own only their fresh files/tables; malformed/invalid inputs assert both
the actual target state and subsequent connection reuse. Optional PyArrow is
needed only for the existing Parquet fixture, not for core import/CSV/JSONLINE.

## 5. Search metadata, predicates and scoring

### Operation-scoped schema and index identity

Replace indefinitely cached `_metadata_columns`/`_id_column`/`_index_info` as
correctness authorities with a fresh operation-scoped observation. Reuse that
one observation within the operation, including result extraction. No TTL,
watcher, retry-on-unknown-column layer or global schema cache is needed. A
concurrent ALTER between observation and execution may still produce an honest
server error; this is not an immutable catalog snapshot guarantee.

Observe the requested vector column and the matching index column list. Prefer
parameterized native catalog metadata already used by vector/index utilities,
scoped to the correct database/table/column; normalize duplicate physical rows
of the same logical index. If the existing SHOW CREATE parser remains, capture
and compare the **current matched index's** column list with properly escaped
identifiers, never test membership in the whole DDL string. Dimensions and
parameters must come from the selected vector column/index. Ambiguous multiple
indexes with differing metrics on one requested column must fail actionably;
there is no principled arbitrary first-index choice. A valid nonindexed vector
table may retain documented L2 search defaults, but must not be described as a
proved IVF index. Do not broaden HNSW mutation permissions based on a guessed
algorithm; actual server restrictions remain authoritative.

An existing handle after ADD/DROP metadata columns observes the current schema,
without caller recreation. Metadata-column discovery excludes the primary key
and requested vector column; final match metadata preserves the current result
policy, including its existing primary-key inclusion. Preserve the current
inclusion/alias policy for other columns. No removed-column SELECT is
retained from a prior query. Table/database identity is part of observation;
reconnect and USE cannot reuse a stale schema from another target.

### Metric and filter meaning

Map observed `l2` → `l2`, `cosine` → `cosine`, and `ip` → `inner_product` at one
shared search owner; sync and async pass the distance_type explicitly. Preserve
MatrixOne's existing distance-valued score contract and ascending nearest-first
ordering. Native `inner_product` here is the negated-dot distance (see scalar
metric implementation/tests), so do not introduce descending order or promise
Pinecone's higher-is-better score convention. Direct native SQL is the numerical
oracle for each metric and server, including float32 tolerance. Invalid/unknown
observed operators fail; they cannot silently become L2.

Compile filter objects recursively as a map conjunction. Every field-operator
entry participates, including multiple comparisons on one field. `$and`/`$or`
process all nested operands and coexist with sibling fields; a first matching
operator never suppresses the rest. Validate operator names/container types and
all branches before publishing output. Values remain ordered parameters and
fields use safe identifier rendering. Reuse the lexical safe renderer to pass
the final WHERE string into the current similarity API; remove ad hoc sequential
`replace('?')` from sync/async search. Preserve $in/$nin empty-list identity
behavior, document/reject unsupported empty logical arrays, and retain nesting
parentheses. Marker-looking strings, NULL, quotes and binary values must never
alter the SQL structure. No new query language or execution retry is added.

### Current database for IVF stats

Explicit `database=` wins. Otherwise query `SELECT DATABASE()` through the
operation's actual executor/owned connection; no fallback to constructor state
or invented `'test'`. Missing selection is an explicit error. Resolve qualified
table arguments consistently and qualify catalog/physical entries queries using
that one database decision. A Session uses its bound connection; a client-level
multi-step read owns one connection for admission/identity resolution, then can
use qualified SQL without depending on another checkout's session state.
`Client.execute('USE ...')` does not configure every existing pool connection:
callers needing stable multi-call schema state use an explicit database or a
Session. Tests prove actual one-connection USE behavior and competing same-name
tables, not a fictitious pool-wide USE guarantee.

### Per-query fulltext algorithm

Only an explicitly requested algorithm overrides the server's session default;
use a separate unset state rather than applying the currently ignored default
field to all callers. Validate BM25/TF-IDF spelling before I/O. For an explicit
choice, acquire one connection, read previous algorithm, register restoration,
SET the requested value, execute/fetch the complete buffered query on that same
connection, then restore before release. The WHERE and score projection see
the same setting. Never issue SET through Client.execute followed by a second
pooled query, and never mutate global settings.

Reuse existing low-level execution/logging/result translation at this owned
connection boundary; keep the builder's ResultSet return type. If the query
fails and restore also fails, preserve the query error and discard the dirty
connection. If the query succeeded but restore fails, report cleanup failure
explicitly and invalidate; do not return an apparently reusable dirty session.
No algorithm cache, background restore or automatic retry is introduced.

## 6. Physical connection hooks and Session ownership

### Hook admission

Attach hooks before the first authentication/readiness query, using one
SQLAlchemy physical `connect` event. Remove the before_cursor_execute duplicate
and automatic separate immediate execution. The event already identifies new
physical connections; it does not require a process-long set of Python object
IDs. If an adoption path attaches to an already live engine, initialize each
uninitialized connection at one clearly identified checkout boundary using
ConnectionRecord-local state tied to the physical generation, never proxy IDs.

Predefined actions use the supplied physical DBAPI connection. For async
engines the sync SQLAlchemy event uses the supported greenlet/AdaptedConnection
bridge (`run_async` where available, with a version-compatible await bridge if
needed) to await the callback's returned awaitable before admission. Never
schedule an unowned task, call asyncio.run inside the loop, or inspect only a
function's coroutine flag: sync callables may return awaitables. Keep one-arg
`callback(client)` compatibility. The runtime matrix must exercise the chosen
bridge in SQLAlchemy 1.4 and 2.x; unsupported minimum-driver APIs need an explicit
dependency floor or a tested compatibility path.

Ordinary callback exceptions retain the existing logged best-effort policy:
one attempted initialization per physical connection, no retry on every query.
Cancellation propagates and the connection under construction is discarded by
its owner. Publish connection availability only after callback completion.
Do not swallow cancellation as an ordinary logged setup failure. Test two
physical connections using explicit pool checkout/barriers, not SLEEP races.

A hook runs while its physical connection is being admitted. Recursive checkout
of the same client's pool from that callback can deadlock or recursively create
connections. Do not silently claim such recursion is safe. Either route explicit
connection-local callback SQL through a narrow bound owner, or reject same-client
pool-reentrant operations immediately with an actionable error; the selected
implementation must choose/test one. This design selects **explicit rejection**
for this PR, using a scoped per-execution marker (reset in finally), and documents
predefined connection actions as the supported physical-session initialization
path. It does not add a callback worker/facade API. Independent non-database
async setup remains supported.

### Session transaction admission and cancellation

For `Client.session()` / `AsyncClient.session()`, SQLAlchemy's logical Session
transaction remains the single commit/rollback owner. After logical begin,
select that Session's actual connection and issue server SQL BEGIN **once**
before yielding the context. Do not get another engine connection, flip the
whole pool's autocommit mode, begin only after the first CLONE, or compensate by
dropping clones after rollback. Local current/release controls establish that
this backend distinction is real for CLONE.

Initialization failure closes without yielding. Normal exit commits; body
failure/cancellation rolls back then closes. Terminal cleanup must cover
BaseException cancellation while preserving the original control/error result;
secondary cleanup failure cannot replace it. A failed/unknown COMMIT does not
authorize retry or imply rollback. Existing wrapped externally owned Sessions
do not acquire a second BEGIN/commit owner merely by being wrapped. Manual
commit/rollback inside a context retains SQLAlchemy's documented transaction
rules; do not silently start a new independent transaction after the owner closes.

Only operations supported by the real backend transaction are atomic. Remove
examples promising snapshot CREATE or MERGE/PICK atomicity inside a Session;
the server rejects those in an explicit transaction. Never escape the Session
to autocommit an unsupported operation. Keep direct-client snapshot operations
separate. DML plus CLONE success/failure/cancellation are required public proofs;
other lifecycle managers receive only the scope demonstrated by their tests.

One AsyncSession is used sequentially by one task at a time. Correct every
same-session gather recommendation, including bulk loads and transaction
examples, to sequential awaits; show separate Session contexts for independent
parallel transactions. No lock/queue/worker is added to serialize unsupported
sharing. Separate sessions cannot be described as one atomic transaction.

## 7. Preserve initialized dialect ownership

The existing engine's driver dialect owns server_version_info, current default
schema, charset/sql_mode/quoting decisions, isolation behavior, driver ping and
connection/error adaptation. A blank generic MySQL dialect cannot replace it.
Do not fix view reflection/isolation with another per-method fallback or a
whitelist copying only today's two failing fields.

Preferred implementation: keep the original driver-dialect instance and install
the narrowly required MatrixOne compiler/type/reflection traits on that same
owner, preserving initialization and captured connection-event references.
A bounded mixin/adaptation helper can be shared by `_create_engine` and
`from_engine`; it must retain the actual driver's class behavior and all observed
instance state, and be idempotent. MatrixOne vector types, Boolean reflection,
statement/type compilers and dialect name semantics remain present. Existing
compiled statement caches must not reuse SQL compiled before materially changed
compiler traits. Do not reinitialize/reset an active caller connection, issue
transaction-control SQL during dialect adaptation, or close an adopted engine.

If a replacement object is unavoidable, preserving all driver behavior and
state before any consumer sees it is mandatory, including the lazy/uninitialized
engine case and pool first-connect callback that initializes the original object.
Merely copying __dict__ from an uninitialized original is insufficient. This
alternative requires an explicit same-instance initialization discriminator
before implementation approval; the preferred retained-owner approach does not.

Tests cover lazy SDK creation and adoption of both lazy and already initialized
PyMySQL engines, default/explicit-schema view listing, MetaData.reflect(views=True),
isolation lookup, vector/custom-type compile/reflection and pre-ping after reuse.
Other advertised MySQL drivers must retain their original driver behavior; no
claim that PyMySQL evidence certifies every driver. Async engine's aiomysql
adapter must retain the separately repaired ping semantics.

## 8. Resource bounds, compatibility and alternatives

New code owns no durable files, tables or background workers. Catalog observations
are bounded by the selected table/index schema and discarded at operation end.
Removing indefinite metadata/hook-ID caches reduces retained state. A search
may add one current schema/index observation per operation; share it within the
operation, record query-count cost, and do not add a benchmark wall-clock gate.
Hooks add only bounded actions and caller-controlled setup under existing connect/
query budgets. A hook callback that never finishes must remain cancellable and
must not publish a usable connection. Session adds one backend BEGIN per context;
fulltext adds read/SET/restore only for explicit algorithm choices.

The original Python/core/analytics dependency matrix and MatrixOne 4.2.5/current
source identities remain the qualification targets. New grammar/metadata cells
must either work on each target or fail with a precise documented unsupported
result. Missing privileges are not unsupported syntax. Changes are SDK-only;
there is no persistent format migration, rolling cluster protocol or kernel
upgrade. Rollback installs the previous package; already committed clones,
branches, publications, snapshots and LOAD rows are ordinary server state and
are not deleted by rollback. Test cleanup removes only test-owned resources.

Alternatives considered:

* Per-issue parameter forwarding and parser regex patches retain independent
  option/schema authorities and leave nearby drop paths; select shared owners.
* TTL schema caching/retry-on-column-error misses silent ADD omissions and adds
  lifecycle state; select current operation observations with explicit race limits.
* Version threshold changes still fail backports/dev banners; dispatch once to
  the authoritative server instead of manufacturing capability certification.
* Pool-wide SET, callback tasks and post-error clone DROP lose session/resource
  ownership; select scoped connection settings, awaited admission and real BEGIN.
* Session serialization and a general callback/metadata framework expand API and
  state beyond these fixes; correct usage and use existing event/executor seams.
* Dialect field-by-field patching hides initialization drift; preserve the
  initialized driver owner with a narrow shared adaptation boundary.

Applicable primary precedent is SQLAlchemy's
[AsyncSession concurrency contract](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html#using-asyncsession-with-concurrent-tasks)
and [async event bridge](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html#using-awaitable-only-driver-methods-in-connection-pool-and-other-events).
MatrixOne native SQL/current parser and the issue/local controls are the
authorities for branch suffixes, negated inner-product distance, LOAD options,
name normalization and transactional CLONE. Do not substitute generic MySQL
assumptions for those measured MatrixOne boundaries.

## 9. Required proof and delivery gate

| Closure | Cheapest deterministic proof | Real-service proof on current source and 4.2.5 |
| --- | --- | --- |
| Branch gate/snapshot | No version-only pre-dispatch veto; sync/async forwarded suffix and invalid snapshot; original builder oracle. | Historical/live two-row distinction for table/database branches; source isolation and unsupported/privilege error reuse. |
| Name/wildcard/readback | Shared canonical identity, ALL vs ordinary account SQL, injected post-CREATE lookup failure preserving acknowledgement. | Mixed-case snapshot create/get/exists and duplicate retry; database/table ACCOUNT ALL metadata; mixed-case publication get, truly missing name. |
| LOAD | Cross-entry-point option table and strict invalid-combination oracle; shared complete SQL tail, no swallowed kwargs. Grammar-token NUL/backslash rejection asserts zero executor calls and unchanged query/target state; quote/newline/dollar text round-trips. | Same files/inline bytes versus native controls; gzip `.payload`, JSONLINE header/mapping/NULLIF, Parquet column/default, inline skip/map/escape, malformed/invalid control plus row state/reuse. |
| Search/filter/schema | All field operators and logical siblings, nested grouping, parameters/identifiers, exact-column index match; no stale values across operation snapshots. | Minimal 3-vector metrics with different rankings and exact score oracle; two vector columns with distinct index algorithms/metrics; existing handle ADD/DROP; sync/async range filter vs explicit AND/native; two databases with same-name tables and different IVF counts. |
| Fulltext | One-connection exact read/SET/query/restore order; invalid algorithm before I/O; query/restore error precedence and invalidation. | Small frequency/length-varying corpus, BM25 versus TF-IDF scores, default preservation, pool reuse plus injected query failure restoration. |
| Hook lifecycle | ConnectionRecord/physical generation, exactly one attempt, sync-returned awaitable, async bridge, cancellation, new physical connection, reentrant fail-fast and no global ID ledger. | Two held physical connections admitted with barriers, callback completed before each query; repeated same CONNECTION_ID causes no new hook; invalidation/reconnect starts one new attempt, no unawaited warning, pool capacity returns. |
| Session/usage | One BEGIN on owned connection; commit/body-error/cancel/initialization failure and cleanup precedence; executable sequential/separate-session examples. | Two-clone success and second failure, mixed DML+CLONE rollback, explicit-BEGIN native discriminator, source unchanged, cancelled Session rollback and pool reuse. Unsupported snapshot in Session never auto-commits. |
| Dialect | Initialized owner/driver state preserved; lazy/adopted engine and compiler/type cache consumers. | Default-schema view listing and reflect, isolation, explicit schema control, vector type and pre-ping reuse through connect/from_engine. |
| Metadata distinct | Projection/result-shape and invalid columns combination; complete object-name set oracle. | Persisted tiny table, nonempty native object-name set equals sync/async SDK set; default scan retains complete shape, source unchanged. |

Reuse existing SDK fixtures/tests and the original design's owned two server
deployments. Add no service per test and no scheduler sleeps. Each scenario uses
zero/one/two rows or the smallest three-vector/four-document discriminator; no
scale/soak claim is needed. New online contracts must be collected/nonempty and
have zero unexpected skips. Required errors assert actual data/catalog/settings,
not merely exception existence. Ensure public Session and async consumers share
the corrected owners, rather than validating only private builders.

Local discrimination still required where only historical issue/native evidence
exists: the exact multi-vector catalog response on both targets; chosen async
event bridge at the supported SQLAlchemy floor; fulltext setting restoration
against both services; metadata persistence readiness and projected-result shape.
These are implementation validation tasks with specified oracles, not unresolved
architectural choices. If a preferred mechanism is invalidated (notably dialect
owner retention or backend BEGIN), stop that closure and revise this supplement;
do not mask the evidence with retries or a new support claim.

Design decision: **PASS for the specified mechanisms and boundaries**. No
production implementation is approved by this document alone. Final complete
review must include all extension hunks, affected original findings, package/
static/old-runtime/dependency evidence and both server matrices. Closure of any
issue requires its corresponding actual proof, not inclusion in this table.
