# Python SDK quality and compatibility contracts

Revision: 1, 2026-10-26. Status: accepted design; implementation and validation
remain required. Owner: Python SDK. Primary issues: #31202–#31213; related
requirements: #30242, #30067 and #30069. Implementation: one SDK quality PR from
`codex/python-sdk-quality-20261026` (PR URL to be added on publication).

Reviewed source base: `b466f4ae9b74813a4ff93eaf43a6c81e6cbb8b0b`.
The external task record records the exact reviewed document hash, model and
decision. Updating a material decision requires a new revision and design review.

## 1. Problem and evidence

Users can receive plausible but wrong results without an exception: `rightjoin`
emits LEFT JOIN, a filter can lose or exchange its values, a snapshot query can
mix old and live tables, and a type declaration can lose precision or signedness.
Other valid calls fail after the server has returned rows or before DDL reaches
the server. These are client ownership/compilation failures, rather than reasons
to change server SQL semantics.

The issue snapshots contain independent native-SQL controls. Task evidence E01
(`baseline-probes.txt`, outside the repository) reproduces all twelve primary
defects through public builders at the reviewed base. Source inspection finds:

* `BaseMatrixOneQuery` interpolates strings through several different paths and
  lets `filter` consume the front of a queue owned by earlier `filter_by` calls.
  It silently ignores unknown columns and strips qualified SQLAlchemy columns.
* Sync and async `all()` duplicate materialization and instantiate a string table
  name as a model. `SQLBuilder` receives an already aliased table and attaches
  the snapshot suffix after the alias.
* `SnapshotClient.execute` rewrites the first simple FROM with a regular
  expression; qualified tables and subsequent JOIN tables escape the rewrite.
* The dialect duplicates MySQL scalar compilation with incomplete visitors;
  BIGINT and TIMESTAMP overrides discard attributes.
* JSONL export omits its explicit format clause. Client version managers share
  one mutable singleton, allowing clients of different servers to contaminate
  each other's version metadata. Unknown/development versions are treated as
  broad support by legacy checks.
* pytest settings are duplicated; some legacy offline modules modify imported
  dependencies, and `conftest.py` repairs selected filenames. This makes an
  isolated passing test weaker than an order-independent owning-suite pass.

E02 additionally demonstrates cross-client contamination: setting client 1 to
1.0.0, then client 2 to 4.2.5, changes client 1's reported version to 4.2.5.
Baseline offline suites pass with SQLAlchemy 2.0 (1675 tests plus 95 subtests)
and 2.1.6 (1683 plus 95); those passes do not cover the twelve reproductions.
The task-owned release 4.2.5 service reports
`8.0.30-MatrixOne-v4.2.5`, build `1a44f3d`; constant RIGHT JOIN returns the
unmatched right row and the HNSW setting is observable as `off`.

The existing lexical literal renderer, Session execution boundary, connection
transport changes, analytics implementation and its design documents already
exist. They are inputs to this change, not new work to claim again.

## 2. Scope and issue disposition

| Issue | Disposition and evidence required |
| --- | --- |
| #31202 | Fix shared sync/async JSONL SQL generation; parse the actual output for both `.jsonl` and `.txt` paths. |
| #31203 | Preserve TIMESTAMP `fsp`; compiler oracle plus actual precision metadata and microsecond round trip. |
| #31204 | Delegate standard scalar types to the MySQL compiler; runtime qualification only for native types actually tested. |
| #31205 | Preserve BIGINT unsigned; compile, reflect and round-trip `2**63` and `2**64-1`, with signed controls. |
| #31206 | Replace partial raw-SQL snapshot rewriting with the complete-or-reject contract in section 5. |
| #31207 | Honor string-table `filter_by`; reject unknown mapped fields before dispatch and never discard a supplied predicate. |
| #31208 | Compile `filter_by(x=None)` as `IS NULL`; retain non-NULL controls. |
| #31209 | Render snapshot between table and alias; use the same placement rules as raw snapshot SQL. |
| #31210 | Share row materialization; string-table star queries use returned metadata and preserve empty/first behavior. |
| #31211 | Emit actual RIGHT JOIN with resolved target and unchanged ON semantics. |
| #31212 | Keep each predicate's values with its SQL; no mutable FIFO consumption across clauses. |
| #31213 | Preserve qualified SQLAlchemy expressions through filter/group/having/order/join compilation. |
| #30242 | Verification-first: exercise the reported 2.0.43/aiomysql 0.3.2/PyMySQL 1.2.3 tuple through reused connections. Keep existing fixes; if still failing, fix the async dialect ping adapter at its owner. No blanket dependency downgrade or disabled pre-ping. |
| #30059 | Existing merged lexical renderer is retained and its parameter/Session regression suite rerun. Do not claim a new fix for the merged work. |
| #30067 | Deliver bounded observational capabilities and per-client version isolation. Safe probes cover RIGHT JOIN and experimental settings. Arrow LOAD/export/snapshot can remain explicitly unknown without a safe probe. Keep the issue open for any remaining early analytics detection/driver coverage; do not claim full acceptance from version hints. |
| #30069 | Deliver shared ORM parameter/result contracts, truthful runtime/extra matrix, packaging/import checks and consolidated test ownership. Driver-bound execution remains the compatible Core/Session path; no forced public API migration. Keep the broad issue open if oldest-runtime or full-extra qualification is incomplete. |
| #30068 | Structured outcomes and phase telemetry remain separate. Preserve existing exception identities, causes and analytics commit outcomes; no telemetry/retry framework here. |
| #30066 | Async analytics remains separate: it needs its own bounded streaming, staging, cancellation and uncertain-COMMIT design. Existing async core APIs are tested here. |
| #30048, #30049 | Existing synchronous file-relation/Arrow parent features; acceptance is wider than this PR. Preserve APIs and run affected regressions; do not close umbrella acceptance from these fixes. |
| #30050, #30051, #30052, #30053 | Existing implementation and designs are present from merged work. Verify affected analytics tests; do not reimplement or claim new closure. |

This PR changes Python SDK contracts and supporting CI/docs, with no kernel,
catalog format, wire protocol or persistent migration. Scope is a major refactor:
it crosses query construction/execution/ORM/dialect ownership and materially
changes the client/server compatibility contract. The design gate applies
regardless of final production-line count.

## 3. Invariants and first owners

| Contract | Invariant | First owner / affected consumers |
| --- | --- | --- |
| Predicates | Every accepted predicate appears exactly once with its original values in SQL lexical order; invalid input never broadens the query. | Shared query construction; sync/async query, Session query, SQLBuilder. |
| SQL expressions | Qualifiers, aliases, parentheses and typed SQLAlchemy meaning survive compilation. | Shared expression compiler; WHERE/HAVING/GROUP/ORDER/ON/projections. |
| Snapshot | Every base-table read of an accepted snapshot statement uses the same requested snapshot, or no statement is dispatched. | Pure snapshot rewriter/table-reference rendering; sync/async wrappers and ORM. |
| Types | SDK DDL preserves every supported MySQL scalar attribute; compilation success does not certify server runtime support. | MySQL-derived type compiler; SQLAlchemy DDL/reflection/ORM. |
| Results | Real models retain existing model results; string-table results preserve every returned value/ordinal and use actual column metadata. | Shared materializer; sync/async all/first/one and projections. |
| Export | `to_jsonl` selects JSON Lines explicitly; a rejected server interface cannot silently become CSV. | Shared export SQL builder; sync/async/stage delegation. |
| Compatibility | A client owns its version/capability observations for its current connection generation; metadata alone cannot prove a capability. | Client instance, including AsyncClient inheritance; no shared mutable server state. |
| Resources | Construction is pure; each probe/query connection and result has one lexical owner and a terminal release path. | Existing executor/engine context managers; no new pool, worker or retry owner. |

Negations are tested directly: exchanged Decimal/id values, discarded typo,
`= NULL`, orphan rows missing from RIGHT JOIN, mixed-time JOIN values, lost
microseconds/uint64 endpoints, and output that parses as another format.

## 4. Shared query construction and results

### Parameters and expressions

Retain the public legacy `execute(sql, tuple/list/mapping)` conventions and the
existing `SQLBuilder.build() -> (sql, params)` contract. The selected ORM design
uses the existing safe renderer **per fragment**, consuming only that fragment's
values and recording complete SQL. Most existing ORM paths already return
complete SQL and an empty parameter list; this removes the exceptional deferred
FIFO rather than introducing new parallel predicate state. No new predicate
pair-storage framework is introduced. Existing parameter-bearing `logical_in`,
SQLBuilder and CTE paths retain their valid deferred lists and ordered build
contract; unrelated fragment rendering cannot consume those lists. No ORM
method pops earlier values or performs its own `str.replace` interpolation.

Each parameterized raw fragment validates lexical marker cardinality as it is
rendered: missing/extra values cannot borrow from later fragments. Reuse
`_sql_literals.py`; quoted/comment markers and inserted marker-looking data
remain inert. Construction must not query SQL mode from a different pool
connection. Reject mode-ambiguous raw fragment SQL containing backslashes;
backslashes in parameter **values** remain supported through safe encoding.
Direct Client/Session execute keeps its existing execution-connection-dependent
rendering. No new driver-bound API is required to fix these legacy builders.

`filter_by` treats string-table keys as column identifiers and emits the
predicate, letting the server reject nonexistent columns. For mapped models,
resolve existing mapped names/keys and raise `ValueError` for an unknown name
before dispatch. Quote identifiers with the existing identifier authority;
never interpolate a key as raw SQL. `None` emits `IS NULL`, while other values
are rendered as data through the shared safe authority. Native SQL, strings and SQLAlchemy expressions must
agree on rows for equivalent predicates.

Use a MySQL-derived dialect with qmark parameter style to compile SQLAlchemy
expressions with expanded postcompile binds, preserving the ordered bind
values. Pass that SQL and its values through the shared renderer, including
expanded IN/empty-IN behavior. Do not depend on SQLAlchemy `literal_binds` for
unsupported Decimal/date/binary conversions or implement another literal path.
Remove qualifier-stripping regular expressions from all affected consumers,
including nested fulltext combinations. Preserve table/alias identity and
identifier quoting. A column from the base model used after `.alias('a')` is
adapted only by explicit expression/table identity, never by replacing text
inside the SQL string; explicitly aliased SQLAlchemy expressions retain their
alias. Raw expression strings remain caller-authored SQL. RIGHT JOIN shares
normal target resolution and ON compilation with JOIN and emits RIGHT JOIN;
there is no LEFT JOIN fallback or table-swapping approximation.

### Result contract

One materializer accepts result rows plus result column names. Sync and async
terminal methods differ only in awaiting execution; they call the same
materializer. Mapped/custom model queries retain existing model construction.
String-table `SELECT *` returns the same row-like representation already used
for explicit projections, populated using `ResultSet.columns` or SQLAlchemy
`result.keys()`. Empty `all()` is `[]`; `first()` is a row or `None`; bounded
`first()` construction does not mutate the reusable query. Positional indexing
and NULL values are preserved. Duplicate column names keep all ordinal values;
the documented legacy attribute lookup policy is retained, so callers should
label duplicate names for unambiguous named access. Missing metadata for a raw
star result is an explicit materialization error, not a guessed model/schema.

Do not merge buffered `ResultSet`, SQLAlchemy results and Arrow streaming into
one result object. Publish their existing lifetimes and types. Update affected
annotations with import-safe typing for the declared Python versions; annotations
do not make unsupported runtime/dependency combinations supported.

## 5. Complete-or-reject snapshot rewrite

Use one pure bounded lexical scanner/table-reference rewriter, shared by sync
and async snapshot wrappers and structured table rendering. No new SQL parser
dependency is required for the accepted grammar. It returns a completely
rewritten statement or raises `ValueError` before calling the executor. The
wrapper preserves parameters unchanged and delegates once. It never sends
partially rewritten SQL and never returns the original unsupported statement.

Accepted minimum grammar is one SELECT with one optional final semicolon;
unquoted or backtick-quoted `table`/`database.table`; optional explicit or
implicit aliases; explicit INNER/LEFT/RIGHT/FULL/CROSS JOIN table references;
ON/USING expressions; and ordinary WHERE/GROUP BY/HAVING/ORDER BY/LIMIT/OFFSET.
Single/double-quoted literals and ordinary line/block comments are opaque and
retain their bytes. Parentheses in scalar/boolean/function expressions must
balance. A SELECT with no table reads can be returned unchanged. Every accepted
base-table reference receives `{snapshot = <safe literal>}` immediately after
the qualified table and before alias. A snapshot name is a literal rendered by
the shared safe authority, never concatenated inside a quoted string.

The initial grammar rejects CTEs, nested SELECTs/derived tables, set operators,
comma joins, table functions, existing snapshot/time-travel suffixes, table
partition/index hints, DML/DDL, SELECT INTO/OUTFILE, locking reads, multiple
statements, executable/version comments, dollar-quoted strings, malformed
quotes/comments/parentheses, double-quoted table identifiers and any ambiguous
backslash-containing raw SQL. Reject any unconsumed or unclassified table
source suffix rather than assuming it is an alias. These restrictions are
intentional: use direct SQL with explicit snapshot hints for broader grammar.
Nested scalar functions are accepted only when the scanner can prove they
contain no subquery/table source. Keyword-looking text inside inert tokens must
not cause false rewriting or bypass validation.

ORM construction keeps table name, snapshot and alias separate until emission;
render `table{snapshot = ...} AS alias` in that order. ORM snapshot JOINs either
apply the shared rewriter to the complete supported SELECT or render every
structured base table with the same hint. They must not hint only the root
table. If an ORM feature emits a CTE/subquery outside the accepted grammar,
snapshot execution rejects it under the same policy rather than reading live
data. Snapshot existence, scope and privileges remain server-authoritative:
a table-scoped snapshot used for an unrelated JOIN must error, never omit that
table's hint. Tests use a database-scoped snapshot to prove same-time JOINs.

The scanner is O(SQL bytes), with O(tokens + table references) temporary memory,
no recursion over arbitrary nesting, no service I/O and no global state. A finite
nesting guard rejects excessive depth; no test asserts a wall-clock threshold.

## 6. MySQL scalar compilation and JSONL

Use one `MatrixOneTypeCompiler` derived from SQLAlchemy's MySQL type compiler for
all supported SQLAlchemy series. Keep only genuine MatrixOne vector/custom type
handling and proven MatrixOne deviations. Delete duplicate generic visitors
rather than extending a visitor whitelist after each failure. Ordinary MySQL
scalar compilation must match SQLAlchemy's same-version MySQL compiler,
including `fsp`, unsigned, numeric precision/scale, text/blob lengths, ENUM/SET
and BIT syntax. This is a compilation guarantee; online support is a separate
matrix. In particular this PR makes no DECIMAL UNSIGNED enforcement claim and
does not equate a MySQL type name with MatrixOne runtime semantics.

JSONL's shared builder appends `FORMAT 'jsonline'` irrespective of extension;
path/stage delegation and existing error categories remain compatible. Reuse
safe literal rendering for the path at this changed boundary. Do not retry with
extension-based format inference after a server error. A caller-owned output
can exist after a failed/uncertain export; the SDK must not delete arbitrary
caller files in compensation or promise transactional filesystem rollback.

## 7. Server versions and bounded capabilities

### Compatibility meaning

The qualification targets are released MatrixOne **4.2.5** and a source build of
the reviewed current main, identified by its actual source SHA and raw
`SELECT VERSION()` output. The latter may advertise `v1.3.0`; an old-looking
banner does not invalidate source provenance. These targets do not certify all
releases between them, future versions, production cloud deployments or a
mixed-version pool. Maintain a documented per-feature result for each target.

Keep existing version parsing/comparison APIs and synthetic development
versions as legacy metadata. Instantiate a per-client VersionManager with a
copied default feature-requirement registry; do not copy a global client's
current server version. Client methods/decorators must select the instance's
manager, or its owning `self.client` manager, when either exists. The standalone global accessor remains for
backward compatibility. Connecting or changing one client cannot change
another client's version checks. Unknown/development metadata never populates
an observed capability as supported.

### Additive observation API

Expose `client.get_capabilities(refresh=False)` and the awaited equivalent on
AsyncClient, returning a fixed-name mapping of immutable capability records:
`state` (`supported`, `disabled`, `unsupported`, `unknown`), safe `reason`,
`source` (`probe`, `override`, `unprobed`), and raw server identity/generation.
An optional `capability_overrides` mapping at connection setup supplies explicit
caller assertions with the same state vocabulary. Validate names/states; mark
override provenance and do not turn them into SDK certification. No global
registry mutation, extension system, TTL worker or background refresh is added.

Initial fixed capabilities and probes:

| Capability | Observation |
| --- | --- |
| `right_join` | Read-only SELECT over two constant derived tables with a deliberately unmatched right row; supported requires the exact expected rows, not merely successful execution. |
| `ivf_index`, `hnsw_index`, `fulltext_index` | `SHOW VARIABLES LIKE 'experimental_<name>_index'`; a recognized enabled setting is supported for the **setting/interface**, false is disabled, absent is unsupported. This does not certify index creation or algorithm semantics. |
| `snapshot`, `export_jsonline`, `arrow_load` | Unknown with an actionable unprobed reason unless caller overrides. No file creation, LOAD, DDL, feature-enabling SET or access to user tables is permitted merely to discover support. |

Probe only via existing connection/execution ownership. Settings describe the
observed session after its existing hooks; they are not a guarantee that another
pool session has identical settings. Record that scope in the reason. Do not
replace hooks or enable a feature. Cache only observations within the client's
current generation; disconnect/reconnect, endpoint/credential/database change,
or detected raw server identity change clears them. A refresh acquires a fresh
owned connection and republishes one complete snapshot after all probes finish.
Never publish partial/cancelled results. Concurrent callers may perform bounded
duplicate probes; no per-query lock or task registry is needed. Recheck the
generation before publication so an old request cannot overwrite a reconnect.
Mixed-CN support requires a future per-physical-server observation policy;
operators must qualify the least-capable node and may use explicit overrides.

Classify only known driver/SDK database exceptions using their preserved errno,
SQLSTATE/original cause. A proven syntax/unknown-variable response may establish
unsupported; privilege denial and transport/timeout establish unknown with an
actionable sanitized reason. Unexpected implementation errors propagate.
Async cancellation propagates as CancelledError after the existing owner
releases/invalidate its connection, and cannot be cached as unsupported.

Capabilities are additive diagnostics. Existing successful APIs continue to
dispatch when support is unknown; known disabled/unsupported interfaces may
fail early with a precise existing SDK error. In particular analytics cannot
claim Arrow LOAD support from a version. Its safe preflight can report unknown,
and its real attempted operation remains authoritative. A complete no-artifact
early Arrow detector is explicitly outside this PR and #30067 stays open for
that requirement. No automatic feature fallback, cross-server retry or uncertain
write retry is introduced.

## 8. Failure, cancellation, security and resources

Construction/compilation/rewrite failures occur before connection checkout and
cannot issue a broader query. Build/rebuild must not consume parameters or
mutate query state. Existing execution contexts own connections, transactions
and result release on success/error/cancellation; the refactor does not add a
second owner. Preserve QueryError/MatrixOneError catch behavior and chained
driver causes. Do not catch BaseException as ordinary capability failure.

Async helpers contain no blocking I/O; bounded local compilation finishes before
the next await. No cancellation parameter is added to pure builders. A task
cancelled while waiting for a pool slot or query must propagate cancellation,
release capacity and permit a subsequent query. Use barriers/injected awaits to
test entry, wait and publication. The existing driver may not cancel server
execution immediately; a client cancellation is not proof of server rollback.
Network inactivity timeouts remain the driver's contract, rather than a new
SDK-wide deadline or cancellation service. Late cancellation cannot reinterpret
an acknowledged result as a rollback. Existing analytics committed=True/False/
None and primary-versus-cleanup error semantics must remain unchanged.

New accumulating state is bounded by a fixed capability key count per client
and O(statement/predicate/result size) for existing buffered APIs. No workers,
threads, staging files, global caches, metrics cardinality or retry budget are
added. Large buffered results and `all()` remain explicitly materializing;
bounded Arrow streaming remains its existing separate API. SQL compilation
adds no network round trips; capabilities run on explicit observation calls,
not every execute. The MySQL compiler delegation removes repeated compiler
instances and the predicate owner removes FIFO `pop(0)` behavior.

Preserve authorization boundaries: probes are read-only and do not inspect user
tables or issue feature-enabling writes. Quote identifiers and render data via
existing authorities. Reasons/logs must omit passwords, parameter values and
credentials embedded in URLs. Keep original exceptions for programmatic
diagnosis, with existing redaction behavior for user-visible diagnostics.

There is no persistent migration. Rollback is an SDK package rollback; a schema
created with corrected TIMESTAMP/unsigned attributes retains its native server
definition. A rollback cannot recover precision already lost by old SDK DDL.
Do not rewrite existing schemas or make upgrade/downgrade data-repair promises.

## 9. Engineering and validation contract

### Test ownership and dependency matrix

Make pytest.ini the single active pytest configuration and remove contradictory
shadow settings. Local Make/tox and CI must select the same commands/dependency
families and report actual selections; no hidden global coverage target or
unavailable-service skip may become a pass. Keep strict markers and asyncio
ownership. Replace dependency-module mutation/filename-based repair with scoped
mocks at the actual executor/connection seam, and test real SQLAlchemy classes
in offline contracts. A lightweight SQLite fixture can prove shared SELECT
semantics but is explicitly not MatrixOne compatibility evidence.

| Layer | Required matrix / purpose |
| --- | --- |
| Minimal core wheel install/import | Python 3.8 and 3.9 with compatible resolver-selected SQLAlchemy/driver versions; no PyArrow/pandas/Polars installed. Import package/public clients and exercise representative result, query-build and diagnostics APIs in a fresh process. Preserve declared core support; failures require fixes or an explicitly reviewed metadata/support-policy change. |
| Full offline contracts | Existing Python 3.10/3.12 × SQLAlchemy 1.4/2.0 and Python 3.11/3.12 × 2.1 cells. Resolve valid ranges and capture installed versions. No impossible Python 3.10/SQLAlchemy 2.1 cell. |
| Driver regression | Exact reported Python 3.12/SQLAlchemy 2.0.43/aiomysql 0.3.2/PyMySQL 1.2.3 plus compatible current 2.0/2.1 and 1.4 controls; include pre-ping after at least two checkouts, not connect alone. |
| Analytics optional dependency | SDK qualification is Python >=3.10 with PyArrow >=18; core import still succeeds without it and analytics access gives actionable missing-extra diagnostics. Minimum Arrow 18 and current compatible Arrow in selected supported cells. Python 3.9 meets Arrow 18's dependency floor but is not an SDK analytics qualification claim. |
| DataFrame adapters | Separate pandas-only and Polars-only environments plus Arrow; round-trip a minimal nullable frame. Adapter import must remain lazy. No new DataFrame extra names are required. |
| MatrixOne public contracts | Release 4.2.5 and current-source build; Python 3.12/SQLAlchemy 2.0 representative tuple on both, plus the reported driver tuple for pre-ping. Full offline dialect/compiler variants cover other series; do not imply every cross-product was run online. |

PyArrow 18 metadata requires Python >=3.9; SQLAlchemy 2.1 requires >=3.11.
Sources: [PyArrow 18 package metadata](https://pypi.org/project/pyarrow/18.0.0/)
and [SQLAlchemy 2.1 package metadata](https://pypi.org/project/SQLAlchemy/2.1.0/).
The analytics extra cannot be certified on Python 3.8, and the SDK's supported
analytics matrix starts at Python 3.10. Do not silently remove
its dependency with an environment marker and claim a working extra. Keep
distribution metadata and public `__version__` consistent, including a sensible
source-tree fallback; inspect built wheel metadata and dialect entry points.
Make pyproject.toml the metadata authority: reduce setup.py to its compatibility
`setup()` entry point, explicitly discover only `matrixone*` packages, remove
stale wrapper-repository links and reconcile supported classifiers. Add a core
test dependency extra without Arrow while preserving the existing test extra
for callers; minimum-core CI must install the former or only core dependencies.
Do not promise typed-package/stub completeness from annotation edits alone.

### Risk-to-proof map

| Risk | Deterministic internal proof | Real MatrixOne proof |
| --- | --- | --- |
| Predicate loss/value exchange/injection | Named table cases for call order, Decimal/date/binary/Unicode/quotes/backslashes, NULL, typo, marker literals/comments, count errors and repeated build; exact SQL+params plus returned values. | Two discriminating rows, nullable third/control where needed; Client/AsyncClient and Session consumers return identical complete values. Rejected input sends zero statements. |
| JOIN/qualified expression semantics | Standard compiler comparisons; nested AND/OR/fulltext, aliases, qualified group/having/order and resolved model/string join targets. | One matched and one unmatched row on each side, NULL key control, duplicate column names, exact rows for RIGHT JOIN and grouped query. |
| Snapshot partial rewrite | Table-driven accepted/rejected grammar, inert comments/literals, qualified/quoted names, multiple JOINs, aliases, hostile names, depth/empty input, zero executor calls on reject and no param mutation. | Snapshot one/two rows then change live rows; database snapshot JOIN returns old value for both tables. Sync/async and ORM alias compare with explicit native snapshot SQL; rejection leaves connection usable. |
| Type data loss | Same-version MySQL compiler oracle for generic/MySQL scalar matrix and custom vectors. | Create SDK/native controls; assert column type/precision, uint64 endpoints and exact microseconds; failed duplicate insert leaves rows unchanged, next query succeeds, pool checked-out count returns to baseline. |
| JSONL format drift | Shared sync/async builder and path literal tests. | Dedicated shared output directory; parse every line from `.jsonl` and `.txt`, compare all values, exercise invalid SELECT and connection reuse. Tests remove only test-owned files. |
| Raw-table materialization | Shared sync/async star/explicit/empty/first, real metadata and duplicate-name ordinal preservation; mapped-model control. | String-table all/first values match native SQL; empty and nullable results plus Session consumer. |
| Capability contamination | Released/dev/unknown identities, two client versions, overrides, all states, typed permission/syntax/network failures, refresh and reconnect generation race controlled by barriers; no caching cancellation. | Read-only probes with existing hooks disabled; disabled setting and least-privilege observations where configured. No table/file artifacts, no SET or feature enablement, connections released and next query succeeds. |
| Async ownership regression | One-slot fake/real pool admission barriers; cancellation before admission, during execution/probe and before capability publication. Assert exact side effects, propagated cancellation and resource reuse. | Reused async connection/pre-ping; cancellation followed by successful checkout/query, with bounded test deadline. This does not certify immediate server cancellation. |

Reuse existing parameter-boundary, query-execution, join, version, connection,
export and analytics fixture suites. Preserve every distinct oracle when
consolidating duplicated tests. Use zero/one/two rows, injected failures and
barriers; no sleeps, probabilistic retries, million-row fixtures, new service
processes per test or performance wall-clock assertions. Existing analytics
stress/chaos evidence remains separate and is rerun only if its contracts change.
The Python changes do not require Go race, kernel BVT or CGo builds; deterministic
async lifecycle checks and real frontend/client tests prove these boundaries.

### CI execution and evidence

Retain the existing read-only, secret-free hosted PR workflow and protected
manual self-hosted integration boundary. Add explicit server-target matrix
selection for integration: 4.2.5 and current source. Configuration must identify
two deliberately provisioned endpoints or task-owned isolated deployments; the
workflow must not silently point both cells at one unspecified server. Assert
expected raw/version identity before tests, record current-source SHA/image
digest where available, SDK SHA, wheel/dependency versions, configured feature
scope, collected tests, counts and JUnit artifacts. A mismatched identity or
unavailable explicitly selected target fails the compatibility job. Provisioning
credentials/runners is operational work, not fabricated by a YAML change.

Keep the selected core compatibility module independent of optional S3/dual-CN/
TLS fixtures so required contract proof has no unexpected skips. Larger optional
topologies report their own scope and skips. Never label a job with skips,
missing endpoints, collection errors or a pending run as full certification.

Acceptance is measurable:

1. All twelve pre-fix public regressions become passing assertions, each with
   its nearest control; shared sync/async behavior is parameterized and build
   paths are tested for non-mutation/reuse.
2. Targeted static/import checks and every affected offline owning suite pass
   with real dependencies; one full SDK offline/analytics run closes shared
   fixture/order interactions. No filename-based dependency repair remains.
3. Required current-source and 4.2.5 contract selections are nonempty, finish
   successfully without unexpected skips, and record exact identity and
   dependency tuples. If a target lacks a feature, its test proves a precise
   rejection/unknown contract and documentation says so; it cannot pass by
   silently returning different data or format.
4. Known failures and cancellation leave no newly checked-out connections,
   probe-owned artifacts or broadened SQL; repeated build/observation after
   errors works. Existing analytics outcome/optional-dependency regressions pass.
5. Build wheel/sdist, validate metadata/import/dialect registration, and publish
   the tested matrix and unsupported grammar. Coverage numbers are descriptive,
   not substitutes for these behavioral cells. No claim that all historical
   SDK tests imply all server features/releases work.
6. Final complete change-map review reconciles implementation with this design,
   mo-self-review and the separately required model review. Evidence records
   exact source/test/config fingerprints and terminal status; unexecuted matrix
   cells remain explicitly unqualified.

## 10. Alternatives and decisions

| Alternative | Correctness/compatibility | Complexity, cost and migration | Decision |
| --- | --- | --- | --- |
| Patch each reported string substitution/regex/visitor independently | Fixes traces while retaining cross-clause consumption, incomplete snapshots and future type drift. | Small initial diff; repeated duplicated tests/maintenance; no stable ownership. | Reject. |
| Replace custom ORM and every SQL API with native SQLAlchemy/driver-bound execution | Strong mature expression/binding model, but changes build outputs, placeholder conventions, vector/fulltext and result behavior at once. | Large migration and compatibility blast radius; cannot validate most bugs in one bounded PR. | Defer as an explicit API migration; preserve native Core/Session path now. |
| Add a general third-party SQL parser and AST rewrite for every MatrixOne statement | Could support broader grammar, but parser must track MatrixOne snapshot syntax and dialect extensions; incomplete parse still risks live reads. | New dependency/version burden and much larger proof surface; no need for the reproduced minimum grammar. | Reject for this PR; bounded lexical complete-or-reject subset wins. |
| Continue first-FROM rewrite or return original SQL on parse failure | Silent mixed-time/live reads violate the snapshot contract. | Cheap, but impossible to make safe with fallback. | Reject. |
| Version-threshold capability table or development=all | Cannot distinguish feature switches, grants, current-source banners or backported interfaces. | Cheap but misleading across released/dev servers. | Reject as evidence; retain only legacy informational version APIs. |
| Probe by creating tables/files/enabling features | Can prove more interfaces, but discovery changes authorization/user state and needs cleanup/transaction outcome semantics. | More latency, privileges and artifact ownership. | Reject automatic mutation; honest unknown plus explicit qualification/override. |
| Selected shared owners + MySQL compiler + bounded snapshot scanner + fixed observational capabilities | Closes the reported invariant families and preserves existing execution/result ownership. Unsupported grammar/interfaces fail honestly. | Linear local processing; fixed small cache; limited additive API and documented stricter invalid-input behavior. | Accept. |

Accepted drawbacks: snapshot wrapper grammar becomes explicitly smaller than
arbitrary SQL; unsupported forms need explicit native hints. Capability unknown
is useful but cannot pre-certify every analytics interface, and session switches
are observations rather than all-pool guarantees. Minimal core Python 3.8/3.9
qualification is narrower than the full current-runtime suite. These limitations
are reported rather than converted into support claims. There are no unresolved
design blockers; unavailable runtime/deployment evidence blocks the corresponding
delivery claim, not a new speculative implementation.

Applicable precedent: [PEP 249](https://peps.python.org/pep-0249/) defines
driver parameter styles and ordered cursor metadata, but does not require the
SDK's legacy interpolation interface. SQLAlchemy's
[MySQL types](https://docs.sqlalchemy.org/en/20/dialects/mysql.html#mysql-data-types)
are the scalar compiler authority, with MatrixOne runtime differences proved
separately. Python's [task cancellation contract](https://docs.python.org/3/library/asyncio-task.html#task-cancellation)
requires propagation after cleanup; cancellation does not itself establish
database commit outcome. MatrixOne's parser and native SQL controls remain the
authority for snapshot suffix placement and JSON Lines export syntax.
