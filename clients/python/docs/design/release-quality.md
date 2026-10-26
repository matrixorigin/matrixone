# Python SDK release quality: shared contracts and real release qualification

Owning work: #30067, #30068, #30069, #30963 and #31023; implementation PR pending.

Design revision 1, 2026-10-26. Reviewed source:
`24c1ac897bf2ab89b5f77fcb14c8ed006299f2e4` in
`/mnt/nvme/sdk-release-quality-20261026`; source was clean during inspection.
Design task/session: `/root/sdk_release_design`. The parent task record owns the
explicit model/effort selector evidence for the required `gpt-6.1-sol` / `xhigh`
invocation. This document is a design artifact, not implementation or validation
completion evidence.

Decision: implement the confirmed shared corrections below. Do not implement a
new CLONE postcondition or a release-specific workaround until the pending native
transaction discriminator is resolved. Release qualification remains open until
the matrix below is executed and reconciled. Materially different new causes
require a design addendum before their implementation.

## 1. Scope, evidence, and existing work

The request covers the SDK as a product: execution, SQL construction, ORM and
SQLAlchemy integration, managers, analytics, errors, resource ownership, and
delivery/qualification. Assigned bugs are inputs to the audit. Another person's
open PR does not remove a defect that remains on this source revision.

PR #31262 is merged and its complete discovery must not be repeated. Its task
directory is `../sdk-quality-20261026`, especially `pr31262-issue-validation.md`,
`evidence.md` E13/E15/E16/E19/E28/E30/E31/E32, the final content manifests, and
the three final offline plus two native JUnit reports. The parent compared the
previous reviewed SDK runtime to this main revision and found unchanged runtime
inputs; the small later offline test change is considered separately. Reused
proof concerns its actual environments: release 4.2.5 and an identified source
binary, not 4.1.x or all 4.2 patches.

Current discriminating evidence, all outside source:

| ID | Fact and causal discriminator | Artifact / limit |
| --- | --- | --- |
| D1 | `MetadataRow.from_sqlalchemy_row` converts a real SQLAlchemy Row containing integer `1` to `is_hidden=False`; its implementation compares only with string `'true'`. | Parent executed public conversion; retain its probe in TASK. #30963 is not fixed by unrelated merged work. |
| D2 | On actual 4.2.5 / `1a44f3d`, SDK Stage CREATE with a dash in its name fails, while native backtick CREATE and SDK catalog readback succeed. | `probe-admin2-baseline.log`, `probe_admin2.py`. |
| D3 | On that server, SDK account CREATE USER with an apostrophe password fails; native doubled-apostrophe SQL succeeds and authentication using the exact password returns SELECT 1. | Same artifact. This is an executable value-integrity control, not just string inspection. |
| D4 | Native correctly escaped Stage apostrophe comment fails too, due to the server's catalog SQL reserialization. | Same artifact. No end-to-end Stage apostrophe support claim or SDK workaround. Verify failed-write catalog state. |
| D5 | Three fulltext public builders omit explicit LIMIT 0 while None and positive limits have distinct intended meanings. | `issue31023.txt` plus exact-source guards in both fulltext modules. Execute the nine-cell regression before editing; do not reuse an older report as current reproduction. |
| D6 | Actual 4.1.0 / `c58a0e6`: native fully hinted qualified JOIN returns historical id 1; `compile_select_to_sql` and ordinary `Client.execute(snapshot_select)` both return live id 2. `.with_snapshot` mutates the original; snapshot A/B cache keys are equal. | `probe-snapshot-baseline.log`, `probe_snapshot.py`. Source owner is a second regex snapshot implementation and arbitrary non-cache-key metadata. |
| D7 | Public missing-table QueryError has no `.orig`, no `.errno`, and suppressed `__cause__`, although SQLAlchemy/PyMySQL supply the source. | Same public native probe. |
| D8 | The contract runner accepts only 4.2.5/current-source and checks only the pinned release expectation; it cannot qualify 4.1/4.2 patch releases. | `scripts/run_contracts.py:22`, `:46`; current TESTING/validation docs. |
| D9 | Initial 4.1.0 broad-suite failures include corrected host/export mapping and permission setup failures; these are infrastructure, not SDK incompatibility. Fulltext nested MATCH and the exact in-transaction CLONE sequence remain under native discrimination. | `baseline-4.1.0*` and parent forthcoming terminal logs. Do not infer from partial output. |

## 2. Whole-SDK inventory and proof ownership

This inventory distinguishes retained coverage, concrete new closure, and an
unqualified surface. A file being inventoried is not a claim that every feature
of it was exercised on every release.

| Surface / owning components | Contract and existing proof | Work and release evidence |
| --- | --- | --- |
| Client/AsyncClient, config, `_connection_options`, connection hooks, logger | Authenticated admission, per-physical-generation hooks, URL/secret handling, timeout/cancellation and pool release. E15/E16/E31; offline connection/hook contracts and existing native hook cases. | Retain ownership. Core matrix exercises actual sync/async connection, reuse and failure. Real TLS peers/certificates are a separate configured deployment, not implied by plain Docker success. |
| Raw execution, both exported and legacy Session classes, `_session_execution` | Safe lexical parameters, connection-dependent SQL mode, original SQLAlchemy routing/ORM result processors, BEGIN/COMMIT/rollback and one-task Session ownership. Prior literal/Session/batch regressions. | Add source-preserving errors and snapshot statement execution; rerun every directly affected Session consumer in all three SQLAlchemy series. |
| `orm`, `async_orm`, `sql_builder`, `_query_compilation`, `_utils` | Predicate/value ownership, alias/schema identity, joins, NULL, DML limits and result materialization. Existing issue ledger/E13/E30/E31. | Keep merged correct mechanisms. No issue-number patches or blanket rewrite. Check normal expression compilation after the snapshot compiler is shared. |
| `_snapshot_sql`, public `select`, SQLAlchemy snapshot compiler, `sqlalchemy_select` | Complete-or-reject raw/ORM snapshot support exists, but the public SQLAlchemy consumer violates it (D6). | Close all public Select/compile/export/Client/Session paths with one rewriter and cache-safe metadata. |
| Dialect/type reflection, vector/JSON expressions, table/index builders, HNSW/IVF config | Driver identity and initialized state retained; scalar attributes/native Decimal preserved; offline compiler/oracle tests plus selected native values. | Snapshot compiler must retain driver-specific dialect and binds. Broad type-name compilation is not all-type runtime certification. |
| Stage, account/user/role, PITR, pubsub, snapshot create/delete | Shared manager SQL builders, executor ownership, observed catalog readback, stable public exception categories. | Close grammar STRING and identifier correctness at common helpers; test both async/sync consumers and acknowledged-write/readback failures. Keep existing empty Stage comment support. |
| CLONE and branch plus builder facades | Client vs Session execution, snapshot source and cancellation rollback have existing coverage. | Quote identifiers/snapshot tokens at shared builders. Resolve exact 4.1 transaction behavior before adding runtime verification. Both AsyncClient's local facades and exported manager classes are consumers. |
| Restore and mo_ctl | Restore already quotes names; mo_ctl interpolates expression arguments. Existing offline management tests. | Audit through the same identifier/literal contracts; use safe expression literals for mo_ctl. Do not issue destructive cluster/account restores merely to exercise a compiler. |
| CDC and CLI consumers | Shared task SQL builders and row/frequency/watermark parsers; no new CDC delivery engine. | Render names, URI/options grammar tokens and catalog predicates using the same authorities. Actual replication endpoints are separately qualified; offline SQL proof does not certify delivery. |
| Metadata | Object identity/aggregation/statistics and raw-vs-structured result contracts, E31. | Correct driver boolean normalization once at row conversion; native persisted-object scan plus real SQLAlchemy Row controls. |
| Fulltext managers, scoped execution and both builder modules | Algorithm restoration, exact term membership and grouped OR already covered. | Fix explicit zero across all three consumers. Native supported/unsupported nested MATCH results must be distinguished without skips or runtime version vetoes. |
| Vector manager, Pinecone facade, IVFRank and index metadata | Exact distances, filters, identities, atomic upsert, live schema and result-column collisions have E31 proof. | Retain proof, run compact actual native vector/query controls per patch. Native IVF controls do not certify HNSW indexing semantics. |
| LOAD, export and Stage file delegates | Format/options, local-infile opt-in, safe paths, output parsing and Session ownership already covered. | Snapshot-aware SQLAlchemy export is a consumer of new compile closure. Native export paths require a correctly shared, writable owned mount. |
| Optional analytics/FileRelation/ArrowStream | Existing bounded streaming, file checks, staging ownership, cleanup, commit-unknown errors and adversarial suite. | Preserve APIs and resources. Qualify release features individually; no all-release Arrow LOAD/S3/multi-CN/crash/performance claim from core passes. No async analytics feature is introduced. |
| Package exports, CLI entry point, wheel/sdist, docs/examples and test runner | Core import without PyArrow, Python/dependency matrix and distribution validation exist. | Update truthful release qualification docs and durable artifacts. Build/import/install final distributions and run incremental Python checks. |

The complete change crosses SQL construction, execution, compiler/cache and
release compatibility boundaries: design-first applies independently of diff
size. Pure helpers are R2; cached compilation and transaction/error boundaries
are R3. There are no kernel, wire-format or persisted-format edits planned.

## 3. Invariants and selected mechanism

1. An accepted name/value occupies exactly its intended grammar position. Invalid
   or unrepresentable input is rejected before dispatch; it cannot change the
   number/scope of statements. Construction performs no pool checkout or SQL-mode
   query.
2. An accepted snapshot Select reads every base table at the same snapshot or
   fails before dispatch. Live/A/B Selects have distinct cache identities and
   deriving one cannot mutate another.
3. None means an omitted pagination bound; zero means a present zero bound across
   every fulltext SQL builder. Existing controls remain intact.
4. Protocol boolean truth is preserved in structured metadata; textual `'false'`
   is not Python truthiness and integer `1` is not string comparison.
5. SDK domain error categories/messages remain compatible while the original
   source and numeric server code remain accessible. Cancellation is never
   converted into a query/domain error or interpreted as rollback.
6. Release support evidence identifies actual binaries and nonempty selected
   cases. A native limitation is an explicit observed outcome, never a skip or
   inferred capability based on a version number.

### SQL names and values

Retain `_sql_literals.py` as the single authority. Distinguish:

* `sql_literal`: expression positions (catalog WHERE, mo_ctl function arguments);
  its hex/CONVERT representation supports backslash/NUL data there.
* `sql_string_token`: parser STRING positions (Stage URL/comment/credentials,
  CDC URI/options, passwords/comments, snapshot suffix). Reuse apostrophe
  doubling. Generalize its LOAD-specific diagnostic without weakening existing
  rejection. Until a tested mode-independent grammar representation exists,
  reject backslashes/NUL at this pure boundary; do not insert CONVERT into a
  STRING-only grammar or query sql_mode on another connection.
* `sql_identifier`: one identifier versus a qualified table reference. Reuse
  dialect quoting and validation; no SQL-fragment interpolation.

Make `_escape_string` on both clients delegate to the STRING authority, because
its account password/comment consumers require that grammar. Catalog predicates
may use the expression authority directly. Retain `_escape_identifier`'s legacy
always-quoted output while delegating validation/escaping to the same authority.
Do not change public return types or duplicate escaping implementations.

For manager APIs that historically accept a fully backtick-quoted name, preserve
only a complete one-token or qualified-token input by a small bounded lexical
normalizer, then re-emit canonical quoting. Reject leftover SQL suffixes. Keep
atomic names containing dots distinct from `database.table` references. Existing
ordinary names must preserve their semantic names; do not interpret caller SQL
inside a name. Shared table normalization must handle ORM-resolved schema/name
components without splitting dots inside an already resolved component. Do not
extend the arbitrary SQL snapshot parser to become a general DDL parser.

Migrate Stage, snapshot create/delete, clone/table branch/drop/diff/merge, CDC and
the common client string helper at their shared owners. Audit PITR/pubsub/account,
restore and async facade reverse arcs to confirm they use the owner. Enumerated
grammar clauses (conflict strategy, range units, privilege/object types) remain
validated grammar, not quoted values. Preserve their existing accepted public
semantics; reject dynamic non-enumerated clauses if a concrete reachable input
could escape the documented grammar.

No broad renaming or framework is necessary. The alternatives are repeated
per-method quote replacements (incomplete and mode-sensitive), universally bound
DDL (not all parser positions/build-string APIs support it), or a new SQL parser
and manager framework (higher complexity without a needed contract). Existing
three lexical authorities plus narrow name normalization close the mechanism.

### Snapshot Select compilation, caching and all consumers

Replace `_matrixone_snapshot` mutation with immutable SQLAlchemy annotations
that participate in the structural cache key, e.g.
`statement._annotate({'matrixone_snapshot': name})`. `.with_snapshot` returns a
new Select; subsequent `.where`, `.order_by`, `.limit` retain it. Reject invalid
or empty explicit snapshot arguments rather than truthiness-dropping them.
The same original statement can safely derive live, snapshot A and snapshot B.

Add snapshot handling in `MatrixOneCompiler.visit_select`: use the ordinary
MySQL compiler for binds, processors and result shape, then pass the resulting
complete supported SELECT through `_snapshot_sql.rewrite_snapshot`. Both adopted
driver dialects already use MatrixOneCompiler; retain that same driver dialect
object and initialization owner. No global replacement of unrelated SQLAlchemy
Select compilation is introduced. The existing monkey-patched convenience
method may remain for API compatibility, but its behavior becomes generative.

`compile_select`, `compile_select_to_sql`, `compile_expression` for relevant
statements, and export must use that compiler and the shared safe bind/literal
renderer. Delete the second regex rewriter. Explicit engine compilation must
retain its dialect's bind/type settings, and unsupported foreign-engine snapshot
compilation must fail explicitly rather than drop metadata. Ordinary
Client/AsyncClient and both Session implementations execute the typed statement;
do not route it through a string fallback that loses ORM results, bound runtime
parameters, execution options or connection routing.

Snapshot annotations on nested Selects/CTEs or on a statement whose generated SQL
is outside the existing grammar must reject before dispatch. This is consistent
with raw/ORM complete-or-reject support. Do not silently apply one hint inside a
broader live query. Reuse the existing O(SQL bytes) scanner/nesting bound. Snapshot
names at suffix positions follow the STRING token restriction above.

Cache tests must prove distinct live/A/B keys and actual compiled-cache execution
against distinct native snapshot/live rows. Attribute presence or generated SQL
alone is insufficient. Also prove generative use does not change the original,
bind values/SQLAlchemy result processors survive, aliases/qualified JOINs all get
one hint, and unsupported statements dispatch zero commands.

### Metadata and pagination

Normalize `is_hidden` at `MetadataRow.from_sqlalchemy_row`, shared by structured
sync/async projections. Accept the observed protocol bool and integer 0/1 plus
legacy case-insensitive `'true'/'false'` and `'0'/'1'`; define NULL consistently
with existing non-hidden behavior if it is a documented nullable representation.
Reject unknown representations instead of publishing a plausible false value.
Do not add a general coercion framework. CDC's nullable `no_full` is a neighboring
truthiness hypothesis; only expand the same helper there if its actual/public
row contract supplies those alternate forms, and keep None nullable.

Change all three fulltext limit guards to `is not None`, retaining None, zero and
positive controls plus combinations with offsets/order/score. Preserve explicit
zero offset as appropriate without broadening offset-only native syntax. No
pagination class or client-side short circuit is needed. The SQL engine should
execute LIMIT 0 and retain the normal result/metadata/error behavior.

### Original errors and unsupported features

Keep QueryError and all existing manager error classes. At the single classifier
boundary attach the original exception (`orig`) and numeric errno, preserving an
already classified SDK error rather than wrapping it repeatedly. Existing
user-facing message selection stays stable. Every Client/AsyncClient and both
Session call sites retain an accessible original chain; low-level SQLAlchemy
Session APIs still expose their documented exception behavior. Touched manager
wrappers use explicit chaining. Do not create a new error catalog or retry rule.

Native error text can contain caller SQL/secrets; original exceptions are for the
caller, not additional automatic logging. Keep redacted connection diagnostics
and capability reasons. Unsupported classification must not turn permission,
authentication, transport, cancellation or arbitrary syntax failures into
certified feature absence. Error identity does not imply retry safety.

Stage apostrophe comments on affected releases are an explicit server limitation:
properly encode the request, preserve the native error, inspect owned catalog
state, and never compensate by deleting a caller-owned object or modifying the
data to make it succeed. Nested MATCH, if the native discriminator proves it
unsupported on 4.1.0, follows the same policy: no query broadening, full scan
fallback, write probe, version-based early veto or silent skip.

## 4. Ownership, deadlines, results and resources

The execution owner remains the selected Client/Session connection. Pure SQL
construction acquires no resources. Sync calls retain driver read/write budgets;
AsyncClient's existing end-to-end wait_for budget covers admission through fetch
and commit. This change must not start a fresh timeout for a handoff, swallow an
outer task cancellation, or substitute an unconstrained task/context. Do not add
a timeout parameter to every pure helper.

Construction/rejection -> admitted driver operation -> selected result or error
-> transaction commit/rollback -> connection release remain distinct states.
Domain wrappers catch Exception, not BaseException. A body error/cancellation is
primary over rollback/close failure; attach cleanup information without erasing
the primary. After a submitted COMMIT, timeout/error can leave the durable outcome
unknown. No retry or rollback promise is inferred from a stopped wait. Existing
analytics committed/commit-unknown outcomes remain unchanged.

Each result or connection has its existing lexical owner; new tests register
cleanup immediately. New catalog checks, if justified below, must use the same
executor and release their result before returning. No worker, queue, background
task, new cache, retry owner or global mutable registry is added. Capability
observations remain immutable, per-client/generation, read-only and bounded;
version strings never populate runtime capability states.

String/name normalization and snapshot rewriting are linear in input bytes with
bounded parser nesting. Common query execution gets no new metadata roundtrip.
Compilation annotations participate in the existing bounded engine cache; no
parallel cache is added. Pagination fixes can only bound previously unbounded
results. Metadata conversion is O(1) per row. Native release instances are
sequential, or at most two within the recorded 31 GiB host budget and 6 GiB caps;
do not contend for owned ports/paths.

## 5. Pending CLONE transaction discriminator

The 4.1.0 missing-source CLONE control outside a transaction errors normally;
do not claim general silent success. Run the exact native BEGIN -> INSERT ->
successful CLONE -> missing CLONE sequence and compare command completion, target
existence, transaction visibility from another connection, rollback and reuse.
Compare sync/async SDK Sessions and native SQL before selecting a correction.

If native correctly errors, repair the first SDK ownership/result-consumption
boundary responsible, preserving Session execution and original errors. If native
acknowledges a missing target in this sequence, a possible minimal SDK contract is
one same-executor SHOW CREATE TABLE/DATABASE after CLONE. Its promise is target
existence after acknowledgement, not kernel-level clone-content certification.
It preserves existing IF NOT EXISTS no-op semantics and costs one administrative
roundtrip. A readback failure states CLONE was acknowledged but target not
verified, preserves cause, does not DROP a target, and does not imply rollback
outside an explicit transaction. Both public async facade and exported managers
must participate. Review this choice against the discriminator before editing.

Regardless, use a guaranteed duplicate-key or other proven error in the general
rollback regression, so that proof does not accidentally depend on a release's
missing-source semantics. Retain a separate regression for the discovered CLONE
behavior; changing the failure trigger alone does not fix a confirmed defect.

## 6. Release qualification and unsupported outcomes

The intended release set is actual stable tags 4.1.0 through 4.1.6 and 4.2.0
through 4.2.6, subject to registry verification of every tag. Record missing tags
as unavailable, not tested. Source main is a separate optional forward target,
not a replacement for released binaries.

| Dimension | Required evidence |
| --- | --- |
| Every available patch in both families | Actual image digest, VERSION()/git_version(), nonempty compact public core contract suite, terminal result and per-feature observations. |
| Earliest/latest patch of each family | Full existing required contract suite plus new regressions; explicitly asserted native limitations count as tested negative outcomes, not success of the unavailable operation. |
| 4.2.5 historical matrix | Reuse unchanged existing native proof; run changed-contract regressions because new source invalidates those mapped cells. |
| Python / SQLAlchemy | Full offline ownership suite with 1.4.54, exact 2.0.43 and 2.1.x in their supported Python environments. Reuse unchanged oldest-runtime core import proof only if new syntax/import graph is compatible. |
| Optional deployments | TLS, HNSW/native Arrow, S3, multi-CN, chaos/restart/soak remain individual cells with actual configuration evidence; unexecuted cells remain unqualified. |

Generalize `--server` to an exact semantic release target or current-source, keep
the current 4.2.5 invocation compatible, and require expected release metadata to
agree. Verify actual parsed release identity exactly, not a loose substring.
Source targets require expected revision. Record both raw identity values.

Add a small explicit core/full selection only if needed for the patch matrix;
default server selection remains the current full contract selection plus new
contracts. The compact selection must exercise sync/async CRUD and Session
rollback, values/NULL/Decimal, qualified snapshot Select/cache separation,
metadata, representative vector/fulltext operations, admin value/name behavior,
original failures and pool reuse. Required configuration is selection-specific;
a core suite need not demand an export mount it does not use. Full suite does.

Retain provenance hashes, nonempty selection, no unexpected skips/errors/failures
and input-stability checks. Use observed native control outcomes to assert
unsupported behavior in tests: only a narrowly identified native unsupported
error permits that negative branch; permission, setup, transport and arbitrary
errors fail the test. Report feature outcome properties/cells so a green suite
cannot be read as universal operation support. No runtime version-feature table
is introduced.

## 7. Risk-to-validation map and acceptance

| Risk / closure | Cheapest focused proof | Real consumer / broader proof |
| --- | --- | --- |
| Grammar tokens and names | Shared helper boundary controls: apostrophe, Unicode, backtick/reserved/dash name, qualified components, empty optional comment, injection-shaped input, backslash/NUL rejection before dispatch. Use real Client helpers, not mocks redefining escaping. | Native Stage dash lifecycle; exact apostrophe user password authentication; snapshot/clone/CDC compiler consumers in both modes; server limitation/cause and no unintended catalog changes. |
| Snapshot mutation/cache/mixed-time reads | Generative identity, live/A/B distinct cache keys, complete JOIN rewrite, retained bound params and scalar/model metadata; unsupported shape dispatches zero calls. | Cached typed Client/AsyncClient, both Session classes, compile_select_to_sql and export produce exact historical rows on actual 4.1/4.2; live/A/B controls and connection reuse. |
| Protocol boolean conversion | Real SQLAlchemy Row with bool, 0/1 and legacy text true/false, including requested projections. | Persisted native metadata raw value versus structured sync/async value; retain object/statistics tests. |
| Explicit zero LIMIT | Three builders x None/0/positive; offset/order/score neighboring controls, repeat build unchanged. | Populated native fulltext table: zero returns no rows; positive/unbounded exact controls. |
| Error identity / unsupported outcome | Original exception object and errno survive classification, double wrapping, Session and manager boundary; cancellation propagates unchanged. | Public missing-table/unsupported/native limitation with original driver cause; one-slot pool reuse after error; existing cancellation/rollback cases. |
| CLONE transaction behavior | Native versus SDK exact ordered sequence; no sleep or speculative retry. | Same-session visibility and second-session rollback/commit, catalog teardown and pool reuse; only after cause-based design decision. |
| Runner/release truth | CLI valid target/wrong target/missing config/empty selection/skip/failure/drift controls and provenance schema. | Real patch matrix with actual binary identities and explicit per-feature outcomes. |
| Packaging/compatibility/performance | Changed-file Black, repository fatal flake8 closure, applicable configured mypy with baseline comparison; syntax/import under declared runtimes. | Offline three-SQLAlchemy owning suites; build/twine/wheel imports and CLI entry point. Inspect no new query I/O/state; benchmark only if implementation introduces measurable hot-path cost. |

Use existing fixtures and extend coherent contract modules. Register cleanup as
soon as an object is created; assert catalog/file teardown and zero pool checkouts.
Do not add sleeps, timing assertions, duplicate heavy clusters, broad fixtures or
tests that mirror implementation while lacking a user-visible oracle. Required
SDK public integration tests replace no kernel behavior, so new Go UT/BVT/race
runs are inapplicable for Python-only edits. Supported deterministic async
cancellation, ordering, rollback and pool tests provide the relevant concurrency
proof. If source touches Go later, recalculate that gate rather than claiming N/A.

Completion requires the final complete change map, all mapped regression and
negative cells, real release matrix, three dependency suites, incremental static
and distribution checks, and a required-model overall review at final reviewed
content. Pending CI, skipped optional deployments, infrastructure failures and
unexecuted native cells must remain explicitly separate. An audit inventory and
passing old issue ledger alone do not complete this request.
