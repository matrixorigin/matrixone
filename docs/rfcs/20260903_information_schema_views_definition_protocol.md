- Status: in progress — independently accepted design; implementation under review
- Start Date: 2026-09-03
- Authors: MatrixOne maintainers
- Implementation PR: https://github.com/matrixorigin/matrixone/pull/27716
- Issue for this RFC: https://github.com/matrixorigin/matrixone/issues/27655

# Parser-derived `information_schema.VIEWS` definitions

## Independent design assessment and revision

[Independent review 5495923812](https://github.com/matrixorigin/matrixone/pull/27716#pullrequestreview-5495923812)
assessed this RFC at implementation revision
`5db81447b4afffea9a3a1cbfec528fb1cca901d1` on 2026-10-12. Its decision was
REQUEST_CHANGES for the function IDs newly occupied by official main; it found
the ownership model and architectural tradeoffs otherwise acceptable. The
accepted decisions include CREATE/ALTER ownership of frozen metadata,
side-effect-free parser fallback, transaction-owned bounded reconciliation,
the historical raw-wildcard limitation, separate parsing for both legacy
metadata columns, serial maintenance ordering, and forward-only recovery after
the monotonic protocol floor.

Revision `3ba7fee3155d332f94ddedb83a9883faa5e7d5ab` reconciles the allocation with
official main `fa7cc1d9b38e3324572a4efc5617033522de0cbf`: FORMAT_BYTES and
FORMAT_PICO_TIME retain 596/597, VIEWS uses 598/599, and MORPC remains 112 above
main's 111. Registry, shared raw-ID detection, FORMAT negative controls, and
the persisted/Prepare/send/receive tests passed together on this combined tree.
The remaining non-blocking duplicate persisted-expression walk is removed by
consuming the existing `ViewMetadataFunctions` result; the persisted marker and
read/authoring admission remain separate consumers of that shared detection.

[Independent review 5496922722](https://github.com/matrixorigin/matrixone/pull/27716#pullrequestreview-5496922722)
records a distinct design **PASS** for the corrected revision
`a766542fb6083e3198e0f7bc5eece33fa20193a6` on 2026-10-12. It confirms the
function allocation and shared persisted-feature detection, accepts the
documented tradeoffs above, and reports no additional actionable code defect.
Its remaining requirement is to record that acceptance and advance this RFC
to the accepted `in progress` state. This status follows that independent
decision; implementation approval remains a separate GitHub review state.

## Summary

`information_schema.VIEWS.VIEW_DEFINITION` must expose the defining SELECT,
not the original CREATE statement. New views persist a parser-derived definition
and legacy rows are read through parser-aware metadata functions. The functions
are new distributed plan functions (IDs 603 and 604), so the catalog contract is fenced by MORPC
v126.

## Problem and invariant

Schema-diff and migration clients replay `VIEW_DEFINITION`. A full CREATE
statement is not a standalone SELECT and falsely marks aggregate views as
updatable. Current rows, and legacy rows that have already acquired the additive
metadata, return their parser-derived frozen SELECT. A legacy row that contains
only the historical `Stmt` is parsed into a defining SELECT, but a raw
`SELECT *` cannot be expanded to its creation-time column list because that
snapshot was never persisted. Such a row therefore preserves the stored
wildcard expression instead of claiming a historical freeze. Malformed catalog
rows may return NULL. No CN that cannot resolve either function ID can receive a
pipeline or catalog view that references it.

## Design

The CREATE/ALTER owner derives `ViewData.Definition` from the stabilized view
AST, after wildcard expansion and separately persists `CheckOption`. Explicit
view column names are exposed through a derived-table column list around that
frozen SELECT, including UNION output, so replay preserves the view's public
column names without renaming aliases referenced by the inner `ORDER BY` or
`HAVING`. The same parser-tree helper is used when regenerating a legacy
definition and when the metadata function parses a legacy `Stmt`. The catalog
remains the single owner of that frozen metadata.
`mo_view_definition(viewdef)` and `mo_view_check_option(viewdef)` return the
stored fields without writes; for an older row that lacks them, they parse only
the stored statement using its persisted SQL mode and identifier-case settings.
This bounded, side-effect-free fallback avoids a second SQL regexp lexer and
does not depend on background recovery.

When either function identity occurs in a persisted view expression, the
planner writes `required_protocol_version: 126` into `ViewData`, including when
the call is nested under another expression. The real view bind/Prepare path
reapplies that marker. It uses the existing two-floor admission lifecycle:
HAKeeper publishes the durable read floor during the v126 decoder barrier, while
the authoring floor remains closed until the all-CN admission and catalog fence
complete. Therefore an authoring or read floor of 0, v104, v105, v106, v108, v109, v110, v111, v112, v113, v114, v115, v116, v117, v118, v119, v120, v121, v122, v123, v124, or v125 fails
closed, an all-v126 fenced CN can create the VIEWS definition, and an ordinary view
without either function remains unmarked.

NAME_CONST output names use the same AST rule in planning and legacy metadata
export, rather than the generic function spelling. View-definition formatting
retains ordinary NAME_CONST calls, but exports a backslash-containing text value
as its equivalent mode-independent literal expression. Its implicit projection
name is retained as an alias, including in derived tables. Parentheses and
explicit COLLATE on the value remain intact. Binary literal values keep their
function call and binary type. Escaped text uses a character-set-introduced
hex string (`_utf8mb4 X'...'`), decoded by the existing string-literal owner.
It retains the inferred character width without an unsized cast's widening
or the declared `VARCHAR(n)` limit of 65535. Both current and legacy export
therefore preserve values and column types even beyond that declared-size limit.
This export mode does not mutate the AST or relax
normal binding: arbitrary CAST/function arguments remain invalid NAME_CONST
inputs. Both CREATE/ALTER metadata and Stmt-only reads use the same formatter;
already-frozen definition strings remain unchanged.

Output-name preservation applies to every query block, including derived
tables, CTEs, UNION branches and scalar/filter subqueries. Current metadata
uses the resolved projection capture even when a query contains no wildcard;
only expressions whose rendering changes their name need a generated alias.
Legacy metadata applies the parser heading rule during each SELECT projection's
export formatting, including beneath explicit view/derived column lists.
Explicit aliases, names used by HAVING/ORDER BY, and ordinary star/SAMPLE
semantics remain intact. Formatting does not mutate the source AST.

The fallback cannot infer metadata that is absent from a legacy row. In
particular, it does not promise to reconstruct the historical expansion of a
wildcard; the authoritative freeze begins when the additive fields are written
by CREATE/ALTER or by the bounded regeneration path. Adding a historical column
snapshot to the legacy catalog format would be a separate compatibility
migration and is outside this PR.

The post-upgrade reconciliation uses the exact current
`InformationSchemaViewsDDL` as its idempotence marker. After the common v126 gate
is available, a bounded page may repair any tenant whose
`information_schema.VIEWS` is missing or whose definition is not exactly that
current DDL. This includes the v105 predecessor definition and older
metadata-view definitions; it does not modify user views, user data, or any
information-schema object other than `VIEWS`. The broader convergence rule is
intentional: a tenant can have been created while capability discovery was
incomplete, can have stopped between a drop and create, or can be carrying an
older view definition from a prior upgrade. Treating only one literal
predecessor as repairable would leave those states permanently stale.
The catalog probe recognizes both `VIEWS` and the default case-folded `views`
name. It still compares the full persisted CREATE statement exactly; name
folding must not turn a stale definition into an installed marker. The
maintenance repair uses this probe, so a committed repair is a no-op on every
later pass, regardless of catalog name casing.

Historical upgrade identities are immutable. In particular, v4.0.7 retains
its released offset zero, protocol-61 requirement, flags, and index provenance
migration. It has no VIEWS entry: adding one would change its derived offset
and make startup reject an interrupted upgrade recorded at offset zero.
The existing periodic maintenance owner schedules VIEWS independently, scanning
all account IDs without filtering `create_version`; tenants already at v4.0.6,
v4.0.7, or the final version therefore converge after the local authoring fence
and common-CN v126 gate are both ready. HAKeeper publishes that protocol floor
from binary capability through the log-service admission owner, independently
of historical bootstrap handlers. A pending v4.0.7 upgrade resumes first;
VIEWS reconciliation runs after the final version is ready.

The reconciliation transaction owns one account page (32 accounts by default)
and publishes its process-local account cursor only after commit. A replacement
failure, cancellation, or mid-page protocol loss rolls back the complete page
and leaves the durable VIEWS definition and cursor as the retry marker. If an
account disappears after page enumeration, its account-local objects are
already gone; the transaction intentionally skips that account and may commit
the remaining repairs and cursor. This is the only account-disappearance
exception to page-wide rollback. The periodic upgrade owner retries the same
page on its next 10-second maintenance tick; a successful empty-page scan wraps
the cursor so late-created accounts remain discoverable. The final-version pass
intentionally invokes VIEWS reconciliation before orphan-privilege maintenance
and returns a visible error when reconciliation fails. Consequently,
orphan-privilege maintenance is skipped on every failed VIEWS pass, including
repeated failures; once VIEWS succeeds, that same pass proceeds to orphan
cleanup. This serial ordering keeps a pass that failed to establish the v126
public catalog contract from being reported as fully successful, while the
bounded VIEWS page remains independently rollback-safe.

MORPC v126 is allocated as `MORPCLatestVersion + 1` from official main v125 at
`a648b13e53cf0a07ff19211203a69e37b2d1dd93`. Main v125 owns LOAD IGNORE foreign-key admission and diagnostics. Main v124 owns exact fractional TIMESTAMPADD SECOND intervals; main v123 owns capability-gated nonblocking row-lock admission; main v122 owns unary TIMESTAMP and dynamic TIME result contracts; v121 owns exact unsigned DECIMAL DIV result domains; v120 owns current-session KILL authorization and proxy disconnect cleanup; v119 owns parameterized subquery programs; v118 owns transactional
database defaults and table charset alteration. Main owns v117 collation-aware
LEAST/GREATEST overloads, v116 SELECT division diagnostics, v115 calendar interval
normalization, and the earlier independent metadata/result contracts. This
rebase advances VIEWS from v125 to v126; historical measurements retain their
original version and revision identities.
Main's hyperbolic functions retain IDs 591–595, FORMAT_BYTES and
FORMAT_PICO_TIME retain IDs 596 and 597, and TO_CALENDAR_INTERVAL retains 598.
Main retains CHECK_ASYNC_DATALINK_SOURCE at ID 599 and
INTERNAL_TABLE_COLLATION at ID 600, and TIMESTAMPADD_SECOND_INTERVAL at ID 601 and LOAD_FK_IGNORE_CHECK at ID 602. The current main function end marker is 603;
the two VIEWS function IDs are 603 and 604, and the end marker advances to 605. These IDs are allocated from the rebased official main and are
distinct from all existing function registrations.
The capability is specific to these functions and the persisted VIEWS definition.
A sender probes the selected destination CN as well as its local runtime before
encoding a pipeline containing either function ID; an unknown or unavailable
destination capability fails closed. The v4.0.7 VIEWS upgrade waits for common
v126. New tenant initialization installs the complete current information-schema
set only after a positive common-v126 check. A mixed, unknown, RPC-failing,
or incomplete capability result aborts CREATE ACCOUNT and rolls back its
catalog rows and final-version marker. It must not commit predecessor views
under the final account version. Once the cluster is ready, retrying the same
account name initializes the current COLUMNS, TABLES, SCHEMATA and VIEWS
contracts in one transaction. Older binaries remain responsible for creating
tenants at their own catalog versions; their persisted versions are handled
by the normal tenant upgrade route. This initializer does not synthesize an
older catalog or lower its version marker.

The bounded VIEWS reconciliation pass remains available for existing legacy
catalog rows. Bootstrap prerequisites still defer the v118 TABLES function
until the existing post-admission publication phase. Historical protocol
adapters remain for bootstrap and persisted metadata compatibility.
Pipeline preparation, remote marshal, and
remote unmarshal reject a
pipeline containing either function ID below v126. The receiver check protects
stale prepared work as well as normal sender dispatch. Before the v126 HAKeeper
protocol-floor activation is durably committed, a cancelled rollout may stop
the v126 upgrade, keep or restore `InformationSchemaViewsLegacyDDL`
transactionally, wait for catalog and in-flight work to converge, and admit
v125 only after verifying that no v126 maintenance worker can re-install the
new definition. After the v126 floor is committed, the floor is monotonic and
v104/v105/v106/v108/v109/v110/v111/v112/v113/v114/v115/v116/v117/v118/v119/v120/v121/v122/v123/v124/v125 binaries must not be admitted: restoring the view text alone cannot
undo the decoder barrier. The supported recovery is forward recovery with v126-
compatible log/CN services, or a coordinated restoration of cluster state from
before the activation barrier; an ordinary in-place downgrade is unsupported.
Merely draining below-v126 requests is not sufficient because the new persisted
view text references the functions. The new JSON fields are additive and old
binaries keep treating them as unknown.

## Alternatives

Keeping raw SQL regexp extraction was rejected because it repeatedly diverged
from the SQL lexer for comments and quoted strings. Eagerly rewriting every
legacy row was rejected because the existing recovery lifecycle is deliberately
inactive and a metadata read must not perform unbounded catalog writes. Allowing
the DDL before v118 was rejected because an old CN cannot bind the metadata functions.

## Bounds, security, and operations

The compatibility parse is per visible legacy row and is linear in that row's
stored statement; current rows return their stored definition directly. If both
legacy metadata columns are requested, the row may be parsed once per function;
each parse is still bounded by the stored statement and query cardinality. It
creates no durable work, goroutine, queue, retry, or cache. Existing visibility
joins remain the authorization boundary; the functions do not broaden the
selected row set. A mixed-version request fails before dispatch with a stable
NotSupported error rather than returning wrong metadata.

The sender detects both VIEWS function identities in its existing
`RequiredRemoteExpressionFeatures` walk. That feature feeds the shared placement,
local/receiver, and destination gates; it does not add another pipeline traversal.
Persisted-expression admission consumes the same feature result to record v126,
without another expression traversal or a separate copy of the function IDs.
The owner walker skips scalar containers, including `MembershipFilter`,
`LiteralVec.Data`, and `FoldVal.Data`. Ordinary pipelines therefore add only a
constant-time feature check and no destination RPC. A pipeline with VIEWS
functions shares one destination observation with its other expression floors,
including decimal division, but obtains fresh evidence on each send. An unknown
capability or a destination rolled back to v115 still rejects the send. Probe
timeout, cancellation, and response cleanup retain their existing owners; there
is no capability cache across sends.

## Validation

Focused parser/function tests cover current and legacy definitions, quoted and
commented inputs, malformed rows, frozen wildcard expansion, the documented
legacy raw-wildcard boundary, explicit derived-table column lists with inner
alias preservation, and CHECK OPTION.
Protocol tests cover rejection at v104, v105, v106, v108, v109, v110, v111, v112, v113, v114, v115, v116, v117, v118, v119, v120, v121, v122, v123, v124, and v125 and acceptance at v126 at
prepare, sender, and receiver boundaries, including a mixed-version destination
probe and the all-CN capability fence.
Generated ViewData tests cover both function IDs, nested detection, the
authoring/read floor values 0, v104, v105, v106, v108, v109, v110, v111, v112, v113, v114, v115, v116, v117, v118, v119, v120, v121, v122, v123, v124, and v125, and binding the exact generated
metadata at the immediate predecessor and current protocol.
Real CREATE VIEW controls using FORMAT_BYTES and FORMAT_PICO_TIME succeed with
the VIEWS authoring floor closed and persist no protocol marker.
Current account-initialization tests reject old, missing, malformed and
RPC-failing capability results before issuing view DDL. A real SQL recovery
test verifies that failed initialization leaves no account row, the account
name is reusable after capability recovery, and the resulting version and
COLUMNS/TABLES/VIEWS metadata agree. Existing VIEWS reconciliation tests cover
legacy rows and the guarded replacement path. Recovery tests use the real
`initUpgrade` registry to validate an interrupted v4.0.7/offset-zero target with created, tenant-pending,
and already-committed route steps through the startup checker. Handler tests
pin its original protocol-61 requirement and zero offset, while standalone
VIEWS repair tests retain the v126 gate before DROP/CREATE. The predecessor-init
test proves that the restoration target has no function reference before an
older CN is admitted;
the cancellation-after-staging test additionally proves that a cancelled
transaction preserves the legacy marker and page cursor, and that a later
generation can retry and publish only after a committed v126 gate.

Maintenance acceptance additionally covers the broader convergence rule: a
missing VIEWS object and a stale/non-current VIEWS definition are both repair
candidates, while an exact current definition is an idempotent no-op. The
retry, mid-page protocol-loss, cancellation, commit-failure-after-restart, and
late-account tests prove that a failed page publishes neither a partial
replacement nor its cursor; the persisted predecessor definition remains the
recovery marker. Account disappearance is deliberately skipped so that other
accounts in the bounded page can still commit, while replacement and
transaction failures retain page-wide rollback. The service-level ordering is
intentionally visible in logs and return status: a failed VIEWS page is retried
by the periodic owner before the same pass is considered complete, and
orphan-privilege cleanup is skipped on every failed VIEWS pass rather than used
to hide that failure. Once VIEWS succeeds, the same pass proceeds to orphan
cleanup.

### Sender check performance

`BenchmarkViewMetadataSenderProtocol` uses actual protobuf pipeline owners and
measures the existing shared feature analysis plus local and destination gates.
Fixture allocation and protobuf marshaling are outside the timed loop. It covers
ordinary membership-filter, literal-vector, and folded-value byte payloads with
0, 1 MiB, and 4 MiB of data. This isolates compatibility-check work; it is not a
SQL throughput or complete `RemoteRun` benchmark.

Measured on 2026-10-11 with Go 1.27.1, Darwin/arm64, Apple M4, the repository CGo
test wrapper, and the same native artifacts. Each version had one separate
warm-up invocation followed by three measured rounds (`-benchtime=200ms`);
warm-up results are excluded below. The pre-fix measurement at
`7b5db1bce8822469caecd301f9f4b5e57a000874` times the removed
`validateRemoteViewDefinitionDestinationProtocol(nil, p)` with
`p = &pipeline.Pipeline{DataSource: &pipeline.Source{MembershipFilter: payload}}`.
The post-fix measurement includes the shared analysis as well as its sender gates,
so it conservatively includes work that the sender already performed before this
fix. Median membership-filter results across the three measured rounds:

| Payload | Pre-fix duplicate VIEWS walks | Shared analysis and sender gates |
| --- | --- | --- |
| 0 bytes | 1.047 microseconds/op | 0.238 microseconds/op |
| 1 MiB | 6.231 milliseconds/op | 0.301 microseconds/op |
| 4 MiB | 24.962 milliseconds/op | 0.251 microseconds/op |

All post-fix payload variants report 0 B/op and 0 allocs/op; literal-vector and
folded-value medians are 0.333–0.539 microseconds/op across these sizes. The
byte-length-dependent traversal is removed. Reproduce the post-fix protocol with
separate warm-up and measured invocations:

```sh
.agents/skills/mo-dev/scripts/mo-cgo-test -run '^$' \
  -bench '^BenchmarkViewMetadataSenderProtocol$' -benchtime=200ms -count=1 \
  -timeout=120s ./pkg/sql/compile
.agents/skills/mo-dev/scripts/mo-cgo-test -run '^$' \
  -bench '^BenchmarkViewMetadataSenderProtocol$' -benchtime=200ms -count=3 \
  -timeout=120s ./pkg/sql/compile
```

Revalidated on 2026-10-14 after rebasing onto v112 main
`1b66ff135c64ef1c642a297105bf599cd3173dee`
and advancing VIEWS to v113, with the same Go 1.27.1/Darwin/arm64/Apple M4
environment and separate one-round warm-up plus three measured rounds.
Membership-filter medians for 0, 1 MiB, and 4 MiB were respectively 201.5,
201.6, and 196.7 ns/op; literal-vector and folded-value medians ranged from
335.3 to 359.6 ns/op. All nine measured variants still reported 0 B/op and
0 allocs/op. Warm-up samples and first-build time are excluded.

Revalidated again on 2026-10-15 against v113 main
`8c8f829f9d3e9576d3b7371333e0eb1147a008e4` with VIEWS at v114, using the same environment and separate one-round warm-up plus
three measured rounds. Membership-filter medians for 0, 1 MiB, and 4 MiB were
226.9, 229.2, and 240.8 ns/op; literal-vector and folded-value medians ranged
from 412.7 to 434.9 ns/op. All nine measured variants reported 0 B/op and
0 allocs/op; warm-up samples and first-build time are excluded.

Revalidated after rebasing onto v114 main
`493d8b8181b7d74bed91e360874a3dd35223f276` on 2026-10-15, with VIEWS at v115,
using the same Go/platform and separate one-round warm-up plus three measured
rounds. Membership-filter medians for 0, 1 MiB, and 4 MiB were respectively
863.6, 654.3, and 456.6 ns/op; literal-vector and folded-value medians ranged
from 711.3 to 1201.0 ns/op. All nine variants reported 0 B/op and 0 allocs/op
in every measured round. Warm-up samples and first-build time are excluded.
Round-to-round latency varied, so these samples establish the post-rebase cost
shape rather than a controlled absolute-latency comparison with earlier sessions.

Revalidated on 2026-10-16 after rebasing onto v115 main
`99f52528308289e08ab6f07f87b0a54719b877a3`, with VIEWS at v116 and function
IDs 599/600. The same Go/platform and native artifacts were used for one separate
warm-up invocation followed by three measured rounds. Membership-filter medians
for 0, 1 MiB, and 4 MiB were 577.2, 424.2, and 321.7 ns/op;
literal-vector and folded-value medians ranged from 441.6 to 489.7 ns/op.
All nine variants reported 0 B/op and 0 allocs/op in each measured round.
Warm-up samples and first-build time are excluded. These are post-rebase cost
samples, not a controlled absolute-latency comparison with earlier sessions.

Sender regressions assert no probe for ordinary scalar payloads, both registry
identities alone and alongside decimal-division, signed-date-result, DECIMAL statistical-result, or calendar-interval
normalization floors, one released response
per send, unknown-capability rejection, and renewed rejection after a successful
v118 send followed by destination rollback. Placement tests additionally verify
fallback with a v117 worker and remote placement with all-v118 workers. Existing
prepare/receive predecessor tests and the complete plan, compile, and planner
package tests pass. The metadata result/catalog contract and BVT inputs remain
unchanged by this compatibility-check optimization.

## Unresolved questions

None. The independent design PASS for the corrected revision is recorded above,
and the RFC is accepted and in progress. Implementation approval remains a
separate GitHub review state.

Revalidated on 2026-10-16 after rebasing onto v116 main
`76deafc8062f9a7e33a31a4df858ebcdb9477cb5`, with VIEWS at v117 and function
IDs 599/600 unchanged. The actual sender benchmark ran one separate warm-up
invocation (`-count=1`) followed by three measured rounds (`-count=3`), using
Go 1.27.1 on Darwin/arm64 Apple M4, the CGo wrapper, `-p=2`, and
`-benchtime=200ms`. All nine payload variants (27 measured samples) reported
0 B/op and 0 allocs/op. Median ns/op for 0/1 MiB/4 MiB were:

- `membership`: 321.1/343.9/388.1.
- `literal_vec`: 638.5/659.4/649.7.
- `fold`: 672.8/695.8/684.9.

Compilation and warm-up are excluded from these measured samples. Other host
work was concurrent, so these values are not a controlled absolute latency
comparison or a SQL throughput claim. The mixed sender regression also binds
SELECT scalar division alongside each VIEWS identity: division diagnostics keep
the main-owned v116 execution-only floor, VIEWS requires v117 for execution and
persistence, and a v117 success followed by a v116 destination rollback is
rejected with exactly one capability probe and one released response per send.

The final base refresh to `b24d1b311555e0b6f3d56eb37f9a2e9617e9a5b7` adds temporal
window interval parsing and parameter handling without changing MORPC allocation,
function IDs, or the sender's feature walker/gates. Sender benchmark evidence
above is reused for those unchanged inputs; planner/compile and public SQL
validation are refreshed for the window execution and parameter dependencies.

Revalidated on 2026-10-16 after rebasing onto v117 main
`7c61f67e5dc46b0b8e591fe957219308b078bfa4`, with VIEWS at v118 and IDs
599/600 unchanged. One separate sender benchmark warm-up (`-count=1`) preceded
three measured rounds (`-count=3`), using Go 1.27.1 on Darwin/arm64 Apple M4,
the CGo CPU wrapper, `-p=2`, and `-benchtime=200ms`. All nine payload variants
(27 measured samples) reported 0 B/op and 0 allocs/op. Median ns/op for
0/1 MiB/4 MiB were:

- `membership`: 230.7/325.3/472.4.
- `literal_vec`: 1379.0/1057.0/1153.0.
- `fold`: 1121.0/1324.0/905.9.

Compilation and warm-up are excluded; shared-host activity prevents a controlled
absolute latency comparison or SQL throughput claim. The existing sender table
also binds native collation-aware LEAST overload 4 beside each VIEWS identity:
the main-owned collation floor stays v117, VIEWS execution/persistence requires
v118, and success followed by same-endpoint rollback to v117 is rejected with
one probe and one released response per send. Legacy metadata consumer tests
replay the actual exported COLLATE SELECT under binary and native Unicode
collations, asserting opposite comparison results in two rounds on the same CN.
Legacy general-ci retains its historical byte/PAD behavior and is not used as
the native case-folding oracle.

The final base refresh to `b3d6b21fe9d260c91e1777529a48a9d93061af33` adds session/
handshake collation admission without changing MORPC v117, the existing
expression-collation resolver, protocol feature walker/gates, registry IDs, or
benchmark inputs. All 127 rebased patches remain identical. Core, parser,
protocol, benchmark, and static evidence is reused for those unchanged inputs;
the two public SQL roots are refreshed to exercise the updated handshake path.

A subsequent pre-push refresh to `5f9a8c313813d7e1f89c39c5e78f182fa8ea4551` adds
schema-qualified ALTER VIEW targeting and INSERT VALUES subquery admission,
without changing MORPC allocation or the sender feature/probe contract. All 127
patches remain identical. Planner/compile owning-package tests and the two public
SQL roots are refreshed for these binder/DDL dependencies; parser/function,
maintenance, benchmark, and static evidence retains its unchanged semantic inputs.

The last base refresh to `14a8d854b0f9346065ff8432eb979e35f32f50d8` only
formats sequence functions in EXPLAIN group keys. All 127 PR patches remain
identical; the EXPLAIN package normal suite is refreshed. Protocol allocation,
metadata execution, parser, binder, and sender benchmark inputs are unchanged,
so the preceding validation results remain applicable.

The 2026-10-17 base refresh keeps MORPC v117 and VIEWS v118, while main
allocates ID 599 to the asynchronous datalink admission function. VIEWS moves
to IDs 600/601. The shared protobuf identity table rejects 599 as a VIEWS
capability and recognizes both new identities; function registration, persisted
read/authoring, Prepare, local/remote receivers, and sender placement keep the
v118 floor, including immediate-predecessor v117 rejection. Historical
measurements above retain their original function identities and revisions.

At these new IDs, the sender benchmark ran one separate warm-up followed by
three measured rounds (Go 1.27.1, Darwin/arm64 Apple M4, CGo CPU wrapper,
`-p=2`, `-benchtime=200ms`). All nine variants and 27 measured samples reported
0 B/op and 0 allocs/op. This refresh checks the new identities; shared-host
activity prevents a controlled absolute latency or SQL throughput claim.


The subsequent 2026-10-17 refresh rebases onto official main
`60aa668db26c3e1f3e1d100fac5610a669c841bb`. Main owns MORPC v118 and
INTERNAL_TABLE_COLLATION ID 600; VIEWS advances to v119 and IDs 601/602.
Shared feature detection distinguishes the v118 TABLES function from both VIEWS
functions. Persisted read/authoring, Prepare, sender placement and receiver tests
include immediate-predecessor v118 rejection. At that historical revision,
tenant initialization retained v118 TABLES/SCHEMATA when their local fence and
peers were ready, while VIEWS waited for v119; unknown or failed capability probes kept the conservative
fallback. The current account-initialization contract above supersedes that
behavior. System bootstrap preserves the database-default prerequisites even
with its authoring fence closed and defers the TABLES function to main's
existing post-admission publication phase.

On the combined tree, the merged SQL grammar was regenerated and compared.
Normal tests passed in 12 owning/dependency packages, including the new v4.0.15
upgrade owner; 23 targeted frontend/bootstrap roots and the explicit bootstrap
and placement boundary controls passed. Both public SQL integration roots
passed their two rounds on the same CN with teardown checked between rounds.
The sender benchmark executed one separate warm-up and three measured rounds
at the new allocation (Go 1.27.1, Darwin/arm64, CGo CPU wrapper, GOMAXPROCS=2,
`-p=2`, `-benchtime=200ms`): all nine variants and 27 measured samples reported
0 B/op and 0 allocs/op. Shared-host activity prevents an absolute latency or SQL
throughput claim. Vet and golangci-lint passed for the 16 affected packages.
Historical measurements above retain their original inputs and revisions.


The final pre-push refresh to official main
`b111224b0c624773389b390cd3cf1223126b6ee2` keeps MORPC v118 and function
end marker 601. All 129 PR patches remain identical. Main changes pipeline
error ownership, catalog refresh-cause classification, and shared test-fixture
cleanup. The compile normal suite, 34 CN admission roots, and both public SQL
roots (with their same-CN two-round checks) passed again on this combined tree;
the integration cleanup deadline control also passed. Other parser, registry,
persisted metadata, bootstrap, and sender benchmark evidence above is retained
for its unchanged semantic inputs.


The 2026-10-19 refresh rebases onto official main
`75d16a2b875905ffa4121783dcda4f538d3c05f7`. Main's parameterized subquery
programs retain MORPC v119; VIEWS advances to v120 with IDs 601/602 unchanged.
The shared feature detector retains both capabilities independently. Controls
using actual dependent/slot expressions verify that the v119 feature remains
visible without being classified as VIEWS. Persisted read/authoring, Prepare,
sender placement, receivers and catalog upgrades reject immediate-predecessor
v119 for VIEWS while keeping main's independent v118 TABLES/COLUMNS contracts.
Main's capability-probe error propagation is preserved by the caller-context
wrapper; optional placement and best-effort tenant discovery retain their
separate failure policies.

The merged parser exactly matches regeneration. Normal suites passed in 14
owning/dependency packages, including the new v4.0.16 collation upgrade, followed
by 23 targeted frontend/bootstrap roots, 34 CN admission roots and both public
SQL roots with two rounds on the same CN and verified teardown. The actual
sender benchmark ran one separate warm-up and three measured rounds (Go 1.27.1,
Darwin/arm64, CGo CPU wrapper, `matrixone_test`, GOMAXPROCS=2, `-p=2`,
`-benchtime=200ms`). All nine variants and 27 measured samples report 0 B/op and
0 allocs/op. Shared-host activity prevents an absolute latency or SQL throughput
claim. Vet and golangci-lint passed for the 16 affected/consumer packages.

A subsequent 2026-10-19 refresh rebases onto official main
`8ef2eaf4d923bdb4136dd81a50850e8eced0f368`. Main retains KILL capability v120;
VIEWS uses v121 with unchanged IDs 601/602. Immediate-predecessor v120 controls
cover persisted read/authoring, Prepare, placement, send/receive and catalog
admission. Normal suites passed in 14 packages (8,189 roots), plus 23
frontend/bootstrap roots, 34 CN admission roots, the forward COLUMNS upgrade
and both public SQL roots with same-CN repetition and teardown checks. The
parser inputs and sender benchmark/feature-walk bodies are unchanged, so the
regeneration and separate warm-up/three-round measurements above remain valid.
Incremental vet and lint passed for the 16 affected/consumer packages.

The next 2026-10-19 refresh uses official main
`873282742714436bff407dfcdbbe879147162bd3`: DECIMAL DIV keeps v121 and
VIEWS advances to v122, with IDs 601/602 unchanged. Immediate-predecessor v121
controls cover persisted read/authoring, Prepare, placement, send/receive and
catalog admission. The two NAME_CONST regressions were reproduced before the
fix: legacy implicit output names changed, and a backslash-containing text
value exported an explicit cast that normal NAME_CONST binding rejected.
Shared heading rules and export-only literal formatting now close both cases.
Planner and real SQL CREATE/export/replay checks compare names, bytes, character
sets and collations for current and Stmt-only metadata, including parentheses,
COLLATE, derived tables, binary values and escaped Unicode text. Normal invalid
NAME_CONST argument controls remain passing.

On this combined tree, normal suites passed in 14 packages (8,203 roots), plus
24 frontend/bootstrap roots, 34 CN admission roots and both public SQL roots
with same-CN repetition and teardown. The corrected SQL row-header helper was
rerun after adding its terminal rows.Err check. Vet and lint passed for the 15
affected/consumer packages. The sender benchmark refreshed its shared detector
inputs with one separate warm-up and three measured rounds (Go 1.27.1,
Darwin/arm64, CGo CPU wrapper, `matrixone_test`, GOMAXPROCS=2, `-p=2`,
`-benchtime=200ms`). All nine variants and 27 measured samples report 0 B/op and
0 allocs/op. Shared-host activity prevents an absolute latency or SQL throughput
claim; real multi-CN rolling-upgrade fault injection is not claimed.

The subsequent nested-output-name repair reuses resolved names for all current
query blocks and moves legacy heading preservation into export formatting.
Before the fix, the planner replay matrix reproduced missing binary-literal,
qualified-arithmetic and escaped-NAME_CONST names; explicit aliases were
passing controls. The final matrix covers derived and explicit column lists,
CTEs, UNION, deeper nesting, scalar/filter subqueries and both replay SQL modes.
Existing stable-column/integer SQL-shape checks remain unchanged and pass.
Normal tests passed across the six parser/function/planner/explain packages
(6,514 roots). Both public SQL roots passed, including two cleanup-checked
rounds on the same CN comparing current and Stmt-only replay names, bytes,
charset and collation. That owning integration package took 7.383 seconds on
this shared Darwin/arm64 host; this is fixture execution evidence, not a speed
claim. Incremental vet and lint passed for seven affected/consumer packages.
No protocol or sender-feature inputs changed, so the preceding v122 allocation
and separate warm-up/three-round sender measurements remain applicable.

The following refresh rebases all 133 PR commits onto official main
`d82e828b0a4df9ce635fe573c1e90c5cde978bf9`. Main's unary TIMESTAMP and
dynamic TIME result contracts keep v122; VIEWS advances to v123 with IDs
601/602 unchanged. The main temporal cached/CTAS admission function and
generated protobuf definitions are preserved exactly. Immediate-predecessor
v122 controls reject VIEWS across persisted read/authoring, Prepare, placement,
send/receive, tenant initialization and catalog upgrade. A mixed TIMESTAMP
and VIEWS sender fixture retains both feature flags and takes the maximum
requirement v123; main's standalone temporal controls still use v122.

On this combined tree, normal suites passed in 15 owning/dependency packages
(8,368 roots), followed by 24 frontend/bootstrap roots, 34 CN admission roots
and both public SQL roots with two cleanup-checked rounds on the same CN.
Vet and lint passed for 17 affected/consumer packages. The sender benchmark
was refreshed for main's changed feature-detector inputs: one separate warm-up
and three measured rounds (Go 1.27.1, Darwin/arm64, CGo CPU wrapper,
`matrixone_test`, GOMAXPROCS=2, `-p=2`, `-benchtime=200ms`). All nine variants
and 27 measured samples report 0 B/op and 0 allocs/op. Shared-host activity
prevents an absolute latency or SQL throughput claim; no real mixed-binary
rolling-upgrade fault injection is claimed.

The final pre-push refresh uses official main
`090b14fbb4c288819728f628e009311a11812e1f`. Its only additional change counts
confirmed local shard replicas; the capability allocation remains main v122
and VIEWS v123. All 134 PR patches rebase unchanged. The two local-replica
predicate roots, 34 CN admission roots and both public SQL roots passed on
this final combined tree. The unchanged metadata/protocol/feature-detector
inputs retain the complete suites, static checks and sender measurements
above; no new performance or topology claim is inferred from the refresh.

### 2026-10-20 nonblocking-lock protocol rebase

Rebased onto official main `c7bd2e7c1d9ca53daf80d640b3b307902d54161c`.
Main owns nonblocking row-lock capability v123; VIEWS uses v124. All 601
existing main function IDs remain unchanged, VIEWS retains IDs 601/602, and
its end marker remains 603. Of the 137 rebased patches, only the protocol
constant conflict required resolution; the other patches retain their changes.
Sender, receiver, Prepare, persisted read/authoring, tenant initialization and
upgrade gates now agree on v124. Immediate-predecessor v123 controls reject
VIEWS functions, preserve legacy VIEWS alongside native COLUMNS/TABLES, and
roll back a staged maintenance page without advancing its cursor.

Normal owning-package validation passed 10,625 top-level tests across 11
packages. One existing frontend root and two existing planner subcases remain
skipped; none belongs to these protocol controls. The final bootstrap/sysview
control changes additionally passed their 110 owning-package roots. The real
SQL replay case passed both rounds on one CN, including long escaped literals,
column types and cleanup; the real V406 metadata catalog upgrade also passed.
Incremental vet and lint passed for all 15 PR Go packages, with the last two
test-only package edits checked again. No sender traversal, payload analysis,
fixture ownership or synchronization algorithm changed; preceding performance
and lifecycle evidence is reused for those unchanged contracts. This record
claims no fresh mixed-binary deployment or throughput measurement.

### 2026-10-20 fractional TIMESTAMPADD protocol/identity rebase

Official main `743e749a1e33d6c845bb73999e8c82d297a2b657` owns MORPC v124
and TIMESTAMPADD_SECOND_INTERVAL ID 601. VIEWS advances to v125 and IDs
602/603, with end marker 604. All 602 existing main function IDs are preserved.
The shared protobuf identity table distinguishes the new normalizer from both
VIEWS functions. Mixed TIMESTAMPADD/VIEWS sender cases use one probe and the
maximum v125 floor; main's standalone normalizer still uses v124. Prepare,
receiver, persisted read/authoring, tenant initialization, upgrades, and staged
repair rollback include the immediate predecessor v124 controls.

Normal validation passed 10,385 top-level tests across nine owning
packages, plus both real SQL integration/upgrade roots. Three existing skipped
cases remain outside this allocation change. The initial combined command
passed eight packages but exceeded its local 300-second planner-package guard
while TableDumpDefaultTimeZones was advancing through rejection cases. Complete
logs and stacks were retained. The full planner package then passed all 3,962
top-level roots in 190.473 seconds with a 1,200-second outer guard and CPU
profiling; no assertion, internal candidate budget, or product code was weakened.
The profile places 70.46% of CPU samples under AnalyzeTableDumpBindings and
69.17% under its bounded timezone enumeration, including timezone file reads.
This is a completed local validation follow-up, not a CI-flaky or pre-existing
product-defect classification. Incremental vet/lint passed for all 15 PR Go
packages. Sender traversal and lifecycle algorithms are unchanged, so their
prior warm-up/three-round benchmark and race evidence remain scoped to those
unchanged contracts; no fresh mixed-binary or SQL-throughput claim is made.


### 2026-10-21 LOAD IGNORE protocol/identity rebase

Official main `63a964a82fbaae4759cfabb45655e0185bc0979c` owns MORPC v125
and LOAD_FK_IGNORE_CHECK ID 602. VIEWS advances to v126 and IDs 603/604,
with end marker 605; all 603 existing main identities remain unchanged. Shared
feature analysis keeps LOAD admission at v125 and VIEWS at v126. Sender,
receiver, Prepare, persisted authoring/read, tenant initialization and
maintenance gates agree on the new floor. Immediate-predecessor v125 controls
reject VIEWS while retaining the existing COLUMNS/TABLES contracts. Mixed LOAD
and VIEWS sender tests require the maximum v126 floor without duplicating
feature traversal. Historical validation above retains its original revisions.

At the first v126 validation revision, owning bootstrap, v4.0.6/v4.0.7,
protobuf feature detection, compile, sysview and tree suites passed, along with
view protocol/replay and frontend/function consumer selections and both real
SQL integration roots. Bootstrap passed under race; mid-page protocol loss,
concurrent-call serialization and commit-failure/restart each passed 100
race repetitions. Vet and incremental lint passed.

The final base refresh includes official main
`a648b13e53cf0a07ff19211203a69e37b2d1dd93`, whose derived View planning
consumer is opt-in. All 141 PR patches replayed without semantic conflict;
MORPC v125 and function end 603 on main remain unchanged, so VIEWS keeps v126
and IDs 603/604. The ordinary and scoped View/CTE planning consumers are
revalidated after this base-side change; historical receipts remain pinned
to their original validation revisions.

On that refreshed base, all 4,030 planner roots and 41 frontend consumer roots
passed, as did both `integration`-tagged native ViewSchema roots and the two
public VIEWS repair/replay roots. Plan/frontend vet and incremental lint passed.
The first public rerun stopped before SQL because a foreign `issues.test`
process held port 10060; the rerun using the existing port-lease coordination
selected 11060 and passed. No product or harness workaround was added.
