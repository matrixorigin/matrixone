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
are new distributed plan functions (IDs 598 and 599), so the catalog contract is fenced by MORPC
v113.

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
planner writes `required_protocol_version: 113` into `ViewData`, including when
the call is nested under another expression. The real view bind/Prepare path
reapplies that marker. It uses the existing two-floor admission lifecycle:
HAKeeper publishes the durable read floor during the v113 decoder barrier, while
the authoring floor remains closed until the all-CN admission and catalog fence
complete. Therefore an authoring or read floor of 0, v104, v105, v106, v108, v109, v110, v111, or v112 fails
closed, an all-v113 fenced CN can create the VIEWS definition, and an ordinary view
without either function remains unmarked.

The fallback cannot infer metadata that is absent from a legacy row. In
particular, it does not promise to reconstruct the historical expansion of a
wildcard; the authoritative freeze begins when the additive fields are written
by CREATE/ALTER or by the bounded regeneration path. Adding a historical column
snapshot to the legacy catalog format would be a separate compatibility
migration and is outside this PR.

The post-upgrade reconciliation uses the exact current
`InformationSchemaViewsDDL` as its idempotence marker. After the common v113 gate
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
folding must not turn a stale definition into an installed marker. Both the
upgrade entry and periodic maintenance use this same probe, so a committed
repair is a no-op on every later pass, regardless of catalog name casing.

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
cleanup. This serial ordering keeps a pass that failed to establish the v113
public catalog contract from being reported as fully successful, while the
bounded VIEWS page remains independently rollback-safe.

MORPC v113 is allocated as `MORPCLatestVersion + 1` from official main v112 at
`1b66ff135c64ef1c642a297105bf599cd3173dee`. Main owns the v112 signed
WEEK/DAYOFYEAR result contract and the v111 IndexSearchScan contract. This
rebase advances the candidate VIEWS capability from v112 to v113; the historical
design assessment above retains the version used by that reviewed revision.
The independent v110 MEDIUMINT and v109 COLUMNS contracts remain unchanged.
Main's hyperbolic functions retain IDs 591–595, and FORMAT_BYTES and
FORMAT_PICO_TIME retain IDs 596 and 597. The current main function end
marker is 598; the two VIEWS function IDs are 598 and 599, and the end
marker advances to 600. These IDs are allocated from the rebased official main and are
distinct from all existing function registrations.
The capability is specific to these functions and the persisted VIEWS definition.
A sender probes the selected destination CN as well as its local runtime before
encoding a pipeline containing either function ID; an unknown or unavailable
destination capability fails closed. The v4.0.6 VIEWS upgrade waits for common
v113. New tenant initialization installs the new VIEWS DDL only after the local
coordinator and every CN in the current inventory have positively confirmed v113;
a mixed, unknown, RPC-failing, or incomplete capability probe records the
predecessor VIEWS definition while still committing the final-version tenant
row. A bounded post-upgrade reconciliation pass later rechecks common v113 and
reuses the guarded transactional entry to converge missing or stale VIEWS
definitions to the current contract. Any cluster with a CN below v113, including
the predecessors v104, v105, v106, v108, v109, v110, v111, and immediate predecessor v112, preserves the predecessor VIEWS
definition while retaining each existing protocol-specific metadata contract,
including the independent v100 and v109 COLUMNS contracts. A local v113 CN
that defers VIEWS uses the VIEWS DDL of the immediate v112 predecessor, preserving native Unicode
collation metadata. COLUMNS installation still requires a positive common-v100
or common-v109 probe according to its selected definition; unknown local runtime versions retain
the minimum supported catalog contract. Pipeline preparation, remote marshal, and
remote unmarshal reject a
pipeline containing either function ID below v113. The receiver check protects
stale prepared work as well as normal sender dispatch. Before the v113 HAKeeper
protocol-floor activation is durably committed, a cancelled rollout may stop
the v113 upgrade, keep or restore `InformationSchemaViewsLegacyDDL`
transactionally, wait for catalog and in-flight work to converge, and admit
v112 only after verifying that no v113 maintenance worker can re-install the
new definition. After the v113 floor is committed, the floor is monotonic and
v104/v105/v106/v108/v109/v110/v111/v112 binaries must not be admitted: restoring the view text alone cannot
undo the decoder barrier. The supported recovery is forward recovery with v113-
compatible log/CN services, or a coordinated restoration of cluster state from
before the activation barrier; an ordinary in-place downgrade is unsupported.
Merely draining below-v113 requests is not sufficient because the new persisted
view text references the functions. The new JSON fields are additive and old
binaries keep treating them as unknown.

## Alternatives

Keeping raw SQL regexp extraction was rejected because it repeatedly diverged
from the SQL lexer for comments and quoted strings. Eagerly rewriting every
legacy row was rejected because the existing recovery lifecycle is deliberately
inactive and a metadata read must not perform unbounded catalog writes. Allowing
the DDL before v113 was rejected because an old CN cannot bind the metadata functions.

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
Persisted-expression admission consumes the same feature result to record v113,
without another expression traversal or a separate copy of the function IDs.
The owner walker skips scalar containers, including `MembershipFilter`,
`LiteralVec.Data`, and `FoldVal.Data`. Ordinary pipelines therefore add only a
constant-time feature check and no destination RPC. A pipeline with VIEWS
functions shares one destination observation with its other expression floors,
including decimal division, but obtains fresh evidence on each send. An unknown
capability or a destination rolled back to v112 still rejects the send. Probe
timeout, cancellation, and response cleanup retain their existing owners; there
is no capability cache across sends.

## Validation

Focused parser/function tests cover current and legacy definitions, quoted and
commented inputs, malformed rows, frozen wildcard expansion, the documented
legacy raw-wildcard boundary, explicit derived-table column lists with inner
alias preservation, and CHECK OPTION.
Protocol tests cover rejection at v104, v105, v106, v108, v109, v110, v111, and v112 and acceptance at v113 at
prepare, sender, and receiver boundaries, including a mixed-version destination
probe and the all-CN capability fence.
Generated ViewData tests cover both function IDs, nested detection, the
authoring/read floor values 0, v104, v105, v106, v108, v109, v110, v111, and v112, and binding the exact generated
metadata at the immediate predecessor and current protocol.
Real CREATE VIEW controls using FORMAT_BYTES and FORMAT_PICO_TIME succeed with
the VIEWS authoring floor closed and persist no protocol marker.
System-view tests prove mixed, unknown, and RPC-failing CN capability probes
fall back to the predecessor VIEWS DDL, while an all-v113 inventory uses the
parser-derived DDL. Tenant initialization keeps the final account version but
records the predecessor VIEWS definition when capability discovery is
incomplete. The post-upgrade bounded reconciliation pass rediscovers that
durable definition marker, retries only after a positive all-CN v113 check, and
uses the same guarded transactional entry to publish the parser-derived
definition once it is safe. Upgrade tests prove both the v4.0.7 handler and
its VIEWS entry require v113. The predecessor-init test proves that the
restoration target has no function reference before an older CN is admitted;
the cancellation-after-staging test additionally proves that a cancelled
transaction preserves the legacy marker and page cursor, and that a later
generation can retry and publish only after a committed v113 gate.

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

Revalidated on 2026-10-14 after rebasing onto the v112 main revision above
and advancing VIEWS to v113, with the same Go 1.27.1/Darwin/arm64/Apple M4
environment and separate one-round warm-up plus three measured rounds.
Membership-filter medians for 0, 1 MiB, and 4 MiB were respectively 201.5,
201.6, and 196.7 ns/op; literal-vector and folded-value medians ranged from
335.3 to 359.6 ns/op. All nine measured variants still reported 0 B/op and
0 allocs/op. Warm-up samples and first-build time are excluded.

Sender regressions assert no probe for ordinary scalar payloads, both registry
identities alone and alongside decimal-division or signed-date-result floors, one released response
per send, unknown-capability rejection, and renewed rejection after a successful
v113 send followed by destination rollback. Placement tests additionally verify
fallback with a v112 worker and remote placement with all-v113 workers. Existing
prepare/receive predecessor tests and the complete plan, compile, and planner
package tests pass. The metadata result/catalog contract and BVT inputs remain
unchanged by this compatibility-check optimization.

## Unresolved questions

None. The independent design PASS for the corrected revision is recorded above,
and the RFC is accepted and in progress. Implementation approval remains a
separate GitHub review state.
