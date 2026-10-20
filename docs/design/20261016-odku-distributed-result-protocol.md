# Distributed ODKU Result Protocol

Current implementation alignment: [r2 addendum](20261016-odku-distributed-result-protocol-r2-addendum.md), with the subsequent candidate-allocation overlay recorded there.
Against actual main `743e749a1e33d6c845bb73999e8c82d297a2b657` (capability prefix
through MORPC124), the isolated candidate retains main's MORPC123 row-lock and
MORPC124 fractional TIMESTAMPADD SECOND contracts and provisionally assigns JSON
ELT/opaque text to MORPC125, typed JSON MIN/MAX to MORPC126, and the complete
ODKU/session-baseline contract to MORPC127. Independent unmerged branches also
use provisional MORPC125; these assignments do not establish cross-branch
deployment compatibility and must be revalidated at authoritative main admission.
The r2 approval remains scoped to its earlier reviewed allocation; this overlay
does not claim new design approval.

The r1 specification below is retained as historical design text. Its normative
references to MORPC119 describe the original inspected/reviewed revision, not
the current allocation. Use the r2 addendum for the changed boundaries. The
original reviewed content remains addressable by its immutable blob and head
below. The v1 approval record is unchanged and applies only to that reviewed r1
content; **r2 independent review is pending**, with no new approval asserted.

## Retained r1 specification

Status: r1 approved by independent design review v1; see `20261016-odku-distributed-result-protocol-review-v1.md`

- Design revision: r1, 2026-10-16.
- Owner: VioletQwQ-0.
- Owning issue: [#28162](https://github.com/matrixorigin/matrixone/issues/28162).
- Implementation: [PR #28444](https://github.com/matrixorigin/matrixone/pull/28444).
- Inspected implementation revision: `286b60837041d819573a260f7ab6e346125c6d3a`.
- Design gate: [mo-bot assessment at the inspected revision](https://github.com/matrixorigin/matrixone/pull/28444#pullrequestreview-5543546119).
- Reviewed design revision: document blob `914d5ddd19635d07924ebbe22cbb5eaa6f192fa2`
  at head `038207ff2e5fd84d79d7d6c851c467b68b05a4e8`.
- Approval record: PASS with no conditions in
  `20261016-odku-distributed-result-protocol-review-v1.md`, bound to document
  SHA256 `ddff85b82ca7775ce492d68fb904a3bd5952c59e4f6390a8733ddf347953e756`.

## Background / Problem

An ODKU statement can allocate an AUTO_INCREMENT candidate, resolve a duplicate
against a different stored row, perform a changed update or a no-op, and finish
on a different CN from its input producer. Allocation order, numerical ID order,
RPC completion order, and logical input order are different orders. Taking a
minimum generated ID or the last remote reply cannot reliably identify the
client-visible result.

The contract has two consumers: the insert ID in the successful MySQL OK packet,
and the session value read by a subsequent `LAST_INSERT_ID()`. An update fallback
may populate the packet without changing that session value. A failed attempt
must not populate the next attempt's result. Generated candidates consumed by
duplicate updates and hidden fake-primary-key allocation are not successful
user-column inserts.

The existing [ordered ODKU design](20260906-ordered-odku-semantics.md) describes
action ordering, affected rows, constraints, and physical writes. This document
adds the distinct placement, provenance, reduction, and version contract. It is
a design proposal informed by existing code, not retrospective approval of that
code. Implementation acceptance remains blocked on independent design approval.

## Non-goals

- Change ODKU target arbitration, left-to-right assignments, affected-row rules,
  constraint enforcement, index maintenance, storage commit, or transaction retry policy.
- Make AUTO_INCREMENT allocation transactional or reclaim gaps after rollback.
- Define a deterministic SQL order for an unordered INSERT SELECT or tied ORDER
  BY keys. Require a total ORDER BY when applications need repeatable order.
- Add a durable result service, a new catalog format, a per-row result RPC, a
  new background worker, or a general distributed sequence allocator.
- Redesign side effects of `LAST_INSERT_ID(expr)`, multi-table INSERT, ordinary
  INSERT/REPLACE reporting, or unrelated expression protocols.

## Current behavior and source binding

All source references below are `file:line` at the inspected implementation
revision. The links are immutable. They identify existing behavior and review
entry points; they do not assert test execution or implementation approval.

| Contract | Source anchor |
| --- | --- |
| Gather tracked input before PRE_INSERT; recognize one coordinator stream | [pkg/sql/compile/compile.go:9196](https://github.com/matrixorigin/matrixone/blob/286b60837041d819573a260f7ab6e346125c6d3a/pkg/sql/compile/compile.go#L9196), especially :9201; [same file:6090](https://github.com/matrixorigin/matrixone/blob/286b60837041d819573a260f7ab6e346125c6d3a/pkg/sql/compile/compile.go#L6090) |
| Single-worker merge and bounded receiver construction | [pkg/sql/compile/compile.go:9864](https://github.com/matrixorigin/matrixone/blob/286b60837041d819573a260f7ab6e346125c6d3a/pkg/sql/compile/compile.go#L9864) |
| Shared process owner and child aliasing | [pkg/vm/process/process2.go:94](https://github.com/matrixorigin/matrixone/blob/286b60837041d819573a260f7ab6e346125c6d3a/pkg/vm/process/process2.go#L94), :158 |
| Reduced facts and statement state | [pkg/vm/process/types.go:571](https://github.com/matrixorigin/matrixone/blob/286b60837041d819573a260f7ab6e346125c6d3a/pkg/vm/process/types.go#L571), :636 |
| Reset, checked range reservation, generation fence, packet selection | [pkg/vm/process/types.go:958](https://github.com/matrixorigin/matrixone/blob/286b60837041d819573a260f7ab6e346125c6d3a/pkg/vm/process/types.go#L958), :984, :1008, :1085, :1102 |
| Capture provenance before allocation; emit hidden vectors | [pkg/sql/colexec/preinsert/preinsert.go:301](https://github.com/matrixorigin/matrixone/blob/286b60837041d819573a260f7ab6e346125c6d3a/pkg/sql/colexec/preinsert/preinsert.go#L301), :360, :425 |
| Action-owner reduction and transfer | [pkg/sql/colexec/dedupjoin/join.go:1754](https://github.com/matrixorigin/matrixone/blob/286b60837041d819573a260f7ab6e346125c6d3a/pkg/sql/colexec/dedupjoin/join.go#L1754), :1781, :1788, :1829 |
| Transient operator cleanup | [pkg/sql/colexec/preinsert/types.go:103](https://github.com/matrixorigin/matrixone/blob/286b60837041d819573a260f7ab6e346125c6d3a/pkg/sql/colexec/preinsert/types.go#L103); [pkg/sql/colexec/dedupjoin/types.go:481](https://github.com/matrixorigin/matrixone/blob/286b60837041d819573a260f7ab6e346125c6d3a/pkg/sql/colexec/dedupjoin/types.go#L481), :526 |
| Protocol version and plan / pipeline metadata | [pkg/defines/const.go:157](https://github.com/matrixorigin/matrixone/blob/286b60837041d819573a260f7ab6e346125c6d3a/pkg/defines/const.go#L157); [proto/plan.proto:778](https://github.com/matrixorigin/matrixone/blob/286b60837041d819573a260f7ab6e346125c6d3a/proto/plan.proto#L778), :1573; [proto/pipeline.proto:237](https://github.com/matrixorigin/matrixone/blob/286b60837041d819573a260f7ab6e346125c6d3a/proto/pipeline.proto#L237), :366, :782, :822 |
| Encode / decode tracking fields and reject missing baseline | [pkg/sql/compile/remoterun.go:804](https://github.com/matrixorigin/matrixone/blob/286b60837041d819573a260f7ab6e346125c6d3a/pkg/sql/compile/remoterun.go#L804), :1159, :1428, :1853, :2618 |
| Actual process baseline encoding / decoding | [pkg/vm/process/process_codec.go:183](https://github.com/matrixorigin/matrixone/blob/286b60837041d819573a260f7ab6e346125c6d3a/pkg/vm/process/process_codec.go#L183), :506 |
| Source / destination capability and actual-stream floor | [pkg/sql/compile/remoterun.go:140](https://github.com/matrixorigin/matrixone/blob/286b60837041d819573a260f7ab6e346125c6d3a/pkg/sql/compile/remoterun.go#L140), :2361; [pkg/sql/compile/remoterunClient.go:120](https://github.com/matrixorigin/matrixone/blob/286b60837041d819573a260f7ab6e346125c6d3a/pkg/sql/compile/remoterunClient.go#L120), :655; [pkg/sql/compile/remoterunServer.go:211](https://github.com/matrixorigin/matrixone/blob/286b60837041d819573a260f7ab6e346125c6d3a/pkg/sql/compile/remoterunServer.go#L211) |
| Producer-only remote range reservation | [pkg/sql/compile/remoterunClient.go:297](https://github.com/matrixorigin/matrixone/blob/286b60837041d819573a260f7ab6e346125c6d3a/pkg/sql/compile/remoterunClient.go#L297), :324 |
| Terminal JSON schema, server success fence, client reduction | [pkg/sql/compile/resource_accounting.go:55](https://github.com/matrixorigin/matrixone/blob/286b60837041d819573a260f7ab6e346125c6d3a/pkg/sql/compile/resource_accounting.go#L55), :74; [pkg/sql/compile/remoterunServer.go:489](https://github.com/matrixorigin/matrixone/blob/286b60837041d819573a260f7ab6e346125c6d3a/pkg/sql/compile/remoterunServer.go#L489), :1015, :1370; [pkg/sql/compile/remoterunClient.go:767](https://github.com/matrixorigin/matrixone/blob/286b60837041d819573a260f7ab6e346125c6d3a/pkg/sql/compile/remoterunClient.go#L767), :1052 |
| Retry baseline and generation reset; frontend publication | [pkg/sql/compile/compile2.go:1094](https://github.com/matrixorigin/matrixone/blob/286b60837041d819573a260f7ab6e346125c6d3a/pkg/sql/compile/compile2.go#L1094), :1124; [pkg/frontend/mysql_cmd_executor.go:6406](https://github.com/matrixorigin/matrixone/blob/286b60837041d819573a260f7ab6e346125c6d3a/pkg/frontend/mysql_cmd_executor.go#L6406), :6422; [pkg/frontend/status_stmt.go:420](https://github.com/matrixorigin/matrixone/blob/286b60837041d819573a260f7ab6e346125c6d3a/pkg/frontend/status_stmt.go#L420) |
| Cached / persisted semantic floors | [pkg/sql/compile/expression_protocol.go:122](https://github.com/matrixorigin/matrixone/blob/286b60837041d819573a260f7ab6e346125c6d3a/pkg/sql/compile/expression_protocol.go#L122); [pkg/sql/plan/persisted_ip.go:144](https://github.com/matrixorigin/matrixone/blob/286b60837041d819573a260f7ab6e346125c6d3a/pkg/sql/plan/persisted_ip.go#L144) |

At this revision the compiler gathers tracked PRE_INSERT input if there is more
than one scope, more than one worker, or a non-coordinator location. It retains
an existing single coordinator worker. PRE_INSERT emits generated/not-generated
and ordinal provenance; the main-table DEDUP_JOIN reduces action results.
A successful remote terminal returns its reduced facts through the existing
analysis envelope. Frontend status handling chooses the ODKU packet value and
changes the session only for a successful generated result.

## Proposed architecture

### Coordinator and CN roles

The initiating coordinator owns the statement-attempt result and is the only
publisher to the client/session. For tracked input it establishes one PRE_INSERT
stream before parallel deduplication. Upstream scans and downstream eligible
operators may still run remotely; this proposal does not gather the entire
statement into a retained row cache.

```text
source streams -> existing global ordering, when required
               -> coordinator gather / one PRE_INSERT
               -> rows + generated marker + input ordinal
               -> local or remote main-table DEDUP_JOIN action owners
               -> constant-size local summaries / successful remote terminals
               -> initiating attempt's reducer -> successful frontend status
```

A CN executing a remote fragment owns its local process, operator buffers,
local summary, and terminal response. Nested remote summaries reduce into that
CN's local summary before it sends its own terminal. Its summary is intermediate
state, not a client publication or a storage commit acknowledgment.

### Messages and fields

The wire contract uses existing PipelineMessage / ProcessInfo transport plus
optional terminal JSON. MORPCVersion125 is the semantic floor; the independent
`terminal_resource_version = 4` describes resource accounting, not an ODKU
protocol version.

| Carrier | Fields and meaning |
| --- | --- |
| Plan PreInsertCtx | `track_odku_result = 15`, `odku_ordinal_column = 16`, `odku_auto_increment_column = 17`; existing generated marker fields 13/14 identify provenance output. |
| Pipeline PreInsert | `track_odku_result = 19`, `odku_ordinal_column = 20`, `odku_auto_increment_column = 21`; generated marker fields 17/18. Physical positions must survive remapping and cloning. |
| Plan DedupJoinCtx | Column refs 13–16 identify stored target ID, generated marker, ordinal, and incoming user AUTO_INCREMENT ID. Only the main-table action owner tracks results. |
| Pipeline DedupJoin | `odku_target_auto_increment_col = 30`, `odku_generated_col = 31`, `odku_ordinal_col = 32`, `odku_result_tracking = 33`, `odku_generated_auto_increment_col = 34`. |
| ProcessInfo | `odku_ordinal_base = 18` reserves a remote producer range; zero is a valid base. Existing `remote_execution_id = 12` identifies a physical attempt for the surrounding execution protocol. |
| SessionInfo | `last_insert_id = 23`, `last_insert_id_set = 24` carry the actual serialized process's captured baseline. The presence bit is mandatory for tracked remote ODKU, including a zero baseline. |
| PipelineProtocolCheck | Stream ID, method, status, and protocol version confirm support on the exact stream before sending executable pipeline data. |
| Terminal envelope | Optional `odku_result` object with the nine scalar fields below; absent when there are no result facts or the remote attempt fails. |

The JSON object's exact field names are:

```text
has_generated, first_generated_ordinal, first_generated_id
has_action, last_action_ordinal, last_action_id
has_successful_action, last_successful_ordinal, last_successful_id
```

Each `has_*` is a bool; each ordinal/ID is an unsigned 64-bit integer. Flags,
not nonzero IDs, determine validity. Consumers must preserve full uint64
precision rather than route these JSON numbers through floating-point values.
An absent object is an empty summary, not permission to bypass version checks.
The local statement generation is deliberately not a terminal field: the sender
captures it when the RPC is created and compares it at merge time. It is distinct
from both `remote_execution_id` and the row ordinal.

### Version gate

A tracked plan or captured-session-baseline expression requires at least 119 at
source admission, cached/persisted-plan admission, and remote destination
validation. The sender then repeats a PipelineProtocolCheck on the actual
stream (five-second existing handshake timeout). A stale capability cache or a
replacement old CN must fail before executable pipeline transmission. The
receiver independently checks decoded tracking metadata, its runtime floor,
baseline presence, and range validity before execution. Unknown appended
protobuf/JSON fields alone cannot provide semantic compatibility.

## Ownership & lifecycle

| State / resource | First owner and bound | Transfer and terminal rule |
| --- | --- | --- |
| ODKUResultState | One process Base per attempt owner; one mutex, generation, constant-size summary | Local child processes alias Base. Remote processes own independent Base. Merge summaries at batch/terminal boundaries, not under a lock per row. |
| Input ordinal counter | Shared Base; checked uint64 reservations | Coordinator reserves contiguous batch ordinals before allocation. A remote producer gets one bounded range before dispatch. Overflow fails without advancing the counter. |
| Generated marker and ordinal vectors | PRE_INSERT; current reusable batch plus existing pipeline queues | Transfer with batch ownership, retain row association through projection/shuffle, release through existing batch/operator cleanup. Temporary bool/uint64 slices are per batch. |
| Action summary | Main-table DEDUP_JOIN container; nine scalars | Fold rows locally, merge and clear at flush. Reset/Free clear summary; index/internal allocation cannot be a second result owner. |
| RPC terminal | Message sender / remote receiver; one summary per remote terminal | Server snapshots successful facts before clearing compile state. Client serializes terminal consumption with terminalMu/terminalSeen and merges into its captured generation. |
| Session baseline and packet result | Initiating frontend and attempt entry | Keep baseline immutable for retry restoration. Publish packet/session result only after successful statement completion. No remote CN writes the session. |

Lifecycle: admitted -> running -> reducing -> successful statement publication
or failed/canceled -> quiesced -> reset/retry or release. Generation advances
when ResetODKUResult clears the summary; resetting the ordinal counter returns
it to its immutable base. Reset preserves the session value; the retry owner
separately restores the attempt-entry session baseline and clears the legacy
statement ID.

Quiescence is essential: local merges do not carry a generation argument.
The retry transition must cancel/join old local operators and remote execution
before resetting shared state. The generation fence is additional protection
against stale remote callbacks, not a replacement for draining producers.
Freeing pooled operators clears their local summary and provenance slices.
A CN restart loses all transient result state; the transaction/execution layer
must resolve the interrupted attempt before retry. There is no result journal
to replay after restart and no new background cleanup owner.

Memory is O(batch rows + existing bounded in-flight queue rows + active
operators/fragments), not O(total statement rows) for this added protocol.
Full-row gather traffic and existing ODKU arbitration state remain separate
costs. Finite statement memory admission must bound oversized batches; existing
backpressure must prevent the gather from outrunning its consumers.

The protocol uses existing authenticated execution/account context and carries
only result scalars and provenance, with no new cross-tenant lookup namespace.
A receiver must not reuse a process or summary between unrelated attempts or
accounts. Range arithmetic and batch-memory admission bound malformed/oversized
inputs; this design adds no per-row logs or unbounded metric labels.

## Ordering guarantees

1. Assign each admitted row a unique ordinal before allocation/deduplication.
   VALUES uses row-list order. INSERT SELECT with ORDER BY must present the
   globally ordered stream at this boundary. A generic gather only interleaves
   its inputs; it does not itself sort independently ordered streams.
2. Unordered SELECT is ordered only by this execution's observed stream.
   ORDER BY ties likewise do not promise repeatable row order without a
   tie-breaker. Do not infer SQL order from CN number, dispatch order, ID
   magnitude, or terminal arrival.
3. `NextODKURemoteOrdinalBase` reserves blocks of 2^32 ordinals. It provides
   disjointness, not SQL order. A remote producer is valid only where an
   independent stream has no required cross-producer SQL order; ordered tracked
   inputs must use the coordinator boundary. Only subtrees actually containing
   a tracked PRE_INSERT reserve blocks; read-only remote children do not.
   A bounded remote range cannot silently subdivide into unlimited forwarded
   producers: a further full-block reservation without capacity fails closed.
4. Reduce the first successful generated insert by minimum input ordinal.
   Reduce the last logical action and last changed/inserted action by maximum
   ordinal. A successful-action flag means a changed update or inserted row,
   not merely that the SQL statement finished successfully.
5. Summary merge is associative and order-independent for disjoint ordinals,
   or identical repeated facts. Equal ordinals with conflicting IDs violate
   the single-action-owner invariant; the current reducer does not diagnose
   that conflict and must not be treated as conflict resolution.

The packet selection contract at the inspected revision is:

| Statement facts | Successful packet insert ID | Session LAST_INSERT_ID |
| --- | --- | --- |
| At least one successful generated insert | ID at the first generated ordinal | Publish that generated ID |
| No generated insert; VALUES has a changed/inserted action | Stored/incoming user ID at the last logical action, including a trailing no-op | Preserve entry baseline |
| All actions no-op, or no actions | 0 | Preserve entry baseline |
| INSERT SELECT with no successful generated insert | 0, including a changed-update fallback | Preserve entry baseline |

Separate validity flags preserve explicit ID zero. For example, with
non-generated VALUES actions at ordinals 0 (changed, ID 90) and 1 (no-op, ID 4),
the packet fallback is 4 and the session baseline is unchanged. If instead
successful generated inserts occur at ordinals 2 (ID 900) and 3 (ID 10), the
packet and session use 900. Neither numerical minimum nor reply order is valid.
An allocated candidate that becomes a duplicate UPDATE does not count as a
successful generated insert.

## Failure/retry/reset semantics

| Event | Required outcome |
| --- | --- |
| Admission / missing baseline / range overflow | Reject before execution at the affected boundary; no partial reservation on overflow. A preallocated remote block abandoned on encode/send failure is discarded with the attempt, not recycled into concurrent work. |
| Allocation, constraint, operator, or remote participant failure | Fail the statement through existing execution/transaction handling. Remote error terminals omit ODKU facts; the client also suppresses facts on remote error or external cancellation. Do not publish partial local results. |
| Cancellation / deadline / disconnected peer | Use existing cancellation and cleanup; no successful frontend publication. Existing internal ErrPipelineStopped is handled as an execution-specific success signal only when the surrounding statement succeeds. |
| Malformed terminal JSON / missing terminal | Malformed JSON fails terminal decoding. Missing terminal/stream failure cannot establish success. Do not turn transport failure into an empty successful statement. |
| Duplicate terminal | terminalSeen consumes a successfully decoded envelope once per sender. Repeated identical summaries are mathematically idempotent, but transport retries are not a new right to execute DML. |
| Late terminal after retry/reset | Captured generation mismatch drops its ODKU facts even if the old RPC was otherwise successful. Do not read the new generation when delivering the old callback. |
| Retry / prepared execution / next COM_QUERY statement | Quiesce old producers, clear attempt state, advance generation, reset ordinal to base, and restore/capture the correct session baseline. Retry recomputes facts from the new execution. |
| Partial initialization / release / CN restart | Unpublished local buffers stay under existing operator/process cleanup. No result survives process destruction; no new persistent recovery protocol is introduced. |

The ODKU object is separate from the older `statement_last_insert_id` terminal
scalar. At this revision the older scalar is processed outside the ODKU
generation/publication fence. Tracked PRE_INSERT suppresses its legacy
allocation publication (`preinsert.go:772`); generation safety must not be
generalized to all envelope fields. Independent implementation review must
verify that quiescence and producer separation hold for every affected caller.

The reduction protocol neither commits storage nor provides exactly-once
transaction execution. Normal transaction outcome/rollback owns data effects.
A successful fragment may contribute provisional facts before a later fragment
fails; only whole-statement success permits frontend publication.

## Mixed-version compatibility

| Situation | Policy |
| --- | --- |
| Source, destination, and actual stream support 119+ | Admit only with valid tracking metadata, explicit baseline, and valid ordinal range. |
| New source runtime floor is below 119 | Reject tracked ODKU / baseline-dependent plans, including cached local execution; gathering locally does not waive the source gate. |
| New source targets an old/unknown destination | Reject before executable dispatch. If a replacement stream advertises less than 119, the stream check rejects it even after an earlier capability success. |
| Old source sends ordinary untracked work to a new CN | Retain the existing legacy behavior/defaults. Do not claim the new tracked-result guarantee for an old plan. |
| Tracked decoded pipeline lacks baseline presence or required capability | Fail closed; a zero baseline is accepted only when its presence bit is set. |
| Downgrade with cached/persisted 119-dependent expressions | Rebind/reject through semantic-floor admission. No blind field stripping or silent execution on older semantics. |

No new user-table or catalog migration is introduced by the transient result
summary. Existing persisted expression metadata still has a semantic version
obligation; it is not permission to run a cached plan after a downgrade.
Rollout must upgrade participating binaries and establish the required runtime
floor before enabling these semantics. Downgrade must quiesce affected work
and prevent new 119-dependent admission before lowering the floor. An all-new
cluster with a lowered runtime floor must exercise the same rejection policy.

There is no automatic fallback that silently clears tracking or substitutes a
numerical minimum. Rejection is the selected correctness/availability tradeoff
during incompatible placement. Actual old/new-binary behavior remains a
separate validation gate; capability fakes are not mixed-version acceptance.

## Alternatives considered

| Alternative | Correctness / operational tradeoff | Decision |
| --- | --- | --- |
| Keep allocation minimum and completion-order replies | Smallest change and no gather, but wrong for arbitrary ID values, changed/no-op fallbacks, and retry leakage; cannot supply a common action owner. | Reject status quo as the result contract. |
| Run the entire tracked statement on one coordinator | Simple ordering and no remote result reduction; sacrifices remote scan/dedup capacity and concentrates CPU/memory. Mixed-version source semantics still need a gate. | Reject as the general architecture; retain a local execution control for validation. |
| Distributed producers reserve ranges and send only summaries | Constant-size summaries and scalable input processing, but dispatch ranges do not encode global VALUES/ORDER BY order. A correct extension needs an ordered sequence assignment/merge protocol, compatible fields, and additional retry ownership. | Defer until measured gather cost justifies that larger protocol. |
| Return every action result and sort centrally | Makes provenance visible but retains/transfers O(N) result history and needs a bounded external sort, cancellation, and spill ownership. | Reject for this result-only requirement. |
| Gather once before PRE_INSERT, then reduce bounded facts | Gives one provenance boundary while keeping eligible downstream/scan work distributed. Adds full-row transfer and a single-worker allocation stage. Existing stream teardown and optional terminal envelope are reused. | Proposed r1 choice, subject to independent approval and the thresholds below. |

## Performance acceptance criteria

These are proposed acceptance thresholds, not measured results. None of the
benchmarks, builds, tests, or deployed scenarios below was run for this
documentation-only revision. Root must bind later evidence to an exact build,
topology, configuration, case, and terminal outcome.

Use a pinned pre-protocol comparison build with the same storage/ODKU action
semantics and an explicit record of any differing result-reporting behavior.
Compare exact data state and logical affected rows in both builds; use the
specified result oracle, not a known-wrong baseline packet, for result IDs.
Hold CPU/memory limits, batch size, indexes, durability, seed, and client
concurrency constant. If that comparison cannot be isolated, report
BLOCKED_BASELINE rather than silently substituting another baseline.

Measure 1/2/4 CNs; DOP 1/4; row payloads 64 B / 4 KiB; VALUES,
unordered SELECT, and totally ordered SELECT; 0/50/100% duplicate ratios
including changed and all-no-op updates. Use 1, 1,000, and 100,000 input rows,
with one warmup and five measured runs per comparison; report medians and p95
statement latency. Bound each run to 60 seconds and retain timeouts as failures.
The comparison owner may narrow redundant combinations only with a recorded
reason while retaining all distinct ordering, topology, and payload dimensions.

| Metric | Proposed threshold / oracle |
| --- | --- |
| Results and failure isolation | Zero packet/session mismatches against the source-specific oracle; zero stale/failed-attempt publications under delayed, duplicate, reordered, canceled, and retried terminals. Performance success cannot waive this gate. |
| Existing single coordinator input, no additional gather | Median rows/s at least 95% of comparison; p95 statement latency at most 110%, allowing a 1 ms absolute latency floor for tiny statements. |
| Added remote gather | Median rows/s at least 80% of comparison; p95 latency at most 125%, allowing the same 1 ms floor. Failure requires an explicit architectural decision, not a silent exception. |
| Result/provenance retained memory | For R simultaneously live tracked rows, additional retained bytes at most 32R + 1 MiB per active pipeline above comparison, plus measured existing gather queue payload. At fixed batch/queue size, increasing total rows from 1,000 to 100,000 adds no more than 1 MiB of result-only retained memory per pipeline. Account for temporary slices as well as vector capacity. |
| Terminal traffic | At most one consumed ODKU object per remote fragment/hop; serialized object at most 512 bytes (nine scalars, including maximum uint64 values), independent of row count. No per-row result RPC. Measure existing full-row gather traffic separately. |
| Synchronization | Summary-state mutex acquisitions scale with batch/terminal flushes, not row count; profiles show at most 5% of statement CPU in result-state synchronization. Ordinal CAS is at batch reservation boundaries. |
| Cancel/retry cleanup | All injected attempt-owned operators/streams terminate and added live result buffers/goroutines return to baseline within 5 seconds of cancellation on the controlled topology. No old-generation publication during the following successful attempt. |

The CPU and memory profiles must distinguish the new result mechanism from
existing arbitration, allocator, storage, and queue costs. Report coordinator
CPU, peak memory, network bytes, allocations, blocked time, and total wall time.
An overloaded/unready environment is BLOCKED_ENVIRONMENT, not a passing sample.
The small fixed summary does not prove that the full-row coordinator gather is
cheap. No throughput, deployment QA, or mixed-version PASS is implied here.

## Verification plan

Existing source tests are starting points, not evidence of execution in this
worker. Their scope limits are part of the plan.

| Invariant | Existing source / required next proof |
| --- | --- |
| Placement and provenance | `pkg/sql/compile/compile_test.go:3978` (TestCompileODKUPreInsertSingleInputStream); `pkg/sql/colexec/preinsert/preinsert_test.go:218` area. Inspect and execute focused coverage for already-local, remote, multi-worker, batch boundaries, hidden allocator, and explicit zero. |
| Reduction and result consumers | `pkg/vm/process/process2_test.go:231`; `pkg/sql/colexec/dedupjoin/odku_semantics_test.go:37`; `pkg/frontend/multi_insert_last_insert_id_test.go:84`, :97, :109, :190. Validate generated-first, trailing no-op, zero-ID and VALUES/SELECT split through an actual client OK packet and following LAST_INSERT_ID query. |
| Terminal / retry ownership | `pkg/sql/compile/resource_accounting_test.go:945` (TestRemoteODKUTerminalFailureCancellationAndRetry); deterministic barriers must additionally prove producer quiescence, duplicate/reordered replies, malformed terminals, and next-statement/prepared reuse. |
| Admission, wire precision, and range bounds | `pkg/sql/compile/odku_result_protocol_test.go:39`, :69, :87, :128, :150, :191; `pkg/vm/process/process2_test.go:536`. Exercise real protobuf/JSON round trips including maximum IDs; old/new actual binaries, a replacement stream, floor rollback, missing presence, and overflow before dispatch. |
| Production action path | Existing `test/distributed/cases/dml/insert/on_duplicate_key_modern.sql` and matching result, plus `test/distributed/cases/auto_increment/last_insert_id_hidden_pk.sql`. Verify actual coverage before claiming the packet, ordered multi-CN, direct/S3, retry, or compatibility terminal. Extend cases only in a separately authorized implementation task if a required oracle is absent. |
| Capacity and ordering | Run the acceptance matrix above with actual multi-CN placement and totally ordered input. Prove ORDER BY is globally merged before ordinal assignment; a one-worker shape assertion is insufficient. Use race/fault validation for the shared-state and reset boundary before implementation acceptance. |

## Open questions and review decisions

- Independent design reviewer / root: approve or reject the r1 choice of one
  coordinator provenance boundary, its source-specific packet rules, and the
  fail-closed mixed-version policy. This is blocking for design acceptance.
- Implementation reviewer: prove that every supported ordered plan reaches
  PRE_INSERT as one globally ordered stream and that nested producers cannot
  violate ordinal ownership. Any counterexample requires a separate code/design
  revision before implementation acceptance; the generic gather is not proof.
- Implementation reviewer: verify quiescence of local reducers and isolation
  from the legacy statement-ID scalar at reset/reuse. The generation guard
  applies to ODKU summaries only; approval must not broaden that guarantee.
- Independent reviewer / performance owner: ratify these numeric budgets before
  accepting the design; collect exact-build measurements before rollout.
  Thresholds must not be rewritten after observing a failing measurement
  without an explicit reviewed tradeoff.
- QA / rollout owner: provide actual old/new-binary, cached-plan downgrade,
  multi-CN packet, cancellation, and performance evidence before deployment
  acceptance. The owner assignment and scheduling belong to root.

## Review and approval ledger

| Design revision | Source revision | Independent review permalink | Decision |
| --- | --- | --- | --- |
| r1 (this publication) | 286b60837041d819573a260f7ab6e346125c6d3a | PENDING — root must link the immutable reviewed document commit and review | DRAFT — pending independent design review |

A review must identify the document commit, reviewer, decision, and any accepted
tradeoffs. Material changes need a new design revision and affected re-review.
Publishing this file does not satisfy approval, resolve the CR thread, authorize
implementation acceptance, or establish runtime/performance acceptance.
