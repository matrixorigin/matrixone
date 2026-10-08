# Service construction and close ownership

Version: v3, 2026-10-08. Scope: [PR #29676](https://github.com/matrixorigin/matrixone/pull/29676), fixes [#29639](https://github.com/matrixorigin/matrixone/issues/29639) and [#29640](https://github.com/matrixorigin/matrixone/issues/29640), references [#29249](https://github.com/matrixorigin/matrixone/issues/29249).
Implementation reference: `23cb69d1d95d34698caab5b93b9de00bf65f086f`; baseline: `f20f5ca147c44b012343d0f48dfcd84b6a5986a1`.

## Review status

The implementation already existed when this public design was prepared. Earlier local design and implementation reviews did not close the mandatory public, versioned design gate. This v1 makes the whole ownership contract reviewable; it does not retrospectively claim public approval before the original implementation.
An independent `gpt-6.1-sol` / `xhigh` design review must approve the exact design-only commit and link its immutable revision in the PR before the follow-up CI test implementation and renewed overall implementation approval. The linked approval record, rather than this document's presence, closes that gate.

## Problem and invariants

A return value cannot transfer an acquired service owner when an option panics or calls `runtime.Goexit`. An incomplete constructor or partial service batch can consequently lose the only cleanup authority. Separately, RPC shutdown does not join asynchronous ants jobs; joining a job without canceling its request can wait until a distant deadline. Finally, a CN's remote withdrawal error can coexist with completed local teardown, so interpreting every error as incomplete cleanup can retain the entire parent cluster indefinitely.

The contract is: every acquired resource has one reachable owner; publication precedes fallible post-acquisition work; rollback works during ordinary errors, panic and Goexit; admission is sealed before drain; accepted borrowers finish before dependencies retire; a diagnostic error does not erase a positive local-completion certificate; an incomplete drain retains dependencies and admission. Repeated Close preserves its terminal diagnostic and does not duplicate resource retirement.
This changes internal Go construction and lifecycle contracts, not SQL behavior or distributed membership policy. It does not force an uncooperative handler to finish, add a cleanup retry service, or introduce another owner registry.

## Construction protocol and owners

CN, TN, lock allocator, shard service and shard server constructors require an owner-publication callback. Validate owner-independent arguments first; after allocating the partial aggregate, install its deferred rollback before calling the callback or options. The callback retains that same object in the caller's existing authoritative holder; it must not start, close, or concurrently expose the partially constructed service. Successful construction commits the aggregate; unsuccessful return, panic or Goexit runs the same nil-safe Close. Defers do not handle process termination such as `os.Exit`.

| Boundary | Acquisition and retirement authority |
| --- | --- |
| Launcher, embed and service wrappers | Retain the published aggregate before fallible construction/start; retain partial batches in existing collections. |
| CN | Serialize lifecycle operations; drain ingress and local producers before downstream services. Only completed local drain authorizes dependency retirement. |
| TN | Acquire prerequisites before starting process I/O work; quiesce admission, drain admitted transactions/tasks, then close dependencies. Reject Start after quiescence. |
| LOG | Retain the acquired store immediately, including metadata initialization failure; close RPC ingress before data-sync consumers. |
| Allocator and shard owners | Publish before options/acquisitions; close their RPC ingress and drain producers before queues, channels and clients. |
| Root file services and admission | Remain caller-owned; children borrow them. Parent deduplicates root FS Close, stops its worker owner, then releases the exclusive lease. |

On incomplete cleanup the caller keeps the owner and its borrowed dependencies reachable. Cleanup diagnostics remain available through existing Close results/logs. This is fail-stop retention, not a promise that another Close retries the failed drain. Restart/reuse requires terminal retirement of the old generation; a failed generation cannot release its lease and silently overlap a replacement.

## Method-server request context and close

V3 supersedes v2's blanket native parenting. The v2 candidate was not delivered: 100-caller precompiled ABBA samples showed synchronous +10.4% and mixed +13.1% median time despite lower async allocations. Ambient workloads limit attribution, but those results did not meet the no-regression acceptance criterion. Constructor, publication, completion and parent retirement contracts above stay unchanged. The immutable v3 design-only revision needs a distinct gpt-6.1-sol/xhigh approval before implementing this replacement.

The existing handler map owns method dispatch. A monotonic atomic capability records whether any synchronous handler has been registered, stored before publishing that handler. Once set it never resets, including a later asynchronous replacement: conservatively retaining the existing path is safe. Registration still obeys the existing handler-map synchronization requirements; this adds no live map mutation API. A private immutable codec callback selects the lifecycle root once per frame only while this capability is unset. Mixed servers keep independent timeout contexts, avoiding additional synchronous parent registration. No map scan, Start snapshot, new option for callers or deadline reconstruction is required.

Previously decoded native frames retain their linkage after the capability changes, including a frame later dispatched by a synchronous registration. Frames selected after the change retain the original independent synchronous cancellation contract. Direct `Handle(ctx)`, raw RPC codecs and clients remain caller-owned/independent. Caller `CodecOptions` backing arrays are copied before adding private wiring.

Native selection reuses the existing timeout child. A cold private owner marker identifies its method server. The deadline codec records that native child's `Done` immediately after construction in one private transient RPCMessage channel field. Async dispatch skips `AfterFunc` only when this provenance exists, the owner matches and the effective handler context has the same `Done`. Trace/stream value wrappers preserve it; `WithoutCancel`, a new detached timeout, cross-owner and unmarked manual contexts cannot acquire native authority merely by retaining values. Unverified contexts retain the existing `AfterFunc` and removable stop closure. These are mutually selected cancellation techniques, not a second request registry or state machine; arbitrary caller cancellation graphs are not introspected. Frame width/copying and guard costs must be measured as part of acceptance.

`asyncMu` seals async admission and increments the WaitGroup before submission. Close performs **seal admission -> cancel lifecycle root -> close RPC ingress -> join accepted async jobs -> return**. RPC Close joins synchronous callbacks; the WaitGroup joins asynchronous jobs, including inline fallback after ants rejection. No admission lock spans cancellation or either join. Cancellation must precede RPC Close, which can itself wait on inline work. Cancellation is not completion; context-ignoring handlers still block their owner's join.

Request CancelFuncs retain deadline/cause authority. Decoder failure cancels before decode succeeds; the RPC callback owns cancellation until delegation, including internal messages and pre-handler failures; delegated method execution owns cancellation through handler/write cleanup. Headerless method-server frames are rejected before pooled acquisition based on option presence, also in mixed mode; generic codec behavior stays unchanged. Root cancellation retires no messages, buffers or dependencies. Failed construction cancels the acquired root. Distinct method servers never share it. There is no new wire field, configuration surface, background worker, queue or retry timer.

## Completion, diagnostics and parent ordering

Real CN Close holds its lifecycle mutex through synchronous teardown. Its one `closeOnce` body writes `closeComplete = (localErr == nil)` and caches `errors.Join(withdrawErr, localErr)`. The certificate is final when Close returns; there is no asynchronous false-to-true refresh contract.
The CN wrapper reports Closed after a nil Close result or a positive local-completion certificate, while retaining the error. The parent uses that status to distinguish completed diagnostics from incomplete local teardown. Completed CN diagnostics allow remaining CN/TN/LOG retirement and are returned together with any later error. Incomplete CN cleanup stops before later services, root FS and lease release. TN/LOG retain their existing nil-error completion contract; this change does not invent a completed-error certificate for them.
Parent Close uses the same ordering before Start, after a partial Start and after a successful Start. It closes CN, then TN, then LOG, then root FS and stopper, then admission and optional data removal. Errors remain observable on repeated calls. A later incomplete TN/LOG prevents retirement of its dependencies even if earlier CN retirement completed with a diagnostic.

## Alternatives and cost

| Alternative | Decision |
| --- | --- |
| Status quo: return-only constructors and caller-only cleanup | Cannot publish owners on panic/Goexit or retain failed construction that never returns normally. |
| Return a partial object plus error, or use a generic construction transaction | Partial returns still miss nonreturning unwind; a transaction adds another owner framework. Explicit publication plus aggregate rollback uses existing holders and Close. |
| Return from async Close without joining | Would let live handlers access retired parent dependencies. |
| Track active requests in a custom map/pool | Duplicates request cancellation bookkeeping. Native context ancestry reuses the existing timeout child. |
| Parent only asynchronous requests after decoding | Requires deferred deadline finalization, preserving original timeout accounting, arbitrary header values and intervening failure cleanup. The extra codec state/path is not justified by dispatch-only measurements. |
| Treat all Close errors as incomplete, or discard completed diagnostics | The former retains completed owners; the latter hides withdrawal failures. Existing CN certificate plus preserved errors distinguishes them. |
| Refresh certificates later or generalize all wrappers' status contracts | Real CN's certificate is immutable after Close; TN/LOG have no corresponding completed-error backend. Additional states, loops and fixtures have no required consumer here. |

The external three-sample real-constructor/onMessage/ants no-op benchmark measured 490.1–500.8 ns/op, 230–231 B and 3 allocations before; 623.3–652.7 ns/op, 366–367 B and 5 allocations after. Median overhead is approximately **134 ns, 136 B and 2 allocations per async dispatch** (27% for this primitive). This is historical v1 evidence, not an end-to-end RPC or SQL throughput result. V1 async shard reads/control and vector-cache eviction pay it; ordinary synchronous handlers do not register AfterFunc. V2 removes that registration; it must be measured separately.
V3 optimizes the all-async capability while retaining the existing mixed-server linkage. Blanket parenting adds synchronous native registration/removal and failed the v2 performance gate. Selection from the handler capability plus per-frame provenance avoids caller promises and registration-before-Start assumptions. No custom cancellation map or deferred header finalization is introduced.

The historical v1 dispatch numbers do not validate v3. Candidate acceptance requires actual-codec and precompiled real-RPC async, synchronous and mixed comparisons at low and high concurrency, with complete alternating paired results. The new frame metadata and dispatch checks must be measured, including raw/client paths. Allocation reduction alone does not establish throughput restoration or absence of regression. Native parent locking remains a cost; all-async cancellation state follows unfinished decoded requests, while mixed-server registrations follow unfinished async requests and are removed at completion. Terminal pre-handler paths cancel immediately. Existing RPC/ants limits remain authoritative; there is no new capacity guarantee. Existing TN drain retains its four-minute fail-stop budget. No workload-level throughput guarantee or forced completion of context-ignoring handlers is claimed.


## Compatibility, rollout and containment

The callback parameter is a source-level Go API change; all in-tree launcher/embed/wrapper consumers migrate together in this PR. No wire method, protobuf field, persisted format, SQL surface, configuration or authorization rule changes. Different deployed versions continue to use existing protocols; this local ownership correction adds no new mixed-version handshake or migration. Existing data durability and backup contracts remain unchanged because children still borrow root FS.
Use normal release deployment; there is no new feature flag. Rollback restores prior lifecycle behavior and therefore its known ownership risks, but needs no data conversion. Retain old-generation resources/admission on incomplete drain and report the existing Close error; do not recover by admitting a replacement over live borrowers. Existing constructor/Close logging and blocked-goroutine stacks expose failures without new metrics or background monitors.
Publication and cancellation remain scoped to their service/request owner; no cross-tenant registry or new input-controlled allocation is introduced. The additional allocation follows existing accepted request volume. Authentication, tenant isolation and existing admission limits remain the owning subsystems' contracts.

## Validation and acceptance

V3 adds these orthogonal checks to the unchanged ownership matrix: actual codec hour-deadline/trace/cause and native cancellation; real-wire async, mixed-async and deliberately rejected ants submission with held exit barriers; original synchronous independence after capability selection; earlier native/later independent frames across a synchronous registration and conservative replacement; detached `WithoutCancel` plus timeout, cross-owner and unmarked contexts; native requests avoid duplicate registration; decode/pre-handoff failures, internal ping and malformed streams; headerless rejection also in mixed mode; shared config and independent generations; constructor unwind and direct/raw/client independence. Use bounded observations, independent failure cleanup and missing-parent/cancel-after-ingress/missing-join/provenance negative controls. Owning morpc, shardservice and queryservice normal/race plus incremental configured SCA are required; reconcile unchanged evidence explicitly. Existing SQL/protocol tests remain controls; no separate SQL BVT is required for this internal linkage change.

Identical-toolchain benchmarks compare v1 and v3 using actual codec encode/decode/dispatch and precompiled low/100-caller async/sync/mixed RPC. Record time, bytes, allocations and frame metadata/copying effects with bounded alternating samples. Profile contention if a regression appears. Synchronous/mixed regression cannot be accepted merely because hot async allocations fall. Exact v3 public approval precedes implementation; final overall model review reconciles this replacement with the entire ownership change.

| Risk | Required proof |
| --- | --- |
| Error/panic/Goexit and partial batches lose owners | Constructor unwind and actual caller/collection tests; partial nil-safe and repeated Close; borrowed FS retained on incomplete cleanup. |
| Cancellation/join wrong order or late admission | Real NewMessageHandler and RPC Close witness; hour-deadline request released by lifecycle cancellation; held exit barrier, late request rejection and repeated Close. Omit-cancel and omit-join controls must fail at bounded assertions. |
| Diagnostic mistaken for completion or incompleteness | Actual parent Close tests: completed diagnostic retires next owners once; incomplete CN retains them; later incomplete TN preserves both errors. Both opposite mutations fail. |
| Public consumers or reuse break | Owning package normal/race, launcher compile, real embed startup/SQL/expansion and stopped-cluster restart with catalog/disk artifacts. |
| CI leader test uses inconsistent observations | Wait only for real single-voter election readiness; directly test leader state and locally fenced nonleader nil state separately. Neither target oracle may retry an incorrect result into success. |

Existing nine-package static analysis, normal/race, exact shutdown stress, four negative controls and benchmark evidence are reusable only while their relevant source/configuration/native inputs are unchanged. Documentation alone does not invalidate them; the corrected logservice test requires its own normal/race, bounded stress, opposite-oracle controls and configured incremental SCA before renewed final approval.
The unrestricted tests/service suite has a verified baseline `Test_buildTNOptions` Config.Validate panic; whole short-mode normal/race passes are disclosed with existing heavy real-CN skips. Real embed lifecycle/restart supplies production consumer proof. An initial 180-second whole-embed alarm was underbudget: the exact CDC case passes with 360 seconds on baseline (131.602 s) and final (130.327 s). These scoped passes do not claim the unrestricted service suite or pending CI passed.
Acceptance requires the exact public design approval, unchanged-evidence reconciliation, corrected-test gates and a renewed independent overall review with no unresolved material blocker. Workload-level async throughput and forced completion of context-ignoring handlers are not claimed; no additional open design decision is required for this correction.
