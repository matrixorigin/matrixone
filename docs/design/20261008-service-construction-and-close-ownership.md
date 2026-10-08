# Service construction and close ownership

Version: v1, 2026-10-08. Scope: [PR #29676](https://github.com/matrixorigin/matrixone/pull/29676), fixes [#29639](https://github.com/matrixorigin/matrixone/issues/29639) and [#29640](https://github.com/matrixorigin/matrixone/issues/29640), references [#29249](https://github.com/matrixorigin/matrixone/issues/29249).
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

## Async method-server close

`asyncMu` serializes admission with the existing closed flag. Each accepted asynchronous request registers `context.AfterFunc` on one server-owned lifecycle context and increments the existing WaitGroup before unlocking/submitting. Normal completion removes its registration before decrementing the join count. If ants rejects submission, the accepted request executes inline with the same cleanup and join ownership.

Close performs **seal admission -> cancel accepted request contexts -> close RPC ingress -> join accepted jobs -> return**. No admission lock is held during cancellation, RPC Close or Wait. Cancellation must precede RPC Close: inline fallback can make an RPC callback wait for that same request context. Late requests are canceled and their pooled messages released without invoking the handler. Constructor failure cancels the acquired lifecycle context.
The callback only calls the codec's standard idempotent CancelFunc; it never resets messages, closes buffers or releases service dependencies. The request keeps its original context, values, deadline and cause authority. Handler/write completion owns message/buffer cleanup. `AfterFunc` stop does not join an already-started cancellation callback; this is safe because that callback owns only idempotent context cancellation, while the WaitGroup joins the actual resource borrowers.

This follows the existing codec's `context.WithTimeoutCause` ownership and Go's documented [AfterFunc](https://pkg.go.dev/context#AfterFunc), [CancelFunc](https://pkg.go.dev/context#CancelFunc) and [WaitGroup](https://pkg.go.dev/sync#WaitGroup) contracts. Cancellation registration does not start a waiting goroutine; cancellation may launch a callback for each still-associated request. No new request queue or retry timer is added.

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
| Track active requests in a custom map/pool | Can work if canceled before RPC Close, but duplicates admission/completion bookkeeping. A post-RPC cancellation snapshot has the inline-fallback wait cycle. Standard removable registration avoids that registry. |
| Treat all Close errors as incomplete, or discard completed diagnostics | The former retains completed owners; the latter hides withdrawal failures. Existing CN certificate plus preserved errors distinguishes them. |
| Refresh certificates later or generalize all wrappers' status contracts | Real CN's certificate is immutable after Close; TN/LOG have no corresponding completed-error backend. Additional states, loops and fixtures have no required consumer here. |

The external three-sample real-constructor/onMessage/ants no-op benchmark measured 490.1–500.8 ns/op, 230–231 B and 3 allocations before; 623.3–652.7 ns/op, 366–367 B and 5 allocations after. Median overhead is approximately **134 ns, 136 B and 2 allocations per async dispatch** (27% for this primitive). This is an accepted correctness cost, not an end-to-end RPC or SQL throughput result. Async shard reads/control and vector-cache eviction pay it; ordinary synchronous CN/TN handlers do not register it.
Live cancellation state is proportional to accepted unfinished async requests, one removable registration per request. It inherits existing RPC/ants admission capacity; this PR adds no global capacity guarantee. Existing TN drain has a four-minute fail-stop budget. A context-ignoring handler can still hold Close and dependencies indefinitely; timeout-return would violate the ownership invariant. Optimize registration only if actual workload evidence justifies the extra bookkeeping.

## Compatibility, rollout and containment

The callback parameter is a source-level Go API change; all in-tree launcher/embed/wrapper consumers migrate together in this PR. No wire method, protobuf field, persisted format, SQL surface, configuration or authorization rule changes. Different deployed versions continue to use existing protocols; this local ownership correction adds no new mixed-version handshake or migration. Existing data durability and backup contracts remain unchanged because children still borrow root FS.
Use normal release deployment; there is no new feature flag. Rollback restores prior lifecycle behavior and therefore its known ownership risks, but needs no data conversion. Retain old-generation resources/admission on incomplete drain and report the existing Close error; do not recover by admitting a replacement over live borrowers. Existing constructor/Close logging and blocked-goroutine stacks expose failures without new metrics or background monitors.
Publication and cancellation remain scoped to their service/request owner; no cross-tenant registry or new input-controlled allocation is introduced. The additional allocation follows existing accepted request volume. Authentication, tenant isolation and existing admission limits remain the owning subsystems' contracts.

## Validation and acceptance

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
