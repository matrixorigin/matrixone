# CN Pipeline Connection Lifecycle and Ephemeral-Port Exhaustion

- Status: Implemented and locally validated
- Tracking issue: [matrixorigin/matrixone#25690](https://github.com/matrixorigin/matrixone/issues/25690)
- Issue baseline commit: `3728f38`
- Last updated: 2026-07-14

## 1. Summary

Sustained distributed queries can exhaust the ephemeral ports used for CN-to-CN
pipeline traffic. The failure is observable as `EADDRNOTAVAIL` / `cannot assign
requested address`, even though both CN processes remain healthy.

This is not a classic permanent connection leak. The current code eventually
closes each TCP connection, but it couples logical pipeline termination to TCP
connection destruction. Under a high-fanout shuffle workload, the connection
create/close rate exceeds the kernel's ability to reclaim TCP tuples from
`TIME_WAIT`. The configured application limit of 100,000 backends per remote
host does not protect the much smaller ephemeral-port budget.

The first repair should separate logical stream cleanup from physical connection
cleanup. A successfully drained pipeline stream should use an explicit FIN/ACK
handshake and return its still-healthy, exclusively locked backend to the pool.
Protocol failures and ambiguous teardown must continue to close the entire
backend connection.

## 2. Scope

This document covers:

- CN-to-CN remote pipeline streams created by `PipelineClient`;
- the client/server protocol used to terminate a logical pipeline stream;
- ownership and reuse of morpc backend connections;
- admission control needed to protect TCP tuple and file-descriptor budgets;
- functional, lifecycle, failure-path, and resource acceptance criteria.

The first implementation phase does not include:

- changing SQL semantics or optimizer rules;
- replacing morpc with another RPC framework;
- allowing concurrent pipeline streams to share one backend connection;
- redesigning the complete distributed query scheduler;
- relying on operating-system TCP tuning as the primary repair.

## 3. Reproduction and Evidence

Issue #25690 uses two CNs, 16 persistent frontend sessions, and 25 executions
per session of a distributed three-table join. Frontend connections are not
repeatedly opened, so frontend short-connection churn is excluded.

The issue environment reported approximately 5,800 established CN-to-CN
connections per CN and 4,000-5,000 `TIME_WAIT` sockets after failures. The Linux
ephemeral port range was `32768-60999`, or 28,232 ports.

The workload was also reproduced locally with two CN services and the complete
dataset from the issue. During the run, the socket state reached:

| State | Observed count |
| --- | ---: |
| `ESTABLISHED` | 2,773 |
| `TIME_WAIT` | 8,741 |
| `CLOSE_WAIT` | 22 |
| `FIN_WAIT_1` + `FIN_WAIT_2` | 45 |

The run then produced the same failure signature:

```text
ERROR 20505 (HY000): cannot connect to backend:
dial tcp4 127.0.0.1:18200: connect: can't assign requested address
```

After load stopped, established connections and `TIME_WAIT` entries declined,
and both CNs continued to answer `select 1`. This confirms connection churn and
temporary TCP tuple exhaustion rather than a CN crash or a permanently retained
socket.

The local reproduction used macOS and a native multi-CN process, so it validates
the mechanism and error signature rather than exact Linux timing or an exact
binary-build equivalence.

## 4. Current Lifecycle

The relevant path is:

```text
remote scope
  -> messageSenderOnClient
  -> PipelineClient.NewStream
  -> morpc client.NewStream(backend, lock=true)
  -> one exclusively locked backend/TCP connection
  -> remote execution returns End or Error
  -> local pipeline cleanup
  -> stream.Close(true)
  -> backend.Close()
  -> TCP close and TIME_WAIT
  -> inactive backend GC removes the backend from the pool
```

Important code locations:

- `pkg/cnservice/cnclient/types.go`: `dfMaxSenderNumber = 100000`;
- `pkg/cnservice/cnclient/client.go`: pipeline streams use `lock=true`;
- `pkg/sql/compile/remoterunClient.go`: sender cleanup calls
  `streamSender.Close(true)`;
- `pkg/common/morpc/backend.go`: `stream.Close(true)` closes the complete
  backend connection;
- `pkg/sql/compile/remoterunServer.go`: after sending the result End message,
  the server waits for connection or message-context termination before final
  related-pipeline cleanup.

The final server behavior is an important constraint: receiving a result End
message does not currently prove that all server-side resources associated with
the stream have been released. Therefore changing only `Close(true)` to
`Close(false)` is not safe.

## 5. Root-Cause Analysis

### 5.1 Q1: Every creation has a destruction

Q1 passes for the TCP connection involved in this issue:

- a remote pipeline creates a stream and backend connection;
- normal and error cleanup reach `messageSenderOnClient.close()`;
- `stream.Close(true)` reaches `backend.Close()`;
- inactive backend GC removes stopped backends from the client pool.

The kernel may retain the closed tuple in `TIME_WAIT`, but that is expected TCP
behavior and not evidence that the application forgot to close the socket.

### 5.2 Q2: Every wait has a termination

The known teardown waits are bounded or context-controlled:

- stop-response waiting is bounded by 30 seconds;
- stream operations use the query deadline, or a default 24-hour deadline when
  the query has no deadline;
- server-side connection/message waits end on connection or context
  cancellation.

Q2 therefore does not establish an infinite wait in the current reproducer.
However, introducing a small connection cap inside an already-running pipeline
DAG can create cyclic resource waits that only terminate through a long query
timeout. Any admission-control change must avoid acquiring scarce connection
slots in the middle of a dependency cycle.

### 5.3 Q3: Every accumulation has a meaningful upper bound

Q3 fails at two distinct boundaries.

First, the configured backend limit is not aligned with the protected resource:

```text
MaxSenderNumber per remote host = 100000
Linux reproducer ephemeral ports = 28232
```

Second, a concurrent connection limit does not bound connection churn. If a
slot is returned immediately after TCP close, sustained traffic can reuse the
application slot thousands of times while old tuples remain in `TIME_WAIT`.
The protected quantity is closer to:

```text
ESTABLISHED + TIME_WAIT budget + in-flight dials
    <= safety factor * ephemeral-port count
```

The high-fanout query plan amplifies the problem because every concurrently
active remote scope exclusively owns one backend connection.

## 6. Design Constraints

1. A logical stream must not be reused until both sides agree that its resources
   and routing registrations have been removed.
2. A connection with an ambiguous protocol state must never return to the pool.
3. Pipeline streams must remain exclusive on a backend in the first phase.
4. Cancellation and cleanup communication must be bounded and context-aware.
5. Admission control must not introduce a resource-acquisition cycle inside the
   pipeline DAG.
6. The first reuse phase is limited to streams that have consumed either a
   successful result End or an explicitly caller-authorized, negotiated
   terminal response for a clean local StopSending/retry path. Unrecognized
   application error, cancellation, malformed response, and any ambiguous state
   retain the legacy connection-close cleanup path.
7. Stream sequence bookkeeping must have a per-stream deletion path; moving
   from short-lived to reused TCP connections must not introduce session-scoped
   memory growth.
8. Pipeline reassembly caches must be removed when assembly succeeds or fails;
   closing a cache object without deleting its session-map entry is not a
   complete destruction path.
9. The repair must work safely for normal completion, remote error, local
   cancellation, peer disconnect, timeout, partial message delivery, and
   caller-authorized clean StopSending/retry termination.

## 7. Repair Plan

### Phase 1: Explicit logical stream teardown and safe backend reuse

Introduce a logical stream teardown protocol for successful completion with
states equivalent to:

```text
OPEN
  -> RESULT_END
  -> CLIENT_FIN
  -> SERVER_CLEANUP
  -> FIN_ACK
  -> IDLE_REUSABLE
```

The intended sequence is:

1. Every initial `PipelineMessage` and `PrepareDoneNotifyMessage` requests
   FIN/ACK teardown using the same additive protobuf field. For a fragmented
   PipelineMessage, every fragment carries the same value and the final assembled
   message retains it only if all fragments agree. A legacy server ignores the
   unknown field.
2. A capable server records the accepted teardown mode in a server-side stream
   lifecycle object before executing the pipeline.
3. Remote execution sends its existing successful result End and echoes the
   accepted teardown mode. A legacy server sends End without the echo.
4. Before sending an accepted End, the original CN request worker registers a
   lifecycle object keyed by `(ClientSession, streamID)`. The object contains a
   FIN signal, cleanup completion barrier, terminal state, and removal callback.
   The original worker remains the sole owner of its existing related-pipeline
   cleanup and waits for FIN, connection close, message cancellation, or a
   bounded server-side FIN timeout. A legacy worker retains the current
   wait-for-disconnect behavior.
5. The client consumes the successful End and completes local downstream and
   child-pipeline cleanup. It sends FIN only when End echoed FIN support.
6. FIN is handled by a separate CN request worker through a dedicated terminal
   dispatch path. It atomically wins the FIN terminal path, signals the original
   owner, and waits on the cleanup barrier. It does not enter the generic
   request path that automatically appends an End or Error response.
7. The original owner removes the related pipeline/dispatch registration and
   publishes cleanup completion. Connection close, message cancellation, and
   FIN timeout use the same once-only cleanup path but do not send an ACK.
8. The FIN worker is the request's only response owner. It calls a morpc
   terminal-stream API that sends exactly one FIN_ACK as the final response and
   atomically retires the stream sequence bookkeeping. A terminal failure closes
   the session and emits no generic End/Error response.
9. After receiving FIN_ACK, the client unregisters the logical stream with
   `Close(false)`. This unlocks the backend while retaining the TCP connection.

The morpc request callback is synchronous, but the CN wrapper starts a separate
worker for each assembled request and immediately returns from that callback.
Consequently FIN can already be read while the original CN worker waits. The
required coordination is between the original execution worker and the FIN
worker; PR 1 must not add another unowned async execution layer or allow the FIN
worker to bypass cleanup owned by the original worker.

Registration occurs before the accepted End is sent. Duplicate lifecycle keys
are protocol errors. If registration or End send fails, the original owner runs
cleanup and removes the lifecycle entry through the non-ACK terminal path.
The server FIN timeout begins only after the accepted End has been successfully
flushed. It uses a dedicated teardown context bounded independently from the
execution context and is also cancelled by session close.

The same registry and teardown path apply to remote execution streams beginning
with `PipelineMessage` and remote dispatch-notify streams beginning with
`PrepareDoneNotifyMessage`. Notify streams are not optional coverage: shuffle
queries create one for each remote receiver, and leaving them on `Close(true)`
would preserve linear TCP churn even if execution streams were reusable.

The connection remains exclusive during the stream lifetime:

```go
client.NewStream(ctx, backend, true)
```

This is intentionally different from the historical reuse attempt that combined
`lock=false` with `Close(false)`. Current morpc delivery can block the backend
read loop when one logical stream's bounded receive channel is full. Concurrent
stream multiplexing would therefore introduce head-of-line blocking and can
deadlock cyclic pipeline traffic.

#### Close disposition

Replace ambiguous boolean state such as `safeToClose` with a synchronized
teardown state machine. One owner performs final disposition; receive,
cancellation, peer-close, and timeout paths submit events. `POISONED` is
monotonic and a late FIN_ACK cannot turn it back into `REUSABLE`.

```text
OPEN -> RESULT_END -> FIN_SENT -> FIN_ACKED -> REUSABLE
  |         |             |
  +---------+-------------+-----------------> POISONED
```

At minimum, distinguish:

| Outcome | Backend action |
| --- | --- |
| FIN_ACK received after complete server cleanup | Reuse with `Close(false)` |
| Peer does not echo FIN support in successful End | Legacy `Close(true)` |
| No pipeline message was successfully sent | Conservative `Close(true)` in PR 1 |
| Remote application error | `Close(true)` in PR 1 |
| Query cancellation or early-stop without a negotiated terminal response | `Close(true)` in PR 1 |
| Caller-authorized StopSending/retry followed by negotiated terminal response and FIN_ACK | Reuse with `Close(false)` |
| FIN or FIN_ACK timeout | Poison and `Close(true)` |
| Send/receive/network error | Poison and `Close(true)` |
| Sequence violation or malformed message | Poison and `Close(true)` |
| Peer closed the connection | Do not reuse |

PR 1 reuses successful completions and two existing clean local control paths:
StopSending completion and the expected `remote receiver is not registered yet`
notify retry. These paths are reusable only when the initiating caller explicitly
authorizes cleanup, the server returns a per-stream negotiated terminal response,
and FIN_ACK follows the common completion barrier. Arbitrary application errors
and user cancellation remain ineligible. Reuse for those cases requires a
separate ABORT state machine and is explicitly out of scope for PR 1.

#### Server ordering requirement

FIN_ACK must be emitted after cleanup, not merely after FIN reception. The ACK
must establish this invariant:

```text
After FIN_ACK(streamID), the server will not read, write, cancel, or look up any
state belonging to streamID through the old logical stream.
```

FIN is not retransmitted on the same connection. TCP already provides ordered,
reliable delivery while the connection remains healthy. If FIN send completion
or FIN_ACK is uncertain, the client poisons and closes the backend. Server-side
cleanup notification remains internally idempotent so FIN racing with session
close cannot run destructors twice; a duplicate wire FIN is not required to
preserve connection reuse.

#### Server lifecycle ownership

The lifecycle registry has one entry per FIN-negotiated active stream. Its state
machine has two competing terminal triggers:

```text
ACTIVE -> RESULT_SENT -> FIN_RECEIVED -> CLEANED -> ACKED -> REMOVED
                    \-> SESSION_OR_CONTEXT_CLOSE -> CLEANED -> REMOVED
                    \-> FIN_TIMEOUT -> CLEANED -> SESSION_CLOSED -> REMOVED
```

Exactly one trigger wins through CAS or an equivalent mutex transition. The
original request worker owns `RemoveRelatedPipeline` and publishes `CLEANED`;
the FIN worker is the only ACK owner and may ACK only after observing `CLEANED`.
The session/context-close path never ACKs. Every terminal path removes the
registry entry, and cleanup is protected by `sync.Once` or an equivalent
once-only primitive.

FIN has a dedicated CN dispatch branch. The existing common response logic must
explicitly exclude FIN, as it already excludes the one-way StopSending method.
After `FinishStream` sends FIN_ACK, no `sendEndMessage` or `sendError` call may
follow for the FIN request.

The existing session-close watcher must signal these lifecycle entries as well
as the existing running-pipeline records. A client that crashes after End must
therefore wake the original owner, run cleanup, unblock any FIN waiter, and
remove the registry entry. A bounded server FIN timeout closes a peer that
negotiated FIN but neither finishes teardown nor disconnects.

#### Atomic morpc stream retirement

morpc currently keeps the last received and sent sequence for every stream ID in
connection-scoped containers. Those entries were previously bounded by the
short TCP lifetime. The received container is currently an ordinary map whose
ownership assumes IO-goroutine-only access, while CN request workers write
responses concurrently. A FIN worker must not delete these containers directly.

PR 1 adds a thread-safe `ClientSession` terminal primitive equivalent to
`FinishStream(ctx, terminalToken, finalResponse)`, where `terminalToken` is an
opaque proof produced when the morpc IO path accepts the FIN sequence. The
implementation owns a morpc
synchronization boundary shared with request-sequence validation and performs:

1. confirm that the already-validated FIN token is still the current terminal
   request; this must not increment or validate FIN as if a second request had
   arrived;
2. verify that the pipeline lifecycle completion barrier has already completed;
3. freeze the FIN_ACK stream header using the existing sent-stream sequence;
4. synchronously write and flush FIN_ACK;
5. delete both received and sent sequence entries before releasing the same
   synchronization boundary used by the next request validation;
6. if ACK write/flush fails or its delivery becomes uncertain, close the session
   through the failure path so all connection-scoped bookkeeping is reclaimed.

Although the client can receive FIN_ACK before deletion finishes, a subsequent
request on the reused backend cannot validate until `FinishStream` releases the
synchronization boundary. This establishes ACK-before-reuse ordering without a
concurrent map read/write. The API, including its failure-close behavior, must be
safe to call from a CN worker even though `ClientSession` is otherwise documented
as not thread-safe.

Stream IDs must not be reused during a backend connection's lifetime. A repeated
or late FIN after terminal bookkeeping deletion is a protocol violation and
closes the backend rather than recreating an unbounded tombstone. Acceptance
tests must prove that sequence container size returns to the number of active
streams after repeated reuse.

#### Pipeline reassembly cache ownership

Every `Status_Last` PipelineMessage currently enters the assembly path, including
an unfragmented message. `CreateCache(streamID)` stores a cache in the morpc
session map, while `cache.Close()` alone does not delete that map entry. A reused
connection would therefore turn completed streams into O(N) closed cache state.

The assembly path must call `ClientSession.DeleteCache(streamID)` on every
terminal branch:

- successful unfragmented assembly;
- successful fragmented assembly;
- fragment teardown-mode mismatch;
- cache pop or assembly error;
- connection/context cancellation or timeout while fragments are pending. The
  periodic/session cleanup path must close and delete the cache, not merely drop
  the map key.

Deletion belongs at assembly completion rather than normal FIN because the cache
is no longer needed once the request has been reconstructed. `FinishStream`
provides a final invariant check that no cache remains for the terminal stream;
if residual state cannot be safely removed or proves an ownership violation, it
poisons and closes the session instead of ACKing reuse.

#### Mixed-version compatibility

FIN is a new wire-level method. Negotiation is per logical stream and occurs
inside existing backward-compatible protobuf messages:

- the initial request includes a requested teardown mode;
- both `PipelineMessage` and `PrepareDoneNotifyMessage` carry the request;
- all fragments carry the same requested mode; mismatch is a protocol error;
- a capable server records that mode before execution and echoes it in the
  terminal End response;
- the client sends FIN only after observing the echo;
- a legacy server ignores the additive request field, keeps waiting for
  disconnect, and returns End without an echo; the client then uses
  `Close(true)`.

This avoids relying on a potentially stale service-discovery capability cache
and ensures the server knows, before its first handler returns, whether it must
transfer lifecycle ownership or retain legacy disconnect cleanup.

The required behavior is:

| Client | Server | Behavior |
| --- | --- | --- |
| FIN-capable | FIN-capable | Use FIN/ACK and reuse a clean backend |
| FIN-capable | Legacy or capability unknown | Use legacy `Close(true)` |
| Legacy | FIN-capable | Server preserves the legacy disconnect cleanup path |

The accepted mode is immutable for that logical stream. A client must not infer
support from a previous stream on the same address or backend; every new stream
requires an echoed acceptance. If a peer is replaced or downgraded, the next
stream naturally falls back to legacy close without a stale-capability window.

### Phase 2: Resource-budget guardrails

Add protection at both the per-remote endpoint and local CN levels:

- maximum active backends per remote endpoint;
- maximum active pipeline backends per local CN;
- context-aware wait queues with cancellation;
- a per-endpoint dial token bucket or equivalent rate limit;
- bounded exponential backoff for connection failures;
- a safety reserve for non-pipeline traffic and recovery operations.

The active-backend limit and dial-rate limit are separate controls. The former
protects file descriptors, memory, and goroutines; the latter protects ephemeral
ports and `TIME_WAIT` capacity.

Do not simply change the default from 100,000 to a small constant. If a stream
inside the running DAG blocks while holding other pipeline resources, a small
limit can convert port exhaustion into a distributed query deadlock.

### Phase 3: Query-boundary admission

Before launching a distributed query, estimate the maximum concurrent outbound
remote streams per target CN. Acquire the required leases in a deterministic
endpoint order. If all leases cannot be acquired, release partial reservations
and wait or fail according to the query context.

Moving admission to the query boundary prevents a query from entering the DAG
with only a partial set of the connection resources needed to make progress.
This phase requires a separate design review because nested remote scheduling and
multi-query fairness must be included in the model.

### Phase 4: Connection-amplification reduction

The first three phases retain one active TCP connection per active remote stream.
Longer term, reduce the number of physical connections per query/CN pair.

Any multiplexed design must provide:

- per-stream credit or flow control;
- a non-blocking connection read loop;
- fair scheduling across logical streams;
- bounded per-stream and per-connection memory;
- reserved progress for FIN, ACK, cancellation, and other control frames;
- explicit handling of connection-level head-of-line blocking.

Potential transports include a redesigned morpc multiplexer or a transport with
native independent streams such as QUIC. This is not required for resolving the
first-phase lifecycle bug.

## 8. Alternatives Considered

| Alternative | Decision | Reason |
| --- | --- | --- |
| Change only `Close(true)` to `Close(false)` | Reject | Server cleanup currently depends on connection/context termination |
| Set `lock=false` and multiplex existing morpc streams | Reject | A full stream channel can block the shared read loop and other streams |
| Only lower `MaxSenderNumber` | Reject as complete fix | Does not bound dial churn and can introduce DAG resource waits |
| Only rate-limit dials | Keep as guardrail | Prevents exhaustion but retains inefficient normal connection churn |
| Expand OS ephemeral port range | Operational mitigation only | Delays failure without correcting lifecycle or admission |
| Enable aggressive TCP reuse or send RST with `SO_LINGER` | Reject | Platform-dependent and can compromise protocol/data reliability |
| Add source IPs to increase tuple space | Reject as primary fix | Operational complexity that masks application resource misuse |

## 9. Implementation and PR Decomposition

Keep the initial change reviewable and rollback-safe.

### PR 1: Protocol and lifecycle

- add requested/accepted teardown-mode fields plus FIN and FIN_ACK messages;
- carry and validate the requested mode across every fragmented PipelineMessage;
- delete the session reassembly cache on every assembly terminal path;
- negotiate and reuse `PrepareDoneNotifyMessage` streams through the same
  lifecycle protocol;
- add a server-side lifecycle registry coordinating the original execution
  worker, FIN worker, and session-close path;
- add a dedicated FIN dispatch path with FIN worker as sole response owner;
- make FIN_ACK follow complete server cleanup;
- add atomic morpc `FinishStream`-style ACK/sequence retirement;
- add a synchronized client teardown state machine with monotonic poison state;
- retain `lock=true`;
- reuse successful FIN_ACKed backends and explicitly caller-authorized,
  negotiated StopSending/retry terminals after FIN_ACK;
- retain legacy `Close(true)` for unrecognized error, cancellation, unnegotiated
  early-stop, legacy peer, and all ambiguous outcomes;
- close the backend on every ambiguous or failed teardown path;
- add unit tests for both protocol endpoints and morpc lock/reuse behavior.

Likely code areas:

- `proto/pipeline.proto` and generated `pkg/pb/pipeline` code;
- `pkg/sql/compile/remoterunClient.go`;
- `pkg/sql/compile/remoterunServer.go`;
- `pkg/cnservice/cnclient`;
- `pkg/common/morpc` for terminal stream bookkeeping cleanup;
- targeted tests under the same packages.

### PR 2: Observability and resource guardrails

- per-remote active/idle/locked backend gauges;
- backend reuse hit/miss counters;
- backend close counters labeled by reason;
- dial rate, wait duration, waiter count, and admission rejection metrics;
- per-endpoint and global connection limits;
- dial rate limiting and backoff.

### Separate design/PR: Query admission and multiplexing

Query-boundary leases and physical-connection coalescing should not be folded
into the lifecycle PR. They have different correctness risks and rollback paths.

### Implementation decision log

- Keep `lock=true`: this phase reuses an exclusive backend but does not
  multiplex logical streams.
- Enable negotiation by default with `disable-stream-reuse` as the rollback
  switch: mixed-version safety is per-stream and a missing acceptance echo
  always falls back to `Close(true)`.
- A negotiated terminal application error is not sufficient by itself for
  reuse. Only the caller-recognized StopSending and notify-registration retry
  paths may authorize FIN after local cleanup; all other errors remain poisoned.
- Hard connection budgets and query-boundary admission remain PR 2 work. The
  lifecycle repair removes normal dial churn but does not redefine the existing
  maximum-concurrency policy.
- Local acceptance uses separate CN processes. A native combined multi-CN
  process shares process-global colexec registries and is not equivalent to the
  issue topology.

## 10. Acceptance Criteria

Sections 10.1-10.5 are mandatory for PR 1 unless an item is explicitly marked
PR 2. Section 10.6 defines benchmark reporting rather than an environment-neutral
merge gate. Resource admission and hard connection budgets are PR 2 criteria and
must not block independent validation of PR 1.

### 10.1 Functional correctness

- Run the exact two-CN reproduction from issue #25690 with 16 persistent
  sessions and 25 executions per session.
- All 400 statements must complete successfully.
- Every statement must return `499999`.
- Both CNs must remain healthy and accept new queries throughout and after the
  run.
- No `EADDRNOTAVAIL`, backend-connect failure, remote-receiver timeout, or
  protocol-sequence error may occur.

### 10.2 Connection lifecycle

- After warm-up, repeated sequential executions against the same remote CN must
  reuse existing healthy backends.
- Backend connect count must plateau near the workload's concurrent high-water
  mark instead of increasing approximately with completed remote scopes.
- A FIN_ACKed stream must leave no server pipeline, dispatch, stream, goroutine,
  or session registration associated with its stream ID.
- Lifecycle-registry size must return to zero after FIN completion, FIN timeout,
  peer close, message cancellation, registration failure, and End send failure.
- After N sequential successful streams on one reused backend, morpc received and
  sent sequence containers must return to zero terminal entries; their size is
  bounded by concurrently active streams, not completed streams.
- Session cache size must equal the number of streams currently awaiting more
  fragments. It must return to zero after both unfragmented and fragmented
  sequential reuse tests and after every assembly error path.
- A backend must not be reused before FIN_ACK.
- A poisoned or ambiguous backend must not return to the pool.
- Idle backend GC must still close genuinely unused connections.

### 10.3 Resource behavior

- `TIME_WAIT` must not grow approximately linearly with the number of completed
  statements after the pool is warm.
- Established connection count must converge to a stable high-water mark under a
  fixed-concurrency workload.
- File descriptor, goroutine, and heap usage must converge after the workload
  stops.
- PR 2: no backend pool may grow beyond its configured per-remote or global
  budget.
- PR 2: admission waits and dial rates must be visible through metrics.

No universal absolute socket-count threshold is specified for PR 1 because the
number of active remote edges depends on CN count, DOP, and the query plan. The
key PR 1 invariant is reuse and convergence rather than continued growth. PR 2
must define deployment defaults and hard resource thresholds.

### 10.4 Unhappy paths

Tests must cover at least:

| Scenario | Expected result |
| --- | --- |
| FIN send completion is uncertain | No retransmit; backend poisoned and closed |
| FIN_ACK is not observed before deadline | Timeout, backend poisoned and closed |
| Duplicate or late FIN | Protocol violation and backend close; no tombstone growth |
| Peer disconnects before FIN_ACK | Backend closed; no reuse |
| Query cancelled before result End | Legacy connection-close cleanup; no reuse |
| Query cancelled after result End but before FIN_ACK | Backend poisoned and closed |
| Server returns an application error | Correct SQL error and `Close(true)`; no reuse in PR 1 |
| Notify receiver is not registered yet and caller retries | Negotiated terminal cleanup, FIN_ACK, and backend reuse before retry |
| Malformed or out-of-order response | Protocol error and backend close |
| Client crashes after result End | Server connection context releases all stream resources |
| Server crashes during teardown | Client wait terminates and connection is removed |
| Legacy server ignores requested teardown mode | End has no acceptance echo; client uses `Close(true)` |
| FIN-capable server receives a legacy request | Server retains wait-for-disconnect cleanup |
| PipelineMessage is split into multiple fragments | Every fragment agrees on requested mode; assembled request preserves it |
| Fragment teardown modes disagree | Protocol error and backend close |
| Unfragmented PipelineMessage assembly completes | Cache entry is deleted immediately |
| Fragmented assembly completes or fails | Cache entry is deleted on every terminal branch |
| PrepareDoneNotifyMessage completes successfully | Accepted FIN/ACK path reuses the exclusive backend |
| Legacy notify client or server | Notify stream falls back to `Close(true)` |
| Notify dispatch returns an error or is cancelled | Correct error and `Close(true)`; no PR 1 reuse |
| FIN succeeds | Exactly one FIN_ACK; no trailing generic End/Error; next stream succeeds |
| Receive channel is full before End | Stream is ineligible for reuse; poison/close terminates cleanup |
| Server sends data after End or FIN_ACK | Protocol violation and backend close |
| PR 2: backend pool is at capacity | Context-aware backpressure; no busy loop |
| PR 2: waiting query is cancelled | Waiter removed promptly; permit accounting remains correct |

### 10.5 Deadlock and concurrency safety

- Existing shuffle-join and remote-dispatch regression suites must pass.
- Existing cyclic pipeline dependency tests must pass with `lock=true` and
  successful connection reuse enabled.
- PR 2 must add a cyclic pipeline test at the configured connection limit. It
  must prove progress or bounded admission failure; a long query timeout is not
  accepted as the normal release mechanism.
- Run concurrent stream close, peer close, FIN reception, and context
  cancellation under the race detector for the affected pure-Go packages.
- The server lifecycle completion barrier and backend close must be idempotent
  when FIN races with connection or context cancellation.
- `FinishStream` must be race-tested against the next stream request on the same
  backend and against session close; no concurrent sequence-map access is
  permitted.
- Client teardown must end exactly once in `REUSABLE` or `POISONED`; poison must
  never be reversed by a late ACK.
- Mixed-version tests must prove per-stream negotiation fallback without using a
  cached capability from an earlier stream or CN incarnation.

### 10.6 Benchmark report

- Report median and P95 query latency before and after the change on identical
  hardware, commit configuration, CN count, DOP, dataset, and query plan.
- Use at least three independent runs, state warm-up count and measurement
  window, and report variance. Any merge threshold is agreed from these results
  rather than assumed by this design document.
- For a deterministic fixed-concurrency reuse test, once the pool reaches the
  required concurrent high-water mark, completing additional sequential waves
  must not create backends unless the high-water mark increases or an existing
  backend becomes unhealthy.
- Teardown must not add one network round-trip to every returned batch; it occurs
  once per logical stream.

### 10.7 Observability

At minimum, expose enough information to distinguish:

- active, locked, idle, stopped, and waiting backends;
- newly created versus reused backends;
- backend closes by normal idle, protocol error, cancellation, peer close, and
  FIN/ACK timeout;
- FIN/ACK latency and timeout count;
- dial attempts, failures, and rate-limit waits;
- active logical pipeline streams and server-side stream registrations.

PR 1 must expose FIN/ACK outcomes, reuse count, close reason, active logical
streams, lifecycle-registry size, and morpc sequence-container size. Admission
wait and resource-budget metrics belong to PR 2.

Alerts should be based on rates and budget utilization, especially backend dial
rate, connect failures, admission wait latency, and connection-budget occupancy.

### 10.8 Local validation record (2026-07-14)

Validation used separate log, TN, CN1, and CN2 processes so process-global CN
registries matched the issue's multi-process topology. The exact issue dataset
and query were run through 16 persistent frontend sessions with 25 executions
per session.

- Two consecutive 400-statement waves completed with 400/400 correct results;
  every result was `499999`.
- CN-to-CN `TIME_WAIT` socket endpoints remained zero throughout both waves.
- Established socket endpoints were 1,548 after warm-up, peaked/ended at 1,636
  in wave 1, and peaked/ended at 1,680 in wave 2. The small second-wave increase
  matched its higher observed concurrency high-water mark rather than completed
  statement count.
- Both CN frontend ports returned `select 1` after the load.
- After the configured idle-backend interval, established CN pipeline sockets
  returned to zero, confirming idle GC still destroys genuinely unused
  connections.
- Targeted package tests and race-detector tests for morpc FIN retirement,
  lifecycle coordination, and client teardown passed.

The benchmark-reporting item in 10.6 still requires a controlled before/after
performance environment; it is not claimed by this local correctness record.

### 10.9 Linux multi-CN validation record (2026-07-14)

Validation on Debian 13 (`linux/amd64`, kernel 6.12.63) used independent log,
TN, CN1, and CN2 processes. It reused the exact issue dataset and ran two
consecutive waves of 16 persistent frontend sessions with 25 executions per
session.

- Both waves completed with 400/400 correct results; every result was `499999`.
- CN-to-CN `TIME_WAIT` remained zero during both waves. Established socket
  endpoints reached 2,778 in wave 1 and increased only to 2,814 in wave 2,
  tracking the concurrent high-water mark rather than the 400 additional
  statements.
- No `EADDRNOTAVAIL`, backend-connect failure, remote-receiver timeout,
  protocol-sequence error, or panic occurred during the workload.
- After idle GC, established CN pipeline sockets returned from 2,814 to zero
  and the two CN processes' combined file-descriptor count fell from about
  2,895 to 63. A post-GC execution through CN2 returned `499999`, and both CN
  frontend ports remained healthy.
- Combined CN RSS peaked near 5.5 GB and was about 4.7 GB after the short idle
  window. Go heap retention makes this insufficient evidence of a leak, but a
  longer soak with heap and goroutine profiles remains necessary for the full
  memory-convergence criterion in 10.3.

## 11. Rollout and Rollback

Use a feature flag or configuration gate for healthy backend reuse during the
initial rollout. Suggested sequence:

1. deploy code that understands requested/accepted teardown mode, with clients
   not requesting FIN teardown by default;
2. enable FIN requests for a small set of successful streams; verify legacy
   responses without an acceptance echo still use disconnect cleanup;
3. verify FIN/ACK cleanup, sequence-container bounds, and completion-barrier
   idempotency metrics;
4. enable successful-completion reuse on a small set of CNs;
5. compare connection churn, query latency, and cleanup metrics;
6. expand successful-completion reuse. Error/cancellation reuse remains disabled
   until a separately reviewed ABORT protocol exists.

Rollback first disables new FIN requests. Streams without an accepted teardown
echo use `Close(true)`, so legacy cleanup remains available throughout. Existing
FIN-negotiated streams must drain or have their backends closed before rolling a
server back to a version that does not understand FIN.

## 12. Open Questions

- Is the existing result End message allowed to be redefined as proof of complete
  server cleanup, or must FIN/ACK remain a distinct phase? The current server
  ordering indicates a distinct phase is safer.
- What is the minimal server lifecycle object that can own the execution context,
  cleanup closure, completion barrier, and connection-close fallback after the
  original request handler returns?
- Should a later ABORT protocol allow reuse after application error or
  cancellation, and how will it prove that dispatch writers have stopped?
- How should ephemeral-port budgets be configured across Kubernetes network
  namespaces and deployments with multiple source or destination IPs?
- Can the compiler calculate a safe upper bound for remote streams per target CN,
  including nested remote execution?
- Which existing cyclic shuffle tests best represent the historical reason that
  unrestricted multiplexing was reverted?
