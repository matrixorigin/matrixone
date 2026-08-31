# Sirius streamed MatrixOne input protocol

Status: proposed for design approval

Design version: 6

Approval: pending distinct reviewer approval

Owner: MatrixOne query execution

Owning issue: [#27586](https://github.com/matrixorigin/matrixone/issues/27586)

Implementation: MatrixOne [#27599](https://github.com/matrixorigin/matrixone/pull/27599), Sirius [#6](https://github.com/matrixorigin/sirius/pull/6), sidecar [#14](https://github.com/matrixorigin/mo-sirius-sidecar/pull/14)

## 1. Decision

MatrixOne may execute an eligible table scan at its transaction snapshot and
stream the resulting MatrixOne-native batches to a separately running Sirius
sidecar. Sirius consumes those batches through a GPU-native scan operator and
returns MatrixOne-native result batches. Arrow Flight supplies authenticated
RPC framing only; Arrow record batches are not the data representation in
either direction.

This is an explicit, single-CN compatibility path selected by
`/*+ SIDECAR STREAM */`. It does not replace or alter direct `TaeRead`, and it
does not silently fall back to MatrixOne execution after the explicit stream
mode has been selected.

The wire protocol is version 4. Wire protocol numbers 4 and 5 existed only on
the unmerged StreamRead feature branches; protocol 4 is the single delivery
revision after the merged direct-`TaeRead` protocol-3 baseline. It requires an exact
capability-document match across MatrixOne, the sidecar, and Sirius. Mixed
protocol revisions fail before result rows are exposed.

After approval, any semantic change to this document increments the design
version and requires fresh approval. Editorial corrections may retain the
version only when they do not change a protocol, ownership, resource, rollout,
or acceptance contract.

## 2. Problem and invariants

Direct `TaeRead` is efficient when the sidecar can read every object needed for
an admitted snapshot. It cannot cover all MatrixOne-visible states, including
unflushed committed rows, visible tombstones, or storage not reachable by the
sidecar. The streamed path keeps snapshot and storage semantics in MatrixOne
while retaining Sirius execution for the remainder of the plan.

The first implementation acknowledged each staged batch and immediately
self-scheduled another `GPU_MO_SCAN` task. That bounded the Flight slot but not
the downstream Sirius repositories or task queue. Version 2 removed that eager
source continuation, but SF10 disproved its claimed end-to-end bound: a `FULL`
partition barrier legitimately asks its producer to finish and retained 2.71
GiB of source-derived host data for Q9. Once the barrier opened, two configured
GPU workers launched `cudf::hash_partition` concurrently on one GPU. Repeated
runs either returned different Q9 values or failed at that overlap with
`cudaErrorInvalidDevice` followed by `cudaErrorIllegalAddress`; the same plan
with one GPU worker was byte-identical to native MatrixOne. Q1 and Q6 remained
byte-identical with two workers, and reduced Q9 fingerprints remained exact
through `part`, `lineitem`, `partsupp`, and `orders`, isolating the failure to
the large concurrent partition phase rather than Flight or the native codec.

Version 3 kept ordinary GPU task parallelism but admitted only one `PARTITION`
execution per GPU until that operator's CUDA stream was synchronized. It also
replaced the false one-published-batch memory claim with a process-global input
budget. Version 4 removed multi-frame `GPU_MO_SCAN` staging: one Sirius source
task consumes one wire frame and produces one host representation, while one
subsequent frame may occupy the sidecar's one-slot prefetch window.

Further review disproved the premise that concurrent partition execution is
itself unsupported. The normal Sirius path runs with multiple GPU streams; its
large scan tasks merely make the failing fine-grained schedule less likely.
The actual violated lifetime is exception quiescence. A GPU operator can enqueue
work and then throw before `run_one_operator` reaches its success-only stream
synchronization. The task then releases processing handles and its reservation,
and the executor returns the borrowed stream to the pool, even though queued GPU
work may still reference those resources or the stream may contain a sticky
CUDA failure. Version 5 fixed that owner boundary and required `PARTITION` to be
reentrant rather than serialized. Version 6 closes the remaining failure-domain
contract: a context/device-fatal CUDA failure seals the whole paired sidecar,
cancels every ticket, makes Flight readiness fail, and terminates the process
for supervisor restart. Ordinary errors whose streams quiesce cleanly remain
query-local.

MatrixOne uses its ordinary synchronous output backpressure. If Sirius stops
pulling, the sidecar withholds the input acknowledgement, `NativeInput.Send`
blocks `Output.Call`, the existing bounded pipeline edge fills, the connector
stops before `ChildrenCall`, and the engine reader stops. MatrixOne therefore
retains the same DOP-proportional bounded read-ahead as a native query, never a
table-sized wire queue.

The primary correctness invariant is:

> For every `StreamRead`, Sirius consumes exactly the projected rows and schema
> produced by the MatrixOne scan at the statement snapshot, once and only once,
> without cross-query reuse or resources surviving terminal execution.

The supporting invariants are:

1. MatrixOne is the sole owner of transaction visibility, native scan,
   MVCC/tombstone application, scan filtering, and scan projection.
2. A stream identity is bound to one account, query, snapshot, schema,
   capability set, and expiry. It is single-use within one execution ticket.
3. Every input frame is acknowledged only after Sirius has copied the bytes it
   needs and the sidecar no longer retains that frame as the current input.
4. Each `StreamRead` has at most one active Sirius source task. One task calls
   `next_batch` once, publishes one batch, never self-schedules, and releases its
   claim only after the batch's synchronized H2D conversion. Downstream demand
   alone admits the next task.
5. Input, result, plan, ticket, and execution counts have hard bounds.
6. Cancellation can interrupt a blocked input acknowledgement, blocked result
   receive, and Sirius worker independently of the data path.
7. MatrixOne does not release snapshot/query resources until local producers,
   the sidecar execution, and all sidecar input handlers are quiescent.
8. Every GPU task retains its input processing handles, reservation, allocator
   attachment, and borrowed stream until that stream is quiescent on success or
   failure. Only a successfully quiesced stream returns to the pool.
9. `PARTITION` is reentrant: tasks on independent streams use immutable operator
   metadata and task-local temporary/output state. Configured GPU concurrency is
   preserved, including concurrent tasks in the same partition stage.
10. Direct `TaeRead` keeps its existing schema, physical-type, lease, and
    fallback contract. It shares the same GPU task-lifetime invariant.
11. The first context/device-fatal CUDA or synchronization failure atomically
    seals sidecar admission before cancellation begins. No prepared, running,
    or subsequent execution can use that process generation; bounded fail-stop
    termination and supervisor restart create the next healthy generation.

The negation of the contract includes lost or duplicate batches, using a stream
from another account/query/snapshot, acknowledging a batch before ownership is
transferred, unbounded buffering under a slow consumer, releasing resources
before quiescence, eagerly scheduling the next Sirius source batch without
downstream demand, executing on DuckDB CPU, or accepting a different wire ABI.
It also includes accepting new work after the process GPU generation is sealed,
or describing a device-fatal failure as statement-local.

## 3. Scope

### 3.1 Included

- Explicit `SIDECAR STREAM` execution for read-only `SELECT` statements.
- One MatrixOne CN and its paired sidecar per execution.
- Up to 16 independently named `StreamRead` inputs in one Substrait plan.
- Native MatrixOne scan filters, projections, offsets, and limits below the
  stream boundary.
- Sirius GPU execution of admitted Substrait joins, filters, projections,
  aggregates, sorts, fetches, and references.
- MatrixOne-native input and result batches over mutually authenticated Flight.
- Success, prepare failure, producer failure, consumer failure, cancellation,
  timeout, disconnect, sidecar shutdown, and result-side early completion.
- Bounded TPC-H execution using MatrixOne's native reader backpressure, one
  Flight frame, one sidecar prefetch slot, and one Sirius host representation
  per active read.

### 3.2 Excluded

- Multi-CN producer fan-in or a remote/distributed scan scheduler.
- Current-transaction writes or a transaction workspace with prior writes.
- Replaying one physical scan node through multiple `ReferenceRel` consumers.
- Transparent replacement of direct `TaeRead`.
- A general remote-execution framework.
- Unbounded blocking plans, spill-to-disk for streamed native input, or a claim
  that one source batch bounds all downstream operator state.
- A stable public MatrixOne batch ABI across arbitrary MatrixOne releases.
- Production enablement without the rollout and acceptance gates in this
  document.

## 4. Alternatives

### 4.1 Direct `TaeRead`

The sidecar reads flushed TAE objects directly. This avoids CN scan and network
serialization and remains the preferred analytical fast path. It cannot
represent every MatrixOne-visible state and requires storage accessibility plus
GC-safe object leases. The streamed path therefore complements rather than
replaces it.

### 4.2 Arrow record batches over Flight

This was the original experiment in #27586. Arrow is interoperable and already
fits Flight, but MatrixOne would need an additional conversion and dependency at
both input and result boundaries. Sirius would then convert Arrow/DuckDB chunks
again for its GPU-native path. That duplicates type mapping, allocations, and
copies in the hot path.

### 4.3 MatrixOne-native batches over Flight (selected)

MatrixOne already owns the source batches and result consumer. Reusing
`Batch.MarshalBinary` avoids Arrow-Go in MatrixOne and lets Sirius reuse its
GPU-native TAE vector decoder. The cost is a deliberately strict, same-release
wire ABI. Exact capability negotiation, codec versioning, endian/size markers,
canonical decoding, and coordinated rollout contain that cost.

### 4.4 One GPU worker for the whole query

Setting `executor.pipeline.num_threads` to one made SF10 Q9 deterministic, but
it serializes joins, aggregates, projections, and scans and hides ownership
bugs that remain reachable with smaller task sizes. It also changes deployment
behavior for direct `TaeRead`. Version 5 therefore requires correctness with at
least two GPU workers and does not accept global serialization.

### 4.5 Selective partition serialization

A per-GPU partition permit would preserve concurrency for other operators, but
it still makes correctness depend on suppressing a schedule that normal Sirius
supports. It does not repair resource release before stream quiescence or
prevent a poisoned stream from being reused after another operator fails.
Version 5 instead makes the task lifetime exception-safe and the partition
execution state reentrant. If an isolated test proves the pinned libcudf
primitive cannot run on independent streams after those fixes, delivery stops
and the design must be reconsidered; serialization is not added silently.

### 4.6 CUDA retry after partition failure

An OOM may be retried only after the task stream has successfully synchronized
while all input owners and reservations remain live. Invalid launch,
invalid-device, illegal-address, or synchronization failure may have partially
executed work or poisoned the stream and is terminal. Clearing CUDA error state,
sleeping, or replaying the task is not recovery.

### 4.7 Immediate migration to current upstream Sirius

Current upstream Sirius has newer partial-barrier, adaptive-join, and task
admission machinery, but the MatrixOne TAE/Substrait stack is based on a fork
hundreds of upstream commits behind it. Porting that stack is the preferred
long-term route to general streaming, but it is a separate migration with a
larger compatibility and validation surface. Version 5 fixes the common task
ownership boundary on the current fork and records unbounded execution as a
non-goal.

## 5. Architecture and ownership

The data path is:

```text
MO planner
  -> export Substrait with StreamRead leaves
  -> prepare one sidecar execution and validate result schema
  -> attach every DoPut input
  -> start local-CN snapshot scan producers
  -> MOB1 / MO Batch.MarshalBinary frames
  -> sidecar one-slot StreamRead sources
  -> Sirius GPU_MO_SCAN and GPU physical plan
  -> MOB1 / canonical flat MO result frames over DoGet
  -> MO result decoder and existing result writer
```

MatrixOne owns the statement transaction, scan pipeline, source batches,
producer goroutine, Flight client, decoded result batches, and final MySQL
write. The sidecar owns Flight admission, ticket/idempotency registry,
query-local input registry, Sirius connection/transaction, result frame slot,
execution worker, and input-handler join. Sirius owns the physical GPU plan,
task scheduling, GPU-native source conversion, pipeline data, and result packer.

Ownership transfer for one input frame is:

```text
MO scan batch
  -> one mpool-charged MO Flight frame (header plus direct marshal)
  -> sidecar one-slot native_batch_view
  -> Sirius staged host representation
  -> consumed acknowledgement
  -> MO releases payload and scan batch
```

The acknowledgement is the linearization point. Before it, MatrixOne retains
the frame's source lifetime. After it, Sirius owns a copy and MatrixOne may
produce the next frame into the sidecar's one-slot input. The acknowledgement
does not admit another Sirius source task: `GPU_MO_SCAN` advances only after the
current published batch has driven downstream demand for more input.

## 6. Admission and plan contract

Stream mode is admitted only when all of these conditions hold:

- the statement is an explicit streamed `SELECT`, not internal or prepared
  execution;
- the transaction workspace is read-only and has no current or snapshot write
  offset;
- every exported scan has one occurrence and an admitted physical input type;
- the plan has 1 through 16 stream inputs;
- all scan scopes remain on the current CN;
- every `StreamRead` schema is the deterministic post-scan native schema;
- the Substrait plan and result schema pass the exact capability contract.
- the process-global execution-capacity owner atomically admits the request's
  input slots, plan/schema overlap, result slot, and configured active Sirius
  representation capacity before ticket publication.

MatrixOne retains semantic operators above the scan in Substrait. It clears
native scan aggregation because the semantic aggregate remains in Sirius. It
keeps native scan filtering, projection, offset, and limit at the source.

`CHAR` and `VARCHAR` are represented as Substrait `VarChar` while the physical
input vector keeps its original MatrixOne OID, width, charset, and nullability.
Sirius accepts both physical OIDs as the same string family. This is a protocol
mapping, not a catalog or planner rewrite.

## 7. Wire protocol

### 7.1 Transport and authentication

All calls use Arrow Flight over gRPC with TLS 1.2 or newer. MatrixOne verifies
the sidecar server certificate and presents the configured CN client
certificate. The sidecar requires a trusted client certificate. Endpoint
redirection is rejected, so the authenticated connection is the only data
channel.

The streamed path does not call the direct-read HTTPS resolver and exposes no
object path or storage credential. The direct `TaeRead` resolver and its
separate sidecar client identity remain unchanged.

### 7.2 Capability negotiation

At connection initialization, MatrixOne calls `GetCapabilities`. The returned
document must be byte-for-byte equal to MatrixOne's document. Its SHA-256 hash
is repeated in `ExecuteSubstraitRequest`, every `StreamRead`, and
`FlightInfo.app_metadata`.

The protocol-v4 capability fixes these values:

- Substrait 0.78.0;
- `StreamRead` version 1 with feature bits 0;
- native batch frame/codec version 1;
- native result schema version 1;
- little endian, 16-byte MatrixOne type records, and 24-byte varlena records;
- at most 16 stream inputs and one buffered input slot per read;
- at most 4 MiB per input batch payload;
- StreamRead host accounting contract `pre-admitted-execution-v2`;
- GPU-fatal recovery contract `process-fail-stop-v1`;
- at most 16 MiB per Substrait plan;
- the exact operator, expression, function, join, and type allow-lists.

Any change to these semantics requires a new protocol or feature bit and a new
capability document. Unknown fields, enums, flags, or feature bits are rejected.

### 7.3 Prepare

MatrixOne sends `ExecuteSubstraitRequest` through `GetFlightInfo`:

| Field | Contract |
| --- | --- |
| `protocol_version` | exactly 4 |
| `substrait_version` | exactly `0.78.0` |
| `capability_hash` | exact negotiated SHA-256 |
| `max_batch_bytes` | non-zero and no more than the sidecar result limit |
| `max_input_batch_bytes` | non-zero and no more than 4 MiB or the sidecar limit |
| `deadline_unix_ms` | future time, capped by the sidecar ticket TTL |
| `plan` | non-empty, at most 16 MiB |
| `query_id` | 16-byte statement identity |
| `account_id` | MatrixOne tenant identity |
| `idempotency_key` | SHA-256 of little-endian account ID followed by query ID |
| `result_schema` | canonical native result schema v1 |

The sidecar fingerprints the whole request. Reusing an idempotency key with a
different fingerprint is terminal. A matching unclaimed prepare may return the
same ticket. Tickets are 32 random bytes and single-use. `FlightInfo` must have
one local endpoint, no redirection, the exact result schema, and the capability
hash.

The prepare response is the last point at which the non-stream direct mode may
choose native fallback. Explicit `SIDECAR STREAM` is strict: prepare or
admission failure is returned to the statement and is never hidden by native
execution. No result row or schema is exposed before prepare validation.

### 7.4 `StreamRead`

Each Substrait extension read contains:

| Field | Contract |
| --- | --- |
| protocol/feature bits | version 1, bits 0 |
| `stream_ref` | 32 cryptographically random bytes, unique in the plan |
| `query_id` | the 16-byte prepare identity |
| `account_id` | equal to the prepare account |
| `snapshot_ts` | 12-byte MatrixOne statement snapshot |
| `schema_digest` | SHA-256 of the deterministic `NamedStruct` |
| `capability_hash` | exact negotiated hash |
| expiry | future and within MatrixOne's signed timestamp range |

Sirius and the sidecar reject identity, schema, capability, expiry, duplicate,
and unknown-field mismatches before binding the query-local `mo_stream_scan`
view.

### 7.5 Native input `DoPut`

Every input is attached before `DoGet` starts. The command descriptor contains
the ticket and `stream_ref`. The server first returns a `ready` acknowledgement
with zero counters. Data frames use `FlightData.app_metadata`; Arrow data bodies
and schemas are forbidden on this stream.

The native envelope is:

```text
offset  size  value
0       4     "MOB1"
4       2     little-endian codec version 1
6       2     zero flags/reserved
8       8     non-zero contiguous sequence
16      8     payload byte length
24      N     canonical MatrixOne Batch.MarshalBinary payload
```

One payload is at most the negotiated input limit and never more than 4 MiB.
MatrixOne splits a larger scan batch at row boundaries; a single row larger
than the limit is rejected. The sidecar validates vector count, physical types,
row lengths, attributes, null maps, areas, metadata, sequence, and trailing
bytes before publishing the frame.

MatrixOne computes `MarshalBinarySize`, allocates the final header-plus-payload
frame from the query mpool, and calls `MarshalBinaryTo` directly into its payload
region. It does not create a separate marshalled payload and then copy it into a
frame. The frame remains charged until the acknowledgement or terminal send
error; the gRPC transport may retain at most one additional bounded send copy.

The server has one input slot per read. One Sirius source task calls
`next_batch` once and copies that frame directly into one final host
representation, capped at 64 MiB after constant-vector expansion. Construction
of that owned representation is the acknowledgement point. MatrixOne may then
send one subsequent frame into the sidecar slot, where its publisher blocks.
That frame is both the one unacknowledged Flight frame and the one prefetched
sidecar frame; those limits describe the same bytes and are not additive.
The current Sirius source claim and host reservation remain owned by the
published representation until synchronized H2D conversion succeeds. Releasing
that claim does not schedule a continuation; existing downstream demand must
reach the source before it can pull the prefetched frame.

MatrixOne's `NativeInput.Send` is the synchronous output callback used in place
of the ordinary result writer. A missing acknowledgement blocks the callback;
the same bounded `PipelineEdge`, connector spool, and pre-`ChildrenCall`
capacity check used by native queries then restrain the storage readers. Scan
DOP may retain its ordinary bounded read-ahead, but only one unacknowledged
Flight frame exists per input. The cumulative acknowledged batch, row, and byte
counters must exactly match both endpoints.

Producer EOF receives a final `complete` acknowledgement only after consumer
EOF. If the plan prunes a read or result completion makes further input
unnecessary, the sidecar returns `complete + not_needed` with exact counters.

### 7.6 Native result `DoGet`

`DoGet` claims the ticket once. Flight emits its mandatory empty Arrow transport
schema first. Every later `FlightData.data_header` contains one `MOB1` result
frame; data bodies, metadata, descriptors, and redirection are forbidden.

The sidecar splits GPU result batches at row boundaries so each payload is at
most `max_batch_bytes`. Result vectors are canonical flat MatrixOne vectors.
MatrixOne remarshal-checks the batch, rejects trailing or non-canonical bytes,
rejects non-flat/constant/dictionary result vectors, validates every physical
type and nullability field against the negotiated result schema, then gives the
batch to the existing result writer. It does not request the next frame until
the writer returns and the current batch is released.

## 8. Type compatibility

The first version deliberately supports the TPC-H family:

| Family | Input | Substrait/Sirius | Result |
| --- | --- | --- | --- |
| boolean | MatrixOne `bool` | `bool` | exact `bool` |
| signed integers | `int8/16/32/64` | `i8/16/32/64` | exact signed OID |
| unsigned extract | not admitted as a streamed physical input | semantic `i64` | checked conversion to MO `uint32` |
| floating point | `float32/64` | `fp32/64` | exact floating OID |
| strings | physical `char` or `varchar` | `VarChar` | negotiated `char` or `varchar` |
| decimal | MO decimal64 for precision up to 18; decimal128 for 19 through 38 | Substrait decimal | exact width/scale and checked result conversion |
| date | MO date epoch | Substrait Unix-day date | checked conversion to MO date |

Timestamp, binary/blob, JSON, arrays, UUID, enum, row-id, and any unlisted
physical type are rejected. Width, scale, charset, nullability, vector class,
and fixed physical size are part of validation, not advisory metadata.

## 9. State machines and terminal ownership

### 9.1 MatrixOne execution

| From | Event / linearization | To | Required side effects |
| --- | --- | --- | --- |
| native | stream admission succeeds | admitted | bind snapshot and create stream identities |
| admitted | exact prepare/schema succeeds | prepared | own ticket and cancellation identity |
| prepared | every input returns `ready` | attached | no result worker has started yet |
| attached | `DoGet` claims ticket | running | start result worker and local scan producer |
| running | result EOF | result-complete | retire/interrupt all producers, join producer |
| any prepared/running | error, cancel, timeout, panic, shutdown | cancelling | abort inputs, interrupt `DoGet`, send `CancelExecution` |
| result-complete/cancelling | sidecar says `quiesced` or `not-found` | quiesced | sidecar worker and handlers are no longer owners |
| quiesced | local producers joined and cleanup succeeds | terminal | release query/snapshot resources |

The result EOF is authoritative success. It retires an input even if the sidecar
pruned it before its first data frame. `Retire` has a cancellation path
independent of the input mutex so it can interrupt a blocked attachment or
acknowledgement.

If prepare may have succeeded but its ticket is unavailable, MatrixOne cancels
by idempotency key. If quiescence or release cannot be proved, the execution is
retained by the reconciliation owner, which retries with bounded exponential
delay. Snapshot resources are not released before quiescence.

### 9.2 Sidecar execution

| State | Accepted events | Terminal transition |
| --- | --- | --- |
| preparing | capacity admission, same-idempotency replay, or cancellation identity | publish one ticket with one capacity lease, or roll the lease back |
| prepared/unclaimed | attach inputs, `DoGet` claim, cancel, deadline | cancellation releases resolutions without starting work |
| claimed/running | input frames/EOF, result reads, cancel, deadline | worker records success/failure/cancel and becomes quiescent |
| quiescent | input-handler detach | remove ticket/idempotency record and release its capacity lease after handler count reaches zero |

`CancelExecution` first publishes cancellation to every input, then interrupts
Sirius and the DuckDB connection, then waits for both worker quiescence and zero
active `DoPut` handlers. Multiple cancellation callers share one serialized
worker join. Deadline reaping and server shutdown use the same terminal path.

### 9.3 Sirius execution

Sirius permits `PREPARED -> RUNNING -> SUCCEEDED|FAILED|CANCELLED` and
`PREPARED -> CANCELLED`. The transition to `RUNNING` is single-use. All terminal
paths release query-local resolved views and stream-source references. The
actual backend is marked `SIRIUS_GPU` immediately before execution; successful
completion without a backend mark is invalid.

For each `StreamRead`, Sirius permits
`idle -> active -> published-await-H2D -> idle` and
`idle|active|published-await-H2D -> exhausted|cancelled|failed`. An atomic claim
grants one source task the `active` state. Construction of the final host
representation acknowledges its one wire frame, but publication does not
release the claim. Synchronized H2D conversion releases the representation's
host reservation and source claim without scheduling a continuation. Only
downstream task demand may claim the next generation. EOF marks `exhausted`;
failure and cancellation admit no new generation.

Each GPU pipeline task permits
`acquired -> reservation-attached -> processing -> quiescing -> completed` and
`reservation-attached|processing -> quiescing -> retryable-oom|failed`. The task
owns its input processing handles, output under construction, reservation
attachment, and borrowed stream through `quiescing`. Success and every exception
path synchronize the stream before any of those owners unwind. A clean OOM may
transfer the still-owned input to one bounded retry generation. Any CUDA launch,
invalid-device, illegal-address, or synchronization failure is terminal; the
stream is discarded instead of returned to the pool, and Sirius publishes one
process-fatal GPU notification to the sidecar runtime.

Error precedence is deterministic. If quiescence succeeds, the original
operator error is reported. If quiescence also fails, the synchronization error
is the terminal class and the original error is retained as bounded diagnostic
context. Only an original `rmm::out_of_memory` with successful quiescence enters
the existing ten-attempt retry budget; every other original or quiescence error
has zero retries.

Partition metadata (keys, casts, partition count, and routing) is frozen before
task publication. Each partition task owns its cast columns, libcudf result,
offsets, output batches, and reservation-tracked allocator state. Every libcudf
allocation receives the task's explicit stream and memory resource. Two tasks
may enter one physical `PARTITION` stage concurrently; neither observes or
mutates the other's state, and output publication happens only after its stream
has synchronized.

Pipeline input claim and `tasks_created` publication are one atomic transition
under the pipeline status mutex. Completion checks use the same mutex and
notify parent pipelines only after unlocking, so an empty input repository
cannot be mistaken for completion while a task is being constructed.

### 9.4 Process GPU health and recovery

The paired sidecar process has one generation-scoped GPU health state:

```text
HEALTHY -> SEALING -> TERMINATING -> process exit
```

The first `gpu_stream_quiescence_error`, illegal-address, invalid-device, or
other context/device-fatal CUDA classification performs the `HEALTHY ->
SEALING` compare-and-swap. That compare-and-swap is the admission-seal
linearization point. Before publishing cancellation, it makes `GetCapabilities`
and every new `GetFlightInfo` return Flight `Unavailable` with stable detail
`GPU_DEVICE_UNAVAILABLE`. An ordinary operator, validation, capacity, or cleanly
quiesced OOM-exhaustion error does not change process health and remains scoped
to its execution.

The sealing owner is the sidecar `flight_runtime`. It stores the bounded fatal
diagnostic, stops ticket admission, gives every prepared/unclaimed and running
entry terminal status `GPU_DEVICE_UNAVAILABLE`, wakes all input/result waits,
cancels Sirius/DuckDB work, and prevents `DoGet` or `DoPut` from attaching to an
old entry. Existing handlers observe that same terminal status. Multiple fatal
notifications share the first transition and cannot launch multiple shutdowns.

After sealing, a dedicated shutdown owner waits at most
`MO_SIDECAR_FATAL_SHUTDOWN_GRACE_MS` (default 5 seconds, maximum 60 seconds) for
workers and handlers to quiesce, shuts down Flight, then exits the process with
status 70 (the dedicated GPU-fatal process status) even if cleanup did not
finish. The OS
and CUDA context teardown are the terminal resource owner when the grace expires.
In-process device reset or executor reuse is forbidden.

The packaged deployment must use a restart policy with capped exponential
backoff no greater than 30 seconds. `/ping` is liveness only; the readiness probe
must call `GetCapabilities`, which becomes ready only after a new process has
initialized Sirius, its GPU executors, the sidecar capacity owner, and Flight.
MatrixOne treats disconnect or `GPU_DEVICE_UNAVAILABLE` as a terminal explicit
stream error, retires local producers, and uses normal reconciliation until the
old ticket is quiesced or absent after restart. There is no post-visibility
fallback.

## 10. Q1-Q3 resource closure

### Q1: destruction ownership

| Resource | Creator | Effective terminal owner |
| --- | --- | --- |
| MO stream identity and scan scope | MO compile | compile release after producer join |
| Flight prepare/ticket/idempotency | sidecar registry | terminal callback after worker and handlers quiesce |
| process input-capacity lease | sidecar ticket registry | terminal callback after worker and handlers quiesce |
| input frame slot | sidecar `stream_input` | consume/not-needed/cancel path |
| Sirius input reservation | Sirius scan task | transferred to its single host representation and released after H2D or terminal destruction |
| Sirius single-frame host data | GPU scan task | representation destruction after conversion or cancellation |
| GPU input processing handles and reservation | GPU pipeline task | release only after success/failure stream quiescence |
| borrowed CUDA stream | GPU pipeline task | return after clean quiescence; discard on synchronization/CUDA failure |
| partition temporary/output state | partition task, then destination repositories | task cleanup on failure; synchronized publication transfers outputs |
| process GPU-health generation | sidecar Flight runtime | first fatal CAS seals admission; one shutdown owner exits the process after bounded grace |
| GPU pipeline data | Sirius repositories | existing pipeline/memory manager cleanup |
| result frame slot | sidecar execution entry | `DoGet` read or cancellation |
| decoded MO result batch | MO result loop | per-frame deferred clean |
| query-local DuckDB views/transaction | sidecar execution entry | resolution destruction and connection rollback |

### Q2: wait-for closure

The possible waits are input-slot publication, input consumption, producer EOF,
result-frame publication, result receive, GPU stream quiescence, worker join,
handler join, and cleanup reconciliation. Every data wait observes
cancellation/not-needed/deadline. GPU stream quiescence is the existing Sirius
device-execution boundary: it ends in successful completion or a CUDA error;
the latter fails the query and removes the stream from reuse.
Cancellation does not take an input's data mutex before cancelling its gRPC
context. Sidecar cancellation wakes input and result condition variables and
interrupts Sirius/DuckDB. All RPCs are bounded by the minimum of caller,
request, lease-safe, and sidecar ticket deadlines.

Fatal-GPU shutdown does not wait indefinitely for the data path it controls.
The independent shutdown owner waits no more than the configured 5-second
default grace, then process exit makes OS/CUDA teardown the final release edge.

### Q3: accumulation bounds

| Accumulation | Hard bound |
| --- | --- |
| plan | 16 MiB |
| stream inputs per execution | 16 |
| MatrixOne reader read-ahead | existing native pipeline-edge/spool bound, proportional to scan DOP rather than table size |
| MatrixOne marshal plus gRPC-send overlap | at most two negotiated frames per globally admitted read; released after acknowledgement or terminal cancellation |
| unacknowledged Flight input / sidecar prefetch slot | one shared frame per read, at most 4 MiB payload |
| Sirius source task | one frame and one final host representation |
| Sirius expanded input representation | 64 MiB, with its host reservation retained through H2D |
| process-global streamed-input admission | fixed configured host envelope, default 2 GiB; active-representation capacity is reserved at startup and one worst-case transport slot per read before ticket publication |
| concurrent partition execution | configured GPU executor concurrency; every task has independent stream, reservation, and temporary state |
| result slot | one frame, at most negotiated result limit (default 64 MiB) |
| result schema | 1 MiB and 4096 columns |
| active tickets | configured limit, default 128 |
| reconciliation retry | one worker per retained execution, delay capped at 5 seconds, stopped by runtime close |

The sidecar runtime owns one process-global streamed-input host envelope,
configured by `MO_SIDECAR_STREAM_INPUT_CAPACITY_BYTES` and defaulting to 2 GiB.
At startup it reserves the maximum active Sirius representation capacity:

```text
active_representation_capacity =
    sum(configured GPU pipeline threads across all GPUs) * 64 MiB

per_read_slot_charge =
    2 * align64KiB(max_input_batch_bytes + 24-byte MOB1 header + 64 KiB transport overhead)

per_execution_charge =
    read_count * per_read_slot_charge
  + 2 * align64KiB(max_batch_bytes + 64 KiB result-transport overhead)
  + 2 * align64KiB(actual plan bytes + actual result-schema bytes + 64 KiB request overhead)
```

The GPU executor's bounded worker pools are the admission owner for active
source tasks, so no additional representation can exist beyond that startup
reservation. The factor of two in the read charge conservatively covers the
gRPC receive/decode allocation and the Arrow buffer retained as the sidecar slot
even when an implementation can share them. With the default 4 MiB frame limit
one read costs 8.25 MiB. The other terms conservatively cover simultaneous
encoder/Flight result buffers and request parse/retention overlap. A
one-GPU/four-thread deployment reserves 256 MiB for active representations and
uses the remaining 1.75 GiB for complete execution charges. Startup rejects a
configuration that cannot reserve one maximum 16-read execution using the
configured result limit and the maximum plan/schema limits.

Before publishing a ticket, the registry atomically reserves
`per_execution_charge`. Admission is non-blocking and returns
`RESOURCE_EXHAUSTED` from Prepare before a ticket is visible or a MatrixOne
producer starts. The charge therefore makes the configured active-ticket limit
a secondary bound rather than multiplying plan, result, or input capacity.
Idempotent concurrent Prepare shares one reservation.
Constructor failure rolls it back, and terminal removal releases it only after
the Sirius worker and all input handlers are quiescent. Current, peak, rejected,
and terminal-balance counters are required.

The MatrixOne process has at most one marshalled payload and one gRPC send copy
per admitted read; the same global read count bounds those copies, while native
reader batches remain charged to MatrixOne's existing allocation account and
bounded DOP pipeline. Sirius reserves the actual 64 MiB worst case before
reading a frame, moves that reservation into the published representation, and
releases it after synchronized H2D conversion or terminal destruction. The
fixed admission envelope is deliberately conservative and may reserve capacity
that is not simultaneously used; it is capacity admission, not a table buffer.
There is no multi-frame staging vector or table-sized wire queue. The ordinary
Sirius memory manager continues to own downstream GPU/operator state.

Measured SF10 Q9 input is 2,708,678,611 bytes, which is cumulative traffic and
must not be simultaneously resident in the input path. SF10 Q1 transfers
5,046,665,324 bytes cumulatively but continuously releases batches, proving why
the bound applies to retained live memory rather than total traffic. Acceptance
records the native-reader window, admitted read envelopes, unacknowledged
frames, prefetched frames, active source generations, retained host
reservations, rejected admissions, and terminal zero balances.

This is the stream-specific host envelope, not the complete Sirius GPU budget.
GPU operators remain subject to Sirius's independent reservation and usage
limits. The current allocator may eagerly create a pool at the configured GPU
usage limit during startup, so deployment must choose an absolute or fractional
limit that fits alongside the driver, sidecar, and concurrent query working
sets. Acceptance records configured, startup-reserved, peak-reserved, and
peak-used GPU bytes; a successful small query is not evidence that an SF10 join
or aggregate fits.

## 11. Security and failure containment

- Flight mTLS authenticates the paired CN; certificates and private keys never
  appear in plans, tickets, logs, or artifacts.
- Account and query identities must agree in prepare and every `StreamRead`.
- Stream/ticket identities are random and single-use; idempotency identities are
  deterministic only within the authenticated account/query pair.
- Capability and schema hashes prevent cross-version or cross-schema reuse.
- Unknown fields, malformed frames, overflow, non-canonical batches, oversized
  rows, unsupported vector classes, and endpoint redirects are terminal.
- Stream mode exposes no TAE path, manifest, object credential, or resolver
  endpoint.
- One sidecar is paired with one local CN. A sidecar is not shared across CNs.
- Ordinary protocol, operator, capacity, cancellation, and cleanly quiesced OOM
  failures are contained to the statement and its query-local sidecar entry.
- A context/device-fatal CUDA or stream-quiescence failure has process-wide
  blast radius by design: admission seals first, every ticket receives
  `GPU_DEVICE_UNAVAILABLE`, readiness fails, and the process exits for restart.

## 12. Compatibility, rollout, and rollback

The MatrixOne native batch representation is an internal ABI. Protocol v4 is
therefore supported only for the exact capability document and pinned
MatrixOne/Sirius/sidecar revisions validated together. It is not a promise that
an arbitrary older or newer MatrixOne batch codec is compatible.

Rollout order is:

1. merge and publish merge-ready Sirius and sidecar revisions;
2. deploy the paired sidecar and verify capability negotiation while stream mode
   remains unused;
3. configure the GPU-fatal non-zero restart policy, capped backoff, and
   `GetCapabilities` readiness probe;
4. deploy MatrixOne with stream mode disabled by default;
5. run the acceptance matrix on one local CN/sidecar pair;
6. permit explicit `SIDECAR STREAM` use only after approval.

Mixed CN revisions are allowed only because each CN negotiates with its own
sidecar and the feature is explicit. A mismatched pair rejects negotiation. A
rolling upgrade must update a pair as one unit before enabling stream mode.

Rollback disables explicit stream use and restores the previously pinned
sidecar image. Direct `TaeRead` and native MatrixOne execution remain available
and unchanged. Protocol or schema mismatch never triggers post-visibility
fallback.

A GPU-fatal restart is recovery rather than protocol rollback. The old process
generation never becomes ready again. Its tickets become terminal or disappear
with process exit; a newly initialized process has an empty ticket registry and
must pass capability/readiness checks before accepting work.

## 13. Observability

Acceptance and production diagnostics must identify one query without exposing
secrets:

- protocol/capability revision and outcome;
- prepare, first-input, first-result, quiescence, and cleanup durations;
- input batches/rows/payload bytes and result batches/rows/payload bytes;
- GPU backend evidence showing `GPU_MO_SCAN` and `SIRIUS_GPU` actually started;
- cancellation source and terminal outcome;
- process GPU-health generation/state, first fatal class, admission-seal time,
  affected ticket count, shutdown grace outcome, exit status, and restart count;
- active tickets, active input handlers, retained reconciliation owners;
- streamed-input admission capacity, current/peak admitted bytes, admitted
  reads, rejected bytes/reads, and terminal zero balance;
- per-read received, acknowledged, H2D-completed, and active-source counts,
  including the observed maximum deltas;
- per-GPU and per-stage active partition tasks, including the observed maximum
  concurrent partition count and discarded CUDA streams;
- MatrixOne allocation-account terminal snapshot;
- sidecar host peak, GPU peak/utilization, and storage/network byte counters.

Query, stream, and ticket values must be logged only as bounded opaque hashes.
Certificate material, SQL values, batch contents, object paths, and credentials
must not be logged.

## 14. Verification and acceptance gates

The feature is not merge-ready until evidence names the exact three revisions
and closes every row below.

| Contract | Required evidence |
| --- | --- |
| protocol and schema | cross-repository codec fixtures plus malformed/version/hash/schema/type/sequence/size controls |
| one-pass ownership | duplicate claim/ref/attachment rejection and exact cumulative acknowledgements |
| success lifecycle | producer join, sidecar worker quiescence, zero input handlers, zero retained execution after result EOF |
| early/pruned input | result EOF before first batch and `not_needed` after current/previous acknowledgement |
| cancellation | cancel while input ack is blocked and while result receive/write is blocked; bounded termination |
| injected failure | MO producer failure, sidecar input failure, Sirius consumer failure, disconnect, timeout, and retryable cleanup |
| slow consumer and full barrier | deterministic maximum-concurrency barriers prove native MO readers stop at their bounded pipeline window, one shared Flight/sidecar-prefetch frame is unacknowledged per admitted read, admission rejects the first envelope beyond capacity before ticket publication, and terminal admitted bytes return to zero |
| GPU task failure lifetime | a deterministic operator enqueues GPU work and then fails; input handles, reservation, and stream remain owned until quiescence, clean OOM alone is retryable, and a failed synchronization discards the stream |
| GPU-fatal blast radius and recovery | injected fatal notification races concurrent Prepare, prepared tickets, running input/result waits, and duplicate fatal reports; admission seals first, every entry receives `GPU_DEVICE_UNAVAILABLE`, readiness fails, one shutdown owner exits within grace, and a fresh process starts with no old tickets |
| GPU execution | query-scoped evidence records `SIRIUS_GPU` and `GPU_MO_SCAN`; two and four configured workers execute concurrent tasks in the same `PARTITION` stage with exact serial-equivalent fingerprints |
| correctness | typed native-MO equality for all 22 TPC-H SF1 queries on one reused process |
| SF10 correctness and decision data | typed equality for all 22 queries on one reused process; Q9 repeats ten times; record storage bytes, rows/bytes before serialization, transferred bytes, CN CPU/peak memory, sidecar host/GPU peak and utilization, time to first row, and total latency |
| partition safety control | repeated direct-TAE Q9 plus deterministic TAE- and MO-derived concurrent partition-content fingerprints prove correctness is independent of source representation and batch size |
| snapshot advantage | unflushed committed tail and visible tombstone cases equal native MatrixOne while direct `TaeRead` rejects them |
| static/build quality | MatrixOne SCA/UT/BVT/coverage; Sirius build matrix and tests; sidecar CUDA build/tests and review |

Functional lifecycle tests use deterministic barriers rather than sleeps.
Performance and capacity measurements run in the performance harness, not as
wall-clock assertions in ordinary unit tests.

## 15. Delivery pins

The final evidence record must replace `pending` with immutable merge-ready
commits:

| Component | PR | Approved commit | CI/evidence |
| --- | --- | --- | --- |
| MatrixOne | #27599 | pending | pending |
| Sirius | #6 | pending | pending |
| sidecar | #14 | pending | pending |

The sidecar submodule must point to the approved Sirius commit. The MatrixOne PR
body must link this design at its approved commit and the final evidence record.

## 16. Decision log

| Decision | Rationale |
| --- | --- |
| retain direct `TaeRead` | it remains the lower-copy fast path for eligible flushed tables |
| select MO native instead of Arrow record batches | avoids Arrow-Go and duplicate conversion while accepting an exact same-release ABI |
| strict explicit stream mode | never hides a protocol, security, or GPU failure behind native execution |
| one local CN per sidecar | keeps snapshot and cancellation ownership local; multi-CN fan-in is a separate design |
| attach all inputs before `DoGet` | prevents a pruned plan from retiring a ticket before its handler can attach |
| native MO output backpressure | synchronous `Send` reuses native output, pipeline-edge, connector, and reader flow control instead of adding a StreamRead-only controller |
| process-global pre-admitted execution envelope | bounds request, result, transport-slot, and active source-representation capacity across all tickets before producers start, without retaining cumulative table data |
| one unacknowledged sidecar prefetch slot | permits bounded transport overlap without a table-sized wire queue |
| one frame per Sirius source task | removes multi-frame coalescing and eager self-scheduling; H2D completion releases the claim without scheduling |
| exception-safe GPU task quiescence | success and failure synchronize before releasing task resources; only clean OOM retries, and a poisoned stream is never pooled |
| reentrant partition execution | immutable operator metadata plus task-local state preserves configured multi-stream concurrency and removes dependence on coarse scan batching |
| fail-stop GPU recovery | ordinary cleanly quiesced errors remain query-local; context/device-fatal errors seal the paired sidecar, cancel all tickets, fail readiness, and exit for bounded supervisor restart |
| bounded TPC-H stabilization on the current fork | delivers the required workload without claiming general unbounded streaming; migration to upstream partial-barrier scheduling is a separate effort |
| flat-only result vectors | prevents tiny compressed frames from expanding into unbounded MatrixOne result work |
| exact capability equality | rejects mixed ABI revisions before execution rather than attempting unsafe compatibility |

There are no deferred correctness or lifecycle decisions. Production enablement
remains blocked on design approval and the acceptance evidence in sections 14
and 15.
