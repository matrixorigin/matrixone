# Embedded Sirius execution

Design version: 3. E implements the owner-approved
[clean cutover design](sirius-embedded-cutover.md), which supersedes the
coexistence and recovery-migration plans in earlier revisions.

Enabled Sirius executes inside MO through the pinned C ABI and normal MO
readers. It is disabled by default and selected by the existing statement hint.
The numeric work in #28968 is complete; #28966 remains open through E acceptance,
merge and verified release availability. The abandoned sidecar repository is
outside this delivery.

## Data and service flow

CN validates configuration, opens the native engine, and publishes it only
after initialization succeeds. The compiler validates the hinted SELECT,
exports query-local named-table bindings, and gives each binding an MO reader
producer. Scan filtering, projection and fetch remain with MO. Above-scan
operators use the exact native semantic profile advertised by the pinned SDK.
The native query streams bounded results back through the existing MO result
callback. Explicit errors never replay SQL through another engine.

Only `enabled`, `native-config-path`, `gpu-streams`, `max-waiting-queries`,
`request-timeout` and `cleanup-timeout` are supported. Both TOML loaders reject
unknown keys in `[cn.sirius]`, including in disabled configurations. There are
no backend selectors, transport aliases, migration-only APIs or lease journals.

## Native ABI and ownership

The native header is `src/include/sirius_c.h` in Sirius; the Go adapter belongs
under `pkg/sql/compile/siriusbridge`. Use opaque engine/query/batch handles and
fixed-width descriptors with explicit lengths, never C++ classes or Go slices.

| Group | Required operations |
| --- | --- |
| Runtime | ABI/capabilities, create, health, stop admission, destroy after queries close |
| Query | Prepare once, start once, schema, incremental result retrieval |
| Input | Acquire capacity, fill native buffers, publish, finish, producer failure |
| Control | Cancel independently of data locks, join, release query resources |
| Batch | Release unpublished input or returned result lease |

Every C entry point catches exceptions and returns bounded, owned error
information. Distinguish unsupported plans, invalid arguments/bindings,
resource exhaustion, cancellation, execution failure and GPU unavailable.
EOF and input-not-needed are explicit states, not empty batches.

A successful input publication transfers ownership. Rejection leaves the
lease with the caller. Returned result buffers remain valid until explicit
release. Native code owns asynchronous buffers: ordinary Go memory must not be
retained by C++/CUDA after the synchronous call. Go fills leased C-owned staging
with bulk copies, without Flight frames or `Batch.MarshalBinary` on this path.
Go copies/consumes each returned native batch through the normal output path
and returns its credit only after consumption.

Go wrappers serialize incompatible operations, close once, stop and join their
cancellation callbacks, and never destroy a handle still used by a CGo call.
Cancellation must run concurrently with a blocked native input/result call.
No native callback may touch released Go state.

The embedding ABI is version 1. Do not bump it without a wire-contract change.
Keep existing standalone C++/DuckDB interfaces working; only the new embedded
path replaces materialized collection.


## Capacity and scheduling

| Resource | Initial default or limit |
| --- | --- |
| Active queries | One on the selected GPU |
| Waiting requests | 16; no readers or native input buffers while waiting |
| Reads per query | Existing maximum of 16 |
| Native input per read | 64 MiB including filling, queued and in-flight buffers |
| Source unit descriptors | At most 128 contributing MO batches |
| Coalescing target | 32 MiB, bounded by the 64 MiB hard limit |
| Result window | 64 MiB including results borrowed by Go |
| Host/GPU/spill capacity | Explicit finite configuration, checked by admission |

Account descriptor/bitmap bytes and relevant expansion before allocation.
Constant-encoded or variable-width data cannot bypass the expanded-source
bound. Split oversized batches at row boundaries; reject an individually
unrepresentable row. Account per-read reservations in a process-wide admission
envelope; a query whose minimum progress envelope cannot fit is rejected,
not admitted into a permanent capacity wait. Physical pool allocation and
logical query reservations must not count the same bytes twice.

Input back-pressure:

The compiler-facing input exposes `Acquire(ctx, payloadBytes)` and returns a
lease with `Capacity`, `Publish`, and `Release`. Acquisition must precede any
additional outgoing payload allocation, expansion, coalescing, or copying;
existing MO reader batches remain governed by their bounded pipeline edge.
Register lease cleanup immediately after acquisition. Successful publication
consumes native ownership and retains no Go buffers after the synchronous call;
failed publication leaves cleanup with the producer. A lease publishes at most
once. Release is idempotent, and a surviving handle after native release failure
transfers exactly once to query cleanup. Query cancellation uses an independent
native control path and interrupts blocked acquisition before joining producers.

1. MO acquires native capacity before allocation/copy.
2. Bounded coalescing creates a GPU source unit on downstream demand.
3. Filling, queueing and H2D ownership consume the same credit budget.
4. Credit remains held through asynchronous use and retry ownership.
5. If Sirius stops consuming, acquiring another lease blocks, MO's existing
   bounded output edge fills, and the reader stops after ordinary DOP-bounded
   read-ahead.

No second unaccounted repository or eager source-continuation queue is allowed.
Do not replace back-pressure with spillable-but-unbounded transport state.

Output uses an incremental sink with capacity-aware scheduling. A full result
window parks result-producing work without occupying every task-creator/GPU
worker. Include in-flight publication in capacity; wakeups are durable and
cancellation-aware. Never collect the full result and only then expose a stream.
MO reader prefetch remains bounded by its existing pipeline and the native
input credit window.


## Lifecycle, failure and recovery

The first owner is the Go query execution owner and its native query handle:

```text
prepared -> running -> draining -> closed
   \----------- cancel/error --------^
```

Cancellation, errors, deadlines, partial initialization and shutdown enter
draining from any live state. The first terminal failure wins. Early LIMIT and
pruned inputs retire unneeded producers with a success-valued signal.

Cleanup order:

1. Seal publication and wake input/output/admission waits.
2. Request both MO and native cancellation; stop and join MO producers.
3. Drain native scans, task creation, GPU tasks and asynchronous transfers.
4. Release query-owned repositories and credits.
5. Release MO scan/snapshot resources.
6. Release admission and destroy the query handle.

Waits must have an independently callable cancellation path; a data-path mutex
must not prevent cancellation from delivering the event that wakes that path.
Queued requests allocate no native inputs and can cancel before engine entry.
An error after schema/rows become visible never triggers native-MO replay.
Preparation failures do not start readers; cleanup failures never count as
safe fallback.

Only a cleanly quiesced failure permits runtime reuse. Fatal CUDA failure or
unprovable quiescence seals admission and requires fail-stop/restart of MO.
A timeout does not authorize freeing live buffers, resetting a device in use,
or starting another query on a poisoned runtime. This larger blast radius is
an accepted consequence of embedding and must be visible in readiness/health.


## Packaging, validation and rollout

See the [development guide](../guides/sirius-embedded.md) and
[bridge build contract](../../pkg/sql/compile/siriusbridge/README.md) for the
CPU, Sirius and combined cuVS build modes and guarded native provenance.
The native pin remains unchanged by E.

The current campaign accepts only complete current execution profiles and
supports the native MO oracle and embedded MO-reader routes. It runs all 22
canonical queries at SF1/SF10 and streams 1/2/4, with 744 timing/correctness
runs and 264 separate resource runs. Existing correctness, performance and
resource gates remain mandatory. Historical captures are immutable and use
their recorded tool revision for inspection.

The offline upgrade uses the previous release to stop Flight admission, drain
consumers and complete recovery before starting this release. The old owner
retains protection until quiescence. The new binary never interprets old state.
Rollback restores the previous binary with its matching configuration. No new
file-service operations, drain-verification APIs or compatibility bridges are
part of E. F and dependency/Docker/query-optimization work remain excluded.
