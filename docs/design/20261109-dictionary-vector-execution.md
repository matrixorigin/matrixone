# Dictionary vector execution

Owner: #32379; coordinated tasks: #30845/#30850; parent: #30838.
Status: engine-wide scope and staged implementation approved by the requester on
2026-11-09. Infrastructure may qualify with no confirmed regression; producer
enablement requires a confirmed predeclared target-query speedup.
Risk: R3, core representation, ownership, public Go contracts and engine consumers.

## Problem, scope and invariant

Retained profiles show repeated gathers and copies around shuffle, joins and
scan filtering. Optimizing one copy kernel does not remove materialization. The
engine currently exposes flat and constant vectors; raw physical access occurs
in at least 137 production files. A general dictionary cannot be introduced by
aliasing a slice or changing one hot caller.

Precedent is DuckDB 1.5.6's pinned
[vector selection representation](https://github.com/duckdb/duckdb/blob/069cc9f9b5be802405797faecc284961b07c70ef/src/common/types/vector.cpp).
MO's existing pool, borrowed leases, row-specific provenance and prepared-value
contracts require an explicit adaptation rather than a copied C++ layout.

For every logical row, a dictionary must expose the same value and NULL/grouping,
string-domain/provenance and prepared-parameter state as the equivalent flat
vector, including repeated/reordered selections and constant payloads with
row-specific metadata. Physical backing remains immutable and accounted until its
last owner releases it. A source may be reused without changing published views.

The feature is an in-memory representation, not a SQL type or a persisted/wire
format. Keep original DECIMAL(15,2) TPC-H schema, join ordering/placement and cache
algorithms. Q15/Q20 plan rewrites, new codecs and decimal SIMD are separate work.

## Representation and Go contracts

Add DICTIONARY alongside FLAT/CONSTANT. A dictionary header owns references to:

1. one immutable flat or constant snapshot;
2. one immutable logical-row selection, shared across columns where possible;
3. the selected logical row count and allocation-account identity.

Selection entries are int64, matching existing selection APIs. Validate bounds
once when constructing an accounted selection handle. Composition maps through
an existing dictionary to its base and produces one mapping, never a chain.
Preserve order and duplicate rows. Empty selections retain no unnecessary data.
Identity/contiguous selections may use the existing safe reference/window form.
Logical constant row domains follow existing broadcasting rules; value index
zero is distinct from the logical index used for per-row metadata.

Dictionary IsConst is false unless construction can canonicalize the result to
an ordinary constant with uniform row metadata. Clear sortedness unless the base
is sorted and the mapping proves preserved order. Construct any logical bitmap
or sidecar needed by existing logical getters with the accounted selection; do
not allocate lazily inside a getter or expose an unselected physical bitmap.

Provide allocation-free ReadView resolution and typed fixed/varlen views. Resolve
base, selection and validity once per batch, with separate flat and selected
loops; do not turn every value read into a virtual getter. Public logical getters
resolve selections. Physical slice/area accessors remain physical-only and never
allocate secretly; Must accessors reject unsupported representation according to
their existing panic contract.

Dictionary construction and EnsureFlat/EnsureWritable accept the caller's pool
and allocation account. Materialize only the selected logical rows and required
varlen payload. Prepare allocations and metadata before publishing a state change;
failure leaves the original valid. Preserve existing flat/constant Length, Size,
Allocated and mutation semantics; distinguish logical payload accounting from
retained physical backing when adding dictionary metrics.

## Ownership, copy-on-write and bounds

Retain existing BufferLeases for immutable payload. For exclusively owned payload,
promote ownership into a shared immutable lease carrying its original deallocator
and allocation account; the source remains readable. Source mutation materializes
writable storage first. Snapshot mutable row metadata independently, so direct
metadata changes in the source cannot alter published selections. Freeze once per
source batch/vector and share that snapshot across its partition dictionaries.

Legacy aliases or unprovable owners use explicit accounted copying, not an unsafe
retained window. Copy-on-write applies to data, area and every owned metadata
backing; low-level mutation escape hatches must be classified in the consumer
matrix before producers can expose dictionaries to them.

Physical reservations belong to the backing owner, not each logical reference.
Selections and copied metadata have their own bounded accounts. Retaining a
small result never charges only that row fraction of a large backing. If pinning
cannot be admitted, use the existing materialization path; if its allocation
fails, preserve the query error rather than exceeding a budget. Do not add a
second cache or an unbounded flattened-result registry.

| Event | Ownership result |
|---|---|
| construct view | retain snapshot and selection after successful admission |
| compose view | retain one base; own composed selection; release temporary owners |
| source reuse/write | copy-on-write before mutation; existing views unchanged |
| flatten/write allocation fails | old representation intact; temporary allocations released |
| consumer finishes/early-stops | release its header, selection and backing references |
| cancel/error/reset | release partial publications and owned scratch once |
| final backing release | original allocator/account receives the physical bytes once |

No backing-release callback may reacquire a cache lock held by its caller. Reset
cannot reuse a generation while old consumers retain its mutable state. Shared
payload may outlive a source header; query/account lifetime must cover all holders.

Check index/count arithmetic and selection allocation before retaining or
publishing a new view. Snapshots cannot be placed in a cross-query cache; pooled
headers must clear all selection/base/generation references. Logical selection
never changes authorization, tenant identity or statement snapshot.

## Consumer and compatibility boundaries

Maintain an exhaustive consumer matrix: dictionary-aware, explicit normalization,
or proven unreachable. Include reads and writes, null/grouping/provenance access,
unsafe/CGo operands, hashing, aggregation, sorting/windows, serialization, spill,
frontend output, external interfaces and parallel/broadcast paths. Classification
is by contract and actual call path, not function spelling alone.

Consumers declare dictionary capability; unknown consumers default to flat.
Install local runtime normalization adapters only where a dictionary producer can
reach an unconverted consumer. Install them after local/remote pipeline decoding,
so no new transport opcode is needed. Normalization uses the destination's pool
and account, reuses bounded owned output, and cleans it on all terminal paths.
Do not add adapters or selection work to an all-flat path merely because the
types exist. Every published consumer receives its own mutable header.

Migrate expressions/filters, key preparation, hash probes, aggregate updates and
vector-copy operations to ReadView. Unsupported functions and native kernels
normalize explicitly at their caller boundary. Readability of dictionary inputs
does not imply permission to mutate their physical source.

Persisted vectors, spill files and network batches remain existing flat logical
vectors. Materialize before current serialization APIs; never emit DICTIONARY
class bytes or raw dictionary pointers. Older peers/plans therefore continue on
flat execution. Existing row-buffer formats historically accepted class byte 2;
their decoded values are materialized and normalize to FLAT, without creating an
in-memory dictionary or rewriting encoded bytes. Unknown class bytes remain
invalid on input. No schema migration is needed. Reverting producer enablement
restores ordinary execution while the neutral foundations can remain.

## Producer rollout and critical path

1. Enable eager scan/filter/projection selections only after compatible consumer
   paths and normalization boundaries exist. Direct projections retain snapshots;
   computed columns can remain flat in the same logical batch. Primary target Q12.
2. Enable local shuffle partition dictionaries over one immutable input snapshot,
   with shared bounded row mappings and existing queue/backpressure ownership.
   Preserve the existing whole-batch transfer route. Producers may advance without
   retaining mutable child buffers or adding an all-consumer acknowledgement barrier.
3. Enable residual-free unique INNER and probe-only SEMI/ANTI join output
   dictionaries. Probe-side selections compose; gather build-side payload when a
   single-base view cannot represent it. Other joins consume through migrated
   readers/normalization and keep existing output production. Primary target Q9.
4. Integrate reader filtering and selected payload construction using an explicit
   dictionary-output request on ReaderFilterRequest. Default remains flat. Reuse
   ReadWithFilter and its original-row selection mapping; materialize remaining
   payload once, or return selected views over safely retained decoded backing.
   Preserve residual evaluation/error order and existing standalone kernels.
   Primary target Q19. Block codecs can still decode every touched block.

The seven production PRs are early filtering; representation/ownership;
normalization/compatibility; consumer migration; scan producers; shuffle/join
producers; reader integration. The first three dictionary stages enable no
production producers. Draft development may stack on prerequisite branches before
merge, with parent dependencies and incremental diffs identified in each PR.
Production readiness and qualification still wait for prerequisites to merge;
one isolated candidate is measured against its actual main baseline.

## Alternatives and verification

Status quo plus faster gather kernels keeps the extra passes. An operator-local
selection envelope is smaller but leaves unrelated consumers/materialization
boundaries intact; the requester explicitly selected engine-wide scope instead.
Eager flattening inside physical accessors obscures allocation ownership and can
repeat work. Shared mutable views are incorrect under source reuse. The selected
immutable snapshot/read-view design makes both savings and costs explicit.

UTs use small independent logical-row oracles over flat/constant/dictionary
representations, composition, duplicates, NULL/grouping, inline/area strings,
sidecars, ownership, errors and cancellation. Exercise pool reuse, pressure,
evicted-held backing and failed materialization with deterministic controls.
SQL/BVT covers filter/projection, join variants, aggregates, sort/window and
spill/transport normalization with exact small expected rows. Run owning-package,
race, CGo and incremental static evidence according to the changed closure.

Flat/tiny/empty/constant/NULL/already-selected/unsupported routes are explicit
performance controls. All neutral infrastructure PRs require no confirmed
regression; enablement additionally needs a confirmed target-query gain at SF10
and SF100 memory profiles. Five independent pairs per memory/disk profile, cold
full suites excluded, all Q1-Q22 plus Total, instructions/CPU/work/retained memory,
Holm query checks and separate Total check. No SF100 spills or benchmark-time
data/result verification. Inconclusive gains do not pass. If current main no
longer exposes the primary target, stop for rescoping rather than pick a favorable
replacement after seeing results. Final all-query MO/DuckDB profiles assess the
combined result; individual percentages are not added into a predicted gain.

Preserve raw datasets and final profiles/symbols/results. Keep native seeds while
comparisons are active; at terminal completion remove only task-owned loaded
images, temporary clones and RAM disks. Use bounded model-free quiet-window
scripts, retaining all valid pairs and never retiming merely for a slower result.

Accepted boundaries: neutral foundations; engine-wide in-memory representation;
explicit legacy boundaries; unchanged wire/storage; SF10/SF100 acceptance; no
Q15/Q20 rewrite. Material deviations require review before implementation.
