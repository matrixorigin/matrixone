# Chunk-native Top-K

- Status: in progress. The design below was approved in the conversation before implementation.
- Design/implementation base: main `4b43f8c99bb22e6a35c389c93bf64c2dc043feaf`.
- Tracking: contributes to [#24097](https://github.com/matrixorigin/matrixone/issues/24097), following #28413; does not close the umbrella issue.
- Delivery: design and implementation in one new PR, branch `perf/chunk-native-topk`.

## Contract and scope

The existing chunked-column converter decompresses each chunk, concatenates its
vectors, serializes the full column, and copies that representation into cache
data before Top-K. Sharing in #28413 avoids duplicate conversions but not this
per-conversion reconstruction. The new path must compute the same results while
holding only one chunk at a time, plus bounded directory and O(K) ranking state.

Use existing `Lz4Chunked` extents only. Preserve the decoded whole-column cache
fast path, legacy encoding and missing/synthetic-column fallback, current
eligibility, distance metrics/ranges, NULL/non-finite behavior and equal-distance
ordering. Stream nil (all-row) or nondecreasing selections, including duplicates;
unsorted selections use the old path rather than changing tie order. Never
mutate caller selections or publish partial winners.

Explicitly excluded by the user: decoded-buffer recycling. Also excluded: new
pools, format/write-policy changes, data rewrites, broader INCLUDE integration,
parallel chunk reads, new pushdown modes and distance-kernel changes.

## Read and ranking flow

Keep `ioutil.LoadColumnDataByTopN`'s signature and delegate to
`objectio.ReadColumnTopN` with the same inputs/results. Resolve metadata once.
Legacy, synthetic, unsupported and unsorted cases use `ReadOneBlock` with its
existing scoped-sharing option and `SearchCachedVectorTopN`.

For an eligible chunked column, use `FileService.ReadCache` for the full decoded
extent first. A hit stays borrowed under its IOVector and uses the whole-vector
Top-K path. A miss reads the existing prefix/directory representation and then
only selected chunks, in physical row order. Do not use the existing ranged
window materializer's returned-data ownership pattern: it releases its IOVector
before returning retained data and therefore cannot transfer a sharing ticket.

Each actual chunk uses the standard validated-column converter, expected decoded
size, validation hook and #28413 sharing descriptor for its physical extent.
Preserve caller cache flags and add `SkipFullFilePreloads` for range reads. No
FileService interface or shared-decode state-machine change is needed. Accumulate
logical bytes/cache provenance only for reads actually executed.

Validate the complete directory using existing bounds and require its row count
to match block metadata. Calculate absolute offsets in widened arithmetic.
Check each consumed chunk's decoded byte length, payload row count, vector kind
and cross-chunk type consistency on cache/shared hits as well as misses. Payloads
of unselected chunks are not fetched or validated. Failures abort without a
whole-column retry after streaming starts.

Factor the existing ranking loop into one private incremental accumulator used
by `TopNVector` and streaming. Initialize the distance function, bounds and local
candidate heap once. Keep the reader-wide `DistHeap` across chunks. Each candidate
carries a block-global row and original selection ordinal; all-row iteration
does not allocate an all-row index slice. Keep at most K local candidates and
finish once with the existing cutoff and ordinal ordering. Do not concatenate
per-chunk winner lists or stop when the heap first fills. Failed-query ranking
state is discarded, not retried.

## Ownership, waiting and bounds

| Audit | Contract |
|---|---|
| Q1: resource ownership | Each iteration owns one IOVector. Free its borrowed vector view before releasing the complete IOVector on success, error or cancellation. Only scalar/owned results escape; decoded storage is never overwritten for reuse. |
| Q2: termination | Check caller cancellation at read/compute boundaries. Retain FileService's existing bounded merge/sharing behavior. No added locks, goroutines, retry loops or shutdown waits. |
| Q3: growth | One decoded chunk (format limit 8 MiB), one compressed chunk buffer, directory bounded by BlockMaxRows, and O(K) ranking state. Cache and other readers' shared references retain their independent existing budgets. No idle pool is added. |

The per-chunk bound applies to streaming, not to the explicitly preserved legacy,
unsorted or already-cached whole-column paths. Directory buffers are released
after parsing; directory metadata is bounded by the existing maximum block rows.

## Alternatives, compatibility and rollout

The existing full-column path retains compatibility but performs avoidable
reconstruction. Window materialization still allocates/copies vectors and cannot
keep a single Top-K state across windows without another layer. The selected
scoped chunk path reuses the existing encoding, validators and sharing instead.
Buffer recycling would require a separate ownership/budget decision and is
deferred. Sorting arbitrary selections would change tie winners and is rejected.

There are no SQL, catalog, protobuf, configuration or on-disk changes. Keep the
existing writer protocol gate (version 29) unchanged. Readers select by extent
encoding; legacy objects are not rewritten. Rollback restores whole-column reads
without migration. All reads remain under the same FileService/path ownership;
there is no new cross-tenant or cross-CN data channel.

Cold reads may issue more range requests; record that cost rather than claiming
all workloads get faster. A selection touching every chunk still decompresses
every chunk. This stage targets copying and transient memory, not a guaranteed
reduction in reclamation syscalls or Wiki-10M recovery.

## Validation and delivery gates

Use tiny valid multi-chunk fixtures for UT, not large rows just to cross the
writer threshold. Cover cross-chunk displacement/ties, prepopulated heaps,
selection variants, invalid coordinates, NULL/non-finite values, bounds and
supported vector types, with explicit expected results and differential checks.
Test malformed directories/payloads, wrong length/rows/types, error after a prior
successful chunk, cancellation, Close, cache pressure and concurrent independent
queries sharing one chunk. Assert unchanged inputs and complete resource release.

Validate persisted ObjectIO and block-reader consumers (including tombstones and
residual selections), then owning packages, applicable race/stress, coverage and
full SCA. Reuse existing IVF BVT for unchanged SQL behavior; do not add a large
BVT fixture. Any SQL performance fixture must verify actual extent encodings.

Compare five matched main/candidate samples. Hot-cache and legacy controls must
stay within 5% median regression. Demonstrate the streaming memory bound, absence
of full-column reconstruction and lower CPU on multi-chunk cache pressure; record
allocations/reclamation, range counts and cold-read latency. Temporary benchmark
hooks must not enter the final diff. No dataset download or remote dispatch.

Merge newest authoritative main immediately before each push to `aunjgr`. Keep
implementation content in the PR's commit-message section and measurements in
its validation section.

## Implementation evidence

The reader and shared accumulator are implemented without changes to FileService,
the encoding, writer rollout or native allocator. Whole-vector and chunked reads
use the same ranking loop. The permanent small fixtures persist valid tiny chunks
through ObjectWriter metadata, without changing production chunk-size constants.

| Change closure | Risk | Evidence |
|---|---|---|
| Incremental ranking | R3: hot path, cross-chunk reader heap | Golden cross-chunk ties/displacement/selections/ranges, all six vector kinds, existing TopN tests; 64 deterministic cases compared bit-for-bit with the exact main ranking function. |
| Chunk read/ownership | R3: scoped shared data, errors and Close | Whole-cache/legacy controls, directory/payload/row/type corruption, partial errors, cancellation, >4 GiB absolute offsets, skipped corrupt payload, concurrent different queries and Close; one outstanding read at a time and sharing reservations drained. |
| ioutil and block-reader consumers | R2: unchanged owned result interface | ObjectIO/ioutil and persisted block-reader suites, including existing tombstone/residual-filter mapping cases. |
| SQL contract | R2: unchanged consumer behavior | Existing vector_ivf_topk_consumers BVT passed twice, 58/58 statements per run, with database teardown checked after each run. No expected results regenerated. |

Normal package and race checks cover ObjectIO, ObjectIO/ioutil and blockio.
The sharing/Close test was measured at 0.04 seconds under race, then passed 100
repetitions in one process. Production SIMD-mode package checks are also run.
The BVT service was test-owned and stopped after validation. The tester's result
path parser cannot handle dots in parent directory names, so the unchanged
case/result pair was staged in a dot-free temporary path for comparison.

Five-run benchmark medians, Linux amd64, Go 1.26.4, GOAMD64=v3,
GOEXPERIMENT=simd, main `4b43f8c99b` versus the candidate:

| Workload | Main elapsed / CPU per read | Candidate elapsed / CPU per read |
|---|---:|---:|
| Whole decoded-cache hit | 333.9 / 347.8 us | 308.9 / 308.5 us |
| Legacy LZ4 under cache pressure | 649.0 / 650.5 us | 615.1 / 614.0 us |
| Four chunks, all rows, disk cache warm | 5.923 / 17.965 ms | 1.234 / 1.242 ms |
| Four chunks, selection in one chunk | 5.584 / 17.201 ms | 0.392 / 0.397 ms |
| Four chunks, local backing store without disk cache | 6.524 / 19.620 ms | 2.397 / 5.642 ms |

The fixture is 4,096 synthetic 768-dimensional vectors, split into four existing-
format chunks with both LZ4 and uncompressed payloads. The 8 MiB memory-cache
budget is pinned in pressure cases; the hot-cache control uses 64 MiB. Setup is
outside timed loops. CPU includes the process's GC work, so it can exceed elapsed
time. These are scoped ObjectIO measurements, not SQL QPS or Wiki-10M results.

For the all-row pressure case, Go allocation volume fell from 61.4 MB/read to
14.8 KB/read. The largest returned decoded allocation fell from 16 MiB to 4 MiB;
this is not a claim that total process RSS equals those amounts. Requested data
was essentially unchanged, but FileService.Read calls increased from one to six
(two directory reads and four chunks). Those counters describe logical calls,
not network requests; no remote-storage latency result is claimed. Both control
medians meet the no-greater-than-5% regression gate.

The main control used the unchanged whole-column read/converter and a temporary
copy of main's exact TopNVector function; shared helpers/native dependencies
were unchanged. That temporary control also supplied the differential oracle.
It was archived outside the repository and removed before final validation.
