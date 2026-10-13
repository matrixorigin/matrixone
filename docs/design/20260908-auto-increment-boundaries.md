# AUTO_INCREMENT statement and arbitration boundaries

## Scope and decision

Latest follow-up: budgeted arbitration and allocation cost, based on reviewed
head `a669b5819c04d2f1273b188de6219336e910da07`. The new evidence and an unresolved
eager-allocation limitation are recorded at the end of this document. Earlier
results below describe their named revisions, not a claim that every behavior
in the module is now MySQL-compatible.

Follow-up to PR #28349 at `e798daaeb94b`. This is a focused correction of
existing planner/allocator contracts, not a new allocation service or wire format.

The allocator owns disjoint table-wide unit-step ranges. Session increment and
offset select members of a range; they must not change persisted table metadata.
The ordered INSERT IGNORE arbiter owns accepted-row primary keys and statement
candidate reuse. Its provenance column is execution metadata, not user data.

## Invariants and implementation

1. Every accepted row carries its final key through the main-table write and
   secondary-index write. Previously computed keys depending on a rewritten PK
   remain excluded by the existing eligibility guard.
2. A provenance column may be followed by computed unique-lock keys. At the
   arbiter's existing input PROJECT, explicitly place retained output columns
   first, then provenance, then key/conflict inputs. Remap downstream column
   positions and expressions together. Do not remove a guard without replacing
   its layout contract. No extra operator or per-row copy is needed.
3. The compiler's column-pruning boundary must map the final PK output ordinal
   too. Logical table ordinals are not physical output ordinals.
4. Exhausting a candidate buffer is not statement reset: retain its explicit-key
   lower bound and type until Reset/Free. These are statement invariants, not
   buffer occupancy state.
5. Non-default series allocate on demand but amortize the allocation transaction
   with the existing CountPerAllocate unit-span minimum. No new prefetch worker,
   session cache, synchronization, or configuration is introduced. Requests
   larger than the minimum retain checked rows*increment sizing. Ranges remain
   globally disjoint; gaps across CNs or session changes remain allowed.

## Ownership, unhappy paths, and cost

| Closure | Owner / termination | Risk / proof |
|---|---|---|
| Projection remap | planner; immutable executable plan | R2: composite/prefix/multiple indexes, unrelated CHECK, pruning |
| Candidate stream | one serialized arbiter; Reset/Free | R2: empty/exhausted/multi-batch, malformed metadata, reuse |
| Range allocation | existing column lock and allocator callback | R3: unchanged wait/cancel/retire graph; allocation count and race tests |
| BVT session state | modifying test restores entry value | R1: sequential files cannot inherit increment=6 |
| Multi-CN SQL | allocator reserves a range per CN | R2: series/start invariants, not globally consecutive IDs |

Planner work is O(columns + expression references) per plan. The change reuses
the existing projection rather than adding a runtime projection/copy. Candidate
buffer compaction retains only scalar statement state. Reservation uses the
existing configured block size, not a block multiplied by the session increment;
high increments still request enough span to guarantee progress. Default-series
behavior is unchanged. Non-default tiny writes trade legal unused IDs on CN exit
for fewer allocator transactions, as default writes already do.

Cancellation/error/retirement still terminate through the existing allocator
owner. No retry, goroutine, queue, or lock is added. Allocation overflow is checked
before publication. Candidate-vector cleanup remains owned by the operator.

## Validation plan

- Reproduce the composite/prefix planner failure on the original head with new
  typed tests, then use the same tests after the fix.
- Real SQL using the existing embedded public-path fixture: composite and prefix
  UKs, duplicates, explicit keys, NULLs, prepared reuse; assert exact base rows and
  indexed lookup agreement. Share startup, isolate tables and sessions.
- Test candidate compaction separately from Reset; cover explicit-key bounds,
  type mismatch, empty stream, and reuse without manufacturing a public SQL claim.
- Count allocation requests across repeated non-default one-row statements;
  verify every generated value and reuse the existing allocator overflow,
  concurrent-apply, retirement, and cancellation suite.
- Benchmark default/non-default one-row allocation before and after using an
  in-memory store; report allocations and store calls, not a claimed TPCC gain.
- Run affected owning packages, focused race plus allocator package race, and
  changed BVT case/result comparisons. Record unavailable topology separately;
  a local single-CN pass is not multi-CN proof.

## Non-goals

No gapless global sequence, allocator rollback, MySQL lock-mode implementation,
new protocol revision, or expansion of generated-column/FK compatibility.

## Accepted-row index maintenance boundary

The follow-up review reproduced a violated consumer contract: FULLTEXT and
MASTER maintenance materialized the pre-dedup image, while ordered IGNORE later
changed an accepted row's PK. The base table used the final PK; index postings
used provisional PKs and could even represent rejected rows. Exact-base and
head public SQL distinguish this regression from legal auto-increment gaps.

For INSERT IGNORE, move the existing shared SINK after all PK/UK arbitration,
not just after the auto-increment special case. Describe the row image explicitly
when constructing that SINK: a DEDUP join's first child is not necessarily the
incoming row. Retain computed lock columns alongside the table-column prefix,
including matching SINK_SCAN metadata, and retag downstream index expressions.
All synchronous maintenance consumers use this one accepted image; no source,
allocator, arbiter, or index expression is evaluated a second time.

Ordinary INSERT/LOAD, ODKU, and tables without synchronous irregular indexes keep
their existing plan shape. No extra SINK, state, protocol, goroutine, retry, or
per-row copy is introduced. A single-key DEDUP root needs a pass-through PROJECT
to expose its accepted payload to SINK column pruning; the coordinated arbiter
already exposes that projection. The moved SINK holds only accepted rows, so
duplicate-heavy input avoids unnecessary tokenization/index expansion.

Retaining lock keys can widen the shared row on all-accepted input; it preserves
the existing computed values instead of evaluating keys again after arbitration.
This is not a claim of zero overhead or measured SQL throughput improvement.

The earlier review checked allocation and base/regular-index writes but missed
the independent maintenance readers. The review unit is now the complete
accepted-row producer/consumer graph, not the allocator alone. Typed plan tests
check downstream placement, single evaluation, connected SINK consumers, and
auxiliary schemas; real SQL checks the base/index association and absence of
orphan or rejected postings, not only base-table counts.

### Follow-up ownership and liveness audit

| Boundary | Invariant and closure |
|---|---|
| Row production (Q1) | One PRE_INSERT/arbiter producer; existing SINK broadcasts the same accepted image to the base, regular indexes and synchronous maintenance. No added allocator or ownership state. |
| Fanout termination (Q2) | No back-edge from a consumer to the source. Existing dispatch/merge context cancellation, terminal signals and Reset abort/deferred cleanup remain the termination owners. Empty, all-rejected, rollback and multi-batch SQL complete. |
| Retained data (Q3) | No new growing container or materialization stage; the existing spool/backpressure lifecycle is retained. Accepted rows only enter maintenance; auxiliary lock-key width is the explicit cost. The existing arbiter's input-dependent memory bound is not changed. |
| Other paths | Plain INSERT/LOAD and ODKU stay at their original sharing boundaries. No synchronous index means no added sharing. No new catalog/wire/native contract, asynchronous-index dispatch or transaction retry policy. |

### Follow-up validation on f86f721d plus this change

- The original three-row FULLTEXT/MASTER counterexample failed at e5618d9 and
  passed on its exact main base fdc1e0c. It now passes with the correct accepted
  PKs, and rejected tokens have no postings.
- Full `pkg/sql/plan`: passed (4.17s), including the new structural/pruning tests
  and existing ordinary INSERT, ODKU and RETURNING coverage.
- `TestIssue28349AutoIncrementPublicPaths`: passed (12.56s test / 13.77s package).
  New scenarios cover FULLTEXT + MASTER with composite UK/CHECK, hidden posting
  IDs, empty/all-rejected input, prepared rebind, rollback, failed INSERT,
  subsequent UPDATE/DELETE, manual real PK and hidden-PK/UK controls. IVF entry
  PKs **and vector values** are compared with base rows rather than trusting
  a nearest-neighbor query that could fall back to a scan.
- The same fixture's 40,000-row INSERT SELECT accepts 20,000 rows across batches;
  exact ID/UK/payload agreement, both indexed lookup counts, no rejected words,
  and no orphan postings passed (0.21s subtest including DDL and checks). This
  adds no cluster startup and is not a before/after throughput benchmark.
- Same-instance mo-tester normal comparison, twice: auto_increment **498/498**
  and system_variables **177/177**, with table cleanup asserted after each pass.
- Focused `-race -run '^Test(DispatchReset|ConnectorReset|MaterializedSinkScanReset)'`
  passed in dispatch, connector and merge, exercising existing error/full-channel
  terminal handling and reader cleanup. No lifecycle implementation was changed.
- Prior allocator/arbiter race and benchmark evidence below remains applicable:
  those implementations and native dependencies are unchanged by this follow-up.
  Full SCA, compose CI, TPCC and mixed-version topology were not rerun.

## Validation results (2026-09-08)

Environment: macOS arm64 / Apple M4, Go 1.26.4, fresh native artifacts from
`make cgo`; package commands use `.agents/skills/mo-dev/scripts/mo-cgo-test`.

- The new planner regression failed on the original head for all three shapes
  (composite UK, prefix UK, composite UK with CHECK) with the reported provenance
  error, then passed with the fix.
- Full normal packages: `pkg/incrservice` (5.19s),
  `pkg/sql/colexec/preinsertunique` (0.84s), `pkg/sql/plan` (3.93s).
- Full race packages: incrservice (7.25s), preinsertunique (1.96s).
- `TestColumnCacheConcurrentSessionSeries`: focused race elapsed 0.04s;
  adaptive stress capped at 100 repetitions, passed in one process. Its oracle
  checks distinct IDs, per-writer residue/progress, and released vector memory.
- `TestIssue28349AutoIncrementPublicPaths`: real SQL with two explicit CN
  endpoints; composite/prefix UK, CHECK, base rows/index lookups, UPDATE, prepared
  reuse, session series, creator/writer isolation, and REPLACE LAST_INSERT_ID.
- mo-tester normal comparison (`-n -g -o` semantics), same embedded instance:
  `system_variable/system_variables.sql`: 177/177 twice;
  `auto_increment/auto_increment.sql`: 473/473 twice. Includes force-index
  queries over composite and prefix UKs and the multi-session DDL cases.
  The local tester connects to CN0; explicit CN0/CN1 coverage is in the public
  Go test. This is not a claim that the entire compose CI suite was rerun.

Benchmark command: `-run '^$' -bench '^BenchmarkColumnCacheStatementSeries$'
-benchtime=300ms -count=3 ./pkg/incrservice`. CountPerAllocate=10,000:

| Session increment | Allocator requests / row before | After | Measured per-row time after |
|---|---:|---:|---:|
| 1 | 0.000099 | 0.000099 | 1.12–1.21 us |
| 3 | 1 | about 0.000306 | 3.35–3.67 us |
| 64 | 1 | about 0.00655 | 73–76 us |

The pre-fix non-default cases cost about 11ms per row in this microbenchmark.
It uses the actual allocator scheduler with an in-memory store, so batching
wakeup cost is included but SQL/storage/client work is not. These figures prove
amortization, not a TPCC throughput improvement or a production latency promise.
Default-path allocation frequency and zero per-row heap allocations are unchanged.

No full SCA/CI wait, mixed-version topology, or TPCC benchmark was performed for
this follow-up. No new wire fields, catalog representation, or native code changed.

## Follow-up: bounded arbitration, exact key identity, and hot-path cost

The module-level review found that accepted keys and rejected candidates have
different growth shapes. Compressing the latter does not bound the former.
With nullable secondary UKs, the final-PK Go map was the only input-sized set;
it bypassed the query mpool even though Reset eventually made it collectible.
This was an admission failure, not a post-statement leak.

### Ownership and publication

| Layer | Contract | Implementation / terminal owner |
|---|---|---|
| Session and plan | Provenance and session series stay statement-scoped | Existing frontend snapshot, codec/version gate and eligibility guards; unchanged in this follow-up |
| Table reservation | Disjoint ranges; only positive manual IDs advance the positive cursor | Fix signed-negative-to-uint64 conversion before `skipped.updateTo`; retain allocator locks, callbacks, epochs and bounded retry policy |
| Ordered arbitration | All constraints agree before accepting a row; equality uses final PKs | Replace the unaccounted Go map with the existing mpool-backed numeric table; secondary UK ownership remains unchanged |
| Retained candidates | Capacity admitted before growth; no interior allocation pointer is freed | Off-heap scalar runs, in-place compaction, retained original base pointer; Reset/Free releases the allocation and statement fence |
| Output and result | Borrowed input is immutable; final PK built once; incomplete output never escapes | Reuse off-heap output vectors, append the final PK directly, copy selected non-PK columns once, publish first generated ID once after the complete batch succeeds |
| Base and indexes | Every consumer sees the same accepted row image | Existing post-arbitration SINK and cleanup protocol; no new source evaluation or fanout |

The integer table stores the supplied 64-bit identity rather than a separate
raw key. Therefore the arbiter supplies an invertible xor-shift/odd-multiply
permutation (the same inexpensive mixer used by ASOF slots), not a potentially
lossy software hash. This preserves exact signed/unsigned integer identity on
all supported CPUs and spreads session strides. Only zero maps to zero; explicit
zero is kept in one scalar flag because the batch API reserves a leading zero
for automatic raw-key hashing. Reset clears both owners. No common hash-table
API or CPU-specific implementation is changed.

Const NULL vectors must be tested with `Vector.IsNull`, not only the null bitmap:
constant NULL is a separate representation. NULL secondary keys never acquire
uniqueness ownership, for both ordinary and ordered IGNORE arbitration.

### Unhappy-path audit and decisions

| Audit | Closure and evidence |
|---|---|
| Q1: allocation ownership | Prepare failure frees partial tables; Call failure returns no batch; Reset/Free clears tables, zero-key state and candidate storage. Output buffers retain capacity only until reuse/Free. Tests assert pool usage returns to zero and a new statement accepts old keys again. |
| Q2: waits and cancellation | The arbiter stays serialized and adds no wait, lock, channel, RPC, goroutine or logging. The allocator's existing cancellation/retirement graph is unchanged. Race, cancellation-to-Reset, multi-batch and real SQL tests complete; this is not a universal no-hang guarantee. |
| Q3: retained growth | Accepted PKs, fragmented candidate runs and output vector data now enter mpool admission. Growth can return the normal capacity error before publication. Hash resize temporarily needs old plus new capacity; rejecting that peak is intentional. Memory remains proportional to accepted distinct keys and fragmented runs, not constant. |
| Publication failure | Failure after PK construction but during payload allocation leaves the input and statement result unchanged. The incomplete batch stays private and cleanup permits reuse. |
| Compatibility | No new protocol/catalog/native/config contract. ODKU allocation-account activation and non-reordering semantics are unchanged; IGNORE output data now also obeys the existing pool cap. |

Decision log:

- Reuse the existing numeric owner instead of adding a bespoke set or accounting
  only the logical Go-map payload; actual capacity and resize peak must be admitted.
- Return candidates by value and construct the final PK directly. Do not replace
  one per-row map with another retained per-row object buffer.
- Unit-step ranges align directly to the requested residue. Preserve the existing
  congruence solver for non-unit metadata and checked uint64 bounds. No allocation
  or allocator request is added by this arithmetic fast path.
- Do not claim SQL throughput from allocator-request counts or Go `B/op` alone.
  Go allocation volume and admitted live native capacity are different metrics.
- The rejected-positive-explicit-ID limitation below remains open. The negative
  manual-ID fix does not solve it, and passing budget/index tests does not prove
  general acceptance-aware allocation.

### Regression and performance evidence

Environment: Go 1.26.4, darwin/arm64, verified same-source CPU CGo artifacts,
`GOWORK=off -mod=readonly` through the repository wrapper. Baseline is `a669b581`;
the PR base/merge-base is `0b195e4f5a23c06a40552a8db3e7fdf182979a3a` (main).

- Unfixed regressions: the nullable-UK final-key budget, fragmented-candidate
  budget and Const NULL cases failed for the intended missing-admission/NULL
  semantics; negative/manual sequence controls exposed the uint64 conversion.
- Added coverage: all eight integer types, explicit zero, signed minima/unsigned
  maxima, wide/strided exact-key identity, empty/exhausted stream, in-place
  compaction after partial consumption, capacity failures at Prepare/growth/output,
  cancellation cleanup and reuse. Assertions use rows/types, typed capacity errors,
  unchanged borrowed input, result publication and final zero pool usage.
- Full normal incrservice, preinsertunique, preinsert, plan and process passed.
  Final numeric-identity coverage rerun passed: preinsertunique **85.4%**, up from
  **72.7%** at the baseline. Incrservice coverage is **79.3%**, from **79.2%**.
- Full final-code race: incrservice **6.215s**, preinsertunique **2.049s**,
  preinsert **1.992s**. Named concurrent-session test: measured T=0.04s, B=30s,
  capped N=100, passed in one process. No sleep-based regression was added.
- Real two-CN public fixture passed (**13.51s** test): existing index/rollback/
  prepared/multi-batch oracles plus ordinary INSERT and INSERT IGNORE with
  negative manual IDs preceding a later positive ID, at increments 1 and 3.
  The subtest restores the session setting with an independent cleanup context.
- Normal mo-tester comparison on that test-owned instance passed twice:
  `auto_increment.sql` **508/508** each, with no ignored/abnormal statements,
  no golden generation and table cleanup asserted between passes. Local BVT
  uses CN0; the public fixture separately uses CN1. Local harness/configuration
  is not delivered. Full compose/SCA/TPCC and mixed-version topology were not run.

Benchmark command for both revisions: `GOMAXPROCS=2 mo-cgo-test -p=1 -run '^$'
-bench '^BenchmarkInsertIgnoreAutoIncrementArbiter$' -benchtime=300ms -count=3`.
One operation includes Prepare, 32 batches of 1,024 input rows, and Free; source
construction is excluded. Medians below are per statement. Native pool peak is
also reported so moving ownership out of the Go heap is visible.

| Workload | Go bytes before -> after | Go allocations before -> after | Pool live peak before -> after |
|---|---:|---:|---:|
| All accepted, distinct UK | 9,093,593 -> 795,697 | 34,685 -> 268 | 3,178,496 -> 3,678,400 |
| All accepted, NULL UK | 9,093,612 -> 795,712 | 34,679 -> 262 | 65,536 -> 1,614,016 |
| Duplicate dense (2,048 accepted) | 388,532 -> 21,349 | 2,695 -> 100 | 229,376 -> 230,080 |
| Explicit/generated mix | 6,601,405 -> 795,692 | 50,525 -> 268 | 3,178,496 -> 3,678,400 |

The final paired samples' elapsed medians were respectively 11.30 -> 7.95ms,
5.81 -> 3.63ms, 3.79 -> 3.56ms and 12.26 -> 7.91ms. Earlier samples were noisy
and the shared machine had 6.6-7.5GiB swap in use and unrelated VM/build work.
Allocation reductions are the robust result; these timings do **not** establish
a production latency/throughput guarantee or prove absence of a regression.
The separate unit-step helper benchmark remains allocation-free and removes the
per-row modular inverse; non-unit controls and an independent enumeration oracle
cover the preserved path.

## Open limitation: a rejected positive explicit ID advances eager allocation

Status: **not fixed by this follow-up**. Independently reproduced at the exact
main base and reviewed head, with MySQL 8.0.44 as a control:

```sql
CREATE TABLE t(id TINYINT AUTO_INCREMENT PRIMARY KEY, uk INT UNIQUE, v INT);
INSERT INTO t(uk,v) VALUES(10,0);
INSERT IGNORE INTO t(id,uk,v)
VALUES(NULL,20,1),(127,10,2),(NULL,30,3);
SELECT id,uk,v FROM t ORDER BY v;
```

MySQL retains `(1,10,0),(2,20,1),(3,30,3)`. Both MO revisions return error 1690
for tinyint 128 and retain only the seed. With BIGINT/100, the last accepted ID
is 101 instead of 3. This is an intra-statement semantic difference, not a demand
for globally gapless IDs. The negative-ID conversion fixed above is a distinct
bug with a different witness.

Root boundary: `PRE_INSERT` scans explicit positives, advances the range/cache,
and materializes/range-checks generated values before PK/UK acceptance. The
downstream arbiter cannot recover discarded lower reservations or an upstream
overflow. Its current candidate reuse is safe only for candidates already checked
and locked by the existing pipeline.

The next implementation needs an acceptance-aware statement cursor over
non-rollbackable, table-owned reservations. Final-key assignment, accepted PK/UK
ownership and validation/locking must agree before emitting the row to the shared
index SINK. This is a proposed direction, **not an implemented or validated new
protocol**. Its acceptance matrix must include:

- ignored versus accepted explicit high IDs, both before/after generated rows;
- cross-row PK/UK conflicts where a rejected row must not reserve a different UK;
- tinyint/uint64 exhaustion, negative/zero controls and session strides;
- stale cached ranges, concurrent explicit writers on another CN, lock-triggered
  statement retry, cancellation, rollback and prepared/multi-batch reuse;
- exact base/index identity and allocator requests per block, with no per-row
  SQL or RPC fallback added merely to make the small example pass.

Do not roll back a shared allocator, swallow overflow, manufacture unprobed IDs
after dedup, or independently pre-dedup UKs and forget later PK rejection. Those
shortcuts change other rows' acceptance or bypass existing concurrency protection.
Closing this inherited allocation boundary requires a coordinated change beyond
the budget/negative-ID corrections above; it must not be reported as completed.
