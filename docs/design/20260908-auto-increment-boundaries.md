# AUTO_INCREMENT statement and arbitration boundaries

## Scope and decision

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
