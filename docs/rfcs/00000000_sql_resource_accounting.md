# SQL Resource Accounting

- Status: implemented on `refactor/sql-resource-accounting`
- Scope: CN-side SQL execution, statement trace projection, physical-plan diagnostics
- Related issues: #18449, #22263

## 1. Summary

MatrixOne previously assembled SQL resource statistics from unrelated counters
late in statement serialization. CPU-like time could become negative, retries
could erase earlier work, remote failures could lose counters, and the memory
field mixed allocation totals with a price-oriented estimate.

This change replaces that behavior with one typed accounting model:

```text
producer-local facts
  -> attempt summary
  -> execution summary
  -> statement root summary
  -> statement_info.stats / CU / physical-plan diagnostics
```

Accounting rules live in `pkg/util/resource`. Producers publish facts once;
serialization and Explain consume immutable summaries and do not recalculate
resources. The existing `statement_info.stats` column and table schema remain
unchanged. Version 6 extends the positional array from 11 to 17 values.

The implementation is a direct replacement. It contains no shadow accounting,
old/new selector, dual writer, or fallback formula.

## 2. Goals

1. Make every persisted quantity have one documented physical meaning.
2. Make active work non-negative by construction.
3. Preserve failed and retried execution work exactly once.
4. Carry local and remote execution facts through terminal paths.
5. Expose incomplete data explicitly through quality flags.
6. Record a useful statement-level memory peak without inventing byte-time or
   pricing semantics.
7. Keep hot-path overhead small and bounded.
8. Preserve the existing statement table schema and derive CU downstream.

## 3. Non-goals

This change does not provide:

- OS CPU time or scheduler attribution;
- a synchronized whole-cluster memory high-water mark;
- per-operator, per-pipeline, per-CN, or allocation-site memory attribution;
- a durable record when the process is killed before statement export;
- billing policy or a new CU pricing model;
- mixed-version CN accounting compatibility;
- fragment slots, memory-domain slots, async leases, or tombstones;
- a second persistence table or a new column schema.

Those features must be justified independently. They are not prerequisites for
useful statement-level accounting.

## 4. Terms and semantic contract

### 4.1 Usage

`resource.Usage` contains additive unsigned facts:

| Field | Unit | Meaning |
| --- | --- | --- |
| `ExclusiveActiveNS` | ns | measured local wall interval minus waits measured inside that same interval and minus explicitly owned child-call wall |
| `WaitNS[kind]` | ns | producer-local blocking time by lock, filesystem, IO merger, remote, output, or other wait kind |
| `S3Requests[op]` | count | physical S3 operations by HEAD, GET, PUT, LIST, DELETE, DELETE_MULTI |
| `S3ReadBytes` | bytes | physical bytes accepted from object storage |
| `S3WriteBytes` | bytes | physical bytes written to object storage |
| `ClientEgressBytes` | bytes | application-protocol bytes accepted by the protocol writer |
| `SpillBytes` | bytes | bytes written through spill paths |

`ExclusiveActiveNS` is an active-work estimate, not OS CPU. Parallel producers
are additive, so statement active work may exceed statement wall time.

The local active calculation is:

```text
exclusive_active = local_wall - local_wait - owned_child_wall
```

The subtraction is valid only within one ownership interval. If the terms are
inconsistent, the producer contributes zero active time and sets
`QualityInvariantFailure`. Unsigned checked arithmetic prevents negative or
wrapped values.

Waits are reported independently. They are never globally subtracted from an
unrelated operator-time total.

### 4.2 Memory

An isolated MPool terminal snapshot contains:

| Field | Meaning |
| --- | --- |
| `AllocatedBytes` | successful allocation bytes in the domain |
| `FreedBytes` | bytes released from the domain |
| `PeakLiveBytes` | maximum live occupancy during the observed epoch |
| `LiveBytesAtSeal` | live occupancy when the domain is sealed |
| `CrossPoolFreeCount` | frees performed by a different MPool |

Each domain is validated with:

```text
allocated - freed = live_at_seal
peak_live >= live_at_seal
cross_pool_free_count = 0
```

Statement memory reduction retains both:

- `MaxDomainPeakLiveBytes`: maximum peak of any observed memory domain;
- `SumDomainPeakLiveBytesBound`: sum of domain peaks, explicitly an upper
  bound rather than a synchronized total.

`statement_info.stats[2]`, named `MemorySize` by existing consumers, stores
`MaxDomainPeakLiveBytes`. It does not represent cumulative allocations,
byte-time, peak duration, price, or charge.

The serialized session MPool uses a statement observation epoch: the current
live occupancy becomes the baseline and subsequent successful allocations
update an atomic high-water mark. Isolated remote execution MPools publish exact
terminal domain snapshots.

### 4.3 Quality

Quality flags are facts about trustworthiness. Missing resources are never
estimated.

| Flag | Meaning |
| --- | --- |
| `partial` | at least one expected component is incomplete |
| `missing-fragment` | a dispatched direct remote scope did not return a terminal envelope |
| `missing-memory-domain` | an expected memory observation was unavailable |
| `invariant-failure` | checked arithmetic or a conservation invariant failed |
| `projection-overflow` | an integer cannot be represented exactly by the float64 StatsArray |
| `cross-pool-free` | allocator ownership was crossed |
| `live-at-seal` | an isolated memory domain retained live bytes at seal |
| `aggregated` | the row represents more than one statement summary |

Consumers, including AI analysis, must treat non-zero quality as a trust gate.
Known subtotals remain useful, but the row must not be interpreted as complete.

## 5. Ownership model

### 5.1 Statement root

One `ResourceRoot` owns the statement summary. It combines:

- root phases such as parse, initial plan, and initial compile;
- one or more logical executions;
- protocol output and statement metadata;
- the statement MPool peak observation.

The root is sealed once at statement completion. The final sealed summary is
the only input to StatsArray v6 and CU.

The current frontend creates the root in `RecordStatement`, after SQL parsing
has produced a computation wrapper. Parse duration is still transferred from
`StatsInfo`, but parser allocations that peak and are freed before
`RecordStatement` are outside the memory epoch. This is a documented limitation
in section 12.

### 5.2 Execution and attempt

An Execution is one logical `Compile.Run` lifecycle. The top-level execution
claims attempt ownership; nested internal executions merge their resource facts
without inflating the client statement's retry count.

An Attempt is one actual Run generation:

```text
attempt 0: first Run
attempt 1: first retry generation
attempt 2: second retry generation
```

Initial parse, plan, and compile belong to root phases. Therefore an initial
build failure before `Run` can legitimately persist `attempt_count = 0`.

On retry:

1. finish the current run or error path;
2. include cancellation, rollback, remote wait, and transition work in the
   failed attempt;
3. seal and merge that attempt;
4. start the next attempt before retry rebuild;
5. record a retry rebuild failure as the new attempt's terminal result.

Execution summaries contain fixed-size totals. Sealed attempts are merged and
discarded, so retained accounting memory is O(1) in retry count.

### 5.3 Producer ownership

Each fact has one authoritative producer:

- operator active/wait/spill facts: the operator analyzer;
- plan and compile phases: the statement `StatsInfo` root-phase claim;
- retry transition and remote wait: the execution coordinator;
- S3 requests and bytes: fileservice and plan/compile counters at their owning
  physical operation;
- client egress and packet count: the protocol writer/completion path;
- isolated execution memory: the owning MPool terminal snapshot;
- serialized session memory peak: the statement MPool epoch;
- statement wall and connection type: the root.

Projection code does not infer missing values from duration or pricing.

## 6. Remote aggregation

Every actually dispatched direct RemoteRun returns one terminal JSON envelope
with:

- the optional physical plan;
- hop-local usage, quality, and outcome;
- already-reduced memory totals;
- nested missing-fragment and missing-memory-domain counts.

The envelope is an internal homogeneous-version protocol detail. It does not
change a table, protobuf, or persisted schema, and there is no dual decoder.

### 6.1 One-hop flow

```text
coordinator
  -> dispatch direct remote scope
  -> remote executes and quiesces pipelines
  -> remote captures local operator and MPool facts
  -> remote sends one terminal envelope
  -> coordinator merges it once
```

The client-side sender uses a terminal-seen guard, so duplicate terminal
processing cannot double count a direct report.

At attempt completion, the coordinator compares the number of expected direct
remote scopes with terminal envelopes received. A missing direct envelope adds
the count difference and sets `partial|missing-fragment`. Extra reports do not
underflow the count. Counter overflow saturates and sets `invariant-failure`.

A scope planned as remote can fall back to local `MergeRun` when it is not safe
to execute standalone on the target CN. That execution is collected as local
work and is not counted as an expected remote report; remote descendants that
are actually dispatched still follow the envelope path.

### 6.2 Nested RemoteRun

A remote CN may itself dispatch a child scope:

```text
CN-1 -> CN-2 -> CN-3
```

Before CN-2 responds, it combines:

1. CN-2's captured local operator delta;
2. CN-2's local MPool domain snapshot;
3. descendant usage, memory, quality, and missing counts already received from
   CN-3;
4. only CN-2's newly detected missing direct reports.

The resulting aggregate is sent to CN-1. Each hop contributes its local facts
once and forwards descendant facts once. Outcomes are not reduced across hops:
the envelope outcome describes the sending hop, while the final attempt outcome
belongs to the top coordinator.

After query-done and dispatch-registration cleanup, the server first copies
operator and descendant facts, then runs `Compile.clear` so scope cleanup frees
attempt-owned allocations. It snapshots the MPool after that cleanup and before
MPool deletion or `Compile.Release`. This preserves non-memory producer state
while making `LiveBytesAtSeal` describe the post-cleanup terminal domain.

The protocol intentionally does not persist generation, fragment, memory-domain,
or CN identity. It can say that a direct report is missing, but not identify the
missing CN.

## 7. Completion and publication

There are two useful statement snapshots:

1. A pre-response immutable summary is attached to the physical plan for
   `EXPLAIN ANALYZE`. It contains completed execution facts but cannot include
   the response bytes currently being written.
2. The final root summary is sealed after protocol completion. It includes
   accepted client egress and is projected into statement trace and CU.

Statement completion publishes through the reliable collector path after
releasing statement-local locks. Queue saturation applies backpressure until
the collector accepts ownership or stops; completed statement rows are not
silently discarded.

## 8. Persistence contract

`statement_info.stats` remains a JSON float64 array. Version 6 has 17 positions:

| Index | Meaning | Unit / rule |
| ---: | --- | --- |
| 0 | version | `6` |
| 1 | exclusive active work | ns, additive |
| 2 | max memory-domain peak | bytes, max |
| 3 | S3 PUT count | count, additive |
| 4 | S3 GET + HEAD count | count, additive |
| 5 | client egress | bytes, additive |
| 6 | connection type | enum, non-additive |
| 7 | output packet count | count, additive |
| 8 | authoritative CU | downstream calculation, additive when rows aggregate |
| 9 | S3 LIST count | count, additive |
| 10 | S3 DELETE + DELETE_MULTI count | count, additive |
| 11 | quality flags | bitwise OR |
| 12 | S3 read bytes | bytes, additive |
| 13 | S3 write bytes | bytes, additive |
| 14 | spill bytes | bytes, additive |
| 15 | total wait | ns, additive across wait kinds |
| 16 | attempt count | count, additive |

Version-aware consumers must decode by index 0. The table schema and first 11
positions are reused; version 6 changes the meaning of positions 1 and 2 to the
documented active-work and peak-memory semantics and appends six fields.

StatsArray projection uses float64 because that is the existing representation.
Integers above `2^53` set `projection-overflow`; they must not be treated as
exact by downstream analysis.

Short-statement aggregation uses the same algebra: additive quantities sum,
memory peak takes max, quality flags OR, connection type is preserved, and CU
sums from already calculated per-statement CU.

## 9. CU

CU remains available, but it is not a producer-side resource fact.

The final sealed summary is first projected without CU. The existing CU formula
then derives one value from those meaningful quantities and statement duration,
and index 8 stores that result. Aggregation sums per-statement CU; it does not
reconstruct component pricing from an aggregate peak.

Changing pricing later therefore does not require corrupting or duplicating the
resource collection model.

## 10. Explain and execution plans

Logical `EXPLAIN ANALYZE` operator rows no longer print the old mixed
`MemorySize`. Operator memory counters are not authoritative statement memory.

Physical-plan Overview reports:

- active work;
- total wait;
- `MaxDomainPeakMemory`;
- `SumDomainPeakMemoryBound`;
- spill and S3 bytes;
- attempt count;
- quality;
- affected rows where available.

Explain text formats memory with `bytes`, `KiB`, `MiB`, and higher binary units.
JSON summaries and `statement_info.stats` retain raw byte numbers.

The physical plan may carry the fuller `StatementResourceSummary`, including
per-kind waits, retry wall, and missing counts. Availability in persisted
`exec_plan` still follows the existing plan-capture/export policy; the compact
StatsArray is the durable baseline for every exported statement.

## 11. AI-assisted analysis

The v6 row is sufficient for first-stage SQL anomaly triage. An analyzer can:

- rank memory-heavy, spill-heavy, IO-heavy, output-heavy, or retry-heavy SQL;
- compare active work, wait, attempts, and CU across fingerprints and releases;
- distinguish complete rows from partial or invariant-failed rows;
- correlate statement status, duration, result size, physical plan, and resource
  distribution;
- detect regressions without interpreting CU as a raw resource measurement.

Recommended analysis order:

1. decode StatsArray by version;
2. reject or downgrade confidence according to quality flags;
3. compare raw resource dimensions before CU;
4. use physical-plan detail when present;
5. correlate suspected OOMs with CN logs, metrics, and profiles.

The accounting row alone cannot prove an OOM root cause or localize memory to an
operator.

## 12. Known limitations

1. The session MPool epoch begins after SQL parsing. Parser temporary peaks and
   parser-time allocation failures are not represented by `MemorySize`.
2. `MaxDomainPeakLiveBytes` is a maximum individual domain peak, not a
   synchronized sum across CNs.
3. Memory is not attributable to an operator, pipeline, CN, or retry attempt.
4. A hard-killed coordinator may not export its final statement row.
5. Missing remote data has a count and quality flag but no durable CN or
   fragment identity.
6. StatsArray persists total wait and attempt count, not retry reason, attempt
   outcomes, or each wait kind. Some detail is available only in the physical
   plan.
7. Uninstrumented blocking may appear as active work.

These limitations are preferable to fabricated precision. Future extensions
should add independently meaningful facts only when a production diagnostic
requires them.

## 13. Performance constraints

- Producer-local updates do not allocate per event.
- Successful MPool allocations update one atomic high-water value.
- Attempts are reduced and discarded; retry accounting does not retain an
  attempt slice.
- Remote terminals contain one compact aggregate per direct scope.
- Statement completion does not run a second accounting formula.
- No shadow or dual-calculation path is enabled.

Performance comparisons must use identical binaries, data, hosts, and benchmark
arguments against the merge base. Relevant microbenchmarks cover MPool atomic
max and resource merge/attempt terminal operations. SQL workload validation
must include point, scan, join, insert/load, spill, retry, and multi-CN paths.

## 14. Verification

Checked-in deterministic tests cover:

- checked active subtraction and overflow;
- usage and memory merge algebra;
- allocator conservation, failed allocation, and cross-pool free;
- retry success, failure, transition failure, and panic publication;
- direct and nested remote terminal aggregation;
- duplicate terminal suppression and missing-report quality;
- StatsArray v6 projection and aggregation;
- CU calculated once from the sealed summary;
- protocol output accounting and statement completion;
- logical and physical Explain semantics.

Local package validation uses the repository's CGO environment:

```sh
make thirdparties
export CGO_ENABLED=1
export CGO_CFLAGS="-I$(pwd)/thirdparties/install/include"
export CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib -Wl,-rpath,$(pwd)/thirdparties/install/lib"
export LD_LIBRARY_PATH="$(pwd)/thirdparties/install/lib:${LD_LIBRARY_PATH}"
```

Then run focused resource, trace, compile, frontend, race, fuzz, vet, and build
checks. The repository does not expose a local resource harness through the
root Makefile; optional orchestration remains an untracked developer script.

A deployed smoke test queries a tagged row from `system.statement_info` and
verifies:

- StatsArray length 17 and version 6;
- non-negative active, wait, bytes, counts, attempts, memory, and CU;
- expected quality flags;
- retry work retained once;
- remote and nested remote facts included once;
- `MemorySize` equals the maximum observed domain peak;
- ordinary query results and plan construction remain unchanged.

Mixed sysbench, TPCC, TPCH, DDL, prepared-statement, retry, spill, and multi-CN
workloads provide distribution and performance evidence. They do not replace
deterministic failure-path tests.

## 15. Delivery

The change is delivered as one PR with reviewable commits:

1. resource algebra and StatsArray v6 projection;
2. MPool and producer instrumentation;
3. root, execution, retry, and remote aggregation;
4. statement completion, CU, and Explain consumers;
5. deterministic tests and BVT fixture updates;
6. deletion of obsolete formulas and final documentation.

There is one runtime state after merge. Rollout uses a homogeneous binary for
all participating CNs. Rollback replaces the binary cluster-wide; it does not
activate an old reducer inside the new binary.

## 16. Acceptance criteria

The implementation is accepted when:

1. active work cannot become negative or wrap;
2. every authoritative producer publishes at most once;
3. failed and retried attempts remain in root totals once;
4. final-plan operator diagnostics represent only the final plan;
5. one-hop and nested RemoteRun facts reach the coordinator once;
6. missing direct terminal reports set truthful partial quality;
7. memory domains satisfy conservation or set explicit quality flags;
8. StatsArray v6 maps exactly to the sealed root summary;
9. CU is calculated once downstream and does not feed accounting;
10. Explain and persisted memory use the documented peak semantics;
11. no table-schema, protobuf, shadow, dual-writer, or compatibility path is
    introduced;
12. focused, race, vet, build, BVT, and representative workload checks pass;
13. performance remains within the agreed workload gates;
14. known limitations remain documented rather than estimated away.

## 17. Decision log

1. Active work is a checked work estimate, not OS CPU.
2. Waits are independent additive diagnostics and may exceed statement wall
   when parallel producers are combined.
3. Initial parse, plan, and compile are root phases; Attempt 0 begins with the
   first Run generation.
4. Retry transition work closes the failed attempt; retry rebuild belongs to
   the next attempt.
5. Remote accounting uses one count-checked terminal aggregate per direct
   scope. It does not introduce fragment slots or durable remote identity.
6. `MemorySize` means maximum observed memory-domain live peak.
7. The sum of domain peaks is labeled as an upper bound and is not persisted as
   the primary memory field.
8. Known partial facts are retained and quality-flagged; missing facts are not
   estimated.
9. StatsArray remains the existing positional JSON column and uses version 6.
10. CU is a downstream projection over meaningful quantities, not an input to
    resource collection.
11. Logical operator memory is removed because it mixed incompatible ownership
    scopes.
12. The implementation contains one accounting path and one writer.
13. Parser memory, operator-level memory attribution, and hard-kill OOM capture
    remain explicit limitations.
