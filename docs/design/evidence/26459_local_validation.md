# #26459 local validation evidence

This file records evidence for the single production path described in
`../allocation_accounted_memory_admission_impl.md`. Removed implementations
are not retained as validation dimensions.

## Static closure checks

- no production allocation-account enable switch;
- no HashBuild logical-size memory reservation token;
- every join/HashBuild expression-owned MPool vector is constructed with the
  attempt account, while opaque library Go heap remains an explicit boundary;
- SpillEngine construction rejects a missing or closed budget generation;
- runtime scan/load clones are attached to the current attempt before worker
  `Prepare`;
- terminal lifecycle validates zero bytes and zero live metadata;
- memory, spill disk, and spill FD admission errors have distinct components.

## Required local test matrix

All Go tests use the repository CGo wrapper so `usearch` headers, libraries,
link flags, and runtime paths match the MatrixOne build contract.

```text
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=240s \
  ./pkg/common/mpool ./pkg/common/bitmap ./pkg/common/hashmap/... \
  ./pkg/container/vector ./pkg/container/batch ./pkg/vm/message \
  ./pkg/vm/process ./pkg/sql/util ./pkg/sql/colexec \
  ./pkg/sql/colexec/hashbuild \
  ./pkg/sql/colexec/hashjoin ./pkg/sql/colexec/dedupjoin \
  ./pkg/sql/colexec/rightdedupjoin ./pkg/sql/colexec/loopjoin \
  ./pkg/sql/colexec/product ./pkg/sql/colexec/productl2 \
  ./pkg/sql/colexec/spillutil ./pkg/sql/compile
```

Selected race coverage:

```text
.agents/skills/mo-dev/scripts/mo-cgo-test -race -count=1 -timeout=300s \
  ./pkg/common/mpool ./pkg/container/vector ./pkg/container/pSpool \
  ./pkg/sql/util ./pkg/sql/colexec ./pkg/sql/colexec/hashbuild \
  ./pkg/sql/colexec/hashjoin ./pkg/sql/colexec/loopjoin \
  ./pkg/sql/colexec/dedupjoin ./pkg/sql/colexec/rightdedupjoin \
  ./pkg/sql/colexec/productl2 \
  ./pkg/sql/colexec/spillutil ./pkg/sql/colexec/sample ./pkg/sql/compile
```

Static checks:

```text
go vet <modified packages>
go build <modified packages>
```

## Fresh local result

The final semantic edit was followed by a clean local run on 2026-08-01.

- the complete package matrix above passed, with Sample, pSpool, compare,
  hashtable, nulls, shuffle, SQL util, vector-index CPU packages, and disttae
  added to the command;
- `-race -p=2 -count=1` passed for mpool, hashjoin, spillutil, Sample, and
  compile;
- `go vet -mod=readonly` passed for every modified production package;
- `go build -mod=readonly` passed for every modified production package;
- `git diff --check` and `gofmt` were clean;
- allocation and performance results are recorded in
  `26459_allocation_accounting_bench.txt`.

No partial or still-running session is counted as a pass.

After the distributed q7 failure exposed the shared remote-MessageBoard
boundary, the final remote-lifecycle amendment was validated separately:

- the complete `pkg/sql/compile` suite passed in normal and race modes;
- `pkg/pb/pipeline`, `pkg/vm/message`, and `pkg/vm/process` passed;
- protobuf regeneration was clean and reproducible;
- vet passed for all four affected packages;
- an independent lifecycle/concurrency review converged with no blocker or
  major finding after its pending-domain cardinality and mixed-version concerns
  were resolved in code or by existing scheduling evidence.

## Behavioral coverage

The local suite covers:

- exact physical allocation/release and capacity rollback;
- owner/site mismatch and sealed-generation terminal errors;
- prepared and retry generation reuse;
- runtime parallel clone attachment;
- JoinMap and spill payload move-only ownership;
- cancellation-safe bitmap mailbox seal/drain;
- spill disk/FD admission and release;
- recursive spill row/schema/file validation;
- minimum-unit and monotonic pressure termination;
- optional runtime-filter degradation;
- nested/selected expression result accounting, capacity rejection, transfer,
  and terminal release;
- HashJoin, LoopJoin, DedupJoin multi-batch finalize, and RightDedupJoin result
  accounting, batch-local reuse, capacity rejection, and prepared
  `Reset -> ClearAllocationAccount` terminal release;
- grouping-aware copy, hash, equality, and ordering;
- late and alternating Sample grouping domains across row, percent, and merge
  modes;
- Product cleanup and account terminal zero;
- accounted JoinMap release after an unaccounted ProductL2 consumer frees it.
- remote scope-graph fragment counting across nested CN execution addresses;
- ProcessInfo topology-map and execution-ID wire round trips, with distinct
  MessageBoard generations across retries;
- remote statement-group board drain after a producer finishes before a later
  sibling registers, including accounted queued-message destruction, exact
  account terminal zero, and aggregate MPool terminal sampling;
- incomplete remote dispatch expiry releases staged account and MPool domains,
  while an unresolved pending terminal marker makes the coordinator summary
  explicitly partial; this includes the case where another registered fragment
  is still active when the old board generation closes;
- fragment failure aborts an incomplete group immediately, and a legacy remote
  plan containing an accounted owner is rejected instead of running without an
  account;
- counted pending/completed group markers resolve independent of
  terminal-response order; a four-fragment lost-final-response case preserves
  all three suppressed reported domains plus the directly missing domain.

## Performance evidence

Performance validation is local and allocation-focused. Benchmarks record
`ns/op`, `B/op`, and `allocs/op` for account acquire/release, vector growth,
hash-map build/lookup, and spill scatter. The acceptance rule is no
new per-row or per-allocation Go object in steady state and no material
regression outside measurement noise.

Remote auto-test is deliberately not part of this validation cycle.
