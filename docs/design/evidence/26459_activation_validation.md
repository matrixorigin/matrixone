# #26459 Allocation-Accounted Activation Validation

## Candidate

- Rebased main: `5b9eeb54ec`
- PR 4 lifecycle: `8633757dc1`
- PR 5 hash-table activation: `5ae8eca00a`
- PR 6 retained batch/JoinMap activation: `8e4b689f45`
- PR 7 expression activation: `c9ad0ea810`
- PR 8 spill/runtime-filter activation: `e072568998`
- PR 9 unified pressure recovery: `eec23dcdc5`
- PR 10 benchmark harness: `383fc6dce3`
- Owner-atomic activation and cleanup: `656e254fe6`

The activated expression owner is deliberately closed: COL, literal, param,
variable, vector/fold, CONCAT, CASE, varchar EQUAL, and the audited string CAST
forms. Build and probe keys are checked together. If any HashBuild, HashJoin,
DedupJoin, or RightDedupJoin key contains another function family, automatic
activation keeps every participating operator in that local statement attempt
on the legacy path. This prevents a partially exact owner from retaining an
estimator-only expression rejection. It does not claim that generic JSON,
regexp, geometry, or spatial execution has already migrated.

## Local correctness matrix

All direct Go tests use the repository CGO wrapper so `usearch` links against
the locally built `thirdparties` artifacts.

```text
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 \
  ./pkg/common/mpool ./pkg/common/hashmap \
  ./pkg/container/hashtable ./pkg/container/vector ./pkg/container/batch \
  ./pkg/sql/colexec ./pkg/sql/colexec/hashbuild \
  ./pkg/sql/colexec/hashjoin ./pkg/sql/colexec/dedupjoin \
  ./pkg/sql/colexec/rightdedupjoin ./pkg/sql/colexec/spillutil \
  ./pkg/sql/compile ./pkg/util/resource ./pkg/vm/message ./pkg/vm/process

result: PASS
```

The same package matrix passes with `-race -p=2 -count=1`. A focused
`-race -p=2 -count=20` stress also passes for concurrent account alloc/free,
seal/open linearization, statement terminal one-shot behavior, owner-atomic
activation, accounted spill/reduction, and all three join-consumer activation
gates. Affected-package `go vet` and `make build` pass on the same candidate.

Incident-mechanism regression mapping:

| Incident | Local regression proof |
| --- | --- |
| #25782 high-cardinality HashBuild/spill | `TestAccountedInitialSpillReducesUnpublishedInputAndPreservesRows`, `TestShuffleHashBuildAccountedSpillLifecycle`, hash cell/descriptor replacement tests |
| #26174 dedup/fulltext INSERT | `TestAccountedDedupScratchAndDeleteBitmapFollowJoinMapLifetime`, `TestAccountedDedupBitmapExactBoundaryRollsBack` |
| #26192 LOAD DATA decoded/rebuild path | `TestSpillAllocationAccountDecodedBatchLifecycle`, `TestSpillAllocationAccountDecodedReuseRetriesFromCleanRecord`, recursive rebuild lifecycle test |
| #26413 external/self-join lifetime | statement-attempt zero/late-Free/cancel/error/panic tests, `TestAccountedJoinMapLateFreeKeepsOriginalGeneration` |
| #26454 string expression false budget | `TestAllocationAccountedExpressionIssue26454AndOneByteShort`, `TestIssue26454ExpressionKeyBuildUsesActualCapacity`, adaptive expression-pressure tests |

The activation boundary itself is covered by
`TestHashBuildAllocationActivationRequiresClosedExpressionOwner` and
`TestAllocationAccountActivationIsStatementAtomic`: a closed #26454
expression activates, an unclosed modulo expression keeps the owner legacy, a
no-map operator does not open a generation, and one unclosed owner rolls back
all already configured owners.

## Local performance evidence

Host: linux/amd64, Intel i7-11700, Go 1.26.4. Unless stated otherwise,
GOMAXPROCS was 16. Medians are from five runs; resident tests used a 200 ms
benchtime and spill scatter used 300 ms.

### Resident HashBuild

| Key/rows | Legacy median | Accounted median | Delta | Allocation effect |
| --- | ---: | ---: | ---: | --- |
| int / 32 | 4,161 ns | 5,841 ns | +40.4% (+1.68 us) | 32 -> 27 allocs/op |
| varchar / 32 | 7,147 ns | 8,907 ns | +24.6% (+1.76 us) | 96 -> 91 allocs/op |
| int / 8,192 | 139,602 ns | 143,254 ns | +2.62% | 43,738 -> 10,401 B/op; 56 -> 29 allocs/op |
| varchar / 8,192 | 410,074 ns | 400,782 ns | -2.27% | 222,055 -> 24,899 B/op; 600 -> 574 allocs/op |

The 32-row cases quantify the fixed first-allocation tax for high-frequency TP
work; they are not presented as a per-row tax. Full-batch resident HashBuild is
within 2.7% for int keys and improves the measured varchar case.

### Spill and expression paths

| Benchmark | Legacy median | Accounted median | Delta |
| --- | ---: | ---: | ---: |
| 4,096-row scatter including hash/select/marshal/coalesce/write | 59,073 ns | 60,571 ns | +2.54% |
| #26454 expression | 909,082 ns | 940,454 ns | +3.45% |
| copied build batch | 18,584 ns | 22,051 ns | +18.7% |

The copied-batch result also reduced 230,032 B/op to 632 B/op and 10 to 6
allocations/op. Scatter reduced 2,711 B/op to 2,392 B/op; syscall and codec work
remain included.

### Generation and release concurrency

| Benchmark | CPU | Median | Recorded latency |
| --- | ---: | ---: | --- |
| full allocation-attempt lifecycle | 1 | 843.5 ns/op | p50 730 ns; p99 2,011 ns |
| full allocation-attempt lifecycle | 8 | 762.7 ns/op | p50 1,995 ns; p99 73,349 ns |
| same-generation release storm | 1 | 558.4 ns/op | 0 B/op, 0 allocs/op |
| same-generation release storm | 8 | 470.0 ns/op | 0 B/op, 0 allocs/op |

The lifecycle benchmark includes generation open, controller/account creation,
one exact 4 KiB allocation/free, terminal completion, and concurrent operation.

## Remote workload gate

The candidate must be compared with the exact rebased main on the same TKE
workflow and resource configuration. Required results are:

| Workload | Required evidence | Status |
| --- | --- | --- |
| TPCH 100G | three rounds, no spill/OOM/restart, total and per-query comparison | pending |
| TPCH 1T | three rounds, spill succeeds, no OOM/restart, total and per-query comparison | pending |
| #26174 fulltext INSERT | workload pass under unchanged cap | pending |
| #26192 LOAD DATA | workload pass under unchanged cap | pending |
| #26413 external self-join | workload pass and zero terminal generation | pending |

No cap increase, reduced dataset, disabled spill, or plan-specific bypass is an
acceptable pass. The candidate and main run URLs, exact SHAs, load times,
query totals, spill evidence, restart/OOM search, and profile comparison belong
in this table before PR 10 is accepted.
