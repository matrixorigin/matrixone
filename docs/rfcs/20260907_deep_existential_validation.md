# Issue #28293 implementation validation

Implementation is on `fix/deep-existential-28293`, based on the approved design
base `96c0a9eeaf` (kernel `e7cadcf03150e9ca7a9bb3eeb789d0735295329a`).
Execution prerequisites were reviewed and recorded in `ac63f7e638` before their
implementation. This report covers the complete implementation series and its
tests, including files initially untracked. No GitHub review, push or PR is
represented as completed.

Production-source SHA256 manifest digest:
`c2c614e592dba5f1bbcc65e199591f079488e69886b8adbb8683d6b1ec8121d4`.
Validated `mo-service` SHA256:
`bb12937f2d0e31d7a9448748839ac23f6233893e91ee8d7bb1dc504119354cc3`.
The binary reports the preceding design commit because implementation changes
were still uncommitted at build time; these hashes identify the actual sources
and binary, rather than treating that displayed commit as a release benchmark.

## Delivered behavior and change map

| Closure | Risk | Contract / evidence |
|---|---|---|
| SQL-block ownership and pre-mutation admission | R2 | Only rejected depth-two existential regions defer. Existing successful paths keep legacy flattening, memo and prepared metadata; ordinary admission allocates no pending descriptors. |
| Typed existential analysis and lowering | R2 | Two-level WHERE EXISTS, NOT EXISTS and truth-only scalar IN; at most eight admitted deepest OR arms. SEMI eliminates witnesses without producing I/J pairs. NULL, empty inputs, outer duplicates and binding identity are preserved. |
| Projection/optimizer integration | R2 | Fresh cloned bindings and metadata; no pending/correlated reference reaches optimization. ANTI's TRUE build slot remains a two-input hash key through projection removal/copy. |
| RIGHT SEMI/ANTI duplicate groups | R2 | With no residual and non-PK lookup only, the existing worker-local bitmap certifies a completed immutable group. Across batch yields, reset and spill, a repeated key cannot enumerate the same group again. Residual/SINGLE/outer behavior stays unchanged. |
| Explicit remote receiver stop | R3 | Existing flow state distinguishes StopSending from connection/query failure. Retiring a receiver completes its registration handler. Other remote/local consumers retain current and subsequent batches. Query-level substantive errors remain authoritative. |
| UT, BVT and benchmarks | R1 | Reuse existing hashjoin, dispatch and two-CN fixtures; independent SQL expectations, public and typed planner checks, deterministic event-based cancellation and cleanup. |
| RFC / this evidence record | R0 | Scope, costs, review decisions and limitations. |

Planner state is QueryBuilder-owned and ends with binding success/error; no
persistent pending plan, new execution-state owner, protobuf/wire format,
catalog or on-disk change is introduced. Clone admission preserves scan
metadata; unsupported views/derived/aggregate/pagination/locking boundaries are
rejected rather than flattened through. This is not general decorrelation.

For the hash shortcut, the first match bit certifies a whole group only after
all `psSelsForOneRow` chunks finish. Probe workers do not mutate each other's
bitmap. Merge/finalize occurs after probing, and reset/spill creates the next
bitmap generation. Work is bounded by probe rows plus first-visited build
members per worker/generation, not a universal single-worker linear bound.

For receiver retirement, successful sends do not evaluate the new certificate.
Error/retirement paths lock WrapCs then briefly read the existing flow mutex;
StopSending/ACK do not reverse that order. Credit wait releases the flow mutex.
The removed registration receives its buffered Err completion before removal.
A bare ReceiverDone, a dead sender/message/connection, or a real Write failure
is not certified as successful. Shuffle retains its strict removed-target
error. The shared-producer context tree and query error normalization were
not changed.

## Functional and lifecycle evidence

Linux amd64, 16 visible CPUs, Go 1.26.4, native thirdparties and CGo built
locally. Go tests used `.agents/skills/mo-dev/scripts/mo-cgo-test`; CGO was not
disabled. Native service builds used `make build-with-prebuilt-native`.

| Selection | Result |
|---|---|
| Owning `pkg/sql/plan` package | PASS; final complete run 6.196 s |
| Owning `pkg/sql/colexec/hashjoin` package | PASS, plus actual shuffle-spill/group-reuse checks |
| Owning `pkg/sql/colexec/dispatch` package | PASS; retirement/remaining-target tests also pass under race |
| Remote registration, flow, stop, terminal-error arbitration selections | PASS; 29 top-level selected tests in focused compile run |
| Focused dispatch / compile / hashjoin race selections | All three packages PASS; final helper/test-cleanup changes rechecked with race in dispatch and compile |
| MySQL 9.6.0 differential, seed 28293 | 250/250 original-query comparisons: ten shapes × 25 independent I/J fixtures |
| Distributed SQL BVT | 34/34 twice on final binary; 0.246 s and 0.248 s; owned test databases absent afterward |
| Two-CN integration, five shapes and streamed-output cancellation/reuse | Two complete runs PASS; actual other-CN address and cross-CN receiver in executed plans |
| Previously failing multi-CN OR | After the fix, three runs × twenty candidate/reference pairs PASS |

Differential fixtures include nonunique independent I/J keys, duplicate O
rows, NULL/all-NULL, both/one empty relation, outer truth gates, transitive
anchors, pure-outer equality bridges and inner IN under outer negation. BVT
expectations were obtained independently from MySQL, not accepted from MO's
own result generation. Prepared reuse is exercised by BVT.

Planner tests cover positive plans, post-remap references/hash keys, old-path
allocation/memo controls and rejected scope boundaries. Executor tests cover a
match group larger than two batches, different Reset generations, residual
matches of different payload rows, real spill and zero mpool/spill-budget
balance after cleanup. Remote tests retain strict failure, credit rollback,
first-abort-wins, handler completion, live remaining consumers, cancellation
and substantive-error controls. Test senders/handlers are canceled and joined
on failure; the broadcast test has a bounded deadline.

The streamed-output case cancels after observing the first returned row and
then successfully queries again. It proves client cancellation and continued
service usability; it is not a claim that every server resource was measured
as zero at the instant of cancellation.

## Performance evidence

The original SQL failed before this feature. Its error latency is not a
performance reference. New SQL is compared with executable equivalent
SEMI/explicit-arm SQL, with independent O/I/J tables and no primary keys.

### Existing successful planning controls

Six alternating baseline/candidate rounds, CPU 7 affinity, one benchmark CPU,
1 s per benchmark per round, after stopping the owned test services. Each iteration parses and builds the original
successful SQL. Baseline production sources are `96c0a9eeaf`; benchmark source
is identical on both sides. Exact final measurements are recorded below. Host timing remained variable,
so these are descriptive medians, not an equivalence proof or a claim that
the unchanged controls became faster. Allocations and byte counts match on
both sides (66,817 / 97,721 / 76,713 B per operation respectively).

| Control | Median µs baseline / candidate | Ratio of medians | Allocations baseline / candidate |
|---|---:|---:|---:|
| shallow | 120.157 / 125.391 | 1.0436 | 691 / 691 |
| depth_two_old | 197.522 / 174.213 | 0.8820 | 1090 / 1090 |
| scalar | 189.275 / 163.333 | 0.8629 | 849 / 849 |

To resolve the remaining process/scheduling noise, an additional same-process
comparison compiled an unmodified copy of the baseline planner package beside
the candidate planner. Both used the same parser/dependencies and independent
mock contexts. After 200 warm iterations, it ran 100 rotated pairs of 100
parse/build operations on CPU 7 with GOMAXPROCS=1. The baseline source copy was
verified against its clean worktree; the temporary packages were removed after
measurement. These simple SELECT controls do not enter the shared index-plugin
initialization hooks. This isolates the changed planning closure, not unrelated
executor code. Raw samples and the temporary test source are in the artifacts.

| Control | Median µs baseline / candidate | Paired median ratio | Bootstrap 95% interval |
|---|---:|---:|---:|
| shallow | 189.778 / 187.547 | 0.9880 | 0.9561–1.0291 |
| depth_two_old | 173.010 / 154.246 | 0.9548 | 0.8993–1.0112 |
| scalar | 140.159 / 142.211 | 1.0250 | 0.9857–1.0452 |

### Duplicate-key operator scaling

Full build/probe/reset microbenchmarks, three runs per sample. These isolate
the repeated-group mechanism and do not replace end-to-end measurements.

| Existing RIGHT join / rows | Baseline median | Candidate median | Allocations baseline → candidate |
|---|---:|---:|---:|
| SEMI / 2,048 | 5.147 ms | 0.216 ms | 317 → 61 |
| SEMI / 8,192 | 102.715 ms | 0.801 ms | 4,166 → 62 |
| ANTI / 2,048 | 4.823 ms | 0.071 ms | 315 → 59 |
| ANTI / 8,192 | 108.697 ms | 0.231 ms | 4,168 → 60 |

### Single-CN end-to-end cases

Thirty paired cases cover 250k/1M rows, NDV 8/high NDV/90% skew and six shapes
(AND, ANTI, IN, deepest OR, selective O, ANTI outer gate). Eighteen further cases
use O/I/J sizes (8,1M,1M), (1M,1k,1M), (1M,1M,1k). All results match references;
new witness plans have no full-relation product or LoopJoin. Observed
intermediate output does not exceed the corresponding input cardinality.

Initial measurements overlapped host compilation and showed unstable ratios.
The four suspect cases were remeasured after compilation, with twenty rotated
pairs of eight statements per side. Raw earlier measurements are retained.

| O/I/J sizes, shape | Paired median candidate/reference | Bootstrap 95% interval | Candidate/reference hash-budget peak |
|---|---:|---:|---:|
| 250k/250k/250k, high-NDV IN | 0.9913 | 0.9550–1.0575 | 24,151,992 / 23,889,824 B |
| 8/1M/1M, IN | 1.0152 | 1.0040–1.0217 | 54,233,776 / 54,233,776 B |
| 1M/1M/1k, AND | 0.9975 | 0.9849–1.0090 | 655,288 / 655,288 B |
| 1M/1M/1k, IN | 1.0053 | 0.9931–1.0181 | 655,288 / 655,288 B |

Hash-budget peaks are allocator-accounted query hash budget values from
EXPLAIN PHYPLAN ANALYZE, not cumulative output bytes and not whole-process RSS.

### Naturally scheduled two-CN execution

4.5M rows per independent relation, eight keys, no force-multi-CN hook. All six
candidate/reference pairs naturally report MULTICN and execute on the second
CN. TCP payload-counter deltas come from established sockets whose server
ports are the two owned pipeline ports, counting each connection once; these
include framing, ACK/control traffic and are not SQL output-size estimates.
Three warm runs per side precede eight rotated pairs. ANTI-gate and AND were
remeasured with twenty pairs after load/flush settling.

| Shape | Median ms candidate / reference | Paired median ratio | 95% interval | TCP bytes candidate / reference |
|---|---:|---:|---:|---:|
| anti_gate | 473.13 / 471.98 | 0.9985 | 0.9846–1.0573 | 54,433,898 / 54,386,580 |
| and | 479.82 / 470.10 | 1.0179 | 1.0065–1.0479 | 54,365,380 / 54,367,662 |
| anti | 356.06 / 361.36 | 0.9954 | 0.9392–1.0840 | 54,371,272 / 54,375,148 |
| in | 491.30 / 488.10 | 1.0098 | 0.9631–1.0612 | 81,406,457 / 81,410,348 |
| or | 944.33 / 1489.46 | 0.6367 | 0.5711–0.6499 | 54,407,250 / 54,420,928 |
| selective | 405.15 / 399.50 | 1.0143 | 0.8751–1.0641 | 36,243,618 / 36,247,440 |

The ANTI truth gate adds a BOOL build column and composite hash key. At 4.5M
rows, its measured query hash-budget peak is 78,552,896 versus 73,352,384 B:
+5,200,512 B / 7.1%, a linear build-row cost, not witness-pair expansion. Its
TCP difference is approximately +0.087%. Other arms/scopes retain bounded
input and scan multiplicity; no new spill was required in these runs. Forced
spill cleanup is covered by the deterministic executor test.

The paired intervals show no statistically supported >5% slowdown in tested
cases. This is not a proof that every future query regresses by less than 5%:
in particular the ANTI-gate interval has an upper bound of 1.0573. The explicit
memory cost above is part of the design and is not hidden behind a latency
claim. Host scheduling, load/flush state, plan choice and this restricted SQL
scope limit extrapolation.

## Review and reproducibility

Independent reviewer `review_decorrelation_design` (Kepler), requested by the
user, approved the design boundaries before implementation and reviewed the
final planner, hash shortcut, remote stop protocol and tests. Concrete findings
were closed: preserving legacy NOT(EXISTS) child memo handling, restoring the
previous multi-CN test-hook value, completing retired handlers, continuing
local delivery after remote retirement, and cancel/join cleanup in test
failure paths. No remaining source blocker was identified. The reviewer also
accepted the explicit BOOL-key memory accounting above without weakening the
latency criterion.

Local durable artifacts: `/d/mo-worktrees/issue-28293-validation/`. They include
source/binary hashes, raw per-pair timings, plans, socket snapshots, generator
scripts, differential results, race/BVT logs and the removed diagnostic trace.
Scripts record the actual dedicated lab ports/paths and should be configured
for an owned local cluster before rerunning. Permanent repository oracles are
the planner/hashjoin/dispatch/remote tests, the DML two-CN integration test and
`test/distributed/cases/subquery/deep_existential.sql` plus its result.

Representative commands (after the documented native setup):

```sh
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=10m ./pkg/sql/plan ./pkg/sql/colexec/hashjoin
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=3m ./pkg/sql/colexec/dispatch
.agents/skills/mo-dev/scripts/mo-cgo-test -v -count=2 -timeout=8m -run '^TestDeepExistentialMultiCN$' ./pkg/tests/dml
.agents/skills/mo-dev/scripts/mo-cgo-test -race -v -count=1 -timeout=5m -run 'Test(SendBatch|Broadcast|HandlePrepareDoneNotify|LiveReceiverStop)' ./pkg/sql/colexec/dispatch ./pkg/sql/compile
.agents/skills/mo-dev/scripts/mo-cgo-test -run '^$' -bench '^BenchmarkRightExistentialRepeatedKeys$' -benchmem -benchtime=200ms -count=3 ./pkg/sql/colexec/hashjoin
.agents/skills/mo-dev/scripts/mo-cgo-test -run '^$' -bench '^BenchmarkExistentialPlanningControls$' -benchmem -benchtime=300ms -count=6 ./pkg/sql/plan
```

The planning acceptance run alternated precompiled baseline/candidate binaries
rather than comparing two unpaired `go test` runs. Full repository CI,
long-duration production workload measurements and maintainer approval are
not claimed by these focused local checks.
