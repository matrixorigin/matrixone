# Dictionary dependent PR delivery design

Owner: #32379. Proposed follow-ups: #32385, #32386, #32387, #32388.
Status: combined consumer and scan/filter producer design approved by the requester
on 2026-11-10; implementation in progress. #32405 keeps its accepted boundary-only scope.

## Baseline and invariant

Build on #32405's producer-owned DictionaryOutputState. Ordinary OperatorBase
and vm.Exec retain main's layout and path. Producers retire their previous
private output at Call entry and normalize their final projected output before
publication when the destination requires flat data. Reset/free/cancellation
retain that ownership contract. No wrapper opcode, global registry or cache.

Logical values, row order, repeated rows, NULL/grouping and string/prepared
metadata must match ordinary execution. Selections are immutable, accounted,
bounded and compose to one base. The full retained backing remains charged
until its last owner releases it. Allocation refusal uses the existing accounted
fallback; query errors and cancellation retain their meaning.

Unknown consumer capability stays flat. An operator may declare dictionary
input only after every reachable physical/native access is either converted or
explicitly normalized at its owning boundary. A dictionary-aware forwarder must
also declare possible dictionary output so an unsupported next edge is safe.

## Combined PR, part A: consumer migration (#32385)

Depends on #32405. Implement this consumer part before enabling part B.
The same implementation PR covers #32385 and #32386; no producer is enabled
until its complete consumer/normalization path is covered.

- Resolve fixed/varlen read views once per input batch. Preserve existing direct
  flat/constant loops and scalar broadcast; selected loops index the immutable
  base and use logical validity/metadata.
- Convert scalar expression parameter wrappers and the predicate/projection
  kernels needed by the enabled paths. Unconverted functions/native calls use
  explicit executor-owned normalization with the existing pool/account.
- Convert key preparation and hash-map probe readers, including selected keys,
  constants, NULLs and composite keys. Hash row ordinals remain logical.
- Convert vector/batch gather/union and join payload copying to read selected
  source rows directly into existing output. Never flatten a complete probe
  input merely to copy a few matched rows.
- Convert aggregate input reads used by these paths; aggregate state/output
  remains ordinary owned storage. Unconverted aggregate contracts stay flat.
- Make shuffle a safe consumer: route selected logical keys, preserve whole
  batch forwarding, and copy selected payload directly into existing flat
  partitions on mixed routes. This does not yet introduce dictionary partition
  production. Forwarding projection/shuffle output follows the boundary owner.
- Gate join capability by proven mode/type/residual support. Unsupported modes
  use the established flat boundary; no blanket declaration for all joins.

Before coding, inventory physical access by expression, key, payload, aggregate
and lifecycle closure in a versioned consumer matrix. Each capability declaration
must name its converted functions and residual normalization points.

Validation: tiny flat/constant/dictionary cases with repeated/reordered/empty
rows, NULL/grouping, inline/area strings and metadata; compare to independently
specified logical outputs. Exercise selected keys and payload separately,
function fallback, failed copy, reset/reuse, stopped cancellation and actual
join/aggregate callbacks. Run affected owning packages; race only shared/async
closures. The completed combined PR uses part B's Q12 speedup and all-query regression gate;
do not run a separate neutral acceptance campaign between these commits.

## Combined PR, part B: scan/filter/projection selections (#32386)

Shares one PR with A. Primary target Q12. Recheck the current physical plan before
implementation and qualification. The retained current plan is:

    LINEITEM reader with pushed predicates -> shuffle -> hash build
    ORDERS reader -> shuffle -> hash join probe -> group

There is no standalone Q12 filter/projection-to-probe chain. Implementing only
that hypothetical path would not establish Q12 activation.

Add minimal explicit selected-output capability to the existing ReadWithFilter
request/result contract. The request is made only for a supported useful consumer
edge. Readers that cannot safely retain their decoded backing return ordinary
owned output. Reuse the selection already computed by predicate evaluation;
retain decoded payload rather than gathering it into an intermediate flat batch.
Do not introduce another predicate engine or read every payload twice.

TableScan owns the generation-local producer state, base snapshots and one
shared row mapping across columns. A capability/ownership/budget refusal retains
the ordinary path. Direct projections preserve the selected view; computed
columns may be owned flat vectors in the same logical batch. Grouping-set and
other unconverted paths stay ordinary.

The useful Q12 path is selected reader output through dictionary-aware shuffle
and HashBuild consumers. Mixed-route shuffle may still produce flat partitions;
whole-batch forwarding can keep the view. HashBuild performs its existing
necessary retained copy/key preparation directly from logical selected rows.
The intended saving is the earlier reader/filter gather, not the required owned
join storage. Producer admission must avoid building a view solely to flatten it
immediately. Expose the needed destination decision through producer-owned state,
without changing ordinary OperatorBase or vm.Exec.

Work proof: count originated selected rows, avoided reader/filter gather bytes,
whole-batch forwarding, downstream materialization bytes, retained physical peak
and fallback reasons. Require real Q12 activation and attributable work removal,
then stable SF10/SF100 memory latency benefit plus all-query/Total checks. If the
latest main has already removed this gather, return for rescoping before coding
another mechanism or selecting a different target after measuring it.

## PR C: shuffle and join output views (#32387)

Depends on A/B's stable view and capability contracts; primary target Q9.
Can develop as a separate stacked draft while B is reviewed.

Freeze an input snapshot once and produce accounted partition selections over
it. Preserve the existing whole-batch move path. Each receiver owns its header;
bounded queues retain references rather than mutable source headers. Producer
reuse performs COW and never waits for an all-consumer acknowledgement barrier.

Start join output views with residual-free unique INNER and probe-only SEMI/ANTI
cases. Compose logical selections onto one base. Gather build payload where the
result cannot be represented by a single base; keep other join outputs flat.
Do not enable outer, mark, ASOF, residual or nonunique output views without a
separate closed ownership/semantic proof.

Validate queue capacity, early consumer stop, partial send, cancellation,
duplicate cleanup, source reuse and final backing release using deterministic
barriers. Count bytes/copies avoided and retained peak. Confirm Q9 activation and
SF10/SF100 memory benefit; run the all-query/Total memory/disk checks.

## PR D: reader early filtering and late payload (#32388)

Depends on A/B; does not inherently depend on C. Reader and shuffle/join work can
proceed in separate files/branches once shared contracts are fixed. Target Q19.

B establishes selected reader output. D moves ready membership/predicate work
to early columns and delays remaining payload work using existing reader paths.
Preserve original-row mapping, residual evaluation/error order and snapshot
identity. Retain safe decoded backing or materialize required payload once.
Do not change codecs, cache replacement/tiering, SQL types or join placement.
Block codecs may still decode each touched block; do not equate reduced gathers
with reduced decoding or I/O.

Validate reordered selections, residual errors, row/block/runtime-filter
composition, held/evicted backing, pressure and refusal fallback. Count decoded
bytes, gathered bytes, native reads and physical pins independently. Confirm Q19
activation/benefit at both scales and all-query/Total behavior.

## Delivery and future verification protocol

Submit one templated draft for combined A/B after real code and cheapest functional
proof exist; C and D remain separate PRs. Do not submit empty placeholders. Draft development may stack;
production enablement and final isolated qualification wait for prerequisites
to merge. #32405 merged as main `00da05219f` on 2026-11-10. Combined A/B was rebased
onto that main without inherited prerequisite commits.

The requester replaced the repeated-process protocol for future PRs:

- One process per build/engine, scale and memory/cache configuration.
- Load/open data once; execute Q1-Q22 six complete consecutive suites.
- Exclude suite1 from reported timing, profiling and accounting.
- Profile/account suites2-6 and retain all five warm results, per query and Total.
- No reload/restart between those suites and no additional five-process-pair
  layer. The five repeats describe within-process variation, not independent
  process samples; do not reuse the old independent-pair confidence claim.
- Preserve matched resource budgets, cold exclusion, no-spill/swap/OOM rules,
  software pins and no benchmark-time dataset/result verification.
- Use local SF10 and server129 SF100. Separate timing/work evidence from changes
  in warm cache residency and disclose profiling overhead consistently.

For producer PRs, require the predeclared target to show repeatable memory-profile
benefit at both scales with attributable copy/work reduction. Publish all22 and
Total warm results, CPU/instructions, I/O and retained memory. Investigate slower
queries before readiness; source changes require fresh affected evidence, not
numerical reruns until a result passes. No process-level statistical certainty
is inferred from one process. Do not introduce an unapproved noise tolerance.

Append results as issue/PR comments. Collapse genuinely superseded result tables
after publishing replacements; preserve source pins and raw evidence. Keep raw
datasets, and retain loaded seeds while active comparisons need them. Q15/Q20
rewrites, additional decimal SIMD, codecs and SF1000 remain outside this wave.
