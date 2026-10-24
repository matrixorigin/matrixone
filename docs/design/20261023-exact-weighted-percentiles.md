# Native exact weighted percentiles with window support

Owning issue: matrixone/matrixone#29944 (kind/feature).
Implementation PR: matrixone/matrixone#31000.
Revision: 5 (2026-10-23; design revision after the independent design review's
fourth REQUEST_CHANGES; revision 4, SHA256 9f02eeeedb217c52758b34919c3757c8ae82e7e938a174f0649311484dedd129, was
otherwise supported — typed weight admission, destination verification,
saved-state contracts and threshold mathematics — but freed constructed
result vectors on the success path, contradicting the framework's
output-ownership contract).
Lineage: follow-up to `docs/design/median_within_group.md`,
`docs/design/approx_percentile_within_group.md`,
`docs/design/20260910-prepared-and-discrete-percentiles.md`, and
`docs/design/20260919-ordered-set-window-functions.md`.

Revision log:

- Revision 1 (2026-10-23): initial design-only checkpoint.
- Revision 2 (2026-10-23): resolved the first four review blockers — (1) weights are
  validated in their original numeric type before any lossy conversion and
  normalized exactly to uint64 (decision D4 rewritten; the FLOAT64 transport
  that let UINT64 2^53+1 and fractional DECIMAL weights pass validation is
  removed); (2) the selection threshold is k = ceil(level · W), computed once
  per group finalize with exact big.Int arithmetic from the level's rational,
  then compared in uint64 — covering the entire accepted level domain
  including 1e-100 and high-scale decimals (replacing fixed 128-bit product
  comparisons); (3) mixed-version remote execution gains a MORPC protocol
  capability gate modeled on the existing percentile gates (new decision D10),
  replacing the incorrect claim that old nodes always reject the name first;
  (4) the zero-weight and zero-total endpoint contract is made consistent
  (decision D6 rewritten) with explicit witnesses for trailing massless
  values, leading massless values at level 0, and all-zero groups.
- Revision 3 (2026-10-23): resolves the two remaining blockers — (1) D10 is
  rewritten to verify the ACTUAL destination: the intended coordinator-local
  `MOProtocolVersion` check (`supportsRemoteXxx(proc.GetService())`) only
  inspects the coordinator's own service runtime, so a coordinator at the new
  gate version with an older worker passed revision 2's story. Mixed-version
  safety is now anchored on the existing send-time destination-stream
  confirmation: the aggregate raises the `requiredProtocol` floor during
  remote-scope encoding (`encodeRemoteScopeWithProtocol`, the site that
  already raises v122/v125/v129 floors), the destination confirms it on the
  live stream (`confirmProtocolOnStream` → `Method_PipelineProtocolCheck`
  before `sendPipeline`), and a compile-time
  `AllCNsSupportProtocol` fast-fail rejects placement when the cluster
  inventory contains any pre-gate CN. (2) New decision D11 fixes the
  saved-state schema: the persisted payload retains the ORIGINAL typed weight
  (exactly the declared `argTypes`), because the generic saved-state
  machinery interprets payloads through `argTypes` (DISTINCT spill
  restoration against FLOAT member widths; Group's generic spill calls
  `SaveSpillIntermediateRows` on every aggregate), so revision 2's
  uint64-normalized payload contradicted the declared types. Typed-to-uint64
  normalization moves to `Flush` only.
- Revision 4 (2026-10-23): resolves the two remaining contract gaps — (1) the
  finalization resource and cancellation contract is completed (new decision
  D12): the executor implements `FlushWithContext` because the framework's
  `aggexec.FlushWithContext` (concat2.go:1507-1520) only forwards context to
  interface implementers; finalization declares cancellation points, owns its
  temporary buffers through the allocation account, and frees scratch and
  partially built results on cancellation, allocation failure, and overflow
  after partial finalization, following the owned finalization precedent of
  `orderedPercentileDiscreteExec.FlushWithContext` (ordered_percentile.go:313-372)
  while the retained state itself is released through the framework's
  `aggExec.Free` path (aggState.go:3439-3449). The resource model now budgets
  the arena overhead and finalization scratch, and the validation plan gains
  deterministic failure/cancellation witnesses asserting resource release
  and a successful subsequent execution. (2) DECIMAL256 weight admission is
  resolved consistently: D4 now names one explicit supported-weight set —
  signed/unsigned integer widths, BIT, FLOAT32/64, and DECIMAL64/128 — with
  DECIMAL256 rejected at bind time (`types.IsNumeric()` includes T_decimal256,
  so the earlier "any numeric SQL type" admission contradicted the
  DECIMAL64/128-only executor rules), and registration, validation,
  saved-state sizing, and tests all state the same set.
- Revision 5 (2026-10-23): resolves the one remaining blocker — finalization
  OUTPUT OWNERSHIP. Revision 4's D12 listed the constructed result vectors
  in the "freed on both success and error" set, and the resource model said
  the growing result vector is released before a successful return. That
  contradicts the framework's ownership contract, verified at
  `freeAggregateResultsOnError` (median2.go:1924-1935: a result is freed
  only when an error exists) and at
  `orderedPercentileDiscreteExec.FlushWithContext`
  (ordered_percentile.go:324 installs ERROR-only deferred cleanup; :373-374
  returns the LIVE vectors), whose callers retain and read the returned
  output across calls (window.go:760-769 caches ordered-set partition
  results and reads them through `UnionMulti`). As written, revision 4
  would have returned freed output. D12 and the resource model are
  corrected: scratch is released on every return; result vectors are freed
  only on error; on success their ownership transfers to the caller, which
  retains, reads, and releases them through its own consumer cleanup. The
  validation plan gains an ownership witness for that transfer.
- Post-approval annotation (2026-10-24; documentation change only, not a
  design revision): the Status section below now records the independent
  design review's final decision — PASS for revision 5, whose reviewed
  content is identified by SHA256
  b2705c13ab4d3f9965ab3d83a6f3e4a83d3426e4ffefdcdc8b6713864c69e083 — in
  place of the pre-review "pending" wording, and the header links
  implementation PR #31000. No reviewed decision, invariant, or validation
  plan changed in this annotation; the approved revision-5 bytes remain
  byte-preserved in the commit that first delivered this document
  (`60d88ff6adb4ae6ea9e1ba0946ee82bc61f26ea7`, inside PR #31000), so the
  reviewed content stays verifiable against the recorded hash.
- Post-merge annotation (2026-10-24; documentation change only, not a
  design revision): D10's protocol capability slot is re-minted from the
  drafted `MORPCVersion130` to the delivered `MORPCVersion131`. Upstream
  main merged #30311 (`5a813a324d`, incomplete temporal string overloads)
  before this branch, independently claiming `MORPCVersion130` and bumping
  the sentinel to 130, which raised the merge conflict in
  `pkg/defines/const.go`. The resolution keeps upstream's temporal 130
  and mints this aggregate's gate at the next free slot 131, moving
  `MORPCLatestVersion` to 131 with it. D10's reviewed decision is that
  remote dispatch is gated by the capability version minted for THIS
  aggregate, whichever slot is free when it merges, enforced through the
  same three verified layers against the actual destination — no decision,
  invariant, layering or validation plan changed; only the concrete slot
  number moved. The regression matrix now explicitly covers a worker
  advertising only the temporal 130 as below this aggregate's gate. The
  approved revision-5 bytes remain byte-preserved at the introducing
  commit recorded above.

## Status

Approved by the mandatory independent design review on 2026-10-24: decision
PASS for revision 5, whose reviewed content (SHA256
b2705c13ab4d3f9965ab3d83a6f3e4a83d3426e4ffefdcdc8b6713864c69e083)
was verified by the reviewer against the inspected code. The review was
performed by an independent review session (separate from the
implementation work) under this repository's contract
`.agents/skills/mo-dev/references/feature-design-review.md`, design-only,
with implementation blocked throughout: revisions 1–4 each returned
REQUEST_CHANGES (every blocker and its resolution is recorded in the
revision log above), and each successor revision was re-reviewed before the
final decision — "Numerical semantics, typed saved state, destination
protocol gates and corrected output ownership are coherent with the
inspected code. No blocking design findings remain." One non-blocking P3
(exercise executor destruction before retained-result reads) was carried
into the implementation's window UT. The subsequent implementation review
reused this recorded approval and found no deviation from it. This
document is the review artifact required by the feature-design gate, with
the approved revision identified above and preserved byte-for-byte in the
introducing commit recorded in the revision log; implementation is
delivered in PR matrixone/matrixone#31000.

## Classification

Design review is mandatory for this change:

- Complexity trigger: the feature adds a new public SQL surface (a new
  aggregate function name and calling convention), which is a public-API
  compatibility contract.
- Complexity trigger: the feature crosses the parser-binding, plan-rewrite,
  compile-configuration, aggregate-execution, and window-execution ownership
  boundaries (more than two subsystems).
- Default size trigger is expected to be met as well: estimator plus executor
  plus binder/compile integration lands in roughly five owning production
  packages (`pkg/sql/plan/function`, `pkg/sql/plan`, `pkg/sql/compile`,
  `pkg/sql/colexec/aggexec`, `pkg/sql/colexec/window`).

No prior or independent design approval covers a weighted percentile;
`docs/design/20260919-ordered-set-window-functions.md` explicitly covers the
unweighted ordered-set forms only.

## Problem and evidence

Users hold datasets whose rows already carry a count or weight: latency
samples with occurrence counts, position-size-weighted prices, and histogram
buckets whose row count is the bucket frequency. The exact percentile of that
distribution requires weighting each distinct value by its multiplicity.
Expanding integer weights into repeated rows is impractical (counts can be
large), and the supported unweighted ordered-set aggregate computes a
different statistic: on the issue's six-row fixture (values
`NULL, 2.0, 4.0, 4.0, NULL, -1.0` with weights `1,2,1,3,1,2`),
`PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY v)` returns 3, while the exact
weighted fiftieth percentile is 2.

Observed on the tested base (issue revision `23d8f61…`, re-verified on this
worktree's `origin/main` base `e3a6ffe27c` by registry inspection and an
executable probe below):

```sql
SELECT quantileExactWeighted(v, w, 0.5) ...
```

fails with `(20105, "not supported: function or operator
'quantileexactweighted'")`. The name is absent from
`pkg/sql/plan/function/function_id.go`'s `functionIdMap`; the dispatcher in
`pkg/sql/plan/function/function.go:1059` is the error site. No direct or
incidental weighted-percentile implementation exists in
`pkg/sql/plan/function` or `pkg/sql/colexec/aggexec`; the aggregates
registered for ordered statistics are `median` (138), `approx_percentile`
(557), `percentile_cont` (567), and `percentile_disc` (568).

Independent reference: ClickHouse `quantileExactWeighted(0.5)(v, CAST(w AS
UInt64))` on the same fixture returns 2, matching the weighted-CDF
computation below; issue #25144's basic ordered-set percentiles already
execute on this build.

## Payload contract (invariants)

### SQL surface

```sql
quantileExactWeighted(value_expr, weight_expr, level)        -- scalar aggregate
quantileExactWeighted(value_expr, weight_expr, level) OVER w  -- window aggregate
```

- `quantileExactWeighted` resolves case-insensitively; the registry key is
  the lowercased name (`quantileexactweighted`). No underscore alias is
  registered (decision D1); SQL docs will use the ClickHouse-compatible
  spelling.
- Exactly three arguments. `DISTINCT` is accepted and deduplicates equal
  `(value, weight)` pairs through the existing generic distinct machinery,
  the same contract `percentile_disc` provides for its value argument.
- The function is an ordinary aggregate: it supports `GROUP BY`, `HAVING`,
  distribution (distributed merge; remote dispatch of the aggregate is
  gated by decision D10), and `OVER` window specifications.
  It is rejected in sliding time-window contexts exactly as
  `percentile_cont` / `percentile_disc` are (decision D9).

### The weighted-percentile invariant

For one aggregate group, let `P` be the sequence of pairs `(v_i, w_i)` after
skipping every row whose value or weight is NULL (paired NULL rule). Values
may appear multiple times; ties are equivalent to one value carrying the sum
of their weights. A pair with weight 0 is retained in `P` as present data
with zero probability mass (decision D6). Let `W = Σ w_i` (uint64
accumulation, overflow aborts) and for each present value let `F(v) = Σ {
w_i : v_i <= v }` (the weighted cumulative distribution). With `level =
a/b` a rational in `[0,1]` from the validated aggregate configuration,
define the selection threshold once per group:

```
k = ceil(level * W) = ceil(a * W / b)        -- exact big.Int arithmetic
```

Because `level <= 1`, `k <= W`, so `k` always fits in uint64 even though `a`,
`b`, or their products can be arbitrarily large (the level text may carry up
to `big.Rat`'s full rational path: `1e-100` has a 10^100 denominator, and
`DECIMAL` levels reach scale 38; the configured level is decoded by the
existing codec that builds a `big.Rat` from the exact level text and requires
it to parse as FLOAT64 in `[0,1]`). The result is then:

```
result = min { v in P : F(v) >= k }            -- empty P: NULL
```

with `F(v) >= k` a plain uint64 comparison. Properties this single formula
fixes, with no special cases:

- the boundary at `level=0.5, W=8, F(2.0)=4` gives `k=4` and selects 2.0 (the
  "lower weighted CDF threshold" the issue profile verified), not the first
  value whose cumulative mass exceeds the threshold;
- with positive mass (`W > 0`): `level = 0` gives `k = 0`, so the result is
  the minimum present value — which may be a zero-weight value; any level
  with `ceil(level·W) >= 1` can never select a value that carries no positive
  mass, so `level = 1` (`k = W`) selects the largest positive-weight value
  and trailing zero-weight values are never selected;
- degenerate `W = 0` (each surviving pair carries zero weight) gives `k = 0`
  for every level, so the result is the minimum present value at every level;
- `NULL` is returned only when `P` is empty (no row survived the paired-NULL
  rule);
- interpolation never occurs: the result is always one of the input values
  (exact member selection). This is deliberately distinct from the
  interpolating `PERCENTILE_CONT` contract for rows/unit counts and is the
  property that makes the unweighted aggregate an invalid substitute.

### Argument domains and runtime rules

- `value`: the numeric domain shared with `MEDIAN` and the exact ordered
  percentiles (`aggexec.MedianSupportedType`): BIT, signed/unsigned integer
  widths, FLOAT32/64, DECIMAL64 and DECIMAL128; DECIMAL256 and width-38
  decimals are excluded like `percentile_cont` (decision D7: ClickHouse keeps
  this aggregate numeric; strings
  and date/time values stay out of scope for this revision). The result type
  is the input value's own type (decision D5: member selection needs no
  interpolation in the return value; the issue's profile returns a DOUBLE 2).
  NULL values are skipped; a group whose pairs were all skipped (empty input,
  or every row carrying a NULL value or NULL weight) returns NULL.
- `weight`: one explicit supported-weight set (decision D4, made consistent
  in revision 4): signed/unsigned integer widths, BIT, FLOAT32/64, and
  DECIMAL64/DECIMAL128. DECIMAL256 is NOT a weight type: `types.IsNumeric()`
  includes `T_decimal256`, so the set is enforced by an explicit bind-time
  rejection in the registration `checkFn` (the same
  invalid-aggregate-parameters failure the ordered-set value check uses),
  keeping registration, per-type validation, saved-state sizing, and tests on
  the same admission list. The weight argument is deliberately NOT cast to
  FLOAT64: the registry keeps each
  concrete numeric weight in its own type, because the ordinary numeric cast
  (a plain Go type conversion `T2(v)` in
  `pkg/sql/plan/function/func_cast.go:3678`) is lossy for magnitudes above
  2^53 and for high-scale decimals, and the executor could never recover the
  discarded information afterwards. Only an untyped (`T_any`) weight
  expression — a bare numeric literal such as `2` or `2.0` — is normalized at
  registration to FLOAT64, the established percentile-parameter convention. At
  fill time each weight is validated IN ITS OWN TYPE and only then converted
  exactly to uint64:
  - integer types (all signed/unsigned widths, BIT): integral by
    construction; reject a negative signed value; reject any value above
    `math.MaxUint64` (impossible for the unsigned widths); then convert
    exactly. UINT64 weights up to `MaxUint64` are accepted, ClickHouse-style;
  - FLOAT32/FLOAT64: reject NaN and ±Inf; reject a negative value; the actual
    stored floating-point value must satisfy `v == trunc(v)` (the value is
    its own exact quantity — there is no preceding conversion to hide
    rounding); reject values above the exact uint64 conversion range
    (integral doubles up to 2^63), then convert exactly. The issue's profile
    (`1.0, 2.0, 3.0` DOUBLE weights) is integral and accepted;
  - DECIMAL64/DECIMAL128: the fractional part is inspected with exact decimal
    arithmetic (e.g. `1.000000000000000001` is fractional and rejected — a
    cast to FLOAT64 would have hidden this), the value must be non-negative
    and within the uint64 range, then converted exactly.
  A violating row aborts the query with an invalid-input error naming the
  aggregate and the violated rule. NULL weights skip the row under the paired
  rule above. Weights are accumulated as uint64 with an overflow check; an
  overflowing total is an invalid-input abort. Weight-0 rows are retained in
  the state (present, massless) per decision D6.
- `level`: the same execution-invariant contract as the existing percentile
  aggregates: a non-null constant or a direct numeric prepared marker
  (validated with the existing `IsPercentileConfigExpr` class), taken in
  `[0,1]` by the existing config codec. The plan never evaluates the level
  per row; it is stripped into aggregate configuration at compile time
  (decision D3). A non-constant, NULL, or out-of-range level is a bind-time
  or compile-time invalid-input error, matching `percentile_cont`.

### Error and edge-path contract

- No candidate of `P` (all pairs skipped) → NULL result for the group, and
  the aggregate is registered as `EmptyResultNull` so planner rewrites cannot
  synthesize a non-NULL empty input value.
- Invalid weight or overflowing total → query abort with invalid input; no
  partial result is produced (the aggregate executor is freed by the existing
  pipeline error path).
- Cancellation, allocation failure, or overflow detected DURING finalization
  (decision D12) → the finalization error is returned, every scratch buffer
  and partially built result of that call is freed before the return, and
  the executor reaches a state the pipeline can `Free` (or reuse via window
  `Reset`) with no leaked allocation; a subsequent execution on a fresh
  executor observes no residue.
- A SUCCESSFUL finalization (decision D12) → the completed result vectors
  transfer to the caller and remain valid and readable until the caller's
  own cleanup releases them; the executor no longer references them and
  frees nothing on the success path except its scratch.
- Frame entry/exit in windows, partition boundaries, and merge order never
  change the result: pair sets commute.

## Standards, precedent, and alternatives

Standards: SQL ordered-set aggregates (ISO `PERCENTILE_CONT`/`PERCENTILE_DISC`)
are unweighted by specification; there is no standard weighted form, so this
is an extension surface and name choice follows the dominant precedent.

Precedent: ClickHouse `quantileExactWeighted(level)(v, weight)` — exact,
weighted, NULL-ignoring, unsigned-integer weights, member selection with the
lower-CDF convention verified live on the issue fixture (returns 2).
MatrixOne's existing percentile family establishes the level-as-config
machinery, the big.Rat level codec, and window integration; this design
reuses those exact mechanisms.

Alternatives evaluated (decision log):

- **A. Status quo + row expansion** (join to a series to repeat rows by
  weight): rejected. Impractical for large counts (the issue's motivating
  case), changes the statement cost class, and is unavailable inside
  windowing/pipelined usage.
- **B. Substitute unweighted `PERCENTILE_CONT`**: rejected by counterexample;
  on the issue fixture it yields 3 where the weighted percentile is 2, and no
  per-row rewrite recovers weighting information. The BVT test keeps this
  counterexample pair with both assertions.
- **C. Extend ordered-set syntax with a weight clause** (e.g.
  `PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY v WEIGHTED BY w)`): rejected.
  It invents nonstandard grammar, conflicts with the interpolating contract
  of `PERCENTILE_CONT`, and grows the parser/AST surface instead of reusing
  the plain aggregate path.
- **D. Weighted approximate estimator** (weighted TDigest and similar):
  rejected. The issue asks for the exact primitive; approximation is a
  separate contract (`approx_percentile` already covers the unweighted case).
- **E. Flat three-argument aggregate** (selected — this design). Reuses the
  ordinary-aggregate binding, registration, config extraction, and window
  paths; needs no parser change (`quantileExactWeighted` is tokenizable as an
  identifier already); matches the issue's probe name and ClickHouse
  comparability. Correctness, scope, and compatibility dominate the
  alternatives at every axis that matters here.

Non-goals: parity with ClickHouse's other quantile estimators or interpolation
variants; DolphinDB all-parameter parity; weighted `PERCENTILE_CONT`;
streaming sliding time-window execution.

Decision log (inline references D1–D12 resolve here):

- D1: one canonical public name, `quantileExactWeighted` (lowercased registry
  key `quantileexactweighted`); no `quantile_exact_weighted` alias.
- D2: a flat three-argument signature `(value, weight, level)` — not
  ClickHouse's curried `(level)(v, w)`, whose call form SQL cannot express,
  and not a WITHIN-GROUP grammar extension (alternative C).
- D3: the level is compile-time aggregate configuration (constant or direct
  prepared marker), reusing the existing percentile config codec.
- D4: the supported-weight set is exactly the signed/unsigned integer
  widths, BIT, FLOAT32/64, and DECIMAL64/128; DECIMAL256 is rejected at bind
  time (revision 4: `types.IsNumeric()` includes T_decimal256, so an
  "any numeric type" admission contradicted the executor rules and the
  saved-state width bound). The weight argument stays in its concrete type
  through
  registration; the executor validates it exactly in that type (integers
  integral by construction within the uint64 range; FLOATs by exact
  `v == trunc(v)` on the stored value within [0, 2^63]; DECIMALs by exact
  fractional-part and range inspection) and only then normalizes exactly to
  uint64. Untyped `T_any` weight expressions normalize to FLOAT64 at
  registration, matching the percentile-parameter convention. Revision 1's
  FLOAT64 transport is rejected because the registration cast itself is
  lossy (UINT64 2^53+1 round-trips under the old bound; fractional
  high-scale decimals read as integral).
- D5: the result type is the value's own type; the selection never
  interpolates.
- D6: one uniform rule defines every endpoint — zero-weight rows are
  retained as present-but-massless data, the threshold is
  `k = ceil(level · W)` computed exactly with big.Int, and the result is
  `min { v present : F(v) >= k }`. Consequently, with positive mass, level 0
  returns the minimum present (possibly massless) value and level 1 returns
  the largest positive-weight (never a trailing massless) value; an
  all-zero-weight group returns the minimum present value at every level;
  NULL is reserved for the empty pair sequence. Revision 1's simultaneous
  "minimum at W=0" and "maximum at p=1" promises contradicted this formula
  for trailing zero-weight values and all-zero groups and are corrected.
- D7: the value domain is numeric in this revision, matching the existing
  ordered-percentile sort machinery rather than extending to strings or
  date/time values.
- D8: no sliding-frame window optimization in this revision; the weighted
  multiset admits an exact inverse, so this remains a compatible future
  extension.
- D9: rejected in sliding time-window contexts, consistent with
  `percentile_cont` / `percentile_disc`.
- D10 (revision 3): mixed-version remote execution is gated by the protocol
  capability version minted for this aggregate (now the `MORPCVersion131`
  constant in `pkg/defines/const.go` — the aggregate's originally drafted slot,
  `MORPCVersion130`, was claimed first by upstream main's incomplete-temporal
  rework #30311, so the delivered gate is re-minted at the next free slot; see
  the post-merge annotation in the revision log), enforced against the ACTUAL
  destination in three verified layers — (1) at compile time, a
  `hasQuantileExactWeighted(node)` plan predicate paired with
  `AllCNsSupportProtocol(proc, <gate>)` fast-fails remote distribution when
  the cluster inventory contains any pre-gate CN (an incomplete inventory
  counts as unsupported, matching the helper's contract); (2) at
  remote-scope encoding, the aggregate's encoded ID raises the
  `requiredProtocol` floor (`encodeRemoteScopeWithProtocol`, the site that
  already raises the v122/v125/v129 feature floors); and (3) at send time,
  the destination confirms the floor on the live stream
  (`confirmProtocolOnStream` → `Method_PipelineProtocolCheck` with
  `ProtocolVersion = minimum`, executed before `sendPipeline`), so a worker
  that is below the gate — including one that rolled back between scope
  placement and sending, or whose capability is unknown — rejects the check
  and dispatch fails before the pipeline is delivered. The incumbent
  `supportsRemoteXxx(proc.GetService())` helpers are service-LOCAL checks
  on the coordinator's own runtime (`MOProtocolVersion` is the coordinator's
  service-local rollout gate); this design keeps a matching self-gate in
  `validateRemoteAggregateProtocol` for the coherent not-supported error
  message but does NOT claim it verifies destinations. Remote aggregates
  travel by encoded ID and configuration
  (`remoterun.go` `convertToPipelineAggregates`), not by SQL name, so an old
  worker that somehow received the new ID would fail inside
  `makeSpecialAggExec` with an internal "unexpected aggID" error; the three
  layers above exist so that this never happens. Local single-node execution
  needs no gate.
- D11 (revision 3): the saved-state schema retains the ORIGINAL typed weight
  rows (payload exactly the declared `argTypes`), and typed-to-uint64
  normalization happens once at `Flush`. The generic saved-state machinery
  interprets payloads through `aggInfo.argTypes` — DISTINCT spill restoration
  validates each member against its declared type including float widths, and
  Group's generic spill calls `SaveIntermediateResult`/`SaveSpillIntermediateRows`
  on every aggregate (`group/helper.go` spill loop) — so revision 2's
  uint64-normalized payload declared under the original weight type would be
  misparsed or rejected by those consumers. Typed rows keep DISTINCT, spill,
  wire, and merge behavior identical to the saved-argument aggregates the
  framework already handles, and `Fill`-time validation plus a pure
  finalize-time conversion preserves the exact D4 rules end to end (the
  maximum stored weight width follows D4's set: 16 bytes for DECIMAL128).
- D12 (revision 4, ownership corrected in revision 5): the finalization
  resource and cancellation contract. The executor implements
  `FlushWithContext` (the framework's
  `aggexec.FlushWithContext` only forwards context to interface implementers,
  concat2.go:1507-1520); cancellation is checked between groups and at a
  bounded per-row cadence inside each group's finalization. Resource
  ownership separates temporaries from output: SCRATCH (sort selector
  slices, collapse accumulators) is allocation-accounted and released on
  the success path and every error path; RESULT VECTORS are freed only on
  an ERROR return through the framework's error-only cleanup
  (`freeAggregateResultsOnError`, median2.go:1924-1935, as
  ordered_percentile.go:324 + :373-374 disposes only failing output and
  returns the live vectors on success), and on SUCCESS their ownership
  TRANSFERS TO THE CALLER, which retains, reads, and releases them through
  its own consumer cleanup (window.go:760-769). Scratch is allocation-accounted
  following the owned-finalization
  precedent (accounted scratch, explicit both-path frees at
  ordered_percentile.go:313-372); on cancellation, allocation
  failure, or overflow after partial finalization nothing of that call
  leaks, and the
  retained state itself is released through the framework's
  `aggExec.Free` (aggState.go:3439-3449), leaving the executor safe for
  immediate reuse or teardown. The validation plan carries deterministic
  witnesses asserting resource release and a successful subsequent
  execution after each failure mode, plus an ownership witness confirming
  successful results stay valid through caller consumption and are
  released by caller cleanup.

## Architecture: owners and end-to-end flow

First owner of each boundary (all in the assigned worktree):

1. Parser: no change. The generic function-call production accepts the
   identifier; `having_binder.BindAggFunc` (ordinary-aggregate path via
   `bindPreparedNumericFuncExpr` → `BindFuncExprImplByPlanExpr`) binds it
   once the registry entry exists.
2. Planner (`pkg/sql/plan`): new name constant `NameQuantileExactWeighted`
   and a `validateQuantileExactWeightedArgs` mirroring
   `validateOrderedPercentileArgs` (arg-count + level-must-be-config-expr +
   value-domain checks) invoked from the special-consumer validation section
   of `base_binder.go`; extend `PreparedPlanHasPercentileParams`
   (`utils.go`) so a prepared level forces the per-EXECUTE physical
   aggregate rebuild; extend the sliding time-window gate to reject the name.
3. Function registry (`pkg/sql/plan/function`): allocate the first
   unassigned catalog function ID past the current `FUNCTION_END_NUMBER`
   sentinel (605; the register's consistency tests audit gaps and the
   sentinel); register `class: Function_AGG`, `layout: STANDARD_FUNCTION`
   with a three-argument `checkFn` (value domain as in the ordered percentile
   continuous check; weight admitted exactly for decision D4's set —
   signed/unsigned integer widths, BIT, FLOAT32/64, DECIMAL64/DECIMAL128
   with DECIMAL256 rejected at bind time, the concrete type retained without
   a registration cast, and only a `T_any` weight literal normalized to
   FLOAT64 so the lossy registration cast `func_cast.go:3678` never touches
   a typed weight; level
   numeric/in-range handled as config) and one overload `isAgg: true`,
   `retType = value type function`, `aggName: quantileExactWeighted`; map
   the lowercased name in `functionIdMap`; keep `function_id_test.go`'s
   consistency checks green.
4. Compile (`pkg/sql/compile/operator.go`): a `constructQuantileExactWeightedConfig`
   equivalent to `constructOrderedPercentileConfig`, but keeping two data
   arguments (`value, weight`) and stripping only the level; wired into the
   `constructAggregateConfigWithError` dispatch, `preflightPercentileConfigs`
   (prepared level validation before scope construction), and
   `validateAggregateConfigs` (which already covers `WinSpecList`, so window
   SQL gets the same extraction).
5. Executor (`pkg/sql/colexec/aggexec`): new `AggIdOfQuantileExactWeighted`
   in `function_id.go`; dispatch case in `makeSpecialAggExec` (`types.go`);
   `EmptyResultNull` case in `empty_result.go`; new executor file
   `quantile_exact_weighted.go` following the saved-argument arena pattern
   (`orderedPercentileDiscreteExec`, `maxByExec` precedents — multi-type
   argument rows retained per group by the generic aggregate state with
   allocation accounting and the merge/wire protocol).
   - State (decision D11): per-group ordered list of `(value T, weight P)`
     payload rows in arrival order, where `P` is the weight argument's own
     numeric type — the saved payload is EXACTLY the declared `argTypes`, not
     a normalized form. The generic saved-state machinery interprets payloads
     through `aggInfo.argTypes`: DISTINCT spill restoration validates each
     member against its declared type (including float widths) and Group's
     generic spill saves every aggregate through
     `SaveIntermediateResult`/`SaveSpillIntermediateRows`
     (`pkg/sql/colexec/group/helper.go:~855`), so any mismatch between
     declared types and stored payloads would be misparsed or rejected by
     those paths. Typed weights satisfy every one of those consumers without
     aggregate-specific code. The level is decoded once from the extra
     configuration with a new `EncodeQuantileExactWeightedConfig`/decode
     pair (versioned like `orderedPercentileConfigVersion`, reusing the
     exact text + `big.Rat` + `[0,1]` validation semantics of
     `decodeOrderedPercentileConfig`).
   - `Fill`: skip paired-NULL rows; validate the weight in its own argument
     type exactly (decision D4 per-type rules); append the received pair
     UNCHANGED (validation only — no conversion happens at fill time).
     `BulkFill`/`BatchFill` as the framework templates them.
   - `Merge`/`BatchMerge`: append the other executor's group rows
     (state order independence), with the existing compatibility preflight
     (same arg/ret types and identical level). Merged states carry only
     already-validated weights, because every row was validated by the
     `Fill` that wrote it; `DISTINCT` deduplicates exact typed pairs.
   - `Flush`: sort one group's pairs by value (collation by the value type
     comparison used by the percentile machinery), collapse equal values by
     converting each stored typed weight exactly to uint64 (the same pure
     per-type rules validated at `Fill`) and summing with an overflow abort,
     then compute `k = ceil(a·W / b)` ONCE with `math/big` from the decoded
     rational level and the group's exact `W` (arbitrary precision, so tiny
     levels like `1e-100` and scale-38 `DECIMAL` levels stay exact; `k <= W`
     so it fits uint64), and scan cumulative weight with the plain uint64
     test `F(v) >= k`; emit the first qualifying value; NULL for an empty
     group (member selection, no interpolation). Moving normalization to
     finalize never changes results: the conversion is a pure function of
     already-validated rows.
   - Finalization resource and cancellation contract (decision D12): the
     executor implements `FlushWithContext(context.Context)`, because the
     framework wrapper `aggexec.FlushWithContext` (concat2.go:1507-1520)
     only forwards the context to aggregate implementations exposing that
     interface — a plain `Flush` alone would leave the group and window
     finalization paths without cancellation. Declared cancellation points:
     `context.Cause(ctx)` between groups and inside each group's
     sort/collapse/scan at a bounded per-row cadence, matching the
     per-row-check precedent. Ownership splits temporaries from OUTPUT:
     (a) SCRATCH — the per-group sort selector slice and the collapse
     accumulators — is allocated from the executor's `mp` through the
     allocation account and is freed on the success path and on every
     error path, so no scratch ever survives any return;
     (b) RESULT VECTORS — constructed per group during finalize — are
     freed ONLY on an error return, via the framework's
     error-only cleanup over the result slice
     (`freeAggregateResultsOnError`, median2.go:1924-1935), exactly as the
     owned-finalization precedent
     `orderedPercentileDiscreteExec.FlushWithContext` does
     (ordered_percentile.go:324 error-only deferred cleanup; :373-374 a
     successful return of the LIVE vectors); on SUCCESS, ownership of the
     completed result vectors TRANSFERS TO THE CALLER — the group-merge or
     window consumer retains, reads, and later releases them through its
     own cleanup path (the window operator caches ordered-set partition
     results and reads them across calls, window.go:760-769), and the
     executor no longer touches them. On cancellation,
     allocation failure, or overflow abort after partial finalization, no
     scratch or partially built result survives the ERROR return; the retained pair state
     remains owned by the framework's `aggExec.Free` path
     (aggState.go:3439-3449), so a failed finalization leaves the executor
     safe for immediate reuse (window `Reset`) or teardown by the pipeline's
     error cleanup.
   - Serialization (decision D11): the arena's saved-argument
     intermediate-result codec round-trips the TYPED pair rows, so CN
     parallel aggregation, the interim wire, `DISTINCT` restoration, and
     Group's generic spill protocol all stay consistent with the declared
     `argTypes` — a spill-enabled executor-specific variant is still
     intentionally not built in this revision (the state follows the
     saved-arg capacity behavior of `percentile_disc`: allocation-accounted,
     same wire protocol).
6. Window (`pkg/sql/colexec/window/window.go`): add the name to
   `orderedSetWindowAggregate` (`window.go:693`) so a full-partition frame
   takes the once-per-partition path (`processOrderedSetFullPartitionRange`:
   exactly `N` fills, one retained result per partition — without it the
   generic evaluator would retain one O(partition) state per output row and
   turn linear input into quadratic resident memory). Explicit frames
   (cumulative or bounded) use the generic per-row frame evaluator, which
   costs the sum of frame cardinalities per output chunk, bounded per
   chunk by `DefaultBatchSize` groups — the same capacity model
   `20260919-ordered-set-window-functions.md` records for the unweighted
   percentiles; no `MergePreservesSource` / `SupportsWindowSliding`
   implementation in this revision (decision D8). The per-argument window
   executor construction (`newAggregateExecutor`) already derives one
   argument type per argument expression for multi-argument window
   aggregates.
7. Mixed-version dispatch gate (`pkg/defines`, `pkg/sql/compile`): new
   protocol capability constant `MORPCVersion131` minted for this aggregate as
   the next free slot (`pkg/defines/const.go`, bumping the
   latest-version sentinel to 131; the drafted slot 130 was claimed first
   by upstream main's #30311 temporal rework, so the delivered constant moved
   to 131 — recorded in the revision log's post-merge annotation), then
   decision D10's three enforcement layers:
   (a) a `hasQuantileExactWeighted(node)` plan predicate added to
   `canCompileShuffleGroup`'s capability family and the remote-compile
   exclusion sites that already consult `hasOrderedSetPercentile`
   (`compile.go:6495`, `compile.go:8207`), paired with an
   `AllCNsSupportProtocol`-based compile-time fast-fail (the helper probing
   the coordinator AND every CN in the inventory, `expression_protocol.go`),
   so distribution is not even attempted while a pre-gate CN exists;
   (b) the required-version floor raised in
   `encodeRemoteScopeWithProtocol` (`remoterun.go:129-166`, beside the
   v122/v125/v129 feature floors) when the encoded pipeline contains the
   weighted aggregate's ID; (c) the send-time destination-stream
   confirmation (`remoterunClient.go:86-93,125-128,616+`
   `confirmProtocolOnStream` → `Method_PipelineProtocolCheck` before
   `sendPipeline`) that rejects an actual destination below the floor. A
   service-local self-gate matching the incumbent aggregate gates
   (`supportsRemoteQuantileExactWeighted(proc.GetService())` in
   `validateRemoteAggregateProtocol`, `remoterun.go:2033+`) supplies only
   the coherent coordinator-side not-supported message.

No new long-lived component, cache, worker, queue, or configuration knob.
No catalog, persistence, protobuf, or wire-format change beyond a new
encoded aggregate ID value (an opaque constant in the existing Function.Obj /
AggFuncExecExpression channels).

## Resource and performance model

Aggregate fill is a validated O(1) append per row (the validation is a few
branches per weight type); resident state is the typed pair ROWS plus the
per-group arena store overhead the framework already accounts
(`rows × (sizeof(value) + sizeof(weight type))` payload bytes plus the
vector/arena bookkeeping that governs `percentile_disc`), all
allocation-accounted. Finalize is `O(n log n)` for the sort plus `O(n)` for
the collapse/scan (including the per-row typed-to-uint64 conversion), and its
PEAK temporary footprint per group is the bounded scratch of decision D12 —
a sort selector slice (8 bytes per row) and the collapse accumulator pair —
allocated via `mp`/the allocation account and released at EVERY return, plus
the in-flight result vector, which is freed only on error and on success
transfers to the caller (output lifetime is the consumer's, window.go:760-769
precedent); peak
resident memory during a group's finalize is therefore the state plus that
bounded O(n) scratch and result growth, never a second copy of the pair
payload. The window
full-partition path performs exactly one
finalization per logical partition per materialized generation, identical to
the unweighted percentiles. Large-count weights are the intended use case and
do not scale state by the weight total (weight expansion never happens).
The level's rational arithmetic allocates once per group finalize, not per
row. The weight argument travels in its original type into the state and is
converted to uint64 once per row at finalize; no registration-time weight
cast exists.

## Compatibility, rollout, operations

- Mixed-version clusters (decision D10, revised after review): SQL name
  binding happens on the coordinator only, and remote pipelines carry the
  encoded aggregate ID plus configuration (`pkg/sql/compile/remoterun.go`
  `convertToPipelineAggregates`), not the SQL name; an old worker receiving
  the new ID would fail inside `makeSpecialAggExec` with an internal
  `unexpected aggID` error. Revision 2 wrongly treated the
  `supportsRemoteXxx(proc.GetService())` family as destination checks —
  those helpers read the COORDINATOR's own service-local runtime, so a
  coordinator at the gate version with a pre-gate worker passed. Revision 3
  anchors mixed-version safety on mechanisms that observe the actual
  destination: (a) compile-time fast-fail via
  `AllCNsSupportProtocol(proc, <gate>)` when a remotely distributable
  plan contains the weighted aggregate and the cluster inventory holds any
  pre-gate CN (incomplete inventory counts as unsupported); (b) the
  `requiredProtocol` floor raised during `encodeRemoteScopeWithProtocol`;
  and (c) the send-time destination-stream confirmation
  (`confirmProtocolOnStream`, executed before `sendPipeline`), which
  rejects dispatch when the destination itself confirms a lower version —
  this also covers a worker that rolled back between scope placement and
  sending, and unknown-capability destinations. When an old binary is the
  coordinator, it rejects the SQL name itself, as today.
- Upgrade and rollback ordering: during a rolling upgrade a mixed cluster
  rejects the remotely-dispatched form — at compile time when the inventory
  already shows a pre-gate CN, otherwise at destination confirmation —
  with the documented not-supported error, until every participating CN
  advertises the gate version; full behavior follows the upgrade. Rolling
  back the binary is
  clean: no persisted state, catalog row, or wire format of this feature
  survives (the aggregate's state is query-local), an older coordinator
  cannot bind the name at all, and a new coordinator never delivers the
  aggregate's pipeline to a destination below the gate.
- No SQL-level feature flag on top of the capability gate; the incumbent
  percentiles ship exactly the same way (a protocol capability, not a
  runtime configuration knob).
- No persisted state, no upgrade/downgrade/migration, no restart coupling.
- Tenancy/security: an aggregate over user-visible columns only; inputs are
  validated (weights bounded integral, level range-checked) so no numeric
  path can be driven into unbounded allocation by argument values alone;
  state size is bounded by input rows through the existing accounting model
  shared by all retained-value aggregates.
- Observability: the aggregate flows through the standard spill/analyze
  counters of its operators where generic (output size, memory analyzer);
  no new metric series.

## Verification plan (cheapest deterministic evidence first)

1. UT `pkg/sql/colexec/aggexec/quantile_exact_weighted_test.go` (new) — the
   component contract. Matrix, all deterministic and synthetic:
   - the issue fixture verbatim (DOUBLE values incl. two NULLs and a negative
     value; weights 1,2,1,3,1,2): level 0.5 must produce 2, and the
     unweighted percentile control on the same values must produce a
     different value (counterexample against substitution);
   - boundary profile: `ceil(level·W)` exactly on a cumulative boundary
     selects the lower value (fixture above);
   - zero-weight endpoints (review finding 4 witnesses): trailing zero-weight
     value — pairs (1, w=1) and (9, w=0) at level 1 select 1 (never 9);
     leading zero-weight value at level 0 selects the massless minimum;
     at any level with `ceil(level·W) >= 1` massless values are never
     selected; an all-zero-weight group at level 1 (and every level)
     returns the minimum present value;
   - ties: repeated values collapse to summed weight; weight 0 rows;
   - paired NULL rules: NULL value, NULL weight, and both;
   - empty group and all-skipped group → NULL (executor and
     `GetEmptyResultKind` registration);
   - typed weight validation before any conversion (review finding 1
     witnesses): UINT64 9007199254740993 (2^53+1) is ACCEPTED with exact
     accumulation through the original-type path (it round-tripped to
     2^53 under revision 1's FLOAT64 transport), UINT64 `MaxUint64`
     accepted, and a total exceeding uint64 rejected at accumulation;
     DECIMAL 1.000000000000000001 is REJECTED as fractional by exact
     decimal inspection (it read as integral 1 under revision 1);
     DECIMAL integral weights accepted; DOUBLE 2.5 rejected, DOUBLE
     NaN/±Inf/negative rejected through the float's own `v == trunc(v)`
     and range rules; FLOAT32 integral weights accepted; an untyped
     `T_any` literal weight normalizes through FLOAT64 as today;
   - finalization failure and cancellation (decision D12, deterministic
     witnesses): context cancellation injected between groups and
     mid-scan/sort surfaces the context error; an injected allocation
     failure at the n-th finalization scratch allocation surfaces the
     allocation error; a mid-finalize overflow abort (huge valid weights
     summing past uint64 after partial results were built) surfaces the
     invalid-input error — each asserting that every scratch buffer and
     every partially built result is released on the error return
     (error-only result disposal, matching freeAggregateResultsOnError;
     allocation-account balance returns to its
     baseline, using the existing aggexec accounting/failure-sweep test
     harnesses) and that a subsequent execution on a fresh executor
     succeeds and returns the correct result; plus an OUTPUT-OWNERSHIP
     witness: after a successful finalize the returned vectors stay valid
     and readable through simulated caller consumption — retain, repeated
     reads, then caller-side release, the window-consumer pattern
     (window.go:760-769) — and the executor's subsequent `Free` does not
     double-release them;
   - types: int8/int64/uint64/float32/float64/decimal64/decimal128 value
     domains (result type = input type);
   - threshold arithmetic across the accepted level domain (review finding 2
     witnesses): level `1e-100` with positive `W` yields `k = 1` and the
     first positive-mass value without over/under-flowing fixed-width
     products; high-scale DECIMAL levels (e.g. a 22-digit 0.333… and
     0.9999999999999999999) resolve exactly through the same
     `k = ceil(a·W/b)` big.Int computation; level exactly 1 and exactly 0
     establish the endpoint behavior;
   - merge equivalence: split a group's rows into several partial states,
     merge in every shuffled order, result matches the single-state result;
     also the interim wire round-trip (save/load) of a partial state;
   - saved-state schema (decision D11, review round-3 witnesses): `DISTINCT`
     typed pair deduplication round-trips including FLOAT32 and DECIMAL128
     weights (the spill-restoration path that validates members against
     `argTypes`, `aggState.go:~909-923`); the generic Group spill path
     (`SaveIntermediateResult`/`SaveSpillIntermediateRows`, `group/helper.go:~855`)
     saves and restores typed rows without width or bit-pattern mismatch;
     config codec: versioned encode/decode (valid level text, invalid
     direction/level guards reuse the ordered-percentile decode semantics).
2. UT for the compile and dispatch boundary: `constructQuantileExactWeightedConfig`
   tests — constant level extraction, prepared-marker evaluation, wrong
   arg-count, non-config level rejection, and that the data arguments
   survive as `[value, weight]` in their original types. Registration
   `checkFn` tests: admission of exactly decision D4's weight set and the
   bind-time rejection of a DECIMAL256 weight (the `DECIMAL(65,0)` witness —
   `types.IsNumeric()` includes T_decimal256, so the exclusion must be
   explicit). Mixed-version gate
   tests per decision D10's three layers: the required-floor raise inside
   `encodeRemoteScopeWithProtocol` when the encoded pipeline contains the
   weighted aggregate's ID; the send-time destination confirmation rejecting
   a worker below the floor (coordinator 130 / worker 129), a destination
   with unknown capability, and a destination that rolls back between
   placement and sending — asserting rejection BEFORE `sendPipeline`
   (before pipeline dispatch), using the remoterun client's existing
   stream-confirmation seams and test harnesses; plus the compile-time
   `AllCNsSupportProtocol` fast-fail with a fake inventory containing a
   pre-gate CN, and the coordinator self-gate error message in
   `validateRemoteAggregateProtocol`.
3. BVT `test/distributed/cases/function/func_aggr_weighted_quantile.test`
   (+ reviewed `.result`): self-contained fixture as required by the issue;
   entry availability; exact issue result (2) beside the unweighted
   `PERCENTILE_CONT ... = 3` control; `GROUP BY` partitions; window
   `OVER (PARTITION BY …)` full-partition results; a cumulative ROWS frame
   with frame entry/exit (including rows that leave the frame); tie-heavy
   data; all-NULL group; empty table; DISTINCT form; levels 0 and 1 with
   trailing zero-weight rows (never selected beyond the last
   positive-weight value); typed weight columns (UINT64 over 2^53, integral
   DECIMAL, DOUBLE) beside literals; the documented error rejections
   (out-of-range level, fractional/negative weight, and the DECIMAL(65,0)
   weight bind-time rejection per decision D4) with error-bearing
   statements; prepared-statement execution with two different level
   marker values. Generated results are reviewed against
   these expected values, not accepted verbatim.
4. Primary red/green proof (implementation phase): the new BVT weight-percentile
   statements executed against a `mo-service` built from this worktree's
   parent/base record the 20105 not-supported failure (matching the issue
   3/3 reproduction), and the identical statements pass on the fixed build
   (issue fixture → 2). Recorded with the exact commands and result files.
5. Static checks: incremental gofmt/vet/lint over the changed package
   closure only.
6. Deferred to CI by cost: the full distributed BVT suite; no unrelated
   package runs in the local proof.

Verification environment prerequisite (recorded at design time): this
worktree and the primary checkout on this host currently lack built native
artifacts (`cgo/libmo.dylib`, `thirdparties/install`), so every `go test` in
the CGo-transitive closure is blocked at the C-header layer until
`make cgo` stages them (`mo-cgo-test` reports the same). The
implementation phase must run that build before its first UT/BVT evidence;
the blocked probe logs are preserved with this issue's artifacts.

## Drawbacks, risks, and open decisions

- The public name is frozen once shipped; the underscore alias was rejected
  to keep one canonical surface (D1) — a reviewer may overturn this.
- Weight domain breadth is now the exact typed validation surface: integer
  weights up to uint64 (ClickHouse parity), integral FLOAT32/FLOAT64 values
  up to 2^63, and exact-range integral DECIMAL64/128; DECIMAL256 weights are
  rejected at bind time (D4, revision 4). FLOAT-typed
  weights remain bounded by what the floating column itself can represent:
  a high-scale fractional decimal forced into a DOUBLE column is rounded by
  that column's own type before the aggregate sees it, and the aggregate
  still validates the stored value exactly; typed columns keep full
  exactness. Documented as a bound.
- Finalization adds allocation-accounted O(n) scratch per group during
  finalize (decision D12): sort selectors, collapse accumulators, and the
  result vector growth. The peak is a bounded multiple of the group's row
  count, never a second full pair-payload copy; scratch is released at
  every return, while the completed result vectors are freed only on
  error and transfer to the caller on success (output lifetime belongs to
  the consumer, window.go:760-769 precedent).
- The mixed-version gate (D10 revision 3) means a rolling-upgrading cluster
  reports not-supported, not wrong results, for remotely distributed
  executions until all participating CNs reach the gate version; the
  destination-level behavior is covered by send-confirmation UT against
  stream stubs (coordinator 130 / worker 129, unknown capability, rollback
  between placement and sending) rather than a live multi-version cluster
  BVT.
- The typed saved-state schema (D11) stores the weight in its argument type
  (up to 16 bytes for DECIMAL128) instead of a fixed 8-byte normalized form;
  the honest per-row cost is `sizeof(weight type)`, exchanged for full
  consistency with the framework's saved-state, DISTINCT, and generic-spill
  consumers.
- Explicit window frames take the generic evaluator; no sliding-frame
  optimization in this revision (D8). A weighted multiset can support an
  inverse (count decrement); deferred as deliberately out of scope.
- Time-window (streaming/sliding) usage is rejected (D9), consistent with the
  existing percentile aggregates.
- Added maintenance breadth (registry, binder validation, compile config,
  executor, window name gate) is the recurring cost of a new SQL aggregate;
  it is reduced by reusing the percentile-level configuration machinery
  verbatim.

Blocking questions: none; the decision log entries carry defaults that a
review can alter before implementation.
