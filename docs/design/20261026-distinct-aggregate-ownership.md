# DISTINCT rewrite statistics and aggregate ownership

Base: `b466f4ae9b74813a4ff93eaf43a6c81e6cbb8b0b` (main verified
2026-10-26). Context: ClickBench investigation following issue #27683;
implementation PR not created. This is a staged optimization, with production
delivery blocked on the performance and correctness gates below.

## Evidence and contract

On the same 30M official rows, four workers and no spill, Q14 takes 1.129s
versus local DuckDB 0.197s. Pair deduplication assigns 75% of its 3.006M
groups to one numeric range owner. The following aggregate estimates one
group instead of the observed 1.898M and performs a serial 328-349ms merge.
A diagnostic full-key hash distribution balances pair owners and reduces
latency about 15%, but leaves the merge. These measurements establish
contributors, not a promised combined improvement.

Invariant: the DISTINCT rewrite preserves equality, NULL behavior and
statistics provenance, and the physical plan performs one complete final
aggregate per final group whenever existing distribution proves that owner.
Unknown statistics are not evidence of a particular cardinality.

## Owners and proposed bounded changes

1. `distinctPairGroupCol` is the common producer of rewritten scalar and
   tuple DISTINCT references. Resolve its source estimate through existing
   statistics and register a trustworthy carried estimate in
   `QueryBuilder.syntheticNDVCols`. Do not introduce another statistics map or
   globally accept arbitrary `Expr.Ndv` values.
2. `determineShuffleForGroupBy` owns physical aggregate distribution. A
   directly preceding aggregate whose complete distribution key is retained
   by the consumer already proves compatible ownership through
   `reusableShuffleChild`. Reuse does not incur an exchange and must not pay
   the cost thresholds for creating one. Inactive grouping keys and the
   existing non-mergeable/grouping-set guards remain authoritative. Restrict
   this closure to aggregate children; join distribution policy is unchanged.
3. Stage A measures preservation of an existing complete-group **hash**
   decision across the single COUNT DISTINCT rewrite. Ask the existing
   aggregate cost policy about the original logical aggregate before the
   rewrite. When it admits hash ownership by a final grouping key, the pair
   stage may use that same key and the final stage can reuse it. Retain the
   existing pair-key marker, renamed to describe pair-stage distribution;
   there is no new execution state or strategy. Numeric range policies are
   not replaced. Scalar/global, small-state, unsupported-domain and unknown
   cases keep their current distribution choices.

Stage A is an experimental design, not approval to ship the third change.
Existing marginal NDV does not describe within-group distinct-state skew.
The dominant-group control below can reject this proposal before delivery.
If rejected, remove the policy change, not its counterexample or assertions.

## Alternatives and cost

- Status quo retains lost statistics, imbalance and avoidable merges.
- Blanket numeric Range-to-Hash was previously rejected by a repeatable
  correlated composite-key regression; marginal distinct CDF also failed a
  weighted composite-group counterexample. Neither is reopened here.
- Disabling DISTINCT rewriting creates many independent distinct sets and
  retains existing expensive generic execution. It is not this proposal.
- A new scheduler, sketch, cache or reference-counted string representation
  would introduce unrelated lifecycle and capacity contracts; not justified
  before testing reuse of the existing partition and Group machinery.

Planning adds bounded work per aggregate/group key. Compatible reuse removes
an exchange/merge and its hash/string materialization. Choosing final-group
ownership can concentrate a dominant distinct group: it is a real acceptance
risk even when global group NDV is high. No universal speedup is assumed.

No API, configuration, storage, protocol or persistent-state change. Existing
Group/Shuffle own admission, spill, cancellation, reset and cleanup. DOP=1
uses the existing compiler fallback. Distributed reuse must still prove a
cluster-global owner and matching DOP, not merely CN-local partitioning.

## Validation and review gate

- Deterministic planner tests: base-stat provenance through scalar and tuple
  rewrites, unknown/stale estimates, exact original grouping identity;
  matching/mismatching partition keys, inactive keys and existing guards.
- Real SQL: nullable DISTINCT, multiple grouping keys, PAD SPACE/equality,
  empty/all-NULL input and repeated prepared execution. Exercise normal and
  forced-spill routes with independent result oracles.
- Measure Q14 on retained 30M data against the exact base, matched resources,
  warmups, repeated phases, exact results and explicit spill instrumentation.
- Controls reuse retained 10M data: small/uniform/clustered/hot/composite
  grouping; scalar distinct and grouped distinct with a dominant large group;
  worker counts 1 and 4. Add only the smallest missing adversarial fixture.
- A viable candidate needs repeatable material primary benefit and no
  demonstrated regression in controls; do not run broad acceptance on a
  rejected policy. Report memory alongside latency and keep cache scopes
  explicit. Do not compile concurrently with performance measurements.
- Before delivery, owning planner tests, affected race/static checks, public
  execution and `mo-self-review` must pass. Reuse unchanged engine evidence;
  add distributed validation if the accepted patch alters that contract.

Design review, revision 1: hot-path topology/capacity trigger applies. Existing
owners and alternatives inspected before code. Self-review permits Stage A
measurement and the isolated provenance correction; delivery is pending the
dominant-group challenge and all acceptance gates. No independent reviewer is
claimed; the user's instruction to avoid subagents is preserved.
