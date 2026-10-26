# DISTINCT statistics and adjacent aggregate ownership

## Contract and owners

DISTINCT rewriting must preserve trusted source cardinalities and equality.
A downstream aggregate may inherit an existing complete owner if it retains
that owner's active grouping key. Establishing a new exchange is a different
cost decision: marginal NDV alone does not bound the distinct state of a hot
group or the amount of data assigned to one worker.

`distinctPairGroupCol` produces references for both scalar and tuple rewrites.
It resolves estimates through `getExprNdv` and registers their existing
`syntheticNDVCols` provenance. Arbitrary carried estimates remain untrusted.
This reuses the same ownership model as grouping-set materialization.

`determineShuffleForGroupBy` proves adjacent aggregate ownership through
`reusableShuffleChild` before applying new-exchange costs. Retained active
keys reuse the established distribution even when their NDV is too small to
justify a fresh exchange. Non-mergeable DISTINCT, grouping-set and lossy
string-range guards remain in force. This does not broaden join reuse. Each planning pass clears stale decisions
and uses its actual column-reference mode when proving ownership.

For incompatible aggregate keys, retain local partial aggregation followed by
MergeGroup. Do not exchange the preceding aggregate's rows before local
reduction solely because its output NDV is large. The aggregate-specific admission boundary covers this case after attempting
compatible reuse. The shared join/aggregate `dontShuffle` policy is unchanged.
A future parallel final merge needs a cost and partial-state transport contract,
not a threshold that assumes uniform state per final group.

## Rejected alternatives and costs

Forcing pair deduplication onto a final grouping key eliminated Q14's serial
merge but regressed a dominant-group workload. Replacing pair rewriting with
the existing native DISTINCT path also regressed that workload. Correcting NDV
without closing the new-exchange admission path exposed another correlated-key
regression. These alternatives are excluded from the implementation.

The retained changes add bounded planning work per rewritten key. They reuse
existing statistics provenance, physical distribution, Group, MergeGroup and
spill ownership. No scheduler, runtime state, cache, wire format, storage or
configuration is introduced. Matching DOP and cluster-global ownership remain
existing compiler/planner contracts. DOP=1 keeps its current physical fallback.

## Validation

Typed planner regressions cover scalar/tuple provenance, stale estimates,
retained/different/inactive keys and high-NDV incompatible owners. Public SQL
covers COUNT/SUM/AVG DISTINCT, NULL, empty input, multiple keys, PAD SPACE,
prepared reuse and forced spill. Performance controls include Q14, existing
numeric ownership, uniform and dominant groups, and correlated composite keys
at one and four workers. Results use independent oracles; comparisons use the
same data and resource budgets. No Q14 speedup is claimed for this retained
patch.
