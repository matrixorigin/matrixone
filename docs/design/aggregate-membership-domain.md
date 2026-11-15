# Membership restrictions below correlated aggregates

Owner: #30843. Approved by the requester on 2026-11-16 as independent PR 4.
Base: f70c37b16e. Reuse existing decorrelation, not pending vector/runtime APIs.

## Contract and design

A mandatory positive membership on an outer key may restrict a correlated
aggregate's matching group key. Keep the original membership and scalar result
comparison; insert a SEMI join below aggregation against a freshly bound copy of
the filtered membership source. Every surviving outer key retains exactly its
original aggregate-input bag. SEMI avoids multiplying facts for duplicate keys.

Clone only a filtered base-table membership source at the same snapshot, never
the complete outer relation or an aggregate-dependent filter. Require direct
ordinary integer equality to a grouping key and total SUM/COUNT inputs. Preserve
composite groups, NULL equality, empty/all-NULL aggregates and missing groups.
Reject OR-dependent membership, NOT IN, computed/mixed-domain keys, volatile or
fallible sources, shared/barrier/recursive dependencies and copied message IDs.
Recognize an existing equivalent restriction and do not add another.

Cost the domain scan/build and fact probes against reduced aggregate input and
state using finite known row/NDV/width estimates. Require strictly lower local
estimated work; unknown/unselective cases retain their valid original plan.
Prove semantic eligibility independently of cost. Bind the full proposal before
publication; generate new bindings and let ordinary later passes generate fresh
runtime-filter/distribution dependencies. Existing operators retain bounded
admission, spill, cancellation and cleanup. No new cache, API or wire format.

## Validation

Typed tests cover equality/correlation mapping, duplicate prevention, binding
and source-snapshot identity, existing restrictions and negative/cost boundaries.
Small independent SQL cases cover duplicate/NULL domain keys, missing groups,
all-NULL SUM, composite correlations, predicate ordering and unsafe forms.
Verify unchanged Q20 reduces aggregate input/groups. Benchmark this branch alone
against main locally in SF10 memory/disk and SF100 disk: one process/configuration,
one excluded cold plus five warm full suites. Publish all 22 queries and Total
with wall/instructions/allocations/copies; no spill/swap or script verification.
R2: relational bag identity/error evaluation and planner dependency construction.
No historical group-count ratio is a speedup forecast. Confirmed regressions
require diagnosis and a reviewed solution before further production changes.
