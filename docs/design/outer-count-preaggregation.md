# Preaggregate COUNT before eligible outer joins

Owner: #31138. Approved by the requester on 2026-11-16 as PR 2 of the independent
TPC-H wave. Base: f70c37b16e. No prerequisite PR is required.

## Contract and design

For one ordinary COUNT(right_column), group filtered facts by a direct integer
join key before a LEFT join to a base scan with a complete SQL-equality-compatible
primary key. Replace the original aggregate with a projection of the preserved
key and COALESCE(partial_count, typed_zero). Preserve COUNT's exact result type
and non-nullability. Matching all-NULL groups and unmatched rows both yield zero.
Orphan facts never create preserved rows. Estimates cannot prove uniqueness.

Retain input filters. Right-only ON predicates may move below aggregation only
when total and deterministic. A local proof may admit canonical two-argument
LIKE/NOT LIKE over a direct bounded CHAR/VARCHAR column and literal pattern in
legacy/general-ci text domains; dynamic/ESCAPE/Unicode-ci/fallible shapes reject.
Reject COUNT(*), DISTINCT/configured/mixed aggregates, unproven uniqueness,
cross-side residuals, shared nodes, barriers, grouping sets and aggregate-local
HAVING/LIMIT/OFFSET. Keep ancestor filters/order/limits above the replacement.

Use finite positive estimates N (facts), G (filtered key NDV), L (preserved rows).
Require N>=4*UnitLimit, N>=4*G and G<=L. Construct and bind the full proposal
before publication, assign fresh tags, remap names/references, clear stale
physical orientation and recalculate statistics before distribution planning.
Honor the existing aggregate-pushdown disable hint. Existing executors own
admission/spill/cancellation/cleanup. No new API, runtime state or wire format.

## Validation

Typed tests prove legality, rejection, reference/type integrity, fresh bindings
and cost boundaries. Small independent SQL cases prove zero/one/many matches,
NULL values/keys, orphan facts, ON/WHERE placement and empty inputs. Verify
unchanged Q13 reaches the transformation. Compare join/probe/materialized work,
then isolate this PR against main using local SF10 memory/disk and SF100 disk.
One process/configuration, one cold plus five warm full suites; report all 22
queries and Total with wall/instructions/allocations/copies. No script data/result
verification, spilling or swap. Confirmed regressions block readiness.

R2: planner shape, SQL null extension and error evaluation are affected. Preserve
existing COUNT executors instead of inventing weighted COUNT/partial SUM, whose
type and overflow contracts would require a broader design. Keep rejection as
the original valid plan, never a new user-facing error. This version does not
support duplicate preserved-key bags without a catalog uniqueness proof.

## Approved regression revision — 2026-11-16

The requester approved the four proposed fixes with “then fix”.

Cap a valid positive group estimate locally by an available exactly representable small-integer min/max span, with matching statistics type and a possible NULL group. Missing or invalid bounds retain the original estimate. Keep all original cost thresholds and catalog legality proofs; change no global statistics.
