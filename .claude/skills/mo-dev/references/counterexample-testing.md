# Counterexample-Driven White-Box And Black-Box Validation

Use this method for subtle kernel, planner, optimizer, rewrite, execution, and `EXPLAIN` defects. The objective is not to preserve one reported scenario. The objective is to identify the violated contract, find the smallest witness, and verify the surrounding behavior without coupling the test to the current implementation.

## Contents

1. Contract And Evidence Model
2. End-To-End Workflow
3. Choosing Test Layers
4. Deriving Counterexamples
5. Designing Independent Oracles
6. Planner And EXPLAIN Guidance
7. Deterministic Test Infrastructure
8. Orthogonalizing The Suite
9. Acceptance Gate
10. Test Matrix Template

## 1. Contract And Evidence Model

Begin with a behavior contract, not the observed stack, SQL spelling, plan text, or proposed fix.

Write these four items before changing production code:

1. **Invariant**: what must remain true for every supported input and state.
2. **Negation**: the smallest observable condition that disproves the invariant.
3. **Reachability**: the public path that produces the state for an externally
   visible claim, or the documented internal domain for an internal-only totality contract.
4. **Oracle**: an observation independent enough to decide correctness.

Use this evidence order:

1. Black-box result, error class, side effect, or externally visible lifecycle state.
2. Typed internal plan, operator, ownership, or state-machine observation.
3. Stable semantic fields in `EXPLAIN` or another diagnostic interface.
4. Logs and comments only as supporting context.

Comments and implementation assumptions are not evidence. A nil check proves defensive behavior only after reachability and the intended meaning of nil have been established.

## 2. End-To-End Workflow

1. Normalize the report into an invariant and its negation.
2. Trace the public entry point to the failing boundary for externally visible
   behavior; for an internal-only contract, define and justify the complete valid domain.
3. For an externally visible claim, build the smallest black-box witness through the supported interface.
4. Add the narrowest white-box test when it proves a distinct internal transformation, totality, or state-boundary claim.
5. Vary independent dimensions around the witness, including both controls and counterexamples.
6. Use differential or metamorphic relationships where a direct expected value would duplicate implementation logic.
7. Run the tests against the unfixed revision and record the intended failure reason.
8. Apply the production fix, then rerun the same tests.
9. Remove redundant cases and retain one reason for every remaining row.
10. Place tests at their natural ownership layer: focused unit coverage near the invariant and public-path regression coverage near the user-visible contract.

A strong regression fails on the unfixed code for the claimed semantic reason and passes on the fixed code without timing luck.

## 3. Choosing Test Layers

Use the cheapest layer that can observe the required contract:

| Required observation | Preferred layer |
|---|---|
| Pure expression, rewrite, formatter, or node invariant | Direct package unit test |
| Parse, bind, catalog resolution, statistics, or optimizer interaction | Planner fixture or lightweight embedded service |
| SQL protocol, session state, transaction behavior, or public error mapping | Embedded frontend black-box test |
| CN placement, remote pipeline, distributed planning, or service lifecycle | Minimal multi-service cluster test |
| Stable end-user SQL behavior across releases | SQL regression case |

Do not start a full cluster when a typed unit test proves the boundary. Do not stop at a synthetic node when the defect depends on a reachable planner path. One bug often needs both layers because they answer different questions:

- The black-box test proves the supported interface remains correct.
- The white-box test localizes the invariant and covers boundary combinations cheaply.

If a synthetic internal state cannot be produced by the real planner, use it only to establish a documented totality or defensive contract. Do not present it as reproduction evidence.

## 4. Deriving Counterexamples

Derive cases from independent semantic dimensions rather than copying the report with small textual variations.

Consider only dimensions relevant to the invariant:

- Logical shape: scan, join side, join depth, subquery, CTE, projection, filter, aggregate, window, or DML context.
- Physical shape: one-CN/multi-CN, local/remote, shuffle/broadcast, runtime filters, parallelism, or materialization.
- Data shape: empty, one row, duplicates, NULL, skew, boundary cardinalities, and values around thresholds.
- Representation: equivalent SQL spelling, aliases, qualification, commuted predicates, casts, and compatible types.
- State transition: prepare, reuse, reset, cancel, error, retry, and cleanup.
- Metadata: absent/present/stale statistics, indexes, constraints, partitions, and catalog changes.
- Rendering or consumer: text, JSON, DOT, verbose, physical plan, or downstream plan consumer.

Start with pairwise combinations. Add a higher-order combination only when code flow or prior evidence shows an interaction among three or more dimensions. Avoid a Cartesian product that raises cost without testing a new claim.

For every counterexample, include the nearest control that changes one dimension while preserving the others. Boundary pairs such as `N-1`, `N`, and `N+1` are more informative than several arbitrary values.

Prefer metamorphic relationships when useful:

- Equivalent relational rewrites must return equivalent results.
- Enabling an optimization may change the plan but must not change query semantics.
- `EXPLAIN` variants may change presentation but must remain total for every valid reachable plan.
- Changing coordinator placement must not change logical results.
- Reuse or reset must produce behavior equivalent to a fresh instance.

## 5. Designing Independent Oracles

### Black-Box Oracle

Exercise the public SQL or service interface and assert the contract directly:

- exact rows and types when deterministic;
- unordered row sets when order is not part of the query contract;
- error code or class, not incidental wording;
- committed side effects and post-failure service health;
- equivalence with a simpler reference query or an optimization-disabled control.

Do not use `EXPLAIN` text to prove query-result correctness. Do not treat absence of panic as proof of correct semantics when a stronger result oracle is available.

### White-Box Oracle

Inspect the typed state at the narrowest relevant boundary:

- plan node kind and typed expression shape;
- transformation preconditions and postconditions;
- ownership or lifecycle state;
- stable semantic annotations;
- total handling of valid variants such as absent optional fields or control markers.

Prefer typed assertions over whole-string snapshots. For rendered output, assert stable semantic fragments and explicitly reject malformed empty labels; do not freeze spacing, cost estimates, node IDs, or incidental ordering.

Keep the two oracles independent. If both compute expected behavior using the same production helper, they can preserve the same defect.

## 6. Planner And EXPLAIN Guidance

Separate planner tests into distinct claims:

| Claim | White-box evidence | Black-box evidence |
|---|---|---|
| A valid plan is constructed | Typed nodes, valid references, required fields | Query prepares or executes through SQL |
| A rewrite preserves semantics | Before/after relational properties | Results match an equivalent query or disabled-rewrite control |
| `EXPLAIN` is total | Every reachable node/annotation variant renders without panic | Public `EXPLAIN` succeeds for SQL that reaches each plan class |
| Diagnostic output is meaningful | Stable field present; empty control-only label absent | Column class and essential operator information are visible |
| Distributed selection is exercised | Typed distribution properties | Public plan class plus execution through more than one coordinator when required |

For an `EXPLAIN` failure, preserve execution as an independent control. A query that executes correctly but cannot be described isolates presentation from execution correctness. Conversely, successful `EXPLAIN` does not prove that execution returns correct results.

Test optional and control-only plan fields by meaning, not by one observed flag combination. Include present, absent, mixed, and stale-metadata controls when each state is reachable. A formatter should be total over the planner's valid output domain, but it should not silently reinterpret malformed states as valid semantics.

## 7. Deterministic Test Infrastructure

Avoid sleeps, polling without deadlines, ambient ports, shared mutable catalog state, and dependence on prior test order.

Use:

- explicit readiness and synchronization signals;
- bounded contexts with failure messages that identify the awaited condition;
- deterministic seeds recorded in the failure output;
- isolated database/schema names and explicit cleanup;
- restoration of session or service settings immediately after mutation;
- one reusable engine or cluster per suite only when state isolation is proven.

An embedded framework is justified when it exposes parse/bind/optimize/frontend behavior that package tests cannot reproduce. Keep it light:

1. Reuse expensive immutable setup.
2. Isolate mutable session, catalog, and statistics state per test.
3. Provide typed helpers for query results instead of parsing logs.
4. Separate one-CN and multi-CN fixtures so ordinary cases do not pay distributed startup cost.
5. Do not run independent subtests in parallel against shared service-wide optimizer settings.

Invest in reusable infrastructure only when the observation boundary is stable and shared, repeated setup or fragile parsing has meaningful ongoing cost, and the abstraction reduces total test complexity. Do not use a fixed incident count as a proxy for that judgment.

Retries are diagnostic only unless the product contract itself allows transient failure. A retry must not turn a deterministic correctness assertion into a probability of success.

## 8. Orthogonalizing The Suite

Each retained case must add one of:

- a distinct invariant;
- a new reachable state;
- a new semantic dimension or boundary;
- an independent oracle;
- a lifecycle transition not covered elsewhere.

Delete or merge cases that differ only in syntax while traversing the same path and asserting the same fact. Parameterize cases sharing setup and oracle, but keep names tied to the semantic distinction.

Keep issue numbers for traceability at the public regression boundary. Name reusable package tests after the invariant, not the incident. This prevents future maintenance from treating the fix as scenario-specific.

## 9. Acceptance Gate

Before accepting a regression change, verify:

```text
□ The invariant and its negation are explicit.
□ For an externally visible claim, the public-path witness proves the internal state is reachable; an internal-only contract is explicitly documented as such.
□ The unfixed revision fails for the claimed reason.
□ The fixed revision passes with the same assertions.
□ A black-box semantic oracle exists when behavior is externally visible.
□ White-box assertions inspect stable typed structure, not incidental layout.
□ Controls vary one relevant dimension at a time.
□ No case depends on sleep, order, ambient state, or probabilistic retry.
□ No assertion, case, or coverage was weakened merely to make CI pass.
□ Every retained case contributes a distinct row in the test matrix.
```

## 10. Test Matrix Template

Record the design before implementation:

| Invariant | Public or internal witness | White-box locator | Varied dimension | Oracle | Unfixed failure | Fixed result |
|---|---|---|---|---|---|---|
| Contract under test | Smallest supported input | Typed boundary | One independent axis | External or metamorphic check | Exact semantic reason | Expected behavior |

If two rows have the same invariant, path, dimension, and oracle, they are probably redundant. If a row has only a white-box locator and no externally visible contract, state why an internal invariant is the complete contract.
