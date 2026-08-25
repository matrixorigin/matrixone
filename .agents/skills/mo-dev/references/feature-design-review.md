# Feature and Major-Refactor Design-First Review Contract

Use this contract to decide whether a MatrixOne feature or major refactor needs
a design review, to review that design before implementation, and to keep
implementation review aligned with the approved design. The gate applies to the
complete feature/refactor, including stacked or split PRs; splitting a change
does not reduce its design risk.

## 1. Classify the change before reviewing code

A feature adds a user-, operator-, or developer-visible capability, or materially
expands an existing capability or architectural contract. A major refactor
materially restructures subsystem boundaries, ownership, state, protocol,
persistence, lifecycle, or core abstractions even when intended behavior stays
the same.

An ordinary bug fix does not require a design document. Neither does a focused
test, documentation, dependency, or mechanical maintenance change. If a change
labeled "fix" actually introduces a capability or grows into a major/architectural
refactor, classify its real scope instead of its PR label.

A design review is mandatory for a feature or refactor when either the default
size trigger or any complexity trigger applies.

### Default size trigger

Require a design when the complete feature/refactor is expected to change either:

- at least 500 non-generated production lines (additions plus deletions); or
- at least 5 owning production packages or components.

Exclude tests, generated files, vendored dependencies, documentation, formatting,
and pure mechanical renames from the line count. Count production configuration,
migrations, build/delivery behavior, and operational scripts when they implement
the feature/refactor. The numbers are review triggers, not proof that a smaller
design is safe or that a larger design is sound.

### Complexity triggers, regardless of size

Require a design when the feature/refactor does any of the following:

- crosses two or more subsystem, service, or ownership boundaries;
- changes a public API, client/server or wire protocol, catalog or on-disk format,
  persistent state, configuration semantics, or compatibility contract;
- adds or materially changes a distributed protocol, state machine, concurrency
  or lifecycle model, retry/idempotency behavior, or failure-recovery protocol;
- affects upgrade, downgrade, mixed-version operation, migration, restart, backup,
  restore, or rollback;
- changes authentication, authorization, tenant isolation, secrets, or another
  security boundary;
- introduces a background worker, cache, queue, scheduler, resource controller,
  framework, plugin/extension point, or other long-lived stateful abstraction;
- can materially affect a hot path, capacity bound, availability, operability,
  rollout, or failure blast radius.

A reviewer may require a design below the numeric threshold when a concrete risk
or architectural decision warrants it. Record the exact trigger. Do not use vague
"this feels large" language, and do not waive a complexity trigger because the
diff is short.

## 2. Required artifact and review order

An RFC is optional; the design content and review evidence are the gate. Accept
an RFC under `docs/rfcs/`, a document under `docs/design/`, or another stable,
versioned design document that the implementation reviewers can access. The
implementation PR must link the exact reviewed revision, and the document must
identify its owning issue and implementation PR or PR series. Prefer a
design-only PR. A document in the implementation PR is acceptable only when the
review record shows a distinct design-first phase and implementation review
remains blocked until that phase passes.

Review order is mandatory:

1. Classify the complete feature/refactor and record the trigger, or record that
   it is an ordinary fix/maintenance change and the concrete reason the gate
   does not apply.
2. Review the design document without assuming that the current implementation
   is the answer.
3. Resolve every design blocker and record approval. For an RFC, its review must
   have advanced it from `draft` to the repository's accepted `in progress`
   state. For any other design document, the reviewed revision and approval
   decision must be traceable; no RFC conversion is required.
4. Only then review or deliver the implementation. Verify the code and validation
   plan against the approved design and its invariants.

A draft, an unreviewed document, an issue description, implementation code, or a
slide deck without the required technical closure is not an approved design.
Material implementation deviations require the document to be updated and the
affected design decisions to be reviewed again before approval.

When the gate fails:

- during an external PR review, stop the implementation approval path and submit
  `REQUEST_CHANGES` with the missing artifact or design blockers;
- during self-review or implementation work, block push/delivery and produce the
  design fix list; do not claim the change is ready.

Do enough implementation inspection to prove scope and cite concrete design
risks, but do not spend a full code-review cycle polishing an implementation
whose design has not passed.

## 3. Design review criteria

Review from first principles and judge the proposal against applicable industry
standards and MatrixOne's actual constraints. The document must close all of the
following areas that apply.

### Problem, evidence, and invariants

- Define the observed problem, affected users/workloads, current behavior, and
  evidence. Start from the violated or missing contract, not from the proposed
  implementation.
- State the invariant, constraints, goals, non-goals, assumptions, and measurable
  success criteria. Separate facts from hypotheses.
- Identify the first owner of every new state, resource, transition, or externally
  visible contract.

### Standards, precedent, and alternatives

- Identify applicable standards, protocols, specifications, and proven industry
  designs, including versions or links when relevant. Distinguish mandatory
  interoperability requirements from optional precedent.
- Explain how MatrixOne's architecture and constraints agree with or differ from
  that precedent; do not copy a design by reputation alone.
- Compare the status quo and at least two credible alternatives when alternatives
  exist. Evaluate correctness, complexity, performance, compatibility,
  operability, testability, and migration cost, and explain why the selected
  design wins.

### Logical and architectural closure

- Definitions, assumptions, invariants, diagrams, APIs, and examples must agree.
  Trace end-to-end data and control flow across every producer, consumer, and
  ownership boundary.
- Define states, transitions, linearization/commit points, and behavior for
  success, error, cancellation, timeout, retry, duplicate delivery, partial
  initialization, reset/reuse, restart, and recovery as applicable.
- Specify ownership and bounds for memory, disk, files, goroutines, queues,
  caches, retries, metrics, and other accumulating resources, including
  backpressure and cleanup.
- Define API, catalog, storage, wire, and configuration contracts precisely
  enough that independent implementations would interoperate.

### Compatibility, delivery, and operations

- Cover upgrade, downgrade, mixed-version operation, migration, rollback,
  backup/restore, and restart where persisted or distributed state is involved.
- Define defaults, feature gates, phased rollout, observability, alerting,
  operational diagnosis, failure containment, and fallback/removal plans.
- State the performance and capacity model for affected hot paths: expected
  cardinality, asymptotic behavior, allocations, copies, I/O, synchronization,
  background work, and explicit resource budgets.
- Analyze security, authorization, tenant isolation, data exposure, and abuse or
  denial-of-service risks at every changed trust boundary.

### Verification and open decisions

- Map each behavior and invariant to its cheapest deterministic validation. Use
  the UT/BVT purposes and cost rules in
  [testing-contract.md](testing-contract.md); add benchmark, compatibility,
  restart, upgrade, fault-injection, chaos, or scale evidence only where the
  contract requires it.
- Define measurable acceptance criteria and evidence needed before rollout. Do
  not use unsupported performance claims, large test data, sleeps, or repeated
  workloads where injection, fake time, barriers, or scoped dynamic
  configuration can prove the behavior directly.
- List drawbacks, risks, and unresolved questions. A blocking question must have
  a decision before design approval; a non-blocking question needs an owner and
  a decision point.

## 4. Decision rule

Pass the design only when the proposed invariant and ownership model are sound,
the document is logically self-consistent, applicable standards and credible
alternatives were evaluated, lifecycle/compatibility/resource bounds/rollout are
closed, validation is sufficient and proportionate, and no blocking question
remains.

Request changes when the mandatory design is missing or unapproved, or when any
of these design-level failures remains:

- the proposal is solution-first and does not define the problem or invariant;
- definitions, assumptions, state transitions, or end-to-end flows contradict
  one another or omit a critical terminal path;
- persistent, wire, or distributed behavior lacks a compatibility, migration,
  mixed-version, restart, or rollback story;
- state or resource growth has no explicit owner or bound;
- applicable interoperability/security requirements or credible alternatives
  were not evaluated;
- rollout, failure containment, observability, or validation cannot demonstrate
  the claimed outcome;
- a critical question is deferred to implementation.

Editorial gaps that cannot affect correctness or the architectural decision may
be non-blocking. Record accepted tradeoffs and rejected alternatives in the
design decision log so implementation review does not repeatedly reopen them
without new evidence.

Use this compact review record:

```text
Change scope: <whole feature/refactor / PR series>
Trigger: <size or exact complexity trigger, or ordinary-fix exemption>
Design: <link, status, reviewed revision>
Blocking findings: <concrete invariant/flow/compatibility/etc. gaps>
Decision log: <accepted tradeoffs and resolved questions>
Decision: PASS | REQUEST_CHANGES
Implementation deviations: <none or design sections requiring re-review>
```
