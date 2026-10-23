---
name: defect-hunt
description: Explore behavioral defects in data/query systems through focused source tracing or executable probes. Use for defect hunting, QA exploration, and compatibility investigation; choose only the methods needed for the requested question.
metadata:
  workflow: defect-hunt
  language: agnostic
---

# Defect Hunt

Find useful defect candidates and distinguish observations from confirmed failures.
**A single source trace or SQL probe is a complete valid use of this skill.**
White-box tracing, black-box probes, reference comparisons, ledgers, and issue
filing are tools to choose from, not a required sequence.

## Choose the scope

Use the user's question and existing evidence to select the smallest useful task:

| Request | Work | Deliverable |
|---|---|---|
| Explore / probe / look for defects | Inspect one suspect path or run focused inputs; expand only when the result warrants it | Candidates or observed behavior, evidence, and uncertainty |
| Confirm a candidate | Resolve the specific uncertainty with an executable counterexample, source trace, or comparison | Confirmed failure or a bounded cannot-reproduce/by-design conclusion |
| File or update issues | Reuse valid evidence, check duplicates and current issue state, then perform authorized writes | Issue links and supporting evidence |

Default to exploration when the user only asks to hunt defects. Do not require a
running instance for source-only work, source access for black-box work, or an
issue tracker for local probing. State what the available evidence cannot prove.
Do not expand a targeted request into a whole-system campaign.

For MatrixOne, use the relevant sections of the
[adapter](references/matrixone-adapter.md). Consult the
[ledger](references/matrixone-ledger.md) when reusing prior MatrixOne findings or
continuing a campaign. A one-off probe needs neither a ledger update nor an issue.

Resource ownership, waits, and retained-memory questions belong to
`unhappy-path-audit`; hand off when the observed path needs that analysis. A
functional probe does not establish a performance conclusion; use controlled
benchmarks when performance is part of the request.

## Choose a method

### Trace a suspicious value

Follow the actual entry point through its semantic owner:

```
accepted -> represented or normalized -> propagated -> consumed -> observable effect
```

Look for accepted-but-ineffective options, partial enforcement, widened
permissions/constraints, metadata/behavior disagreement, or asymmetric operations.
Field-reader searches help locate candidates; they do not prove absence of
behavior. Follow lowering, renamed fields, generated accessors, serialization,
and indirect consumers before concluding that a value is dropped. Rendering-only
metadata may be intentional; check the relevant contract or design decision.

### Run a focused probe

Choose the experiment that can answer the question:

| Method | Useful comparison |
|---|---|
| Disposition | Success/rejection versus actual state or metadata |
| Invariant | A contract that must hold for the selected inputs |
| Equivalent paths | Literal/parameter, index/scan, restart, or default/explicit setting when their semantics must agree |
| Boundary input | Zero, negative, NULL, empty, extreme, or mismatched types against their defined behavior; these inputs are not inherently errors |

Use a positive/control case when needed to show that the path actually executed
or to distinguish competing explanations. Do not require a separate control
when the result itself establishes reachability and the violated contract.
Use only the fixtures, sessions, topology, and data needed for this question.
Keep session state within a case; multi-session scenarios must establish the
required ordering. Reset only test-owned state and arrange cleanup on failure.

Preserve the exact command/SQL, build/configuration, exit status, and complete
stdout/stderr. Previews must point to the raw evidence. On timeout, preserve
partial output and distinguish client timeout from a proven server hang; check
for surviving server work before retrying or starting a conflicting probe.

## Calibrate the conclusion

Before calling something a confirmed defect, establish:

- **Observed failure:** an executed, repeatable case with a defined starting
  state, or a complete source proof of a reachable contract violation. Label
  source-only findings explicitly; do not claim they were reproduced.
- **Expected behavior:** a relevant specification/design contract, verified
  reference behavior, or an internal contradiction. Reference differences alone
  do not establish a compatibility promise.
- **Alternative explanations:** resolve the relevant harness, session/scope,
  caching, output-loss, or design uncertainty. Check those implicated by the
  evidence; a fixed battery is not required.

If evidence is incomplete, keep the candidate and name the missing proof. Do
not turn it into either a confirmed bug or a claim that the feature is correct.
Record localization separately: file/line or failing layer if known; otherwise
“behavioral evidence only.” **Severity follows demonstrated impact, not how far
the source investigation progressed.** An unlocalized but proven data loss or
permission violation is not automatically low severity.

## Issue disposition, only when needed

Before filing, commenting, or reopening, check that the user's current task or
prior authorization covers that write. Reuse existing authorization; otherwise
deliver the finding locally. A request to explore does not itself authorize
external issue mutations.

Search open and closed issues using the feature, exact error, affected field,
and behavior as relevant; inspect likely matches and their full discussion.
Widen the search when results are inconclusive rather than treating a short
result list as proof of no duplicate.

| Evidence | Disposition |
|---|---|
| No matching defect after search | Prepare/file a new issue within the authorized scope |
| Same root cause and affected behavior, OPEN | Add useful new evidence to the existing issue; avoid duplicate comments |
| CLOSED/FIXED, tested build lacks the fix | Record the affected version and fixing commit; do not claim regression |
| CLOSED/FIXED, tested build includes the fix but still fails | Check the fix's scope; report an incomplete fix or unresolved failure unless a known-good boundary proves regression |
| Same case passes on a verified fixed build and fails on a later build under equivalent conditions | Report regression with both versions and evidence; reopen if authorized |
| CLOSED/NOT-PLANNED | Read the reason: deferred work is not proof of correctness; respect a design ruling within its stated scope |

Use the repository's issue template and triage policy rather than inventing a
second label/severity workflow. Include reproduction or source proof, actual vs
expected, impact, version/configuration, and relevant related issues. Add catalog
or localization evidence when it supports the claim, not as ceremonial sections.

## Stop and retain useful evidence

Stop a focused probe when its question is answered or the remaining uncertainty
requires unavailable evidence. For an open-ended campaign, use the user's budget
or state a reasonable initial budget; switch surfaces when additional probes no
longer distinguish hypotheses. Do not widen a user-specified scope just to
produce more findings.

Return the observations/candidates, the evidence that supports them, and any
remaining uncertainty. “No defect observed in these cases” is narrower than
“confirmed correct.” If cross-round reuse is useful, append a ledger row with
scope, conclusion, raw evidence location, build/configuration, and date/timezone.
Correct historical conclusions with a new row. Keep task-specific exclusions in
that campaign's record, not in the permanent project adapter. Revalidate old
conclusions when their relevant code, configuration, or contract changes.
