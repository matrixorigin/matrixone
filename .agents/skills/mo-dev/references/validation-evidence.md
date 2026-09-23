# Risk-Proportional Validation And Review Evidence

The merge bar is complete proof for every affected contract, not a fixed number
of commands, repeated readings of the same diff, or locally reproduced CI work.
Use this contract to keep development and review rigorous without duplicating
work.

## 1. Build one change map

Resolve and record the base, head, merge-base, committed diff, staged/unstaged
diff, and untracked files once. Then read every changed hunk and build one map:

| Artifact/closure | Behavioral contract | Owner and consumers | Risk triggers | Required proof | Existing evidence |
|---|---|---|---|---|---|
| changed files/symbols | invariant or non-behavioral purpose | first owner plus dependent boundary | concurrency, persistence, public behavior, etc. | focused test, package, BVT, benchmark, parser, etc. | exact run/check and revision |

Group files that participate in one contract; do not review each file as an
independent island. Include generated/delivery artifacts and the consumer or
reverse arc of every changed protocol: reader for writer, restore for backup,
receiver for sender, reset/cleanup for create/call, and public path for a public
claim.

The map is the index for the rest of the work:

- read the complete diff once to populate it;
- classify every review lens as applicable to a mapped closure or not applicable
  with a concrete reason;
- group identical applicability/N/A decisions; do not generate a verbose
  closure-by-lens Cartesian product;
- revisit only the hunks/callers needed to close an applicable question or
  verify a candidate finding;
- never substitute a path grep, diff stat, or random spot check for reading all
  changed hunks.

This preserves completeness while removing repeated whole-diff sweeps.

## 2. Assign risk per closure, not per PR

A PR may contain several levels. Apply the deepest work only to the closure that
earns it; one high-risk file must not make unrelated documentation or mechanical
changes pay the same cost.

| Level | Shape | Required review/validation depth |
|---|---|---|
| **R0: non-behavioral** | documentation, comments, formatting, mechanical rename, or generated output whose source contract is unchanged | inspect diff/delivery, links/schema/parser/lint as applicable; no unrelated Go tests |
| **R1: local** | deterministic test-only work or local sequential internal behavior with no public, persistent, shared-state, or cross-owner contract | invariant/oracle review; exact focused validation plus owning package when code/tests changed |
| **R2: contract** | production behavior, public error/result/metadata, cross-package ownership, protocol-facing code, or a changed dependent contract | R1 plus consumer/negative-path proof, owning packages, and UT/BVT or compatibility evidence at the changed boundary |
| **R3: systemic** | concurrency/lifecycle/global state, persistence/wire/catalog format, distributed protocol, restart/upgrade, security/tenant boundary, material hot path, background resource, or large/complex feature/refactor | explicit state/ownership/wait/bound models; unhappy paths; relevant race/fault/restart/upgrade/security/performance evidence in addition to R2 |

Risk is evidence-driven. A large diff can be R0 if it is proven mechanical; a
small state-machine change can be R3. Record why each elevated dimension applies.
The feature-design gate is separate: ordinary fixes do not require a design
document, even when their implementation needs R2/R3 code and validation depth.

## 3. Execute the cheapest discriminating proof first

Order validation by information gained per unit cost:

1. syntax, selection, compile/type, and focused invariant checks;
2. focused regression and nearest negative/control case;
3. owning-package or owning-group checks;
4. dependent consumer and public-path validation;
5. race, topology, restart, upgrade, fault, GPU, benchmark, scale, or full-suite
   work only when the mapped contract requires that dimension.

Do not run `go list`, `go build`, `go vet`, and `go test` mechanically as four
checkboxes when valid evidence already proves the same claim. Preserve every
distinct proof:

- prove package/test selection is non-empty, either with `go list`/`go test
  -list` or named test output;
- `go test` compiles the package and linked test binary, but a command/binary,
  build tag, generated artifact, or non-test delivery path may still require a
  separate build;
- `go vet`/lint supplies distinct static evidence when applicable;
- a dependent consumer is required when the change crosses an ownership/API
  boundary;
- BVT, topology, restart, upgrade, GPU, and performance evidence are never
  replaced by an unrelated package pass.

Run independent checks concurrently only when they cannot contend for the same
cluster, ports, package/global state, CGo/GPU artifact build, or constrained CI
resource. Never start a duplicate command while an equivalent run is still
active; poll the existing run and capture its terminal status.

An upstream gate may short-circuit downstream work only when it makes that work
unstable or irrelevant: wrong/unresolved review range, missing mandatory design,
failed design review, or missing generated/delivery input. Otherwise complete
all applicable mapped closures in the same pass so the next review does not
rediscover a different blocker.

## 4. Reuse evidence by semantic validity

Local output and CI are equally valid when all of these hold:

1. **Identity:** exact head, or proof that every input relevant to the validator
   is unchanged. Record base/head when base-sensitive behavior is involved.
2. **Selection:** package/test/case/artifact selection is visible and non-empty.
3. **Mode:** flags, build tags, race/coverage/GPU/topology, platform, toolchain,
   configuration, and native artifacts match the claim.
4. **Result:** terminal status and causal output are available; pending, yielded,
   cancelled, skipped, or log-only output is not a pass.
5. **Scope:** the evidence proves the mapped invariant/consumer, not merely a
   neighboring package or a weaker layer.

An external reviewer should verify and reuse qualifying author/CI evidence. Run
only missing, stale, ambiguous, or contradiction-resolving checks. Self-review
and implementation work must ensure the evidence exists, but need not reproduce
an identical green CI command locally.

Invalidate evidence when a relevant input changes, including production code,
the test/oracle, fixture/state reset, generated source, build configuration or
tag, dependency behavior, topology, or the base-side contract it validates. Do
not invalidate it merely because of a PR-body/comment edit, unrelated docs,
commit metadata, or an unrelated package change. After a rebase, compare the
old/new base and dependency closure; rerun only affected evidence. When that
comparison is unavailable or ambiguous, treat the evidence as stale.

Record reuse explicitly:

```text
Evidence: <check/command and selected target>
Revision/mode: <head/base, platform/toolchain/tags/topology>
Result: <terminal status/link>
Validity: reused | rerun | stale
Reason: <mapped contract and relevant inputs unchanged/changed>
```

## 5. Review and delivery record

Keep the handoff small and decision-oriented:

```text
Range/mode: <base, head, merge-base; self-delivery or external review>
Change map: <closures, owners/consumers, R0-R3 reasons>
Design gate: <not applicable/exempt, or document revision and decision>
Applicable lenses: <closure -> lens>; N/A lenses: <reason>
Evidence: <reused, newly run, stale/missing>
Findings/decisions: <concrete failure paths and accepted tradeoffs>
Result: PASS | BLOCKED | REQUEST_CHANGES
```

The record demonstrates coverage; review duration, number of commands, and
number of comments do not.
