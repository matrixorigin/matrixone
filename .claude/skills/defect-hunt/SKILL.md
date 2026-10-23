---
name: defect-hunt
description: Find NEW, evidence-grade defects in a data/query system (SQL engines, storage layers, protocol frontends) by combining white-box "produced but never consumed" tracing with black-box disposition, invariant, A/B-equivalence and degenerate-argument batteries; includes a falsification gate that removes false positives before filing, a duplicate-first issue protocol with regression/design triage, and round economics (when to stop, when the yield has decayed). Use when asked to QA a build, hunt bugs on rarely exercised surface, verify compatibility against a reference implementation, or when earlier rounds produced duplicates or doubtful findings.
metadata:
  workflow: defect-hunt
  language: agnostic
---

Applicability: the target system **already runs** (has a local instance), **source is readable**, and **an issue tracker exists**.
Not applicable: performance conclusions (require controlled benchmarks; this method explicitly produces none); resource-leak/concurrency/lifecycle deep dives (use `unhappy-path-audit`'s ownership/wait-for audit; this method stays at the behavioral layer).

Core stance: **a pasteable command + raw output + breakpoint location** is what counts as a finding. Everything else is a hypothesis.

---

# 1. Four gates before filing

A defect stands only if all four hold:

| Gate | Requirement | If not met |
|---|---|---|
| G1 Reproducible | One command, a clean starting point (e.g., new database/table or a fresh data directory), raw output | Do not file |
| G2 Decidable | Reference semantics (spec / another implementation) **or** an internal contradiction (two things that must agree, don't) | Downgrade to "observation"; do not file |
| G3 Non-duplicate | Searched per §6 | Comment with additional evidence instead |
| G4 Located | Can point to the layer where it breaks; if not, say "behavioral evidence only" and rate low | Severity must not exceed low |

Corollary: **without G2 there is no defect**, only "different from my expectation". Expectations must state their source.

---

# 2. Defect typology (eight classes)

Classify first, then design the experiment. Each class has a discriminating question; if you cannot answer it, you have not thought it through.

| # | Class | Discriminating question | Minimal experiment | Expected evidence |
|---|---|---|---|---|
| 1 | **Accepted but no-op** | Statement returns success — did any state change? | Inspect persistent state/metadata immediately after | OK returned, state unchanged |
| 2 | **Produced but never consumed** | Who reads this value? | Repo-wide count of field readers | Zero readers, or only the rendering layer |
| 3 | **Partially effective** | Do all checkpoints look at the same switch? | Trigger each failure face of one statement (duplicate / null / out-of-range) | One face is ignored |
| 4 | **Silent relaxation** (fail-open) | On failure, does it grant more or less? | Constraint / permission / quota scenarios | Restriction silently widened |
| 5 | **Silent narrowing** (fail-closed) | Does it also reject what it should allow? | Positive case with legal input | Legal input rejected |
| 6 | **Metadata/behavior divergence** | What does the catalog say vs what actually happens | Query introspection view + actual behavior | They contradict |
| 7 | **Asymmetric pair** | Write→read, create→drop, upgrade→downgrade — do both directions hold? | Do each, then cross-check | Only one direction works |
| 8 | **Reference drift** | Do defaults / error codes / formats match the reference? | Compare only where the reference is explicitly defined | Explicit deviation |

Priority (default, adjustable by impact): 4 > 1/2/3 > 6/7 > 5 > 8. Default severity: class 4 (data/permission amplification) → high; 2/3 (silent failure / partial effect) → medium. **Not a hard rule**: 5 (fail-closed rejecting legal input) can reach high in availability/migration-blocking scenarios, and 8 (reference drift) in compatibility work — severity follows impact, not class. These defaults presuppose G4 is met (layer located); behavior-only evidence is always capped at low by G4.

---

# 3. White-box: five-question trace

For each suspicious property, ask level by level; every level must name a reader:

```
1 Accepted?    Does the syntax/entry layer recognize this clause/parameter
2 Stored?      Is there a corresponding field in the intermediate representation (AST/IR)
3 Propagated?  Does it reach the plan/catalog structure (does the serialized structure carry it)
4 Consumed?    Is there a branch on the execution path that actually uses it
5 Reflected?   Is it visible through introspection (SQL example: SHOW / information_schema / status)
```

Criteria:

- **Fields at 2/3, zero readers at 4** → silent no-op (the classic "produced but never consumed").
- **No field at 2** → silently dropped (syntax accepted, semantics evaporated).
- **4 exists but bypasses a mode parameter** → partially effective.
- **5 visible, 4 absent** → metadata/behavior divergence.

Counter-example protection: **if only introspection reads it, it may be a deliberate "marker-style" design**. When the sole reader is a display/rendering layer, check the decision record (issue status / design doc) before calling it a defect.

---

# 4. Black-box: four batteries + control branch

| Battery | Answers | Design notes |
|---|---|---|
| **Disposition matrix** | What accepted inputs actually do | Classify each input: truly executed / no-op / clean rejection / fake rejection ("format error" masking "unimplemented") |
| **Invariants** | Falsify without a reference | Empty-set semantics, commutativity/associativity, inverse-order reversibility, row-count conservation, sequence consistency, idempotency |
| **A/B equivalence** | Two paths must agree | Literal vs parameterized, index vs full scan, before vs after restart, default vs explicit config |
| **Degenerate arguments** | Illegal/boundary inputs must error | 0, negative, NULL, extreme values, empty string, overlong, type mismatch |

**A control branch is mandatory**: for every "should have happened but didn't" experiment, include a branch showing "what I would see if it worked". No control = empty test (the largest false-positive source in this method's history: the subject may never have executed at all).

Experiment discipline:

- One process/connection per case; print `rc` + output + error separately; never chain with `&&`.
- Clean start every round: reset to an empty state (SQL example: `DROP DATABASE IF EXISTS`).
- For multi-session tests, make the holder print that it really executed (write/commit); otherwise the reader's stale value means nothing.

---

# 5. Falsification gate (walk through before filing)

| # | Trap | Self-check |
|---|---|---|
| 1 | **Tool/harness itself** | Connection parameters, database selection, timeouts, client short-option spelling |
| 2 | **Misremembered reference semantics** | Does my claimed "standard behavior" have a source, or is it memory? |
| 3 | **Empty test** | Is there a control branch proving the subject executed? |
| 4 | **Scope pollution** | Do same-named objects across scopes (database/namespace/session/account) affect the result? |
| 5 | **Caching and preallocation** | Are counters/statistics/plan caches affected by prefetch? A/B with the switch off |
| 6 | **Output truncation/escaping** | Am I seeing partial output? Are newlines/tabs escaped? |
| 7 | **Design vs defect** | What is the issue status for this behavior? Has a maintainer adjudicated it? |
| 8 | **Wrong facts about the target's grammar/entry layer** | Before claiming "unsupported": did I read the production/route table, or just try once? |

Any hit → go back and fix the experiment; do not file.

---

# 6. Deduplication and triage

Search ladder (at least 3 different phrasings):

```
<feature phrase> -> <verbatim error fragment> -> <field/view column name> -> <behavior phrasing>
```

Decision tree:

| Match | Action |
|---|---|
| None | File new |
| Same root cause and OPEN | Comment with evidence (attach the matrix); do not file |
| Same root cause, CLOSED/FIXED, still reproduces | **Regression**: reopen and state build + evidence |
| CLOSED/NOT-PLANNED or already ruled a design decision | Leave it; if you misfiled, roll back immediately with a note |

Key distinction: **"the code looks like it is missing something" != "defect"**. Maintainers' adjudication of a behavior outranks code shape.

---

# 7. Economics: yield curve, stop-loss, timebox

The numbers are **adjustable defaults**, calibrated per project; the direction (switch surface > double down in the same area) is hard.

- **Yield curve**: the first round on a new surface yields most; decays sharply after ~3–4 rounds; later rounds mostly produce "confirmed it is correct". That is normal, not failure.
- **False-positive rate**: above ~30% means §5 was not executed — fix the process before hunting more. One false positive costs ~one filing (plus the maintainer's time correcting you).
- **Stop-loss**: 2 consecutive rounds with zero new defects → switch surface (change subsystem/layer: black-box → white-box). Do not keep escalating in the same area.
- **Timebox**: single probe round <= 90 min, attribution <= 30 min. Overrunning means the target is too big — split it.
- **Conservation of output**: every round must land in one of three buckets — a new defect, a "confirmed correct" ledger row, or a narrower hypothesis. None of those = spinning.
- **Shutdown**: after 2 surface switches with zero yield, or a persistently >30% false-positive rate → stop the campaign and deliver a ledger report (tested coverage + open hypotheses).

---

# 8. Artifacts

**Ledger** (the sole vehicle for cross-round reuse and deduplication):

- Location: `references/<project>-ledger.md` (same directory as the adapter; never inside the core file)
- Append-only; never rewrite historical rows; to correct an old conclusion, add a new row noting "corrects row N"
- Row format (dates include timezone):

```
area | tested form | conclusion (correct / defect / by design) | evidence (command or issue) | build version | date (with timezone)
```

If no ledger exists, create it; read it before probing, or your first round will duplicate something.

Boundary between the two stores: maintainer-adjudicated facts (external source) go in the adapter's "known by design" table; conclusions from your own tests go in the ledger. If a test merely confirms an adjudicated decision, record the test in the ledger and cite the adapter row — never duplicate the ruling itself.

**Issue template, eight sections** (incomplete = rejected):

1. One-sentence symptom + direction (fail-open / fail-closed)
2. Pasteable reproduction (including schema/bootstrap setup, e.g. `CREATE TABLE`, and a clean starting point)
3. Actual vs expected comparison table
4. Metadata/catalog evidence (introspection view, DDL/structure echo)
5. Localization (file:line, or "breaks at layer N"; if impossible, state behavioral evidence only)
6. Impact (who hits it, in what scenario)
7. Expected behavior (implement it, or reject it explicitly — pick one)
8. Version (build/commit + platform), related issues

---

# Appendix A: Instantiation checklist (for checking; answers go only into the adapter)

| Item | Acceptance criterion |
|---|---|
| Instance startup command | One command starts an instance from zero, including data-directory isolation |
| Connection | Port/credentials/client, plus a connectivity self-check |
| Syntax layer path | The most authoritative productions or route table (not documentation) |
| Intermediate representation path | AST/IR/serialized structure definitions |
| Execution layer path | The directory holding the branches that actually consume the property |
| Introspection views | show/information_schema/status etc. reflecting the state above |
| Issue tracker commands | One pasteable search command (with state filter) |
| Known "by design" list | Every entry carries source and date (goes into the adapter, not this file) |

All eight items must live in `references/<project>-adapter.md` before probing starts; otherwise the next agent pays for the same mistakes.

# Appendix B: Failure ledger (generic, paid for in real mistakes)

| Misjudgment | Truth | Crystallized check |
|---|---|---|
| Claimed "X unsupported" | I got the clause order wrong | Read the grammar productions before claiming unsupported |
| Saw "duplicate rows" | Same-named objects across databases + only the first row printed | Query with scope predicates; print everything |
| Counter value looked wrong | Preallocation/cache upper bound | A/B with the switch off |
| "Feature missing" | Parameter spelling / keyword duplication | Align with the reference implementation's minimal example |
| "Constraint was relaxed" / constraint rebuild error | Still enforced across partitions; the reference implementation is database-level unique | Accepting DDL != relaxing semantics; align the reference before asserting |
| Isolation "passed" | Holder never wrote (empty test) | Commit control is mandatory |
| Rated a design as a defect | Maintainer had already adjudicated | Check the status before reading the code |

# Appendix C: Project adaptation

Project specifics (paths, startup, dedup commands, list of behaviors ruled by design) go into `references/<project>-adapter.md`; the ledger goes into `references/<project>-ledger.md`; maintained apart from this file: the core stays portable, the adapter and ledger are allowed to rot.
