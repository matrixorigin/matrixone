---
name: mo-bug-triage
description: Systematic MatrixOne kind/bug lifecycle triage — intake normalization with `needs-triage`, conservative promotion to active severity (`severity/s-1`, `severity/s0`, `severity/s1`), immediate downgrade to `deferred` with explanatory comments, optional five-level AI effort labeling (`ai-easy`, `ai-light`, `ai-medium`, `ai-heavy`, `ai-manual`) only when explicitly requested for issues/PRs, batched GitHub metadata updates, rollback-safe processing, and drift prevention. Use when asked to triage, classify, review, downgrade, defer, label, AI-effort-assess, or bulk-update MO bugs and related PRs.
metadata:
  project: matrixone
  repository: matrixorigin/matrixone
  workflow: bug-triage
---

Compatibility: designed for Codex CLI and compatible agents. Requires GitHub CLI (`gh`) with authenticated access (5000 req/hr) and repo write scope.

## Enforcement Gates

| Gate | When | Action |
|------|------|--------|
| **G-INTAKE** | When handling new bugs | New `kind/bug` issues enter `needs-triage`. Do not assign active severity during intake unless a maintainer explicitly confirms emergency impact. |
| **G-PROMOTION** | Before applying `severity/s-1`, `severity/s0`, or active `severity/s1` | Evidence must show confirmed urgency/impact or a committed owner. Record the rationale in the report or issue comment. |
| **G-DEFER-COMMENT** | Before applying `deferred` | Add an explicit comment explaining why the issue should not continue as s0/s1 active work. |
| **G-AI-REQUEST** | Before assessing or applying any AI effort label | User must explicitly request AI effort assessment, or the run mode must be `ai-assess`/`ai-final`. Plain `needs-triage` intake must not infer or apply `ai-*`. |
| **G-AI-SOURCE** | Before applying any AI effort label | Record assessment stage (`issue-initial`, `pr-review`, or `user-assisted`), confidence, and one-sentence rationale. |
| **G-AI-FINAL** | During explicit `ai-final` or PR AI-label review | Recheck five-level AI effort against actual implementation effort and update issue + PR labels if stale. |
| **G-STANDARDS** | Before any GitHub write | STANDARDS.md must exist with `Approved: true`. Scripts exit(1) if missing. |
| **G-SNAPSHOT** | Before batch update | `batch_N_before_snapshot.json` must be saved. Rollback requires it. |
| **G-DRYRUN** | Before full batch update | First 5 issues updated + verified. Mismatch → stop. |
| **G-DRIFT** | Before declaring "done" | Phase 3 drift detection must pass. If file edited after generation → re-run classifier. |

---

## TL;DR

```
Default lifecycle
  → New bug: kind/bug + needs-triage; no priority promise at entry
  → Analysis: verify repro, impact, owner, dependency, duplicates
  → Execute: promote only confirmed urgent/high-impact work to s-1/s0
  → Active non-emergency work: use s1 only when owned and committed
  → Downgrade: if not s0/s1 active work, move to deferred + comment
  → AI effort: do not run by default; assess only on explicit request or ai-* mode

Phase 1: LOCK STANDARDS   (~15min, 0 writes)
  → Sample 8-12 bugs by domain → lock lifecycle/severity/defer rules

Phase 2: BATCHED PROCESS  (~5min per batch of 50)
  → Each batch: fetch→classify action→report→snapshot→dry-run→update→progress
  → Batch N failure never touches Batch 1..N-1 (already committed)

Phase 3: FINAL VERIFY      (~10min)
  → Lifecycle audit + label count cross-check + defer-comment check; AI-label checks only for explicit AI modes
```

~310 issues ≈ 7 batches ≈ **30-45 min** (limited by GitHub rate limit: 5000 req/hr).

---

## Core Rules

### Current GitHub Bug Workflow

| Stage | Required Behavior |
|-------|-------------------|
| **Intake** | Label new bugs as `kind/bug` + `needs-triage`. Treat this as the candidate pool, not a priority commitment. Do not pre-assign `severity/s0`, `severity/s1`, or `severity/s-1` from title keywords alone. |
| **Execution** | Promote only confirmed urgent or broad-impact issues to `severity/s0` or `severity/s-1`. Use `severity/s1` only for owned, committed near-term bug work that should stay active but is not s0/s-1. |
| **Downgrade** | After analysis, if an issue should not continue as s0/s1 active work, immediately remove active severity/`needs-triage`, add `deferred`, and comment with the concrete reason. |
| **AI Labels** | Do not apply AI effort labels during default `needs-triage`. Apply exactly one AI effort label (`ai-easy`, `ai-light`, `ai-medium`, `ai-heavy`, `ai-manual`) only when the user asks for AI effort assessment or when running explicit `ai-assess`/`ai-final`. |
| **Ownership** | Each owner is responsible for their own issues and PRs: keep labels, comments, linked PRs, and closure status current without waiting to be driven. |

### Lifecycle Labels

| Label | Meaning | Write Rule |
|-------|---------|------------|
| `needs-triage` | Candidate pool for newly reported or not-yet-analyzed bugs. | Add at entry; remove only when promoted, deferred, closed, or explicitly excluded. Do not add `ai-*` during intake. |
| `severity/s-1` | Confirmed project emergency. | Use only for multi-tenant isolation/data exposure or explicit maintainer-confirmed emergency. If confirmed serious but not s-1 → `severity/s0`; if evidence is incomplete → `needs-triage`. |
| `severity/s0` | Confirmed urgent/high-impact active work. | Requires evidence: stable repro or production signal plus broad impact, data integrity risk, crash/hang/OOM/leak, common-path breakage, or release blocker. |
| `severity/s1` | Confirmed active work that is important but not emergency. | Requires an owner or near-term commitment. Do not use as a parking lot for unconfirmed bugs. |
| `deferred` | Analyzed but not active s0/s1 work. | Requires a comment explaining why and what signal would justify reopening/promoting. Prefer this over creating new `severity/s2` triage unless explicitly requested. |
| `ai-easy` | AI can directly implement; human review is mostly a quick scan. Suitable for batch/parallel AI execution. | Use for clear, localized, low-risk fixes with deterministic validation. |
| `ai-light` | AI can produce a good draft or plan; human must tune details or make a small decision. | Use when scope is bounded but there is minor ambiguity in expected behavior, tests, or local integration. |
| `ai-medium` | Human and AI likely need several rounds, with roughly shared effort. | Use when root cause or fix shape is partially unclear, or the change crosses a few modules. |
| `ai-heavy` | AI can assist with research, snippets, tests, or log analysis; core logic stays human-owned. | Use for broad, risky, or subsystem-level work where human design/debug judgment dominates. |
| `ai-manual` | AI is unlikely to help beyond clerical support. | Use when work depends on unavailable environments/data, security-sensitive access, product decisions, or human-only operational context. |

### Severity Hierarchy

| Severity | Criteria | Examples |
|----------|----------|----------|
| **s-1** | **MUST be conservative.** Multi-tenant isolation/data exposure, cross-tenant corruption, or explicitly confirmed project emergency. Refer to existing project s-1 count (≤5). If confirmed serious but not s-1 → s0; if evidence is incomplete → needs-triage. | tenant isolation break, cross-account data exposure |
| **s0** | Confirmed urgent/high-impact active bug: panic/crash, hung/deadlock, OOM/memory exhaustion, data integrity violation, resource leak, commonly-used feature broken, performance regression on core paths, release blocker. | INSERT panic, subquery hung, LOAD DATA OOM, lockservice leak, ORDER BY wrong, Prisma compat |
| **s1** | Confirmed active non-emergency bug with owner/commitment. Lower-impact partition/streaming/cold-feature bugs can be s1 only when they should be worked soon. | planned partition pruning fix, assigned streaming CTE bug, owned backup/restore issue |
| **deferred** | Not active s0/s1 work after analysis. Use for unstable repro, missing dependencies, non-critical paths, duplicate/consolidated work, feature requests mislabeled as bugs, tech debt, typos, or cleanup. | repro not stable, blocked by dependency, merge into existing issue, cold feature not planned |

### Domain Downgrade Table

These domains default to `deferred` after analysis unless a higher-severity trigger is confirmed or an owner commits to near-term work:

| Domain | Default | Exception |
|--------|---------|-------------------------------------|
| Partition | `deferred` | Confirmed panic/crash/data integrity → s0; owned near-term work → s1 |
| Streaming | `deferred` | Confirmed panic/crash/data integrity → s0; owned near-term work → s1 |
| Cold features | `deferred` | Confirmed panic/crash/data integrity → s0; owned near-term work → s1 |
| Feature Request | **exclude** | N/A — skip bug triage entirely |

**Cold features list**: stored procedures, CTE, window functions, collation, fulltext, backup/restore, role/DCL (GRANT/REVOKE), REPLACE INTO, trigger, event scheduler.

> **Note**: `LOAD DATA` and `import` are NOT cold features — they are commonly-used ETL paths. Confirmed breakage can justify s0, but intake still starts at `needs-triage`.

### s0 Investigation Triggers

These keywords are investigation triggers. They justify a closer look, not automatic promotion from `needs-triage`.

| Trigger | Keywords |
|---------|----------|
| **Panic/crash** | `panic`, `nil pointer`, `index out of range`, `fatal`, `makeslice`, `segmentation`, `nil dereference` |
| **Hung/deadlock** | `hung`, `deadlock`, `stuck`, `blocked`, `infinite`, `never`, `cannot kill`, `can't cancel`, `waiting`, `timeout` |
| **OOM/memory** | `oom`, `out of memory`, `memory leak`, `memory exhaust`, `oomkilled`, `consuming memory`, `memory growth`, `memory blow` |
| **Data integrity** | `data integrity`, `wrong result`, `incorrect result`, `inconsistent`, `duplicate key`, `unique constraint`, `check constraint`, `foreign key`, `corrupt`, `data loss`, `wrong data` |
| **Resource leak** | `leak`, `orphaned`, `stale`, `never cleaned`, `growing`, `accumulate`, `not released`, `not freed` |

### Classification Algorithm

1. Extract `title` (lowercase), `body` (lowercase) from issue JSON
2. If the issue is new/untriaged: ensure `kind/bug` + `needs-triage`; do not add active severity yet
3. Detect domains: is_partition, is_streaming, is_cold_feature, is_feature_request, duplicate/consolidation candidate
4. Detect evidence flags: panic/hung/OOM/data-integrity/resource-leak/common-path/release-blocker
5. Decide action:
   - `promote-s-1`: confirmed multi-tenant isolation/data exposure or explicit maintainer emergency
   - `promote-s0`: confirmed urgent/high-impact active bug with evidence and owner path
   - `promote-s1`: confirmed active non-emergency bug with owner/near-term commitment
   - `defer`: analyzed but not active s0/s1 work; requires comment
   - `keep-needs-triage`: insufficient information and no owner analysis yet
   - `exclude`: feature request or non-bug; remove from bug triage path per repo convention
6. Skip AI effort assessment by default. If and only if explicitly requested or running `ai-assess`/`ai-final`, estimate one AI effort label and record stage (`issue-initial`, `pr-review`, or `user-assisted`), confidence, and evidence in the report
7. Project: `MOEngine-Compute` default; `MOEngine-Storage` if `logservice|tn|wal|replica|storage` in title
8. If the user requested `ai-final` or PR AI-label review, re-evaluate AI label using actual code changes/tests/review complexity and update issue + PR labels

**Key invariant**: no issue leaves `needs-triage` for active severity without evidence and an owner/impact rationale. Keywords alone are never sufficient.

### Deferred Comment Template

When moving to `deferred`, write a short, concrete comment:

```markdown
Deferred after triage.

Reason: <unstable repro | missing dependency | non-critical path | consolidated into #NNNNN | feature request / not a bug | insufficient impact signal>
Current evidence: <one sentence>
To promote later: <specific signal needed, owner action, or linked issue/PR>
```

### Skip List

Skip entirely — do not classify, do not update:
- `heni02`
- `Ariznawlll`

### Severity Inflation Guard

- Before s-1: verify **exactly** matches multi-tenant isolation breach or explicit maintainer emergency. If serious but not s-1 and confirmed → s0; if unconfirmed → keep `needs-triage`.
- Before s0: verify actual panic/hung/OOM/integrity/common-function-break plus urgency/impact. If evidence is only title keywords → keep `needs-triage`.
- Before s1: verify there is an owner or near-term commitment. If not → `deferred` after analysis or keep `needs-triage` before analysis.
- Default for any post-analysis ambiguity → `deferred` with a clear reopen/promote condition.

### AI Effort Assessment

Use AI labels to estimate **how much human involvement is required**, not severity or business priority. This is an opt-in assessment, not part of default `needs-triage`. Normalize spelling to lowercase: use `ai-medium`, not `ai-Medium`.

| Label | Meaning | Strong Signals |
|-------|---------|----------------|
| `ai-easy` | AI directly handles it; human only scans the result. Can be batched and run in parallel. | Clear repro, obvious expected behavior, localized fix, deterministic test, no data/security/consensus risk. |
| `ai-light` | AI writes a useful first draft or plan; human adjusts details or makes one bounded decision. | Localized bug with minor ambiguity, test needs adaptation, existing pattern is clear but exact choice needs review. |
| `ai-medium` | Human and AI iterate for several rounds; roughly half the work is human judgment/debugging. | Root cause not fully isolated, fix touches several files/modules, requires repro refinement, test strategy is non-trivial. |
| `ai-heavy` | AI is support only; core logic and decisions remain human-owned. | Cross-subsystem impact, concurrency/transaction/storage semantics, data correctness risk, production-only symptom, significant design/debug judgment. |
| `ai-manual` | AI mostly cannot help beyond clerical tasks. | Requires private/unavailable environment or data, sensitive access, human-only operational/product decision, unclear report with no actionable evidence. |

Assessment stages:

- `issue-initial`: rough label from issue title/body/logs/repro. Prefer `ai-medium` or `ai-heavy` when evidence is thin.
- `pr-review`: revise using linked PR diff, tests, review comments, files changed, and actual debugging burden.
- `user-assisted`: combine issue/PR evidence with user-provided context such as owner knowledge, hidden dependency, production signal, or known fix plan.

Assessment rules:

- Do not infer AI effort during plain intake or default `kind/bug,needs-triage` triage. Leave all `ai-*` labels untouched unless the user requested AI assessment.
- Apply at most one AI effort label at a time; remove the other four when updating.
- Record `ai_label_stage`, `ai_confidence` (`low`, `medium`, `high`), and one-sentence `ai_rationale` in reports.
- Escalate the label when correctness/security/consensus/storage/transaction risk is present, even if the diff looks small.
- De-escalate only with evidence: linked PR proved localized, deterministic tests pass, or user confirms a simple known fix.
- Near PR close, revise labels based on actual work rather than the initial estimate when an AI label exists or the user asks for `ai-final`.

### Ownership Rule

- For issues/PRs assigned to you or linked to your work, update labels/comments proactively.
- Link PRs to issues, keep existing/requested issue and PR AI labels consistent near close, and close or defer issues explicitly.
- Do not wait for another person to drive routine status, downgrade, AI label correction, or closure hygiene.

---

## Phase 1: LOCK STANDARDS (Read-Only, ~15 min)

### Step 1.1: Discover Total Count

```bash
gh issue list -R matrixorigin/matrixone -l kind/bug -s open --limit 1 --json totalCount
```

Record `total_count`. Plan batches: `ceil(total_count / 50)`.

### Step 1.2: Query Existing Lifecycle Distribution

```bash
gh issue list -R matrixorigin/matrixone -l kind/bug -s open --limit 500 --json labels \
  --jq '.[].labels[].name' \
  | grep -E '^(needs-triage|deferred|severity/|ai-easy|ai-light|ai-medium|ai-heavy|ai-manual)$' \
  | sort | uniq -c | sort -rn
```

Do not force a target ratio. Treat high active severity counts as an audit signal: `severity/s-1` must remain rare, `severity/s0`/`severity/s1` must have evidence/owners, and new work should accumulate in `needs-triage` until analyzed.

### Step 1.3: Domain-Stratified Sampling

Fetch 8-12 issues covering **every major domain** (1-2 each):

| Domain | Keywords |
|--------|----------|
| Transaction | `txn`, `commit`, `rollback`, `transaction` |
| DML | `insert`, `update`, `delete`, `select`, `order by` |
| DDL | `create table`, `alter`, `drop table`, `index` |
| Partition | `partition`, `subpartition` |
| Streaming | `streaming`, `cte`, `changefeed` |
| Proxy/CN | `cn`, `proxy`, `dispatch`, `heartbeat` |
| Logservice/TN | `logservice`, `tn`, `wal`, `replica` |
| OOM/Memory | `oom`, `memory`, `leak` |
| Lock | `lock`, `deadlock`, `lockservice` |
| Prisma/Compat | `prisma`, `mysql`, `compat` |
| Backup/Restore | `backup`, `restore`, `dump` |
| Access Control | `role`, `grant`, `privilege`, `account` |

```bash
gh issue list -R matrixorigin/matrixone -l kind/bug -s open --limit 100 \
  --json number,title,labels,assignees
```

### Step 1.4: STANDARDS.md → User Approval Gate

Present sampled issues with proposed lifecycle actions. Produce file:

```markdown
# MO Bug Triage Standards — [Date]

## Approved: false   ← MUST be set to true before Phase 2

## Lifecycle + Severity Rules (locked after approval)
... (Core Rules tables above)

## Sampled Examples (user-confirmed)
| # | Issue | Domain | Proposed Action | AI Estimate | Deferred Comment Required | User Decision |
|---|-------|--------|-----------------|-------------|---------------------------|---------------|
| 1 | ... | DML | promote-s0 | not requested | no | approved |
| 2 | ... | Partition | defer | not requested | yes: non-critical path, no owner | approved |
```

**Wait for explicit user "approved"/"looks good"**, then:
```bash
sed -i 's/Approved: false/Approved: true/' STANDARDS.md
```

### Step 1.5: Technical Gate (scripts MUST enforce)

All Phase 2 scripts must call before any GitHub write:

```python
import sys, re
content = open("STANDARDS.md").read()
m = re.search(r'^## Approved:\s*(true|false)', content, re.MULTILINE)
if not m or m.group(1) != "true":
    print("FATAL: STANDARDS.md not approved. Run Phase 1 first.")
    sys.exit(1)
```

---

## Phase 2: BATCHED PROCESS (~5 min/batch)

Each batch is a sealed unit: fetch → classify action → report → **snapshot** → dry-run → update → progress. Batch N failure never touches Batch 1..N-1.

### Step 2a: Select Mode + Fetch One Batch

Choose the mode explicitly:

| Mode | Use When | Label Query |
|------|----------|-------------|
| `intake` | Normalize new bugs into the candidate pool | `kind/bug` then filter out issues already carrying `needs-triage`, `deferred`, or active severity |
| `triage` | Analyze the candidate pool; do not assess AI effort unless explicitly requested | `kind/bug,needs-triage` |
| `active-review` | Audit active work for downgrade or stale labels | Run separate batches for `kind/bug,severity/s0` and `kind/bug,severity/s1` |
| `ai-assess` | User explicitly asks to estimate AI effort for issues | Query the user-requested issue set; may include `needs-triage` but must not be implicit |
| `ai-final` | Recheck PR-close AI labels | Run separate linked issue/PR batches for `ai-easy`, `ai-light`, `ai-medium`, `ai-heavy`, and `ai-manual` |

```bash
PAGE=$1
LABEL_QUERY="${LABEL_QUERY:-kind/bug,needs-triage}"
gh api -H "Accept: application/vnd.github+json" \
  "/repos/matrixorigin/matrixone/issues?state=open&labels=${LABEL_QUERY}&per_page=50&page=${PAGE}&sort=created&direction=desc" \
  --jq '.[] | {number,title,body,labels:[.labels[].name],assignees:[.assignees[].login],state,comments:.comments}' \
  > batch_${PAGE}_raw.json
```

**Fallback**: per_page=50 → 30 if rate-limited. Log actual page size.

> Prefer REST API over GraphQL for reads — returns full data, no N+1 rate limit issues. GraphQL only for ProjectV2 writes.

### Step 2b: Classify This Batch

Apply the Classification Algorithm from Core Rules. Output `batch_N_classified.json` with:

- `action`: `keep-needs-triage`, `promote-s-1`, `promote-s0`, `promote-s1`, `defer`, or `exclude`
- `add_labels` and `remove_labels`: exact lifecycle/severity label delta. Include AI label deltas only in explicit `ai-assess`/`ai-final` mode.
- `rationale`: one sentence explaining evidence and owner/impact reasoning
- `deferred_comment`: required when `action == "defer"`

Only when AI assessment is explicitly enabled, also output:

- `ai_label`: `ai-easy`, `ai-light`, `ai-medium`, `ai-heavy`, `ai-manual`, or `unknown`
- `ai_label_stage`: `issue-initial`, `pr-review`, `user-assisted`, or `unknown`
- `ai_confidence`: `low`, `medium`, or `high`
- `ai_rationale`: one sentence explaining expected human vs AI effort

**Skip checking**: filter out heni02 and Ariznawlll issues before classification.

### Step 2c: Generate Batch Report

```markdown
# MO Bug Triage — Batch N
**Generated**: 2026-07-01T10:23:45Z
**Source**: batch_N_classified.json (sha256: abc123...)
**DO NOT EDIT MANUALLY** — re-run classifier to regenerate.

## Batch Summary
| Metric | Value |
|--------|-------|
| Range | #25260 → #24997 |
| Total fetched | 50 |
| Skipped (heni02/Ariznawlll) | 3 |
| Classified actions | 47 |

## Action Distribution
| Action | Count |
|--------|-------|
| keep-needs-triage | 18 |
| promote-s-1 | 0 |
| promote-s0 | 4 |
| promote-s1 | 6 |
| defer | 19 |
| exclude | 0 |

## AI Effort Distribution (only when explicitly requested)
| Label | issue-initial | pr-review | user-assisted | unknown |
|-------|---------------|-----------|---------------|---------|
| ai-easy | 5 | 0 | 0 | 0 |
| ai-light | 7 | 0 | 0 | 0 |
| ai-medium | 16 | 0 | 0 | 0 |
| ai-heavy | 8 | 0 | 0 | 0 |
| ai-manual | 5 | 0 | 0 | 0 |
| unknown | 6 | 0 | 0 | 6 |

## Issue Details
| # | Title | Action | Labels Delta | AI Effort | Project | Deferred Comment | Rationale |
|---|-------|--------|--------------|-----------|---------|------------------|-----------|
| 25260 | ... | promote-s0 | +severity/s0 -needs-triage | not requested | MOEngine-Compute | no | confirmed INSERT panic on common path |
| 25261 | ... | defer | +deferred -needs-triage | not requested | MOEngine-Compute | yes | repro unstable; needs isolated testcase |
```

When AI assessment is explicitly enabled, replace `AI Effort` with `ai-medium/issue-initial/medium` style values and include `ai_rationale`.

Include `generated_at` timestamp + file checksum for drift detection.

### Step 2d: Save Before-Snapshot (Rollback Safety)

Fetch current GitHub state for every issue in this batch → `batch_N_before_snapshot.json`:

```json
{
  "generated_at": "2026-07-01T10:23:45Z",
  "issues": [
    {
      "number": 25260,
      "old_milestone": "MO-v1.1",
      "old_labels": ["kind/bug", "needs-triage"],
      "old_assignees": ["owner"],
      "old_comments_count": 3
    }
  ]
}
```

### Step 2e: Dry-Run (5 issues)

Update first 5 issues. Verify labels and deferred comments on 2 of them:

```bash
gh issue view 25260 --json milestone,labels,comments \
  --jq '{milestone:.milestone.title,labels:[.labels[].name],last_comment:(.comments[-1].body // "")}'
```

**Mismatch, missing deferred comment, or stale AI label in explicit AI mode → stop batch, investigate. Do not proceed.**

### Step 2f: Full Batch Update + Progress

After update completes, mandatory progress display:

```
========================================
BATCH 3 COMPLETE  ✅
Progress: ████████████████░░░░ 57%  (4/7 batches)
Running totals: needs-triage:118  deferred:42  s-1:1  s0:17  s1:28
Next: Batch 4 (#24510 → #24320)
========================================
```

### Rollback Procedure (batch fails mid-update)

1. Read `batch_N_before_snapshot.json`
2. Restore each already-updated issue's old milestone and labels exactly
3. If the update log recorded newly-created deferred comment IDs, delete those comments; if deletion is not permitted, append a correction comment and stop
4. **Never "fix forward"** — always rollback and restart batch from clean state

```bash
# Restore old milestone
gh api -X PATCH /repos/matrixorigin/matrixone/issues/$NUMBER -f milestone="$OLD_MILESTONE"

# Restore labels: compute existing labels first, then remove labels not in snapshot and add labels from snapshot
gh issue edit $NUMBER --remove-label "$LABELS_TO_REMOVE" --add-label "$LABELS_TO_ADD"

# Delete a comment created by this failed batch when comment_id was logged
gh api -X DELETE /repos/matrixorigin/matrixone/issues/comments/$COMMENT_ID
```

---

## Phase 3: FINAL VERIFY (Read-Only, ~10 min)

### Step 3a: Lifecycle Audit

Automated checks:

- **Alert** if open `kind/bug` has none of `needs-triage`, `deferred`, `severity/s-1`, `severity/s0`, `severity/s1`
- **Alert** if `severity/s0`/`severity/s1` issues lack owner/impact rationale in report
- **Alert** if `deferred` issues created in this run lack a comment matching the Deferred Comment Template
- **Alert** if partition/streaming/cold-feature issues remain active without confirmed trigger or owner commitment
- **Alert** if `needs-triage` items are old enough to indicate the candidate pool is not being drained
- **Alert** in explicit AI modes if an issue/PR has more than one AI effort label or an AI effort label without stage/confidence/rationale in the report

### Step 3b: Label Count Cross-Validation

**Assert**: report counts match GitHub for `needs-triage`, `deferred`, `severity/s-1`, `severity/s0`, and `severity/s1`. In explicit AI modes, also assert counts for `ai-easy`, `ai-light`, `ai-medium`, `ai-heavy`, and `ai-manual`.

Mismatch → drift → regenerate + re-update affected batches.

### Step 3c: Full Batch Spot-Check

Scan ALL 50 issues in one complete batch report. 100% of one batch catches systematic patterns that random 5-issue sampling misses: premature severity, missing comments, or owner gaps. In explicit AI modes, also check incorrect AI estimates.

### Step 3d: Sampled API Verify

```bash
for num in $(shuf -e <all_numbers> | head -5); do
  gh issue view $num --json milestone,labels \
    --jq '{milestone:.milestone.title,labels:[.labels[].name]}'
done
```

### Step 3e: AI Effort Final Check (explicit AI modes only)

Run this only when the user requests AI effort review or when existing AI labels are in scope. For PRs close to merge/close, compare issue and PR labels:

- If AI handled the fix directly and human review was only a scan, ensure `ai-easy`.
- If AI produced a useful draft but human tuned or made a bounded decision, ensure `ai-light`.
- If human and AI iterated with shared effort, ensure `ai-medium`.
- If AI only assisted and core logic stayed human-owned, ensure `ai-heavy`.
- If AI provided little practical help, ensure `ai-manual`.
- Remove the other four AI effort labels so only one remains.

### Step 3f: Drift Detection

Compare `generated_at` in report headers vs file mtime. If file was edited after generation (mtime > generated_at + 5min) → the file was manually tampered → re-run classifier, re-update GitHub.

### Step 3g: Final Summary

```
========================================
TRIAGE COMPLETE
========================================
Total open kind/bug: NNN
Actions applied:     NNN
Skipped:              XX (heni02/Ariznawlll)

Lifecycle Distribution (on GitHub):
  needs-triage: NNN
  deferred:      NN
  s-1:            N
  s0:            NN
  s1:            NN

AI Effort Distribution (only if explicit AI mode ran):
  ai-easy:       NN
  ai-light:      NN
  ai-medium:     NN
  ai-heavy:      NN
  ai-manual:     NN

All updated issues have explicit lifecycle state; deferred issues have comments; active severity issues have rationale. AI effort labels are unchanged unless explicit AI mode ran.
========================================
```

---

## GitHub API Reference

### Rate Limit Budget

| Auth | Limit | Capacity |
|------|-------|----------|
| Unauthenticated | 60/hr | ❌ Impossible |
| Authenticated (`gh`) | 5000/hr | ✅ ~16 batches/hr |

300 issues × 2 calls = 600 requests. **Realistic: 15-20 min** with batch overhead.

### Set Lifecycle Labels + Milestone

```bash
# Set milestone
gh issue edit $NUMBER --milestone "MO-202607"

# Intake normalization
gh issue edit $NUMBER --add-label "kind/bug,needs-triage"

# Promote to active severity: remove only labels that are actually present
gh issue edit $NUMBER --add-label "severity/s0" --remove-label "needs-triage,deferred,severity/s-1,severity/s1"

# Defer after analysis
gh issue edit $NUMBER --add-label "deferred" --remove-label "needs-triage,severity/s0,severity/s1,severity/s-1"
gh issue comment $NUMBER --body-file deferred_comment_${NUMBER}.md

# Revise AI effort label on issue or PR: add one, remove the other four
gh issue edit $NUMBER --add-label "ai-medium" --remove-label "ai-easy,ai-light,ai-heavy,ai-manual"
gh pr edit $PR_NUMBER --add-label "ai-medium" --remove-label "ai-easy,ai-light,ai-heavy,ai-manual"
```

> `gh issue edit` with `--add-label`/`--remove-label` is simpler than REST PATCH. Prefer it.

### Set Project (GraphQL — requires `project` OAuth scope)

Field IDs and Option IDs are project-specific. Query them once and cache.

```graphql
mutation {
  updateProjectV2ItemFieldValue(input: {
    projectId: "PVT_..."
    itemId: "PVTI_..."
    fieldId: "PVTSSF_..."       # "Project" field
    value: { singleSelectOptionId: "..." }  # "MOEngine-Compute" or "MOEngine-Storage"
  }) { projectV2Item { id } }
}
```

If token lacks `project` scope: document assignment in report for manual allocation. Do not attempt and fail silently.

### Fallback: curl (no `gh` CLI)

```bash
curl -s -H "Authorization: Bearer $GITHUB_TOKEN" \
  -H "Accept: application/vnd.github+json" \
  "https://api.github.com/repos/matrixorigin/matrixone/issues?state=open&labels=kind/bug&per_page=100&page=1"
```

---

## Common Pitfalls (All from Real Sessions)

| # | Pitfall | Symptom | Root Cause | Fix |
|---|---------|---------|------------|-----|
| 1 | **Page size mismatch** | 139 issues silently missed | per_page=50 vs 100 misalignment | Phase 1: query total_count first. Fallback 50→30 |
| 2 | **Severity inflation** | New bugs jump straight to s0/s1 | Title keywords treated as priority commitment | G-INTAKE + G-PROMOTION: start with `needs-triage`, promote only after evidence |
| 3 | **Whole-batch failure** | User changes rule → all 300+ redone | No rule lock-in before writes | Phase 1 gate: STANDARDS.md approval first |
| 4 | **Severity drift** | Reports say s0, GitHub says s1 | Manual edits after generation | generated_at + checksum. Phase 3f drift detection |
| 5 | **Half-updated batch** | 23/50 updated, 27 not | No rollback safety | Step 2d: before_snapshot. Rollback procedure |
| 6 | **Rate limit blind spot** | Script hangs, user thinks stuck | No rate limit estimation | Progress display. 300 issues ≈ 15-20 min |
| 7 | **Project writes fail** | Token missing `project` scope | Did not verify scope | Document requirement. Fallback: mark in report |
| 8 | **Title is N/A** | Batch 7 titles all N/A | GraphQL N+1 rate limit | Prefer REST API. GraphQL only for project writes |
| 9 | **Cross-batch duplicates** | Same issue in batch 3 and 5 | Pagination race during triage | Track seen numbers globally. Deduplicate |
| 10 | **No progress visibility** | User cancels early | Silent between batches | Phase 2f: mandatory progress bar |
| 11 | **Rule change w/o impact** | User says "降级" → 40 change | No blast radius preview | Show impact analysis before applying |
| 12 | **Silent deferral** | Issue gets `deferred` but nobody knows why | Label changed without comment | G-DEFER-COMMENT: comment with reason and promote condition |
| 13 | **Stale AI label** | PR closes as `ai-easy` after manual design/debug | Existing/requested AI estimate never revised | G-AI-FINAL during explicit AI review; final label should reflect actual human effort |
| 14 | **Owner waits for drive-by triage** | Assigned issues/PRs stay stale | Responsibility not explicit | Ownership Rule: update own labels, comments, linked PRs, and closure state proactively |
| 15 | **Candidate pool bypass** | Open bug has neither `needs-triage` nor active/deferred label | Intake normalization skipped | Phase 3a lifecycle audit |

---

## Script Checklist

Scripts accompanying this skill must:

- [ ] Check `STANDARDS.md` `Approved: true` gate before any GitHub write
- [ ] Normalize new `kind/bug` issues into `needs-triage` before assigning priority labels
- [ ] Require promotion rationale before adding `severity/s-1`, `severity/s0`, or `severity/s1`
- [ ] Require a deferred comment body before adding `deferred`
- [ ] Leave `ai-*` untouched during normal `needs-triage`; only in explicit AI modes, track exactly one of `ai-easy`, `ai-light`, `ai-medium`, `ai-heavy`, `ai-manual` with stage/confidence/rationale
- [ ] Accept `--dry-run` flag (default: first 5 issues per batch)
- [ ] Save `batch_N_before_snapshot.json` before updating each batch
- [ ] Print progress bar + running totals after each batch
- [ ] Detect `generated_at` vs file mtime drift (warn, don't block)
- [ ] Cross-validate lifecycle label counts in Phase 3; cross-validate AI label counts only for explicit AI modes
- [ ] Support `--rollback batch_N` to restore from snapshot
- [ ] Log created comment IDs so rollback can delete or correct failed-batch comments
- [ ] Log every API call to `triage_YYYYMMDD.log`

---

## Refinement Loop (User Feedback Mid-Triage)

1. **User says "change X"** → pause current batch. DO NOT start new batch.
2. **Run impact analysis**: which processed batches affected? How many issues?
3. **Show preview**, get user confirmation:

```
⚠️  Rule change: "partition bugs without owner → deferred instead of s1"
    Affected batches: 1, 3, 5 (already processed)
    Issues to re-classify: 18
    Action changes: 13 (promote-s1→defer) + 2 (promote-s0 stays, confirmed panic) + 3 (no change)
    New comments required: 13
    Proceed? [y/N]
```

4. **Re-classify affected batches** only, regenerate reports (new timestamps)
5. **Re-update GitHub** for affected batches (snapshot → update labels/comments → verify)
6. **Resume** from next unprocessed batch
