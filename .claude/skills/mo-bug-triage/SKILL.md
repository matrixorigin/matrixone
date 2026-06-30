---
name: mo-bug-triage
description: Systematic triage and classification of MatrixOne kind/bug issues — calibrated severity assessment, batched GitHub metadata updates (milestone, severity, project), rollback-safe batch processing, and drift prevention. Use when asked to triage, classify, or bulk-update MO bugs.
compatibility:
  agents: Codex CLI and compatible agents
  requires:
    - GitHub CLI (gh) with authenticated access (5000 req/hr)
    - repo write scope
metadata:
  project: matrixone
  repository: matrixorigin/matrixone
  workflow: bug-triage
---

## Enforcement Gates

| Gate | When | Action |
|------|------|--------|
| **G-STANDARDS** | Before any GitHub write | STANDARDS.md must exist with `Approved: true`. Scripts exit(1) if missing. |
| **G-SNAPSHOT** | Before batch update | `batch_N_before_snapshot.json` must be saved. Rollback requires it. |
| **G-DRYRUN** | Before full batch update | First 5 issues updated + verified. Mismatch → stop. |
| **G-DRIFT** | Before declaring "done" | Phase 3 drift detection must pass. If file edited after generation → re-run classifier. |

---

## TL;DR

```
Phase 1: LOCK STANDARDS   (~15min, 0 writes)
  → Sample 8-12 bugs by domain → user approves STANDARDS.md → gate locked

Phase 2: BATCHED PROCESS  (~5min per batch of 50)
  → Each batch: fetch→classify→report→snapshot→dry-run→update→progress
  → Batch N failure never touches Batch 1..N-1 (already committed)

Phase 3: FINAL VERIFY      (~10min)
  → Domain audit + severity cross-check + batch spot-check + sampled API verify
```

~310 issues ≈ 7 batches ≈ **30-45 min** (limited by GitHub rate limit: 5000 req/hr).

---

## Core Rules

### Severity Hierarchy

| Severity | Criteria | Examples |
|----------|----------|----------|
| **s-1** | **MUST be conservative.** Only: multi-tenant isolation breach. Refer to existing project s-1 count (≤5). If unsure → s0. | #15972 tenant isolation break |
| **s0** | **THE MAIN BUCKET** (~70%). Panic/crash, hung/deadlock, OOM/memory exhaustion, data integrity violation, resource leak, commonly-used feature broken, performance regression on core paths. | INSERT panic, subquery hung, LOAD DATA OOM, lockservice leak, ORDER BY wrong, Prisma compat |
| **s1** | **Lower priority** (~20%). Partition bugs, streaming bugs, cold/infrequently-used features, edge-case functions, subtasks of larger features. | partition pruning, streaming CTE, collation, SP, backup/restore, role admin, REPLACE subtasks |
| **s2** | **Very low priority** (~10%). Feature requests mislabeled as bugs, tech debt, typos, non-functional cleanup. | "support X", refactor internal API, doc fix |

### Domain Downgrade Table

These domains are **auto-downgraded** UNLESS a higher-severity trigger fires:

| Domain | Default | Exception (keeps original severity) |
|--------|---------|-------------------------------------|
| Partition | s1 | Panic/crash → s0; data integrity → s0 |
| Streaming | s1 | Panic/crash → s0; data integrity → s0 |
| Cold features | s1 | Panic/crash → s0; data integrity → s0 |
| Feature Request | **exclude** | N/A — skip bug triage entirely |

**Cold features list**: stored procedures, CTE, window functions, collation, fulltext, backup/restore, role/DCL (GRANT/REVOKE), REPLACE INTO, trigger, event scheduler.

> **Note**: `LOAD DATA` and `import` are NOT cold features — they are commonly-used ETL paths. Their bugs default to s0.

### s0 Triggers (override domain downgrade)

A bug hits s0 if the title+body contains ANY of these keywords:

| Trigger | Keywords |
|---------|----------|
| **Panic/crash** | `panic`, `nil pointer`, `index out of range`, `fatal`, `makeslice`, `segmentation`, `nil dereference` |
| **Hung/deadlock** | `hung`, `deadlock`, `stuck`, `blocked`, `infinite`, `never`, `cannot kill`, `can't cancel`, `waiting`, `timeout` |
| **OOM/memory** | `oom`, `out of memory`, `memory leak`, `memory exhaust`, `oomkilled`, `consuming memory`, `memory growth`, `memory blow` |
| **Data integrity** | `data integrity`, `wrong result`, `incorrect result`, `inconsistent`, `duplicate key`, `unique constraint`, `check constraint`, `foreign key`, `corrupt`, `data loss`, `wrong data` |
| **Resource leak** | `leak`, `orphaned`, `stale`, `never cleaned`, `growing`, `accumulate`, `not released`, `not freed` |

### Classification Algorithm

1. Extract `title` (lowercase), `body` (lowercase) from issue JSON
2. Check s-1: `"tenant isolation"` or `"cross-tenant"` in title → `severity/s-1`
3. Detect domains: is_partition, is_streaming, is_cold_feature (keyword matching)
4. Check s0 triggers in title+body (panic/hung/OOM/data-integrity/resource-leak)
5. **If ANY s0 trigger fires → `severity/s0` regardless of domain**
6. Else if partition/streaming/cold-feature → `severity/s1`
7. Else → `severity/s0` (default common feature)
8. Project: `MOEngine-Compute` default; `MOEngine-Storage` if `logservice|tn|wal|replica|storage` in title

**Key invariant**: s0 trigger (panic/hung/OOM/integrity/leak) always wins over domain downgrade.

### Skip List

Skip entirely — do not classify, do not update:
- `heni02`
- `Ariznawlll`

### Severity Inflation Guard

- Before s-1: verify **exactly** matches multi-tenant isolation breach. If unsure → s0.
- Before s0: verify actual panic/hung/OOM/integrity/common-function-break. If only partition/streaming/cold-feature → s1.
- Default for any ambiguity → **one tier lower** than instinct.

---

## Phase 1: LOCK STANDARDS (Read-Only, ~15 min)

### Step 1.1: Discover Total Count

```bash
gh issue list -R matrixorigin/matrixone -l kind/bug -s open --limit 1 --json totalCount
```

Record `total_count`. Plan batches: `ceil(total_count / 50)`.

### Step 1.2: Query Existing Severity Distribution

```bash
gh issue list -R matrixorigin/matrixone -l kind/bug -s open --limit 500 --json labels \
  --jq '.[].labels[].name' | grep '^severity/' | sort | uniq -c | sort -rn
```

Your output must roughly match: s-1 ≤ 5, s0 ~70%, s1 ~20%, s2 ~10%.

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

Present sampled issues with proposed severities. Produce file:

```markdown
# MO Bug Triage Standards — [Date]

## Approved: false   ← MUST be set to true before Phase 2

## Severity Rules (locked after approval)
... (Core Rules tables above)

## Sampled Examples (user-confirmed)
| # | Issue | Domain | Proposed | User Decision |
|---|-------|--------|----------|---------------|
| 1 | ... | DML | s0 | ✅ |
| 2 | ... | Partition | s1 | ✅ |
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

Each batch is a sealed unit: fetch → classify → report → **snapshot** → dry-run → update → progress. Batch N failure never touches 1..N-1.

### Step 2a: Fetch One Batch

```bash
PAGE=$1
gh api -H "Accept: application/vnd.github+json" \
  "/repos/matrixorigin/matrixone/issues?state=open&labels=kind/bug&per_page=50&page=${PAGE}&sort=created&direction=desc" \
  --jq '.[] | {number,title,body,labels:[.labels[].name],assignee:.assignee.login,state}' \
  > batch_${PAGE}_raw.json
```

**Fallback**: per_page=50 → 30 if rate-limited. Log actual page size.

> Prefer REST API over GraphQL for reads — returns full data, no N+1 rate limit issues. GraphQL only for ProjectV2 writes.

### Step 2b: Classify This Batch

Apply the Classification Algorithm from Core Rules. Companion script `scripts/classify.py` implements the keyword matching. Output: `batch_N_classified.json`.

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
| Classified | 47 |

## Severity Distribution
| Severity | Count |
|----------|-------|
| s-1 | 0 |
| s0 | 35 |
| s1 | 10 |
| s2 | 2 |

## Issue Details
| # | Title | Severity | Project | Milestone | Difficulty | Notes |
|---|-------|----------|---------|-----------|------------|-------|
| 25260 | ... | severity/s0 | MOEngine-Compute | MO-202607 | Hard | panic on INSERT |
```

Include `generated_at` timestamp + file checksum for drift detection.

### Step 2d: Save Before-Snapshot (Rollback Safety)

Fetch current GitHub state for every issue in this batch → `batch_N_before_snapshot.json`:

```json
{
  "generated_at": "2026-07-01T10:23:45Z",
  "issues": [
    {"number": 25260, "old_milestone": "MO-v1.1", "old_severity_labels": ["severity/s1"]}
  ]
}
```

### Step 2e: Dry-Run (5 issues)

Update first 5 issues. Verify 2 of them:

```bash
gh issue view 25260 --json milestone,labels --jq '{milestone:.milestone.title,severity:[.labels[].name|select(startswith("severity/"))]}'
```

**Mismatch → stop batch, investigate. Do not proceed.**

### Step 2f: Full Batch Update + Progress

After update completes, mandatory progress display:

```
========================================
BATCH 3 COMPLETE  ✅
Progress: ████████████████░░░░ 57%  (4/7 batches)
Running totals: s-1:1  s0:89  s1:48  s2:12
Next: Batch 4 (#24510 → #24320)
========================================
```

### Rollback Procedure (batch fails mid-update)

1. Read `batch_N_before_snapshot.json`
2. Restore each already-updated issue's old milestone + severity labels
3. **Never "fix forward"** — always rollback and restart batch from clean state

```bash
# Restore old milestone
gh api -X PATCH /repos/matrixorigin/matrixone/issues/$NUMBER -f milestone="$OLD_MILESTONE"

# Restore old severity (remove new, add old)
gh issue edit $NUMBER --remove-label "severity/s0" --add-label "$OLD_SEVERITY"
```

---

## Phase 3: FINAL VERIFY (Read-Only, ~10 min)

### Step 3a: Domain Clustering Audit

Automated: does any domain anomalously cluster in wrong tier?

- **Alert** if >30% of s1 issues are non-downgrade domains
- **Alert** if >10% of s0 issues are partition/streaming without panic/hung/OOM trigger
- **Alert** if cold-feature issues appear in s0 without trigger

### Step 3b: Severity Count Cross-Validation

**Assert**: `count(severity/X in reports) == count(severity/X on GitHub)` for s-1, s0, s1, s2.

Mismatch → drift → regenerate + re-update affected batches.

### Step 3c: Full Batch Spot-Check

Scan ALL 50 issues in one complete batch report. 100% of one batch catches systematic patterns that random 5-issue sampling misses.

### Step 3d: Sampled API Verify

```bash
for num in $(shuf -e <all_numbers> | head -5); do
  gh issue view $num --json milestone,labels \
    --jq '{milestone:.milestone.title,severity:[.labels[].name|select(startswith("severity/"))]}'
done
```

### Step 3e: Drift Detection

Compare `generated_at` in report headers vs file mtime. If file was edited after generation (mtime > generated_at + 5min) → the file was manually tampered → re-run classifier, re-update GitHub.

### Step 3f: Final Summary

```
========================================
TRIAGE COMPLETE
========================================
Total open kind/bug: NNN
Classified:          NNN
Skipped:              XX (heni02/Ariznawlll)

Severity Distribution (on GitHub):
  s-1:    N
  s0:    NNN
  s1:     NN
  s2:     NN

All updated: milestone=MO-202607, type=Bug
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

### Set Milestone + Severity

```bash
# Set milestone
gh issue edit $NUMBER --milestone "MO-202607"

# Replace severity label
gh issue edit $NUMBER --add-label "severity/s0" --remove-label "severity/s1"
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
| 2 | **Severity inflation** | 8 s-1 assigned, project has 4 | No calibration against existing usage | Phase 1.2: query distribution. Force conservative s-1 |
| 3 | **Whole-batch failure** | User changes rule → all 300+ redone | No rule lock-in before writes | Phase 1 gate: STANDARDS.md approval first |
| 4 | **Severity drift** | Reports say s0, GitHub says s1 | Manual edits after generation | generated_at + checksum. Phase 3e drift detection |
| 5 | **Half-updated batch** | 23/50 updated, 27 not | No rollback safety | Step 2d: before_snapshot. Rollback procedure |
| 6 | **Rate limit blind spot** | Script hangs, user thinks stuck | No rate limit estimation | Progress display. 300 issues ≈ 15-20 min |
| 7 | **Project writes fail** | Token missing `project` scope | Did not verify scope | Document requirement. Fallback: mark in report |
| 8 | **Title is N/A** | Batch 7 titles all N/A | GraphQL N+1 rate limit | Prefer REST API. GraphQL only for project writes |
| 9 | **Cross-batch duplicates** | Same issue in batch 3 and 5 | Pagination race during triage | Track seen numbers globally. Deduplicate |
| 10 | **No progress visibility** | User cancels early | Silent between batches | Phase 2f: mandatory progress bar |
| 11 | **Rule change w/o impact** | User says "降级" → 40 change | No blast radius preview | Show impact analysis before applying |

---

## Script Checklist

Scripts accompanying this skill must:

- [ ] Check `STANDARDS.md` `Approved: true` gate before any GitHub write
- [ ] Accept `--dry-run` flag (default: first 5 issues per batch)
- [ ] Save `batch_N_before_snapshot.json` before updating each batch
- [ ] Print progress bar + running totals after each batch
- [ ] Detect `generated_at` vs file mtime drift (warn, don't block)
- [ ] Cross-validate severity counts (reports vs GitHub) in Phase 3
- [ ] Support `--rollback batch_N` to restore from snapshot
- [ ] Log every API call to `triage_YYYYMMDD.log`

---

## Refinement Loop (User Feedback Mid-Triage)

1. **User says "change X"** → pause current batch. DO NOT start new batch.
2. **Run impact analysis**: which processed batches affected? How many issues?
3. **Show preview**, get user confirmation:

```
⚠️  Rule change: "partition bugs → s1 instead of s0"
    Affected batches: 1, 3, 5 (already processed)
    Issues to re-classify: 18
    Severity changes: 13 (s0→s1) + 2 (s0 stays, has panic) + 3 (no change)
    Proceed? [y/N]
```

4. **Re-classify affected batches** only, regenerate reports (new timestamps)
5. **Re-update GitHub** for affected batches (snapshot → update → verify)
6. **Resume** from next unprocessed batch
