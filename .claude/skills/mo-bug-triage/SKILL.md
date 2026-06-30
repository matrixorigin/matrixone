# MO Bug Triage Skill v4

Systematic triage of MatrixOne `kind/bug` issues with calibrated severity, batched processing, rollback safety, and drift prevention.

---

## When to Use

User asks to triage MO bugs, classify severity, update GitHub metadata (milestone, severity, project), or audit existing classifications. Trigger phrases: "triage bugs", "分析 bugs", "更新 issue", "bug report".

---

## TL;DR Quick Start

```
Phase 1: LOCK STANDARDS   (~15min, 0 writes)
  → Sample 8-12 bugs by domain → user approves STANDARDS.md → gate locked

Phase 2: BATCHED PROCESS  (~5min per batch of 50)
  → Each batch: fetch→classify→report→snapshot→dry-run→update→progress
  → Batch N failure never touches Batch 1..N-1 (already committed)

Phase 3: FINAL VERIFY      (~10min)
  → Domain clustering audit + severity cross-check + full batch spot-check + sampled API verify
```

Total time: ~310 issues ≈ 6 batches ≈ **30-45 min** (dominated by GitHub rate limit: 5000/hr for authenticated REST).

---

## Core Rules

### Severity Hierarchy

| Severity | Criteria | Examples |
|----------|----------|----------|
| **s-1** | **MUST be conservative**. Only: multi-tenant isolation breach. Refer to existing project s-1 count (≤5). If unsure → s0. | #15972 tenant isolation break |
| **s0** | **THE MAIN BUCKET** (~70% of bugs). Panic/crash, hung/deadlock, OOM/memory exhaustion, data integrity violation, resource leak, commonly-used feature broken, performance regression on core paths. | INSERT panic, subquery hung, LOAD DATA OOM, lockservice leak, ORDER BY wrong, Prisma compat |
| **s1** | **Lower priority** (~20%). Partition bugs, streaming bugs, cold/infrequently-used features, edge-case functions, subtasks of larger features, backward-compat-only paths. | partition pruning, streaming CTE, collation, stored procedures, backup/restore, role admin, REPLACE subtasks |
| **s2** | **Very low priority** (~10%). Feature requests mislabeled as bugs, tech debt refactoring, typos, code style, non-functional cleanup. | "support X", refactor internal API, doc fix |

### Domain-Based Severity Override

These domains are **auto-downgraded** unless a higher-severity exception applies:

| Domain | Default | Exception (keeps original severity) |
|--------|---------|-------------------------------------|
| Partition | s1 | Panic/crash → s0; data integrity → s0 |
| Streaming | s1 | Panic/crash → s0; data integrity → s0 |
| Cold features (stored procedures, CTE, window functions, collation, fulltext, backup, role/DCL, REPLACE) | s1 | Panic/crash → s0; data integrity → s0 |
| Feature Requests (even if labeled `kind/bug`) | s2 | N/A — exclude from bug triage entirely |

### Skip List

Skip these assignees entirely — do not classify, do not update:
- `heni02`
- `Ariznawlll`

### Severity Inflation Guard

- Before assigning s-1: verify the issue matches **exactly** the existing project s-1 pattern (multi-tenant isolation break only). If unsure → s0.
- Before assigning s0: verify the issue actually causes panic/hung/OOM/data-integrity/common-function-break. If it's a partition/streaming/cold-feature bug without those → s1.
- Default for any ambiguity → **one tier lower** than your instinct.

---

## Phase 1: LOCK STANDARDS (Read-Only, ~15 min)

### Step 1.1: Discover Total Count

```bash
gh issue list -R matrixorigin/matrixone -l kind/bug -s open --limit 1 --json totalCount
```

Record `total_count`. Plan batches: `ceil(total_count / 50)`. Estimate: ~310 issues → 7 batches.

### Step 1.2: Query Existing Severity Distribution

```bash
gh issue list -R matrixorigin/matrixone -l kind/bug -s open --limit 500 --json labels --jq '.[].labels[].name' | grep '^severity/' | sort | uniq -c | sort -rn
```

This establishes the team's **actual usage pattern**. Your output distribution must roughly match (s-1 ≤ 5, s0 ~70%, s1 ~20%, s2 ~10%).

### Step 1.3: Domain-Stratified Sampling

Fetch 8-12 issues representing **every major domain** (at least 1-2 per domain):

```bash
# Sample by scanning recent issues across domains
gh issue list -R matrixorigin/matrixone -l kind/bug -s open --limit 100 --json number,title,labels,assignees
```

Domains to cover: Transaction, DML, DDL, Partition, Streaming, Proxy/CN, Logservice/TN, Memory/OOM, Lock, Prisma/Compat, Backup/Restore, Access Control, Other.

### Step 1.4: Show STANDARDS.md → User Approval Gate

Present the sampled issues with proposed severities. Output a `STANDARDS.md` file:

```markdown
# MO Bug Triage Standards — [Date]

## Approved: false   ← MUST be set to true before Phase 2

## Severity Rules (locked after approval)
| Severity | Criteria |
|----------|----------|
| s-1 | Multi-tenant isolation breach ONLY |
| s0 | Panic/crash, hung/deadlock, OOM, data integrity, resource leak, common feature broken, perf regression |
| s1 | Partition, streaming, cold features (SP/CTE/window/collation/fulltext/backup/role/DCL/REPLACE) |
| s2 | Feature request, tech debt, typos |

## Domain Downgrade Table
...

## Sampled Examples (user-confirmed)
| # | Issue | Domain | Proposed Severity | User Decision |
|---|-------|--------|-------------------|---------------|
| 1 | ... | ... | s0 | ✅ |
| 2 | ... | ... | s0 | ⬆️ s-1 |
...
```

**Wait for user to explicitly say "approved" or "looks good" before proceeding.** Then:

```bash
# Lock the gate
sed -i 's/Approved: false/Approved: true/' STANDARDS.md
```

### Step 1.5: Technical Gate Check (script requirement)

All Phase 2 scripts **MUST** check this before executing:

```python
def gate_check():
    import sys, re
    with open("STANDARDS.md") as f:
        content = f.read()
    m = re.search(r'^## Approved:\s*(true|false)', content, re.MULTILINE)
    if not m or m.group(1) != "true":
        print("FATAL: STANDARDS.md not approved. Run Phase 1 first.")
        print("  Set '## Approved: true' after user confirmation.")
        sys.exit(1)
    print("✅ Gate passed: STANDARDS.md approved")
```

---

## Phase 2: BATCHED PROCESS (Each Batch Independent, ~5 min/batch)

Each batch is a self-contained unit:
- Fetch → Classify → Report → **Snapshot** → Dry-Run → Update → Progress
- Batch N failure = only N is affected; Batches 1..N-1 are safely committed to GitHub
- Batch N never starts until Batch N-1's update is confirmed complete

### Step 2a: Fetch One Batch (per_page=50, with fallback)

```bash
PAGE=$1
gh api -H "Accept: application/vnd.github+json" \
  "/repos/matrixorigin/matrixone/issues?state=open&labels=kind/bug&per_page=50&page=${PAGE}&sort=created&direction=desc" \
  --jq '.[] | {number,title,labels:[.labels[].name],assignee:.assignee.login,state}' \
  > batch_${PAGE}_raw.json
```

**Fallback**: if per_page=100 fails, retry with 50. If 50 fails, retry with 30. Log the actual page size used.

### Step 2b: Classify with Complete Rules

```python
def classify_severity(issue):
    """
    Full classification logic. Must match STANDARDS.md.
    Returns: (severity, project, notes)
    """
    title = (issue.get("title") or "").lower()
    labels = [l["name"].lower() if isinstance(l, dict) else l.lower() 
              for l in issue.get("labels", [])]
    body = (issue.get("body") or "").lower()
    
    # -- s-1: multi-tenant isolation ONLY --
    if any(kw in title for kw in ["tenant isolation", "cross-tenant"]):
        return ("severity/s-1", "MOEngine-Compute", "tenant isolation breach")
    
    # -- Domain detection (for downgrade logic) --
    is_partition = any(kw in title for kw in 
        ["partition", "subpartition", "sub-partition", "repartition"])
    is_streaming = any(kw in title for kw in 
        ["streaming", "stream", "cte", "changefeed", "cdc"])
    is_cold_feature = any(kw in title for kw in [
        "stored procedure", "backup", "restore", "collat", "fulltext",
        "role", "grant", "revoke", "replace into", "window function",
        "trigger", "event scheduler", "load data", "import"
    ])
    
    has_panic = any(kw in (title + " " + body) for kw in [
        "panic", "nil pointer", "index out of range", "fatal",
        "makeslice", "segmentation", "nil dereference"
    ])
    has_hung = any(kw in (title + " " + body) for kw in [
        "hung", "deadlock", "stuck", "blocked", "infinite", "never",
        "cannot kill", "can't cancel", "waiting", "timeout"
    ])
    has_oom = any(kw in (title + " " + body) for kw in [
        "oom", "out of memory", "memory leak", "memory exhaust",
        "oomkilled", "consuming memory", "memory growth", "memory blow"
    ])
    has_data_integrity = any(kw in (title + " " + body) for kw in [
        "data integrity", "wrong result", "incorrect result", "inconsistent",
        "duplicate key", "unique constraint", "check constraint",
        "foreign key", "corrupt", "data loss", "wrong data"
    ])
    has_resource_leak = any(kw in (title + " " + body) for kw in [
        "leak", "orphaned", "stale", "never cleaned", "growing",
        "accumulate", "not released", "not freed"
    ])
    
    # -- Core severity decision --
    # Panic/crash → s0 regardless of domain (exception to downgrade)
    if has_panic:
        proj = "MOEngine-Compute"
        if any(kw in title for kw in ["logservice", "tn", "wal", "replica"]):
            proj = "MOEngine-Storage"
        return ("severity/s0", proj, "panic/crash")
    
    # Hung/deadlock → s0
    if has_hung:
        return ("severity/s0", "MOEngine-Compute", "hung/deadlock")
    
    # OOM/memory → s0
    if has_oom:
        return ("severity/s0", "MOEngine-Compute", "oom/memory")
    
    # Data integrity → s0
    if has_data_integrity:
        return ("severity/s0", "MOEngine-Compute", "data integrity")
    
    # Resource leak → s0
    if has_resource_leak:
        return ("severity/s0", "MOEngine-Compute", "resource leak")
    
    # -- Downgraded domains → s1 (no panic/hung/OOM/data-integrity/leak detected) --
    if is_partition:
        return ("severity/s1", "MOEngine-Compute", "partition domain downgrade")
    if is_streaming:
        return ("severity/s1", "MOEngine-Compute", "streaming domain downgrade")
    if is_cold_feature:
        return ("severity/s1", "MOEngine-Compute", "cold feature downgrade")
    
    # -- Default: commonly used = s0 --
    # (Anything not in a downgrade domain and not hitting a specific severity trigger)
    return ("severity/s0", "MOEngine-Compute", "default common feature")
```

**Key**: Panic/hung/OOM/data-integrity/resource-leak **always s0** even in partition/streaming/cold-feature domains. Downgrade to s1 only when none of these triggers fire.

### Step 2c: Generate Batch Report (with build metadata)

```markdown
# MO Bug Triage — Batch N
**Generated**: 2026-07-01T10:23:45Z
**Source file**: batch_N_classified.json (sha256: abc123...)
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

... (issue details as before) ...
```

**Drift prevention**: include `generated_at` timestamp and source file checksum. Phase 3 verification compares this against file mtime.

### Step 2d: Save Before-Snapshot (Rollback Safety)

```python
# Before any GitHub writes for this batch, save current state
def save_snapshot(batch_file):
    """
    Read batch_N_classified.json → fetch current GitHub state for each issue
    → save to batch_N_before_snapshot.json
    """
    # For each issue: {number, old_milestone, old_severity_labels, old_project}
    snapshots = []
    for issue in read_classified(batch_file):
        current = gh_api_get(f"/repos/matrixorigin/matrixone/issues/{issue['number']}")
        snapshots.append({
            "number": issue["number"],
            "old_milestone": current.get("milestone", {}).get("title"),
            "old_labels": [l["name"] for l in current.get("labels", []) if "severity/" in l["name"]],
        })
    with open(batch_file.replace(".json", "_before_snapshot.json"), "w") as f:
        json.dump({"generated_at": now_iso(), "issues": snapshots}, f, indent=2)
```

### Step 2e: Dry-Run (5 issues, verify)

Update only the first 5 issues in the batch. Then verify:

```bash
# Dry-run update 5 issues
# ... (REST PATCH calls for first 5) ...

# Verify
gh issue view <N> --json milestone,labels
```

**If any mismatch → stop batch, investigate, rollback if needed.** Only proceed to full batch update after dry-run passes.

### Step 2f: Full Batch Update + Progress Display

```bash
# After every batch update, show progress
echo "========================================"
echo "BATCH $N COMPLETE"
echo "Progress: ████████░░ 43%  (3/7 batches)"
echo "Running totals: s-1:1  s0:89  s1:48  s2:12"
echo "Next: Batch $((N+1)) (#XXXXX → #YYYYY)"
echo "========================================"
```

### Rollback Procedure (if batch fails mid-update)

```bash
# Read before_snapshot for this batch
# For each issue already updated in this batch, restore:
gh api -X PATCH /repos/matrixorigin/matrixone/issues/$NUMBER \
  -f milestone="$OLD_MILESTONE"

# Restore severity labels
gh api -X DELETE /repos/matrixorigin/matrixone/issues/$NUMBER/labels/severity/sX
gh api -X POST /repos/matrixorigin/matrixone/issues/$NUMBER/labels \
  -f labels='["severity/s0"]'  # old value from snapshot
```

**Never try to "fix forward" a half-updated batch** — always rollback and restart.

---

## Phase 3: FINAL VERIFY (Read-Only, ~10 min)

### Step 3a: Domain Clustering Audit (Automated)

Run against all batch reports combined:

```python
def domain_clustering_audit(all_classified):
    """
    Check: does any domain anomalously cluster in wrong severity tier?
    Alert if: >30% of s1 issues are non-downgrade domains
    Alert if: >10% of s0 issues are partition/streaming without panic/hung/OOM
    """
    for severity in ["severity/s0", "severity/s1"]:
        issues = [i for i in all_classified if i["severity"] == severity]
        domains = Counter(classify_domain(i) for i in issues)
        for domain, count in domains.items():
            if domain in ("partition", "streaming") and severity == "severity/s0":
                # Check each has a valid s0 trigger
                without_trigger = [i for i in issues 
                    if classify_domain(i) == domain 
                    and not has_s0_trigger(i)]
                if without_trigger:
                    print(f"⚠️  {len(without_trigger)} {domain} issues in s0 without panic/hung/OOM trigger")
                    print(f"    Examples: {[i['number'] for i in without_trigger[:3]]}")
    
    # Check cold-feature in s0
    cold_in_s0 = [i for i in s0_issues if classify_domain(i) in COLD_DOMAINS and not has_s0_trigger(i)]
    if cold_in_s0:
        print(f"⚠️  {len(cold_in_s0)} cold-feature issues in s0 without trigger")
```

### Step 3b: Severity Count Cross-Validation

```python
def cross_validate(all_classified):
    """
    Assert: count of severity/s-1 in reports == count on GitHub
    """
    report_counts = Counter(i["severity"] for i in all_classified)
    
    # Query GitHub for actual labels
    gh_counts = query_github_severity_distribution()
    
    for sev in ["severity/s-1", "severity/s0", "severity/s1", "severity/s2"]:
        report = report_counts.get(sev, 0)
        gh = gh_counts.get(sev, 0)
        if report != gh:
            print(f"❌ MISMATCH: {sev}: reports={report} GitHub={gh}")
            print("   Drift detected. Regenerate ALL reports and re-update.")
            return False
    print("✅ Severity counts match between reports and GitHub")
    return True
```

### Step 3c: Full Batch Spot-Check

Pick one complete batch report (50 issues). Manually scan ALL 50 classifications in the report file. This is a 5% overall sample but 100% of one batch — catches systematic misclassification patterns that random sampling misses.

### Step 3d: Sampled API Verify

Verify 5 random issues on GitHub match the report:

```bash
for num in $(shuf -e <list_of_all_numbers> | head -5); do
  echo "=== #$num ==="
  gh issue view $num --json milestone,labels --jq '{milestone:.milestone.title,severity:[.labels[].name|select(startswith("severity/"))]}'
done
```

### Step 3e: Drift Detection

```python
def detect_drift(batch_files):
    """
    Compare generated_at in report vs file mtime.
    If mtime > generated_at + 5min → the file was manually edited after generation.
    """
    for bf in batch_files:
        gen_time = extract_generated_at(bf)
        mtime = os.path.getmtime(bf)
        if mtime - gen_time > 300:  # 5 minutes
            print(f"❌ DRIFT: {bf} was edited after generation")
            print(f"   Report generated: {datetime.fromtimestamp(gen_time)}")
            print(f"   File modified:    {datetime.fromtimestamp(mtime)}")
            print(f"   ACTION: Re-run classifier for this batch, then re-update GitHub")
            return True
    return False
```

### Step 3f: Final Summary

```
========================================
TRIAGE COMPLETE
========================================
Total open kind/bug: 311
Classified:         291
Skipped:             20 (heni02/Ariznawlll)

Severity Distribution (on GitHub):
  s-1:    1  (#15972 tenant isolation)
  s0:   221  (panic/hung/OOM/integrity/leak/common)
  s1:    57  (partition/streaming/cold-feature)
  s2:    12  (tech-debt/misfiled)

All issues updated: milestone=MO-202607, type=Bug
Batch reports: /home/xupeng/mo_bug_analysis_batch{1..7}.md
Standards:     STANDARDS.md
========================================
```

---

## GitHub API Reference

### Rate Limit Budget

| Auth | Limit | Capacity for 300 issues |
|------|-------|------------------------|
| Unauthenticated | 60/hr | ❌ Impossible |
| Authenticated (`gh`) | 5000/hr | ✅ ~16 batches/hr |
| GraphQL (token with `project` scope) | 5000/hr | Required for ProjectV2 |

**Realistic timeline**: 300 issues × 2 API calls each (1 read + 1 write) = 600 requests. At 5000/hr → ~7 minutes minimum. With batch overhead → **15-20 min realistic**.

### Set Milestone + Severity (REST)

```bash
# Set milestone
gh api -X PATCH /repos/matrixorigin/matrixone/issues/$NUMBER \
  -f milestone="MO-202607"

# Replace severity label (remove old, add new)
gh api -X DELETE "/repos/matrixorigin/matrixone/issues/$NUMBER/labels/severity%2Fs0"
gh api -X POST /repos/matrixorigin/matrixone/issues/$NUMBER/labels \
  -f 'labels[]=severity/s0'
```

### Set Project (GraphQL — requires `project` OAuth scope)

```graphql
mutation {
  updateProjectV2ItemFieldValue(input: {
    projectId: "PVT_kwDOA4Uifs4AAGab"
    itemId: "PVTI_lADOA4Uifs4AAGabz..."
    fieldId: "PVTSSF_lADOA4Uifs4AAGac..."
    value: { singleSelectOptionId: "04b8c8f2" }
  }) { projectV2Item { id } }
}
```

If token lacks `project` scope, document project assignment in reports for manual allocation.

### Fallback: if gh CLI unavailable

```bash
curl -s -H "Authorization: Bearer $GITHUB_TOKEN" \
  -H "Accept: application/vnd.github+json" \
  "https://api.github.com/repos/matrixorigin/matrixone/issues?state=open&labels=kind/bug&per_page=100&page=1"
```

---

## Common Pitfalls (10 — All from Real Sessions)

| # | Pitfall | Symptom | Root Cause | Fix in v4 |
|---|---------|---------|------------|-----------|
| 1 | **Page size mismatch** | 139 issues silently missed | per_page=50 vs per_page=100 pagination misalignment | Phase 1: query total_count first, plan batches explicitly. Fallback chain: 100→50→30 |
| 2 | **Severity inflation** | 8 s-1 assigned, project only has 4 | Did not calibrate against existing project severity usage | Phase 1.2: query existing distribution, force conservative s-1 |
| 3 | **Whole-batch failure** | User changes rule → all 300+ must redo | Single-pass classification without rule lock-in | Phase 1 gate: STANDARDS.md approval before any writes. Phase 2 batch isolation |
| 4 | **Severity drift** | Reports say s0 but GitHub shows s1 | Manually edited reports after generation, forgot to re-update | Step 2c: generated_at + checksum. Phase 3e: drift detection via mtime comparison |
| 5 | **Half-updated batch** | Batch 4 crashes mid-update → 23 of 50 updated, 27 not | No rollback safety, no before-snapshot | Step 2d: before_snapshot.json. Rollback procedure. Never "fix forward" |
| 6 | **Rate limit blind spot** | Script hangs for hours, user thinks it's stuck | Did not estimate API calls vs rate limit budget | Phase 2f: progress display. API reference with rate limit math. 300 issues ≈ 15-20 min |
| 7 | **Project assignment fails** | Token missing `project` OAuth scope, all project writes fail silently | Did not verify token scope before Phase 2 | API Reference: document scope requirement. Fallback: mark in report for manual allocation |
| 8 | **Title is N/A** | Batch 7 supplement had all titles as N/A | GraphQL N+1 query for batch 7's 119 issues hit rate limit; fallback JSON parse missed titles | Prefer REST API (returns full data). GraphQL only for project writes |
| 9 | **Cross-batch duplicates** | Same issue appears in batch 3 and batch 5 | Pagination race condition: new issues created during triage change sort order | Track seen issue numbers globally. Deduplicate before final count |
| 10 | **No progress visibility** | User doesn't know how far along, cancels early | Batch process silent between updates | Phase 2f: mandatory progress bar + running totals after every batch |
| 11 | **Rule change without impact analysis** | User says "partition 低优" → 40 issues change, unexpected | No preview of rule change blast radius | Phase 4 (review): show impact analysis before applying rule change |

---

## Script Checklist

Scripts that accompany this skill must:

- [ ] Check `STANDARDS.md` `Approved: true` gate before any GitHub write
- [ ] Accept `--dry-run` flag (default: first 5 issues per batch)
- [ ] Save `batch_N_before_snapshot.json` before updating each batch
- [ ] Print progress bar + running totals after each batch completion
- [ ] Detect `generated_at` vs file mtime drift (warn, don't block)
- [ ] Cross-validate severity counts (reports vs GitHub) in Phase 3
- [ ] Support `--rollback batch_N` to restore from snapshot
- [ ] Log every API call (method, URL, status code) to `triage_YYYYMMDD.log`

---

## Refinement Loop (when user gives feedback mid-triage)

1. **User says "change X"** → pause current batch, DO NOT start new batch
2. **Run impact analysis**: which already-processed batches are affected? How many issues?
3. **Show impact preview** to user, get confirmation
4. **Re-classify affected batches** (only those), regenerate reports (with new timestamps)
5. **Re-update GitHub** for affected batches (snapshot → update → verify)
6. **Resume from next unprocessed batch**

Impact analysis template:

```
⚠️  Rule change: "partition bugs → s1 instead of s0"
    Affected batches: 1, 3, 5 (already processed)
    Issues to re-classify: 18
    Severity changes: 13 (s0→s1) + 2 (s0 stays, has panic) + 3 (no change)
    Proceed? [y/N]
```
