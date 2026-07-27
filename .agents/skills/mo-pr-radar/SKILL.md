---
name: mo-pr-radar
description: Inspect and prioritize open MatrixOne pull requests from GitHub. Use when asked in natural language which PRs await the user's review or approval, have merge conflicts, contain outstanding requested changes, have green or failed/pending CI, are ready to merge, or when combining any of those conditions into an actionable PR queue.
---

# MatrixOne PR Radar

Use this skill as the read-only PR operations surface for `matrixorigin/matrixone`.
It turns requests such as “哪些 PR 等我 review、CI 已绿但我没 approve？” into a
deterministic GitHub query and an actionable list.

## Workflow

1. Resolve the requested conditions. Combine conditions with **AND** unless the
   user asks for “任一/any/attention”, which is an **OR** view.
2. Run `scripts/mo-pr-radar` once; it fetches one snapshot of open PR metadata.
   Do not scrape the GitHub web UI or infer status from an old conversation.
3. For a PR whose merge status is unclear, run `scripts/mo-pr-explain <number>`.
   State the exact unmet categories before suggesting any action.
4. Treat the script as diagnostic only. Never approve, dismiss reviews, rebase,
   resolve conflicts, label, or merge a PR unless the user explicitly asks.

## Natural-language mapping

| User intent | Command |
|---|---|
| “等待我 review 的” | `scripts/mo-pr-radar review` |
| “CI 全绿但我还没有 approve” | `scripts/mo-pr-radar green-unapproved` |
| “有冲突的” | `scripts/mo-pr-radar conflicts` |
| “被 request changes 的” | `scripts/mo-pr-radar changes-requested` |
| “request changes 后没有新 commit” | `scripts/mo-pr-radar changes-stale` |
| “我未 approve，且 request changes 后没有新 commit” | `scripts/mo-pr-radar --match unapproved-by-me,changes-requested-no-new-commit` |
| “我未 approve，且没有未处理的 request changes” | `scripts/mo-pr-radar reviewable-by-me` |
| “我 request changes 过、尚未处理的” | `scripts/mo-pr-radar --match requested-changes-by-me` |
| “落后 main、需要更新分支的” | `scripts/mo-pr-radar --match behind` |
| “可合并的” | `scripts/mo-pr-radar ready` |
| “现在所有需要关注的” | `scripts/mo-pr-radar attention` |
| “既冲突又 CI 失败” | `scripts/mo-pr-radar --match conflict,ci-failed` |
| “任一异常：冲突、request changes 或 CI 失败” | `scripts/mo-pr-radar --match conflict,changes-requested,ci-failed --any` |

Pass `--me <login>` when the requested reviewer is not the account authenticated
by `gh`; pass `--repo owner/repo` outside this checkout. `--format markdown` is
useful when pasting a result into an issue or chat; `--format json` is for follow-up
automation. `--help` lists every filter.

## Status semantics

The data comes from GitHub's current PR review requests, latest reviews,
mergeability, merge state, and check rollup.

- `ci-green` means every **observed** check has a successful, skipped, or neutral
  terminal result. It is deliberately not claimed to prove branch protection or
  Mergify eligibility.
- `ready` is a candidate view: non-draft, mergeable, review decision `APPROVED`,
  and observed checks green. It may still be held by queue/order/protection rules.
- `conflict` means GitHub reports `CONFLICTING` or `DIRTY`. `UNKNOWN` is not a
  conflict; inspect it after GitHub finishes calculating mergeability.
- “未我 approve” means the latest review record for the selected account is not
  `APPROVED`. Repository settings can still dismiss or require approvals
  differently, so use `mo-pr-explain` before diagnosing Mergify.
- `changes-requested-no-new-commit` compares the latest `CHANGES_REQUESTED`
  review's commit SHA with the current head SHA. It therefore detects a new
  commit without relying on author, committer, or push timestamps. A conservative
  timestamp fallback is used only when GitHub omits the review commit.
- `changes-requested-resolved-or-none` is its complement: no request changes,
  or the current head differs from the reviewed commit. It is the right
  review-queue filter when paired with `unapproved-by-me`.

## Scripts

- `scripts/mo-pr-radar`: list, count, filter, and export the open-PR snapshot.
- `scripts/mo-pr-explain <PR>`: show the merge/review/CI facts and concise next
  actions for one PR.

Both require authenticated GitHub CLI access (`gh auth status`) and Python 3;
they do not mutate GitHub or the worktree.

Run `python3 scripts/test_pr_radar.py` after changing query or status semantics.
