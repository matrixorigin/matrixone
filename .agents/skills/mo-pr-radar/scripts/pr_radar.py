#!/usr/bin/env python3
"""Read-only MatrixOne pull-request queue diagnostics."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Iterable

DEFAULT_REPO = "matrixorigin/matrixone"
PASSING_CONCLUSIONS = {"SUCCESS", "SKIPPED", "NEUTRAL"}
FAILING_CONCLUSIONS = {
    "ACTION_REQUIRED", "CANCELLED", "FAILURE", "STARTUP_FAILURE", "STALE", "TIMED_OUT",
}
FAILING_STATES = {"ERROR", "FAILURE"}


def run(*command: str) -> str:
    for attempt in range(3):
        try:
            return subprocess.check_output(
                command,
                text=True,
                stderr=subprocess.PIPE,
                timeout=60,
            )
        except FileNotFoundError:
            raise SystemExit("missing required command: gh")
        except subprocess.TimeoutExpired:
            if attempt < 2:
                time.sleep(attempt + 1)
                continue
            raise SystemExit(f"GitHub query timed out: {' '.join(command)}")
        except subprocess.CalledProcessError as exc:
            detail = exc.stderr.strip() or exc.stdout.strip()
            lowered = detail.lower()
            transient = (
                "http 5" in lowered
                or "rate limit" in lowered
                or "timeout" in lowered
                or "connection reset" in lowered
                or "temporary failure" in lowered
                or "unexpected eof" in lowered
            )
            if transient and attempt < 2:
                time.sleep(attempt + 1)
                continue
            raise SystemExit(f"GitHub query failed: {detail}")
    raise AssertionError("unreachable")


def resolve_repo(value: str | None) -> str:
    if value:
        return value
    try:
        repo = run("gh", "repo", "view", "--json", "nameWithOwner", "--jq", ".nameWithOwner").strip()
        return repo or DEFAULT_REPO
    except SystemExit:
        return DEFAULT_REPO


def resolve_me(value: str | None) -> str:
    if value:
        return value
    return run("gh", "api", "user", "--jq", ".login").strip()


FIELDS = (
    "number,title,url,author,isDraft,mergeable,mergeStateStatus,reviewDecision,"
    "reviewRequests,reviews,statusCheckRollup,headRefOid,updatedAt"
)
OPINIONATED_REVIEW_STATES = {"APPROVED", "CHANGES_REQUESTED"}

OPEN_PRS_PAGE_SIZE = 20
OPEN_PRS_QUERY = """
query($owner: String!, $repo: String!, $pageSize: Int!, $cursor: String) {
  repository(owner: $owner, name: $repo) {
    pullRequests(
      first: $pageSize
      states: OPEN
      after: $cursor
      orderBy: {field: UPDATED_AT, direction: DESC}
    ) {
      nodes {
        number
        title
        url
        author { login }
        isDraft
        mergeable
        mergeStateStatus
        reviewDecision
        headRefOid
        updatedAt
        reviewRequests(first: 100) {
          nodes {
            requestedReviewer {
              ... on User { login }
              ... on Team { slug }
            }
          }
        }
        latestReviews: latestOpinionatedReviews(last: 100) {
          nodes {
            author { login }
            state
            submittedAt
            commit { oid }
          }
        }
        commits(last: 1) {
          nodes {
            commit {
              oid
              committedDate
            }
          }
        }
        statusCheckRollup {
          contexts(first: 100) {
            nodes {
              ... on CheckRun {
                name
                status
                conclusion
                detailsUrl
              }
              ... on StatusContext {
                context
                state
                detailsUrl: targetUrl
              }
            }
          }
        }
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
}
"""


def normalize_open_pr(node: dict[str, Any]) -> dict[str, Any]:
    requests = []
    for item in (node.get("reviewRequests") or {}).get("nodes", []):
        reviewer = item.get("requestedReviewer") or {}
        login = reviewer.get("login") or reviewer.get("slug")
        if login:
            requests.append({"login": login})
    rollup = node.get("statusCheckRollup") or {}
    latest_reviews = (node.get("latestReviews") or {}).get("nodes", [])
    return {
        "number": node["number"],
        "title": node["title"],
        "url": node["url"],
        "author": node.get("author"),
        "isDraft": node.get("isDraft", False),
        "mergeable": node.get("mergeable"),
        "mergeStateStatus": node.get("mergeStateStatus"),
        "reviewDecision": node.get("reviewDecision"),
        "reviewRequests": requests,
        "latestReviews": latest_reviews,
        "reviews": [
            review
            for review in latest_reviews
            if review.get("state") == "CHANGES_REQUESTED"
        ],
        "commits": [
            item["commit"]
            for item in (node.get("commits") or {}).get("nodes", [])
        ],
        "statusCheckRollup": (rollup.get("contexts") or {}).get("nodes", []),
        "headRefOid": node.get("headRefOid"),
        "updatedAt": node.get("updatedAt"),
    }


def fetch_open_prs(repo: str, limit: int) -> list[dict[str, Any]]:
    try:
        owner, name = repo.split("/", 1)
    except ValueError:
        raise SystemExit(f"invalid --repo {repo!r}; expected owner/repo")

    prs: list[dict[str, Any]] = []
    cursor: str | None = None
    while len(prs) < limit:
        page_size = min(OPEN_PRS_PAGE_SIZE, limit - len(prs))
        command = [
            "gh", "api", "graphql", "-f", f"query={OPEN_PRS_QUERY}",
            "-F", f"owner={owner}", "-F", f"repo={name}",
            "-F", f"pageSize={page_size}",
        ]
        if cursor:
            command.extend(["-F", f"cursor={cursor}"])
        data = json.loads(run(*command))["data"]["repository"]["pullRequests"]
        prs.extend(normalize_open_pr(node) for node in data["nodes"])
        if not data["pageInfo"]["hasNextPage"] or not data["nodes"]:
            break
        cursor = data["pageInfo"]["endCursor"]
    return prs


def review_logins(pr: dict[str, Any]) -> set[str]:
    return {r.get("login") for r in pr.get("reviewRequests", []) if r.get("login")}


def latest_opinionated_reviews(reviews: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return each reviewer's latest approval or change request."""
    latest: dict[str, tuple[tuple[str, int], dict[str, Any]]] = {}
    for index, review in enumerate(reviews):
        if review.get("state") not in OPINIONATED_REVIEW_STATES:
            continue
        login = ((review.get("author") or {}).get("login") or "").lower()
        if not login:
            continue
        rank = (review.get("submittedAt") or "", index)
        current = latest.get(login)
        if current is None or rank > current[0]:
            latest[login] = (rank, review)
    return [entry[1] for entry in latest.values()]


def my_latest_review(pr: dict[str, Any], me: str) -> str | None:
    for review in latest_opinionated_reviews(pr.get("latestReviews", [])):
        author = review.get("author") or {}
        if author.get("login", "").lower() == me.lower():
            return review.get("state")
    return None


def checks(pr: dict[str, Any]) -> list[dict[str, Any]]:
    return pr.get("statusCheckRollup") or []


def ci_state(pr: dict[str, Any]) -> str:
    rollup = checks(pr)
    if not rollup:
        return "none"
    pending = False
    for check in rollup:
        conclusion = (check.get("conclusion") or "").upper()
        state = (check.get("state") or "").upper()
        status = (check.get("status") or "").upper()
        if conclusion in FAILING_CONCLUSIONS or state in FAILING_STATES:
            return "failed"
        if conclusion in PASSING_CONCLUSIONS or state == "SUCCESS":
            continue
        pending = True
    return "pending" if pending else "green"


def is_conflict(pr: dict[str, Any]) -> bool:
    return pr.get("mergeable") == "CONFLICTING" or pr.get("mergeStateStatus") == "DIRTY"


def is_ready_candidate(pr: dict[str, Any]) -> bool:
    return (
        not pr.get("isDraft")
        and pr.get("mergeable") == "MERGEABLE"
        and pr.get("reviewDecision") == "APPROVED"
        and ci_state(pr) == "green"
    )


def latest_commit_at(pr: dict[str, Any]) -> str | None:
    timestamps = [commit.get("committedDate") for commit in pr.get("commits", []) if commit.get("committedDate")]
    return max(timestamps) if timestamps else None


def changes_requested_without_new_commit(pr: dict[str, Any]) -> bool:
    if pr.get("reviewDecision") != "CHANGES_REQUESTED":
        return False

    reviews = [
        review
        for review in pr.get("reviews", [])
        if review.get("state") == "CHANGES_REQUESTED"
    ]
    if not reviews:
        # The aggregate decision proves that a request is outstanding, but the
        # per-review payload is incomplete. Do not claim a newer commit exists.
        return True

    head_oid = (pr.get("headRefOid") or "").strip()
    if not head_oid:
        commits = pr.get("commits", [])
        if commits:
            head_oid = (commits[-1].get("oid") or "").strip()

    commit_at = latest_commit_at(pr)
    for review in reviews:
        reviewed_oid = ((review.get("commit") or {}).get("oid") or "").strip()
        if reviewed_oid and head_oid:
            if reviewed_oid == head_oid:
                return True
            continue

        # Older GitHub payloads may omit either SHA. Keep a conservative
        # timestamp fallback for only the review that cannot be compared.
        change_at = review.get("submittedAt")
        if not change_at or not commit_at or change_at >= commit_at:
            return True
    return False


def matches(pr: dict[str, Any], token: str, me: str) -> bool:
    requested = {login.lower() for login in review_logins(pr)}
    my_review = my_latest_review(pr, me)
    token = token.replace("_", "-").lower()
    predicates = {
        "all": True,
        "requested-by-me": me.lower() in requested,
        "needs-my-review": me.lower() in requested and my_review != "APPROVED",
        "unapproved-by-me": my_review != "APPROVED",
        "approved-by-me": my_review == "APPROVED",
        "requested-changes-by-me": my_review == "CHANGES_REQUESTED",
        "changes-requested-no-new-commit": changes_requested_without_new_commit(pr),
        "changes-requested-resolved-or-none": not changes_requested_without_new_commit(pr),
        "changes-requested": pr.get("reviewDecision") == "CHANGES_REQUESTED",
        "review-required": pr.get("reviewDecision") == "REVIEW_REQUIRED",
        "conflict": is_conflict(pr),
        "behind": pr.get("mergeStateStatus") == "BEHIND",
        "merge-unknown": pr.get("mergeable") == "UNKNOWN" or pr.get("mergeStateStatus") == "UNKNOWN",
        "merge-blocked": pr.get("mergeStateStatus") == "BLOCKED",
        "mergeable": pr.get("mergeable") == "MERGEABLE",
        "draft": bool(pr.get("isDraft")),
        "ci-green": ci_state(pr) == "green",
        "ci-failed": ci_state(pr) == "failed",
        "ci-pending": ci_state(pr) == "pending",
        "ci-none": ci_state(pr) == "none",
        "ready": is_ready_candidate(pr),
    }
    if token not in predicates:
        raise ValueError(f"unknown filter {token!r}")
    return predicates[token]


PRESETS: dict[str, tuple[list[str], bool]] = {
    "all": (["all"], False),
    "review": (["needs-my-review"], False),
    "needs-my-review": (["needs-my-review"], False),
    "conflicts": (["conflict"], False),
    "changes-requested": (["changes-requested"], False),
    "changes-stale": (["changes-requested-no-new-commit"], False),
    "reviewable-by-me": (["unapproved-by-me", "changes-requested-resolved-or-none"], False),
    "green-unapproved": (["ci-green", "unapproved-by-me"], False),
    "ready": (["ready"], False),
    "attention": (["conflict", "behind", "changes-requested", "ci-failed"], True),
}


def labels(pr: dict[str, Any], me: str) -> list[str]:
    result: list[str] = []
    if pr.get("isDraft"):
        result.append("draft")
    if is_conflict(pr):
        result.append("conflict")
    elif pr.get("mergeStateStatus") == "BEHIND":
        result.append("behind")
    if pr.get("reviewDecision") == "CHANGES_REQUESTED":
        result.append("changes")
    if me.lower() in {login.lower() for login in review_logins(pr)} and my_latest_review(pr, me) != "APPROVED":
        result.append("review")
    if ci_state(pr) == "failed":
        result.append("ci-failed")
    return result or ["—"]


def short(value: str, width: int) -> str:
    value = value.replace("\n", " ")
    return value if len(value) <= width else value[: width - 1] + "…"


def age(updated_at: str) -> str:
    try:
        then = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
        delta = datetime.now(timezone.utc) - then
    except (ValueError, TypeError):
        return "?"
    seconds = int(delta.total_seconds())
    if seconds < 3600:
        return f"{max(seconds // 60, 0)}m"
    if seconds < 86400:
        return f"{seconds // 3600}h"
    return f"{seconds // 86400}d"


def sort_key(pr: dict[str, Any], me: str) -> tuple[int, str]:
    priority = 5
    if is_conflict(pr):
        priority = 0
    elif pr.get("reviewDecision") == "CHANGES_REQUESTED":
        priority = 1
    elif ci_state(pr) == "failed":
        priority = 2
    elif "review" in labels(pr, me):
        priority = 3
    elif is_ready_candidate(pr):
        priority = 4
    return priority, pr.get("updatedAt", "")


def print_table(prs: list[dict[str, Any]], me: str) -> None:
    print(f"{'#':>5}  {'signals':<25} {'ci':<7} {'review':<17} {'author':<18} {'age':>4}  title")
    for pr in prs:
        signal = ",".join(labels(pr, me))
        review = my_latest_review(pr, me) or ("REQUESTED" if me.lower() in {x.lower() for x in review_logins(pr)} else "—")
        print(
            f"{pr['number']:>5}  {short(signal, 25):<25} {ci_state(pr):<7} "
            f"{short(review, 17):<17} {short((pr.get('author') or {}).get('login', 'ghost'), 18):<18} "
            f"{age(pr.get('updatedAt', '')):>4}  {short(pr['title'], 80)}"
        )


def print_markdown(prs: list[dict[str, Any]], me: str) -> None:
    print("| PR | Signals | CI | My latest review | Author | Updated | Title |")
    print("|---:|---|---|---|---|---:|---|")
    for pr in prs:
        review = my_latest_review(pr, me) or ("REQUESTED" if me.lower() in {x.lower() for x in review_logins(pr)} else "—")
        title = pr["title"].replace("|", "\\|")
        author = (pr.get("author") or {}).get("login", "ghost")
        print(f"| [#{pr['number']}]({pr['url']}) | {', '.join(labels(pr, me))} | {ci_state(pr)} | {review} | {author} | {age(pr.get('updatedAt', ''))} | {title} |")


def summarize(prs: Iterable[dict[str, Any]], me: str, matched: int) -> None:
    prs = list(prs)
    ci = Counter(ci_state(pr) for pr in prs)
    print(
        "Open snapshot: "
        f"{len(prs)} PRs | requested from {me}: {sum(matches(pr, 'needs-my-review', me) for pr in prs)} "
        f"| conflicts: {sum(is_conflict(pr) for pr in prs)} "
        f"| changes requested: {sum(matches(pr, 'changes-requested', me) for pr in prs)} "
        f"| CI green/pending/failed/none: {ci['green']}/{ci['pending']}/{ci['failed']}/{ci['none']} "
        f"| ready candidates: {sum(is_ready_candidate(pr) for pr in prs)}"
    )
    print(f"Matched: {matched}")


def radar(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="mo-pr-radar",
        description="Read-only MatrixOne open-PR queue query. Filters combine with AND by default.",
    )
    parser.add_argument("preset", nargs="?", default="all", choices=sorted(PRESETS))
    parser.add_argument("--repo", help="owner/repo; default is current gh repository, then matrixorigin/matrixone")
    parser.add_argument("--me", help="reviewer login; default is gh's authenticated user")
    parser.add_argument("--limit", type=int, default=200, help="max open PRs to fetch (default: 200)")
    parser.add_argument("--match", help="comma-separated filters; AND by default")
    parser.add_argument("--any", action="store_true", help="combine --match filters with OR")
    parser.add_argument("--include-drafts", action="store_true", help="do not hide drafts from filtered output")
    parser.add_argument("--format", choices=("table", "markdown", "json"), default="table")
    parser.add_argument("--summary-only", action="store_true")
    args = parser.parse_args(argv)
    if args.limit < 1:
        parser.error("--limit must be positive")

    if args.match:
        tokens = [token.strip() for token in args.match.split(",") if token.strip()]
        combine_any = args.any
    else:
        tokens, combine_any = PRESETS[args.preset]
    repo, me = resolve_repo(args.repo), resolve_me(args.me)
    all_prs = fetch_open_prs(repo, args.limit)
    show_drafts = args.include_drafts or (args.preset == "all" and not args.match)
    try:
        selected = [
            pr for pr in all_prs
            if (any if combine_any else all)(matches(pr, token, me) for token in tokens)
            and (show_drafts or not pr.get("isDraft"))
        ]
    except ValueError as exc:
        parser.error(str(exc))
    selected.sort(key=lambda pr: sort_key(pr, me))

    if args.format == "json":
        print(json.dumps({"repo": repo, "me": me, "filters": tokens, "any": combine_any, "prs": selected}, indent=2))
    else:
        filter_text = (" OR " if combine_any else " AND ").join(tokens)
        print(f"Repository: {repo} | Reviewer: {me} | Filter: {filter_text}")
        summarize(all_prs, me, len(selected))
        if not args.summary_only:
            if args.format == "markdown":
                print_markdown(selected, me)
            else:
                print_table(selected, me)
    return 0


def check_name(check: dict[str, Any]) -> str:
    return check.get("name") or check.get("context") or "unnamed check"


def check_bucket(pr: dict[str, Any], state: str) -> list[dict[str, Any]]:
    return [check for check in checks(pr) if ci_state({"statusCheckRollup": [check]}) == state]


def explain(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="mo-pr-explain", description="Explain one MatrixOne PR's current blockers.")
    parser.add_argument("pr", type=int)
    parser.add_argument("--repo")
    parser.add_argument("--me")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    repo, me = resolve_repo(args.repo), resolve_me(args.me)
    raw = run("gh", "pr", "view", str(args.pr), "--repo", repo, "--json", FIELDS)
    pr = json.loads(raw)
    pr["latestReviews"] = latest_opinionated_reviews(pr.get("reviews", []))
    if args.json:
        print(json.dumps(pr, indent=2))
        return 0

    requested = sorted(review_logins(pr), key=str.lower)
    my_review = my_latest_review(pr, me) or "none"
    failures = check_bucket(pr, "failed")
    pending = check_bucket(pr, "pending")
    print(f"#{pr['number']} {pr['title']}\n{pr['url']}")
    print(f"Draft: {pr.get('isDraft')} | Mergeable: {pr.get('mergeable')} | Merge state: {pr.get('mergeStateStatus')}")
    print(f"Review decision: {pr.get('reviewDecision') or 'none'} | Requested reviewers: {', '.join(requested) or 'none'} | {me}: {my_review}")
    print(f"Observed CI: {ci_state(pr)} ({len(checks(pr))} checks; failed {len(failures)}, pending {len(pending)})")
    for heading, bucket in (("Failed checks", failures), ("Pending checks", pending)):
        if bucket:
            print(heading + ":")
            for check in bucket:
                suffix = f" — {check.get('detailsUrl')}" if check.get("detailsUrl") else ""
                print(f"  - {check_name(check)} ({check.get('conclusion') or check.get('status') or check.get('state')}){suffix}")

    actions: list[str] = []
    if pr.get("isDraft"):
        actions.append("mark ready for review when the author intends it to enter normal merge gates")
    if is_conflict(pr):
        actions.append("rebase or merge the base branch in the PR branch and resolve the reported conflict")
    elif pr.get("mergeable") == "UNKNOWN":
        actions.append("wait for GitHub to finish mergeability calculation, then re-run this command")
    if failures:
        actions.append("repair or re-run the failed CI checks; inspect their linked logs first")
    elif pending:
        actions.append("wait for the pending CI checks")
    if pr.get("reviewDecision") == "CHANGES_REQUESTED":
        actions.append("address the outstanding change request and obtain a new approving review")
    if me.lower() in {login.lower() for login in requested} and my_review != "APPROVED":
        actions.append(f"{me} still has an active review request")
    if is_ready_candidate(pr):
        actions.append("candidate is ready by visible GitHub facts; inspect Mergify/branch-protection queue state if it does not merge")
    print("Next actions:")
    for action in actions or ["no obvious blocker in visible PR metadata; inspect Mergify or branch protection for repository-specific gates"]:
        print(f"  - {action}")
    return 0


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "explain":
        return explain(sys.argv[2:])
    return radar(sys.argv[1:])


if __name__ == "__main__":
    raise SystemExit(main())
