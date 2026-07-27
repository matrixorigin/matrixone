#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import pathlib
import subprocess
import unittest
from unittest import mock


MODULE_PATH = pathlib.Path(__file__).with_name("pr_radar.py")
SPEC = importlib.util.spec_from_file_location("pr_radar", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
pr_radar = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pr_radar)


class CIStateTests(unittest.TestCase):
    def test_no_checks(self) -> None:
        self.assertEqual("none", pr_radar.ci_state({}))

    def test_success_neutral_and_skipped_are_green(self) -> None:
        pr = {
            "statusCheckRollup": [
                {"conclusion": "SUCCESS"},
                {"conclusion": "NEUTRAL"},
                {"conclusion": "SKIPPED"},
            ],
        }
        self.assertEqual("green", pr_radar.ci_state(pr))

    def test_failure_wins_over_pending(self) -> None:
        pr = {
            "statusCheckRollup": [
                {"status": "IN_PROGRESS"},
                {"conclusion": "FAILURE"},
            ],
        }
        self.assertEqual("failed", pr_radar.ci_state(pr))

    def test_status_context_pending(self) -> None:
        pr = {"statusCheckRollup": [{"state": "PENDING"}]}
        self.assertEqual("pending", pr_radar.ci_state(pr))

    def test_successful_status_context_is_green(self) -> None:
        pr = {"statusCheckRollup": [{"state": "SUCCESS"}]}
        self.assertEqual("green", pr_radar.ci_state(pr))


class ReviewHistoryTests(unittest.TestCase):
    def test_same_head_as_change_request_is_stale(self) -> None:
        pr = {
            "reviewDecision": "CHANGES_REQUESTED",
            "headRefOid": "head",
            "reviews": [
                {
                    "state": "CHANGES_REQUESTED",
                    "submittedAt": "2026-07-27T10:00:00Z",
                    "commit": {"oid": "head"},
                },
            ],
        }
        self.assertTrue(pr_radar.changes_requested_without_new_commit(pr))

    def test_new_head_after_change_request_is_reviewable(self) -> None:
        pr = {
            "reviewDecision": "CHANGES_REQUESTED",
            "headRefOid": "new-head",
            "reviews": [
                {
                    "state": "CHANGES_REQUESTED",
                    "submittedAt": "2026-07-27T10:00:00Z",
                    "commit": {"oid": "old-head"},
                },
            ],
        }
        self.assertFalse(pr_radar.changes_requested_without_new_commit(pr))

    def test_latest_change_request_controls_head_comparison(self) -> None:
        pr = {
            "reviewDecision": "CHANGES_REQUESTED",
            "headRefOid": "second",
            "reviews": [
                {
                    "state": "CHANGES_REQUESTED",
                    "submittedAt": "2026-07-27T09:00:00Z",
                    "commit": {"oid": "first"},
                },
                {
                    "state": "CHANGES_REQUESTED",
                    "submittedAt": "2026-07-27T11:00:00Z",
                    "commit": {"oid": "second"},
                },
            ],
        }
        self.assertTrue(pr_radar.changes_requested_without_new_commit(pr))

    def test_any_outstanding_request_on_current_head_is_stale(self) -> None:
        pr = {
            "reviewDecision": "CHANGES_REQUESTED",
            "headRefOid": "current",
            "reviews": [
                {
                    "state": "CHANGES_REQUESTED",
                    "submittedAt": "2026-07-27T09:00:00Z",
                    "commit": {"oid": "current"},
                },
                {
                    "state": "CHANGES_REQUESTED",
                    "submittedAt": "2026-07-27T11:00:00Z",
                    "commit": {"oid": "old"},
                },
            ],
        }
        self.assertTrue(pr_radar.changes_requested_without_new_commit(pr))

    def test_timestamp_fallback_is_conservative(self) -> None:
        pr = {
            "reviewDecision": "CHANGES_REQUESTED",
            "reviews": [
                {
                    "state": "CHANGES_REQUESTED",
                    "submittedAt": "2026-07-27T10:00:00Z",
                },
            ],
            "commits": [{"committedDate": "2026-07-27T09:00:00Z"}],
        }
        self.assertTrue(pr_radar.changes_requested_without_new_commit(pr))
        pr["commits"][0]["committedDate"] = "2026-07-27T11:00:00Z"
        self.assertFalse(pr_radar.changes_requested_without_new_commit(pr))

    def test_missing_review_details_are_conservatively_stale(self) -> None:
        self.assertTrue(
            pr_radar.changes_requested_without_new_commit(
                {"reviewDecision": "CHANGES_REQUESTED", "reviews": []}
            )
        )

    def test_missing_sha_and_timestamp_are_conservatively_stale(self) -> None:
        pr = {
            "reviewDecision": "CHANGES_REQUESTED",
            "reviews": [{"state": "CHANGES_REQUESTED"}],
        }
        self.assertTrue(pr_radar.changes_requested_without_new_commit(pr))

    def test_current_approval_supersedes_historical_change_request(self) -> None:
        pr = {
            "reviewDecision": "APPROVED",
            "headRefOid": "head",
            "reviews": [
                {
                    "state": "CHANGES_REQUESTED",
                    "submittedAt": "2026-07-27T10:00:00Z",
                    "commit": {"oid": "head"},
                },
            ],
        }
        self.assertFalse(pr_radar.changes_requested_without_new_commit(pr))


class OpinionatedReviewTests(unittest.TestCase):
    def test_comment_does_not_erase_change_request(self) -> None:
        pr = {
            "latestReviews": [
                {
                    "author": {"login": "reviewer"},
                    "state": "CHANGES_REQUESTED",
                    "submittedAt": "2026-07-27T10:00:00Z",
                },
                {
                    "author": {"login": "reviewer"},
                    "state": "COMMENTED",
                    "submittedAt": "2026-07-27T10:01:00Z",
                },
            ]
        }
        self.assertEqual(
            "CHANGES_REQUESTED", pr_radar.my_latest_review(pr, "reviewer")
        )

    def test_later_approval_supersedes_change_request(self) -> None:
        pr = {
            "latestReviews": [
                {
                    "author": {"login": "reviewer"},
                    "state": "CHANGES_REQUESTED",
                    "submittedAt": "2026-07-27T10:00:00Z",
                },
                {
                    "author": {"login": "reviewer"},
                    "state": "APPROVED",
                    "submittedAt": "2026-07-27T10:01:00Z",
                },
            ]
        }
        self.assertEqual("APPROVED", pr_radar.my_latest_review(pr, "reviewer"))

    def test_open_pr_query_requests_latest_opinionated_reviews(self) -> None:
        self.assertIn(
            "latestReviews: latestOpinionatedReviews", pr_radar.OPEN_PRS_QUERY
        )


class FilteringTests(unittest.TestCase):
    def test_normalize_open_pr_flattens_graphql_connections(self) -> None:
        pr = pr_radar.normalize_open_pr(
            {
                "number": 42,
                "title": "title",
                "url": "https://example.test/42",
                "author": {"login": "author"},
                "isDraft": False,
                "mergeable": "MERGEABLE",
                "mergeStateStatus": "CLEAN",
                "reviewDecision": "REVIEW_REQUIRED",
                "headRefOid": "head",
                "updatedAt": "2026-07-27T10:00:00Z",
                "reviewRequests": {
                    "nodes": [
                        {"requestedReviewer": {"login": "person"}},
                        {"requestedReviewer": {"slug": "team"}},
                    ],
                },
                "latestReviews": {
                    "nodes": [
                        {
                            "author": {"login": "me"},
                            "state": "APPROVED",
                            "submittedAt": "2026-07-27T11:00:00Z",
                            "commit": {"oid": "head"},
                        },
                        {
                            "author": {"login": "reviewer"},
                            "state": "CHANGES_REQUESTED",
                            "submittedAt": "2026-07-27T10:00:00Z",
                            "commit": {"oid": "head"},
                        },
                    ],
                },
                "commits": {
                    "nodes": [{"commit": {"oid": "head", "committedDate": "2026-07-27T09:00:00Z"}}],
                },
                "statusCheckRollup": {
                    "contexts": {"nodes": [{"name": "UT", "conclusion": "SUCCESS"}]},
                },
            },
        )
        self.assertEqual([{"login": "person"}, {"login": "team"}], pr["reviewRequests"])
        self.assertEqual("APPROVED", pr["latestReviews"][0]["state"])
        self.assertEqual("CHANGES_REQUESTED", pr["reviews"][0]["state"])
        self.assertEqual("head", pr["commits"][0]["oid"])
        self.assertEqual("SUCCESS", pr["statusCheckRollup"][0]["conclusion"])

    def test_reviewable_by_me_requires_no_current_approval_and_fresh_changes(self) -> None:
        pr = {
            "headRefOid": "new-head",
            "latestReviews": [{"author": {"login": "me"}, "state": "COMMENTED"}],
            "reviews": [
                {
                    "state": "CHANGES_REQUESTED",
                    "submittedAt": "2026-07-27T10:00:00Z",
                    "commit": {"oid": "old-head"},
                },
            ],
        }
        self.assertTrue(pr_radar.matches(pr, "unapproved-by-me", "me"))
        self.assertTrue(pr_radar.matches(pr, "changes-requested-resolved-or-none", "me"))

    def test_conflicting_merge_state_is_conflict(self) -> None:
        self.assertTrue(pr_radar.is_conflict({"mergeable": "CONFLICTING"}))
        self.assertTrue(pr_radar.is_conflict({"mergeStateStatus": "DIRTY"}))
        self.assertFalse(pr_radar.is_conflict({"mergeable": "UNKNOWN"}))


class SnapshotTests(unittest.TestCase):
    @staticmethod
    def node(number: int) -> dict:
        return {
            "number": number,
            "title": f"PR {number}",
            "url": f"https://example.test/{number}",
            "reviewRequests": {"nodes": []},
            "latestReviews": {"nodes": []},
            "commits": {"nodes": []},
        }

    @mock.patch.object(pr_radar, "run")
    def test_fetch_open_prs_paginates_to_requested_limit(self, run: mock.Mock) -> None:
        run.side_effect = [
            json_payload(
                [self.node(1), self.node(2)],
                has_next=True,
                cursor="next",
            ),
            json_payload([self.node(3)], has_next=False, cursor=None),
        ]
        prs = pr_radar.fetch_open_prs("owner/repo", 3)
        self.assertEqual([1, 2, 3], [pr["number"] for pr in prs])
        self.assertEqual(2, run.call_count)
        self.assertIn("cursor=next", run.call_args_list[1].args)

    def test_fetch_open_prs_rejects_invalid_repo(self) -> None:
        with self.assertRaisesRegex(SystemExit, "expected owner/repo"):
            pr_radar.fetch_open_prs("invalid", 1)


class CommandTests(unittest.TestCase):
    @mock.patch.object(pr_radar.subprocess, "check_output")
    @mock.patch.object(pr_radar.time, "sleep")
    def test_timeout_retries_then_reports(self, _sleep: mock.Mock, check_output: mock.Mock) -> None:
        check_output.side_effect = subprocess.TimeoutExpired(["gh"], 60)
        with self.assertRaisesRegex(SystemExit, "GitHub query timed out"):
            pr_radar.run("gh", "api", "user")
        self.assertEqual(3, check_output.call_count)
        self.assertEqual(2, _sleep.call_count)


def json_payload(nodes: list[dict], has_next: bool, cursor: str | None) -> str:
    import json

    return json.dumps(
        {
            "data": {
                "repository": {
                    "pullRequests": {
                        "nodes": nodes,
                        "pageInfo": {
                            "hasNextPage": has_next,
                            "endCursor": cursor,
                        },
                    },
                },
            },
        },
    )


if __name__ == "__main__":
    unittest.main()
