"""Exercise real companion retry/dispatch code against an in-memory Actions API."""
import json
import os
import sys
import tempfile
import urllib.error
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(sys.argv[1]).resolve()))
from scripts import coverage_standby as standby


def exercise():
    head, revision, source = "a" * 40, "b" * 40, "example/matrixone"
    parent = dict(id=42, run_attempt=1, workflow_id=7, path=standby.ENTRY,
                  event="pull_request_target", repository={"full_name": standby.REPO},
                  head_sha=head, head_repository={"full_name": source},
                  status="completed", conclusion="failure",
                  run_started_at="2026-01-01T00:00:00Z", updated_at="2026-01-01T00:01:00Z")
    default_ticket = dict(parent=42, attempt=1, group=0, head=head, source=source,
                          pr=21, revision=revision, title_version=2)
    runs, jobs, dispatches = {}, {}, []

    def add_worker(run_id, ticket, status, conclusion=None):
        runs[run_id] = dict(id=run_id, run_attempt=1, workflow_id=8,
                            path=".github/workflows/" + standby.WORKER,
                            event="workflow_dispatch", repository={"full_name": standby.REPO},
                            head_branch=standby.WORKER_REF, head_sha=revision,
                            display_title=standby.short_title(ticket), status=status, conclusion=conclusion)
        jobs[run_id] = [
            dict(run_id=run_id, run_attempt=1, status="completed", conclusion="skipped",
                 name=standby.title(ticket, "cs") + standby.IDENTITY_SUFFIX),
            dict(run_id=run_id, run_attempt=1, id=run_id * 10, status=status, conclusion=conclusion,
                 name=f"cs / Coverage reservation CS group {ticket['group']}",
                 runner_id=123 if status == "in_progress" else None,
                 runner_name="cs-jed-nc-n1-pr-v1-abc-runner-def" if status == "in_progress" else None,
                 labels=[standby.POOLS["cs"]]),
        ]

    for run_id, change, status, conclusion in (
            (51, {}, "completed", "failure"),
            (52, {}, "queued", None),  # orphan from the failed parent attempt
            (53, {"parent": 43}, "in_progress", None),  # same title and source/head, different parent
            (54, {"group": 1}, "completed", "success")):
        add_worker(run_id, dict(default_ticket, key=standby.digest(str(run_id)), **change), status, conclusion)

    artifacts = [{"name": f"coverage-admit-{run_id}"} for run_id in (51, 54)]
    marker_reads = {}
    granted_attempts = {run_id: runs[run_id] for run_id in (51, 54)}
    original_worker_reads = 0

    def api(path, payload=None):
        nonlocal original_worker_reads
        if payload is not None:
            assert path == f"/actions/workflows/{standby.WORKER}/dispatches", path
            assert payload["ref"] == standby.WORKER_REF and payload["inputs"]["pool"] == "cs"
            ticket = json.loads(payload["inputs"]["ticket"])
            assert (ticket["parent"], ticket["attempt"], ticket["group"]) == (42, 2, 0)
            dispatches.append(ticket)
            add_worker(55, ticket, "in_progress")
            # Use the legacy 204 reconciliation path rather than trust a returned ID.
            return {}, 0
        if path == "/actions/runs/42":
            return dict(parent, run_attempt=2, status="in_progress", conclusion=None), 0
        if path == "/actions/runs/42/artifacts?per_page=100&page=1":
            return {"artifacts": artifacts}, 0
        if path == "/git/ref/heads/main":
            return {"object": {"sha": revision}}, 0
        if path.startswith(f"/actions/workflows/{standby.WORKER}/runs?"):
            return {"workflow_runs": [runs[n] for n in (53, 52, 55)]}, 0
        for run_id, run in runs.items():
            if path == f"/actions/runs/{run_id}":
                return run, 0
            if path == f"/actions/runs/{run_id}/attempts/1":
                if run_id == 51:
                    original_worker_reads += 1
                    if original_worker_reads == 1:
                        raise urllib.error.HTTPError(path, 503, "worker result temporarily unavailable", None, None)
                return granted_attempts[run_id], 0
            if path == f"/actions/runs/{run_id}/attempts/1/jobs?per_page=100":
                marker_reads[run_id] = marker_reads.get(run_id, 0) + 1
                if run_id == 51 and marker_reads[run_id] == 1:
                    raise urllib.error.HTTPError(path, 404, "attempt jobs temporarily unavailable", None, None)
                if run_id == 53 and marker_reads[run_id] == 1:
                    raise urllib.error.HTTPError(path, 503, "unrelated worker jobs temporarily unavailable", None, None)
                rows = jobs[run_id][1:] if run_id == 55 and marker_reads[run_id] == 1 else jobs[run_id]
                return {"jobs": rows, "total_count": len(rows)}, 0
        raise AssertionError(f"unexpected fixture request: {path}")

    with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {
            "GITHUB_RUN_ID": "42", "GITHUB_RUN_ATTEMPT": "2", "PR": "21",
            "GROUP": "0", "HEAD": head, "SOURCE": source, "POOL": "cs", "CHILD": "51",
            "RUNNER_TEMP": tmp, "GITHUB_OUTPUT": str(Path(tmp) / "outputs"),
        }, clear=True), patch.object(standby, "api", side_effect=api), patch.object(standby.time, "sleep"), \
            redirect_stdout(StringIO()):
        try:
            standby.wait()
        except ValueError as exc:
            assert str(exc) == "selected coverage group did not pass"
        else:
            raise AssertionError("failed group should fail the selector")
        assert original_worker_reads == 2
        runs[51] = dict(runs[51], run_attempt=2, conclusion="success")
        standby.choose()
        grant = json.loads(Path(tmp, "grant.json").read_text())
        assert grant["child"] == 55 and grant["pool"] == "cs"
        assert grant["ticket"] == dispatches[0] and len(dispatches) == 1
        assert standby.digest(grant["nonce"]) == dispatches[0]["key"]
        assert standby.proof_name(grant["ticket"]) == "real-pr-retry-42-attempt-2-group-0"
        assert marker_reads[51] == 2
        assert marker_reads[52] >= 1 and marker_reads[53] >= 1 and marker_reads[55] >= 3
    # Share the same metadata with the real cleanup controller. Omit the
    # fixture nonce and expose only fields normally visible on the Actions API.
    return dict(parent=parent, runs=runs, jobs=jobs, artifacts=artifacts,
                retry=dict(child=grant["child"], group=grant["ticket"]["group"],
                           attempt=grant["ticket"]["attempt"], dispatch_count=len(dispatches)))


if __name__ == "__main__":
    print(json.dumps(exercise()))
