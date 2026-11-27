"""Use the merged standby implementation with the isolated push provenance."""
import importlib.util
import os
import subprocess
import sys
from pathlib import Path

REPO = "matrixorigin/matrixone"
REF = "weilu-runner-test"
ENTRY = ".github/workflows/coverage-standby-pr32922.yaml"
WORKER = ".github/workflows/coverage-standby-worker.yaml"
SOURCE = "Ariznawlll/matrixone"
HEAD = "02db9ac905295aedf2ea3ae0edf46e01778177ea"
PIN = "4ef594a63bf7fd8c584e62e108be8faa5e2358d4"


def main():
    mode = sys.argv[1]
    event, workflow = ("workflow_dispatch", WORKER) if mode == "admit" else ("push", ENTRY)
    if (os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_EVENT_NAME"],
        os.environ["GITHUB_WORKFLOW_REF"], os.environ["GITHUB_RUN_ATTEMPT"]) != (
        REPO, event, f"{REPO}/{workflow}@refs/heads/{REF}", "1"
    ) or (event == "push" and os.environ["GITHUB_ACTOR"] != "Ariznawlll"):
        raise SystemExit("Not the protected PR 32922 trial")

    if subprocess.check_output(["git", "-C", ".standby", "rev-parse", "HEAD"], text=True).strip() != PIN:
        raise SystemExit("Standby checkout is not the pinned production CI revision")
    print(f"Using pinned production CI standby {PIN}", flush=True)
    path = Path(".standby/scripts/coverage_standby.py").resolve(strict=True)
    spec = importlib.util.spec_from_file_location("pinned_coverage_standby", path)
    code = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(code)
    code.WORKER_REF, code.ENTRY = REF, ENTRY

    def trial_parent_identity(ticket, run):
        code.check((ticket["pr"], ticket["source"], ticket["head"]) == (32922, SOURCE, HEAD),
                   "not the frozen MatrixOne PR head")
        fields = {"id": ticket["parent"], "run_attempt": ticket["attempt"],
                  "repository.full_name": REPO, "head_repository.full_name": REPO,
                  "path": ENTRY, "event": "push", "head_branch": REF,
                  "head_sha": os.environ["GITHUB_SHA"], "actor.login": "Ariznawlll"}
        for field, expected in fields.items():
            value = run
            for key in field.split("."):
                value = value.get(key) if isinstance(value, dict) else None
            code.check(value == expected, f"trial parent identity mismatch: {field}")

    code.parent_identity = trial_parent_identity
    {"choose": code.choose, "admit": code.admit, "wait": code.wait, "revoke": code.revoke}[mode]()


if __name__ == "__main__":
    main()
