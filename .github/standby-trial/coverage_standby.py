"""Exclusive admission for ONE coverage group. All writes are confined to trusted hosted jobs."""
import hashlib
import io
import json
import os
import re
import secrets
import sys
import time
import urllib.error
import urllib.request
import zipfile
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlencode, urlsplit

REPO = "matrixorigin/matrixone"
API = "https://api.github.com/repos/" + REPO
WORKER = "coverage-standby-worker.yaml"
WORKER_REF = "weilu-runner-test"
ENTRY = ".github/workflows/coverage-standby-pr32922.yaml"
POOLS = {"cs": "cs-node-cache-7c14g", "idc": "amd64-mo-shanghai-ut-safe"}
WAIT = 600


def check(ok, msg):
    if not ok:
        raise ValueError(msg)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args):
        return None


def api(path, data=None):
    check(path.startswith("/") and ".." not in path, "invalid API path")
    req = urllib.request.Request(API + path, headers={
        "Authorization": "Bearer " + os.environ["ACTIONS_TOKEN"],
        "Accept": "application/vnd.github+json", "Content-Type": "application/json"},
        data=None if data is None else json.dumps(data).encode())
    with urllib.request.build_opener(NoRedirect()).open(req, timeout=30) as res:
        raw = res.read()
        return json.loads(raw) if raw else {}, parsedate_to_datetime(res.headers["Date"]).timestamp()


def one(path):
    return api(path)[0]


def run(r):
    return one(f"/actions/runs/{int(r)}")


def digest(nonce):
    return hashlib.sha256(nonce.encode("ascii")).hexdigest()


def title(t, pool):
    return "Coverage standby / {parent}/{attempt}/{group}/{head}/".format(**t) + pool + "/" + t["key"]


PARENT_POLLS = 13
PARENT_POLL_SECONDS = 5


def parent_identity(t, p):
    expected = {
        "id": t["parent"], "run_attempt": t["attempt"], "repository.full_name": REPO,
        "path": ENTRY, "event": "push",
        "head_repository.full_name": REPO, "head_sha": os.environ["GITHUB_SHA"],
        "head_branch": WORKER_REF, "actor.login": "Ariznawlll",
    }
    for field, value in {"pr": 32922, "source": "Ariznawlll/matrixone",
                         "head": "3b95c0878f88b3da58bf3e65c369b7ef4d13104e"}.items():
        check(t.get(field) == value, f"trial PR identity mismatch: {field}")
    for field, value in expected.items():
        actual = p
        for key in field.split("."):
            actual = actual.get(key) if isinstance(actual, dict) else None
        # Do not dump the API response, credentials, or the admission ticket.
        check(actual == value, f"parent {t['parent']} identity mismatch: {field}")


def parent_meta(t):
    """Retry only a known, nonterminal state of the same parent attempt."""
    for poll in range(PARENT_POLLS):
        p = run(t["parent"])
        parent_identity(t, p)
        status, conclusion = p.get("status"), p.get("conclusion")
        check("conclusion" in p and conclusion is None,
              f"parent {t['parent']} not active: conclusion={conclusion!r}")
        if status == "in_progress":
            return p
        check(status in ("queued", "pending", "requested", "waiting"),
              f"parent {t['parent']} not active: status={status!r}")
        if poll == 0:
            print(f"parent {t['parent']} status={status}; waiting without admitting", flush=True)
        if poll + 1 < PARENT_POLLS:
            time.sleep(PARENT_POLL_SECONDS)
    raise ValueError(f"parent {t['parent']} still not active after {PARENT_POLLS} reads: status={status}")


def child_meta(r, t, pool, revision):
    m = run(r)
    check(m["id"] == r and m["repository"]["full_name"] == REPO and m["run_attempt"] == 1
          and m["path"] == ".github/workflows/" + WORKER and m["event"] == "workflow_dispatch"
          and m["head_branch"] == WORKER_REF and m["head_sha"] == revision
          and m["display_title"] == title(t, pool), "dedicated worker identity mismatch")
    return m


def assigned(job, pool):
    regex = (r"cs-(?:mty|mnl2|jed|ruh)-nc-n[012]-pr-v1-[a-z0-9]+-runner-[a-z0-9]+"
             if pool == "cs" else r"amd64-mo-shanghai-ut-safe-[a-z0-9]+-runner-[a-z0-9]+")
    return bool(job.get("runner_id") and re.fullmatch(regex, job.get("runner_name") or "")
                and not (job.get("runner_name") or "").startswith("cs-mty-nc-n2-"))


def observe(r, t, pool):
    data, now = api(f"/actions/runs/{r}/attempts/1/jobs?per_page=100")
    check(len(data["jobs"]) == data["total_count"], "incomplete jobs response")
    target = []
    for j in data["jobs"]:
        check(j["run_id"] == r and j["run_attempt"] == 1, "wrong job identity")
        if j["name"].split(" / ")[-1] == f"Coverage reservation {pool.upper()} group {t['group']}":
            check(j["labels"] == [POOLS[pool]], "wrong worker label")
            target.append(j)
        else:
            # The dispatcher has two mutually exclusive reusable calls. GitHub can
            # surface the non-selected call as a skipped placeholder.
            check(j["conclusion"] == "skipped", "not an exclusive single-group run")
    check(len(target) <= 1, "ambiguous reservation")
    return (target[0] if target else None), now


def decide(job, now, since):
    if job is None:
        check(since is None, "previously queued job disappeared")
        return None, None
    if job["status"] == "in_progress":
        check(assigned(job, "cs"), "unverified CS assignment")
        return "cs", since  # assignment wins, even at the exact deadline
    if job["status"] == "queued":
        if job.get("runner_id") or job.get("runner_name"):
            return None, None
        since = now if since is None else since
        return ("idc" if now - since >= WAIT else None), since
    check(job["status"] in ("requested", "waiting", "pending"), "not a queue timeout")
    return None, None


def dispatch(t, pool, revision):
    parent_meta(t)
    # At most one POST. If the response is lost, fail closed; no blind retries.
    body, _ = api(f"/actions/workflows/{WORKER}/dispatches", {
        "ref": WORKER_REF, "inputs": {"ticket": json.dumps(t, separators=(",", ":")),
                                 "pool": pool}})
    r = body.get("workflow_run_id")
    for _ in range(24):
        if not r:  # also support a legacy 204 response, correlated without guessing
            page = one(f"/actions/workflows/{WORKER}/runs?" + urlencode(
                dict(event="workflow_dispatch", branch=WORKER_REF, per_page=100)))
            ids = [x["id"] for x in page["workflow_runs"] if x["display_title"] == title(t, pool)]
            check(len(ids) <= 1, "duplicate dispatch; refusing to select")
            r = ids[0] if ids else None
        if r:
            child_meta(r, t, pool, revision)
            print(f"{pool} group {t['group']}: https://github.com/{REPO}/actions/runs/{r}", flush=True)
            return r
        time.sleep(5)
    raise ValueError("dispatch not reconciled; inspect Actions before retrying")


def output(**fields):
    with open(os.environ["GITHUB_OUTPUT"], "a") as f:
        for k, v in fields.items():
            print(f"{k}={v}", file=f)


def choose():
    check(os.environ["GITHUB_RUN_ATTEMPT"] == "1", "do not redispatch a previous generation")
    t = dict(parent=int(os.environ["GITHUB_RUN_ID"]), attempt=1, group=int(os.environ["GROUP"]),
             head=os.environ["HEAD"], source=os.environ["SOURCE"], pr=int(os.environ["PR"]))
    check(t["group"] in (0, 1) and re.fullmatch("[a-f0-9]{40}", t["head"]), "invalid input")
    parent_meta(t)
    # A pull_request_target run reports the PR head; a workflow_dispatch run
    # reports the workflow branch head. These are not the same commit.
    rev = t["revision"] = one("/git/ref/heads/" + WORKER_REF)["object"]["sha"]
    nonce = secrets.token_hex(32)
    t["key"] = digest(nonce)
    pool = "cs"
    r = dispatch(t, pool, rev)
    since = None
    while True:
        j, now = observe(r, t, "cs")
        route, since = decide(j, now, since)
        if route:
            print(f"group={t['group']} CS_job={j['id']} first_queued_utc={since} decision_utc={now} route={route}", flush=True)
            break
        time.sleep(10)
    if route == "idc":
        # A concurrently assigned CS worker is still blocked at admit: its nonce
        # was never published. Confirm identity again and cancel ONLY its own run.
        parent_meta(t)
        child_meta(r, t, "cs", rev)
        j, now = observe(r, t, "cs")
        route, _ = decide(j, now, since)
        check(route is not None, "ambiguous assignment at handoff")
        if route == "idc":
            api(f"/actions/runs/{r}/cancel", {})
            for _ in range(60):
                m = child_meta(r, t, "cs", rev)
                if m["status"] == "completed":
                    check(m["conclusion"] == "cancelled", "CS did not acknowledge withdrawal")
                    break
                time.sleep(5)
            else:
                raise ValueError("CS cancellation not confirmed; IDC not dispatched")
            nonce = secrets.token_hex(32)
            t["key"], pool = digest(nonce), "idc"
            r = dispatch(t, pool, rev)
    # Grant is immutable and costs no extra control token on the worker. Until
    # this point neither pool has been authorized to check out or run PR code.
    parent_meta(t)
    Path(os.environ["RUNNER_TEMP"], "grant.json").write_text(json.dumps(
        dict(ticket=t, child=r, pool=pool, nonce=nonce)))
    output(child=r, pool=pool)


def artifact(parent, name, filename):
    data = one(f"/actions/runs/{parent}/artifacts?" + urlencode(dict(name=name, per_page=2)))
    items = data["artifacts"]
    if not items:
        return None
    check(len(items) == 1 and items[0]["name"] == name and not items[0]["expired"]
          and items[0]["size_in_bytes"] < 65536, "ambiguous or invalid control artifact")
    try:
        api(f"/actions/artifacts/{int(items[0]['id'])}/zip")
    except urllib.error.HTTPError as e:
        if e.code != 302:
            raise
        url = e.headers["Location"]
    else:
        raise ValueError("unexpected artifact response")
    check(urlsplit(url).scheme == "https", "unsafe download")
    # Fresh request with NO authorization header; do not print the signed URL.
    with urllib.request.urlopen(url, timeout=30) as res:
        raw = res.read(65537)
    check(len(raw) <= 65536, "control artifact too large")
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        check(z.namelist() == [filename] and z.getinfo(filename).file_size <= 32768,
              "invalid control artifact payload")
        return json.loads(z.read(filename))


def admit():
    t = json.loads(os.environ["TICKET"])
    group, source, head, pool = int(os.environ["GROUP"]), os.environ["SOURCE"], os.environ["HEAD"], os.environ["POOL"]
    r = int(os.environ["GITHUB_RUN_ID"])
    check(os.environ["GITHUB_RUN_ATTEMPT"] == "1" and group == t["group"] and group in (0, 1)
          and head == t["head"] and source == t["source"] and pool in POOLS and t["attempt"] == 1
          and type(t["parent"]) is int and type(t["pr"]) is int and re.fullmatch("[a-f0-9]{64}", t["key"]),
          "invalid worker ticket")
    parent_meta(t)
    check(re.fullmatch("[a-f0-9]{40}", t["revision"]), "invalid dispatch revision")
    child_meta(r, t, pool, t["revision"])
    for _ in range(108):
        parent_meta(t)
        g = artifact(t["parent"], f"coverage-admit-{r}", "grant.json")
        if g is not None:
            check(g["child"] == r and g["ticket"] == t and g["pool"] == pool
                  and digest(g["nonce"]) == t["key"], "grant mismatch")
            break
        time.sleep(10)
    else:
        raise ValueError("no grant; no PR code executed")
    proof = artifact(t["parent"], f"real-pr-snapshot-{t['parent']}-attempt-{t['attempt']}",
                     "real-pr-authorization.json")
    pr = one(f"/pulls/{int(t['pr'])}")
    check(proof and proof["allowed"] == "true" and proof["verified_membership_http_status"] == 204
          and int(proof["pr_number"]) == pr["number"] and proof["author"] == pr["user"]["login"]
          and proof["head_repository"] == source == pr["head"]["repo"]["full_name"]
          and proof["head_sha"] == head == pr["head"]["sha"] and pr["state"] == "open"
          and not pr["draft"] and not pr["head"]["repo"]["archived"]
          and pr["base"]["repo"]["full_name"] == REPO and pr["base"]["ref"] == "main"
          and "kind/test-ci" in {v["name"] for v in pr["labels"]}, "PR no longer matches authorization")
    parent_meta(t)  # Reconfirm after checking the grant and live PR, before returning to checkout.


def wait():
    r = int(os.environ["CHILD"])
    while True:
        m = run(r)
        if m["status"] == "completed":
            check(m["run_attempt"] == 1 and m["conclusion"] == "success", "selected coverage group did not pass")
            return
        time.sleep(30)


if __name__ == "__main__":
    check(os.environ["GITHUB_REPOSITORY"] == REPO, "unexpected caller repository")
    {"choose": choose, "admit": admit, "wait": wait}[sys.argv[1]]()
