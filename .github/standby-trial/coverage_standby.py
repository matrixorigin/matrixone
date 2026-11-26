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
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlencode, urlsplit

REPO = "matrixorigin/matrixone"
API = "https://api.github.com/repos/" + REPO
WORKER = "coverage-standby-worker.yaml"
WORKER_REF = "weilu-runner-test"
ENTRY = ".github/workflows/coverage-standby-pr32922.yaml"
# Kept in sync with coverage_pr_entry.py by the admission contract tests.
CACHE_BASE_REFS = frozenset({"main", "4.2-dev"})
POOLS = {"cs": "cs-node-cache-7c14g", "idc": "amd64-mo-shanghai-ut-safe"}
WAIT = 3600
PROGRESS_INTERVAL = 300
HANDOFF_POLLS = 60
HANDOFF_POLL_SECONDS = 5


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


def short_title(t):
    return f"GROUP{t['group']}"


IDENTITY_SUFFIX = " / Run identity"


class WorkerIdentityPending(ValueError):
    pass


def identity_title(r, m):
    display = m["display_title"]
    if not re.fullmatch(r"GROUP[01]", display):
        return display
    data = one(f"/actions/runs/{r}/attempts/1/jobs?per_page=100")
    check(len(data["jobs"]) == data["total_count"], "incomplete worker identity response")
    marked = [j for j in data["jobs"] if j["name"].endswith(IDENTITY_SUFFIX)]
    if not marked:
        raise WorkerIdentityPending("worker identity is not indexed")
    check(len(marked) == 1, "ambiguous worker identity")
    marker = marked[0]
    check(marker["run_id"] == r and marker["run_attempt"] == 1
          and marker["status"] == "completed" and marker["conclusion"] == "skipped",
          "invalid worker identity marker")
    canonical = marker["name"][:-len(IDENTITY_SUFFIX)]
    check(canonical.split("/")[3:4] == [display[-1]], "worker identity group differs from title")
    return canonical


PARENT_POLLS = 13
PARENT_POLL_SECONDS = 5


def parent_identity(t, p):
    expected = {
        "id": t["parent"], "run_attempt": t["attempt"], "repository.full_name": REPO,
        "path": ENTRY, "event": "push",
        "head_repository.full_name": REPO, "head_sha": os.environ["GITHUB_SHA"],
        "head_branch": WORKER_REF, "actor.login": "Ariznawlll",
    }
    for field, value in {"pr": 33176, "source": "Ariznawlll/matrixone",
                         "head": "78aa93fc7e1b5620f6218cdd5c9a436a7e7fa343"}.items():
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


class DispatchRevisionChanged(ValueError):
    def __init__(self, child, actual):
        super().__init__("dispatch resolved a different workflow revision")
        self.child, self.actual = child, actual


def child_meta(r, t, pool, revision):
    m = run(r)
    check(m["id"] == r and m["repository"]["full_name"] == REPO and m["run_attempt"] == 1
          and m["path"] == ".github/workflows/" + WORKER and m["event"] == "workflow_dispatch"
          and m["head_branch"] == WORKER_REF, "dedicated worker identity mismatch")
    canonical = identity_title(r, m)
    check(canonical == title(t, pool) and (m["display_title"] == canonical
          or (t.get("title_version") == 2 and m["display_title"] == short_title(t))),
          "dedicated worker identity mismatch")
    actual = m["head_sha"]
    check(re.fullmatch("[a-f0-9]{40}", actual), "invalid worker revision")
    if actual != revision:
        raise DispatchRevisionChanged(r, actual)
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
            # GitHub can expose runner assignment before changing the status.
            # Preserve the observed queue start while this transition settles.
            return None, since
        since = now if since is None else since
        return ("idc" if now - since >= WAIT else None), since
    check(job["status"] in ("requested", "waiting", "pending"), "not a queue timeout")
    return None, since


def dispatch(t, pool, revision):
    parent_meta(t)
    # At most one POST. If the response is lost, fail closed; no blind retries.
    body, dispatched_at = api(f"/actions/workflows/{WORKER}/dispatches", {
        "ref": WORKER_REF, "inputs": {"ticket": json.dumps(t, separators=(",", ":")),
                                 "pool": pool}})
    r = body.get("workflow_run_id")
    # A plain GROUP0/GROUP1 is shared across PRs. The dispatch time bound is
    # just a discovery filter; always compare the full identity below.
    recent = datetime.fromtimestamp(dispatched_at - 60, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for _ in range(24):
        if not r:  # also support a legacy 204 response, correlated without guessing
            page = one(f"/actions/workflows/{WORKER}/runs?" + urlencode(
                dict(event="workflow_dispatch", branch=WORKER_REF, created=f">={recent}", per_page=100)))
            ids = []
            unsettled = False
            for candidate in page["workflow_runs"]:
                display = candidate["display_title"]
                if display == title(t, pool):
                    ids.append(candidate["id"])
                elif t.get("title_version") == 2 and display == short_title(t):
                    try:
                        if identity_title(candidate["id"], candidate) == title(t, pool):
                            ids.append(candidate["id"])
                    except WorkerIdentityPending:
                        # Indexing may lag a dispatch. Never pick a worker
                        # while a same-title candidate is still ambiguous.
                        unsettled = True
            check(len(ids) <= 1, "duplicate dispatch; refusing to select")
            r = ids[0] if ids and not unsettled else None
        if r:
            try:
                child_meta(r, t, pool, revision)
            except WorkerIdentityPending:
                pass
            else:
                print(f"{pool} group {t['group']}: https://github.com/{REPO}/actions/runs/{r}", flush=True)
                return r
        time.sleep(5)
    raise ValueError("dispatch not reconciled; inspect Actions before retrying")


def output(**fields):
    with open(os.environ["GITHUB_OUTPUT"], "a") as f:
        for k, v in fields.items():
            print(f"{k}={v}", file=f)


def reserve(t, pool):
    # Retry ONLY a positively identified, ungranted child resolved at the
    # wrong revision. Never retry an ambiguous dispatch or a lost response.
    for attempt in range(3):
        rev = t["revision"] = one("/git/ref/heads/" + WORKER_REF)["object"]["sha"]
        check(re.fullmatch("[a-f0-9]{40}", rev), "invalid branch revision")
        nonce = secrets.token_hex(32)
        t["key"] = digest(nonce)
        try:
            return dispatch(t, pool, rev), rev, nonce
        except DispatchRevisionChanged as changed:
            r = changed.child
            parent_meta(t)
            meta = child_meta(r, t, pool, changed.actual)
            if meta["status"] != "completed":
                try:
                    api(f"/actions/runs/{r}/cancel", {})
                except urllib.error.HTTPError as exc:
                    if exc.code != 409:
                        raise
                for _ in range(30):
                    if child_meta(r, t, pool, changed.actual)["status"] == "completed":
                        break
                    time.sleep(2)
                else:
                    raise ValueError("ungranted worker did not stop; no redispatch")
            print(f"{pool} child {r} ended without a grant after branch movement", flush=True)
    raise ValueError("workflow revision kept changing; no worker granted")


def choose():
    attempt = int(os.environ["GITHUB_RUN_ATTEMPT"])
    check(0 < attempt < 1_000_000, "invalid parent attempt")
    t = dict(parent=int(os.environ["GITHUB_RUN_ID"]), attempt=attempt, group=int(os.environ["GROUP"]),
             head=os.environ["HEAD"], source=os.environ["SOURCE"], pr=int(os.environ["PR"]), title_version=2)
    check(t["group"] in (0, 1) and re.fullmatch("[a-f0-9]{40}", t["head"]), "invalid input")
    parent_meta(t)
    if attempt > 1:
        # A previous selector may have lost its connection after publishing a
        # grant. An old *ungranted* reservation cannot pass parent_meta; an
        # already admitted worker must have stopped before issuing a new grant.
        require_previous_grants_terminal(t)
    # A pull_request_target run reports the PR head; a workflow_dispatch run
    # reports the workflow branch head. These are not the same commit.
    pool = "cs"
    r, rev, nonce = reserve(t, pool)
    print(f"Group {t['group']}: requesting a CS runner — {run_url(r)}", flush=True)
    since = None
    last_queue_report = None
    while True:
        j, now = observe(r, t, "cs")
        route, since = decide(j, now, since)
        if route:
            print(f"group={t['group']} CS_job={j['id']} first_queued_utc={since} decision_utc={now} route={route}", flush=True)
            break
        if since is not None and (last_queue_report is None or now - last_queue_report >= PROGRESS_INTERVAL):
            print(f"Group {t['group']}: queued for a CS runner ({int(max(0, now - since))}s) — {run_url(r)}", flush=True)
            last_queue_report = now
        time.sleep(30)
    if route == "idc":
        # A concurrently assigned CS worker is still blocked at admit: its nonce
        # was never published. Confirm identity again and cancel ONLY its own run.
        parent_meta(t)
        child_meta(r, t, "cs", rev)
        for poll in range(HANDOFF_POLLS):
            j, now = observe(r, t, "cs")
            route, since = decide(j, now, since)
            if route is not None:
                break
            if poll + 1 < HANDOFF_POLLS:
                time.sleep(HANDOFF_POLL_SECONDS)
                parent_meta(t)
                child_meta(r, t, "cs", rev)
        else:
            raise ValueError("CS assignment did not settle at handoff; no worker granted")
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
            pool = "idc"
            r, rev, nonce = reserve(t, pool)
            print(f"Group {t['group']}: switched to Shanghai standby — {run_url(r)}", flush=True)
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


def require_previous_grants_terminal(t):
    page = 1
    while True:
        batch = one(f"/actions/runs/{t['parent']}/artifacts?per_page=100&page={page}")["artifacts"]
        for item in batch:
            found = re.fullmatch(r"coverage-admit-([1-9][0-9]*)", item["name"])
            if not found:
                continue
            m = run(int(found[1]))
            fields = re.fullmatch(r"Coverage standby / ([1-9][0-9]*)/([1-9][0-9]*)/([01])/"
                                  r"([0-9a-f]{40})/(cs|idc)/[0-9a-f]{64}", identity_title(int(found[1]), m))
            check(fields and fields[1] == str(t["parent"]) and fields[4] == t["head"]
                  and m["path"] == ".github/workflows/" + WORKER and m["event"] == "workflow_dispatch"
                  and m["repository"]["full_name"] == REPO and m["head_branch"] == WORKER_REF,
                  "unrecognized previous grant; no redispatch")
            if fields[3] == str(t["group"]):
                check(m["status"] == "completed", "previously granted group still active; no redispatch")
        if len(batch) < 100:
            return
        page += 1


def proof_name(t):
    if t["attempt"] == 1:
        return f"real-pr-snapshot-{t['parent']}-attempt-1"
    return f"real-pr-retry-{t['parent']}-attempt-{t['attempt']}-group-{t['group']}"


def admit():
    t = json.loads(os.environ["TICKET"])
    group, source, head, pool = int(os.environ["GROUP"]), os.environ["SOURCE"], os.environ["HEAD"], os.environ["POOL"]
    r = int(os.environ["GITHUB_RUN_ID"])
    check(os.environ["GITHUB_RUN_ATTEMPT"] == "1" and group == t["group"] and group in (0, 1)
          and head == t["head"] and source == t["source"] and pool in POOLS
          and type(t["attempt"]) is int and 0 < t["attempt"] < 1_000_000
          and type(t["parent"]) is int and type(t["pr"]) is int and re.fullmatch("[a-f0-9]{64}", t["key"]),
          "invalid worker ticket")
    parent_meta(t)
    check(re.fullmatch("[a-f0-9]{40}", t["revision"]), "invalid dispatch revision")
    for _ in range(12):
        try:
            child_meta(r, t, pool, t["revision"])
            break
        except WorkerIdentityPending:
            parent_meta(t)
            time.sleep(5)
    else:
        raise ValueError("worker identity is unavailable; no PR code executed")
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
    proof = artifact(t["parent"], proof_name(t),
                     "real-pr-authorization.json")
    pr = one(f"/pulls/{int(t['pr'])}")
    # In-flight receipts from the old entrypoint retain their original
    # eligibility. New receipts bind the exact base and do not use type labels.
    schema = proof.get("schema", 1) if proof else None
    base = proof.get("base_ref", "main" if schema == 1 else None) if proof else None
    check(proof and schema in (1, 2) and proof["allowed"] == "true" and proof["verified_membership_http_status"] == 204
          and int(proof["pr_number"]) == pr["number"] and proof["author"] == pr["user"]["login"]
          and proof["head_repository"] == source == pr["head"]["repo"]["full_name"]
          and proof["head_sha"] == head == pr["head"]["sha"] and pr["state"] == "open"
          and not pr["draft"] and not pr["head"]["repo"]["archived"]
          and pr["base"]["repo"]["full_name"] == REPO
          and pr["base"]["ref"] == base and base in CACHE_BASE_REFS
          and (schema == 2 or "kind/test-ci" in {v["name"] for v in pr["labels"]}),
          "PR no longer matches authorization")
    parent_meta(t)  # Reconfirm after checking the grant and live PR, before returning to checkout.


def run_url(r):
    return f"https://github.com/{REPO}/actions/runs/{r}"


def worker_progress(r, group, pool):
    target, _ = observe(r, {"group": group}, pool)
    if not target:
        return "waiting for the worker job", run_url(r)
    # Print only known phase text and canonical numeric URLs. Never relay output
    # from PR code into the trusted hosted dispatcher.
    url = f"{run_url(r)}/job/{int(target['id'])}"
    runner = target.get("runner_name") or ""
    site = None
    if assigned(target, "cs"):
        site = runner.split("-")[1].upper()
    elif assigned(target, "idc"):
        site = "Shanghai IDC"
    ut_step = {
        "cs": "Run coverage unit tests",
        "idc": "Run the reviewed Coverage UT group, cold build allowed",
    }[pool]
    ut = next((s.get("status") for s in (target.get("steps") or []) if s.get("name") == ut_step), None)
    if target.get("status") == "completed":
        phase = "worker job finished"
    elif ut == "completed":
        phase = "UT step finished; collecting results"
    elif ut == "in_progress":
        phase = "Coverage UT running (setup and tests)"
    elif site:
        phase = "runner assigned; preparing Coverage UT"
    else:
        phase = "waiting for a runner"
    return (f"{site}: {phase}" if site else phase), url


def wait():
    r = int(os.environ["CHILD"])
    pool = os.environ["POOL"]
    group = int(os.environ["GROUP"])
    print(f"Group {group}: open worker for live UT logs — {run_url(r)}", flush=True)
    previous = None
    last_report = None
    while True:
        m = run(r)
        if m["status"] == "completed":
            check(m["run_attempt"] == 1 and m["conclusion"] == "success", "selected coverage group did not pass")
            print(f"Group {group}: Coverage UT passed — {run_url(r)}", flush=True)
            return
        try:
            progress = worker_progress(r, group, pool)
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            progress = ("worker details temporarily unavailable; still waiting", run_url(r))
        now = time.monotonic()
        if progress != previous or last_report is None or now - last_report >= PROGRESS_INTERVAL:
            phase, url = progress
            print(f"Group {group}: {phase} — {url}", flush=True)
            previous, last_report = progress, now
        time.sleep(30)


if __name__ == "__main__":
    check(os.environ["GITHUB_REPOSITORY"] == REPO, "unexpected caller repository")
    {"choose": choose, "admit": admit, "wait": wait}[sys.argv[1]]()
