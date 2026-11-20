import json, os, urllib.request as u
repo = "matrixorigin/matrixone"
fork = "Ariznawlll/matrixone"
sha = "f2d6e753ddc739e63f3cc3f9d13eda06bcd605b4"
run_id = 44680060080
def check(ok, msg):
    if not ok: raise SystemExit(msg)
def get(path):
    request = u.Request(f"https://api.github.com/repos/{repo}/" + path, headers={"Authorization": "Bearer "+os.environ["API_TOKEN"], "Accept":"application/vnd.github+json"})
    with u.urlopen(request, timeout=20) as response: return json.load(response)
p = get("pulls/32922")
check(p["number"]==32922 and p["state"]=="open" and not p["draft"] and p["base"]["repo"]["full_name"]==repo and p["base"]["ref"]=="main", "wrong or inactive PR")
check(p["head"]["sha"]==sha and p["head"]["repo"]["full_name"]==fork, "PR head mismatch")
check(p["user"]["login"]=="Ariznawlll" and "kind/test-ci" in {x["name"] for x in p["labels"]}, "author or label mismatch")
class NoRedirect(u.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs): return None
req=u.Request("https://api.github.com/orgs/matrixorigin/members/Ariznawlll",headers={"Authorization":"Bearer "+os.environ["ORG_MEMBERSHIP_TOKEN"]})
with u.build_opener(NoRedirect).open(req,timeout=20) as member:
    check(member.status==204,"organization membership not confirmed")
r = get(f"actions/runs/{run_id}")
check(r["repository"]["full_name"]==repo and r["event"]=="pull_request_target" and r["path"]==".github/workflows/entrypoint.yaml" and r["head_sha"]==sha and r["head_repository"]["full_name"]==fork and r["head_branch"]==p["head"]["ref"] and r["run_attempt"]==1, "ordinary pipeline provenance mismatch")
for job_id, name in ((136101028720,"CI preflight"),(136101143990,"Matrixone UT Coverage execution / Check coverage eligibility")):
    j=get(f"actions/jobs/{job_id}")
    check((j["run_id"],j["run_attempt"],j["name"],j["head_sha"],j["status"],j["conclusion"])==(run_id,1,name,sha,"completed","success"),name+" not accepted")
print("Matched official PR preflight and Coverage eligibility for #32922 at "+sha)
with open(os.environ["GITHUB_OUTPUT"],"a") as f:
    print(f"allowed=true\nhead_repository={fork}\nhead_sha={sha}", file=f)
from pathlib import Path
Path(os.environ["RUNNER_TEMP"], "real-pr-authorization.json").write_text(json.dumps({"allowed":"true","verified_membership_http_status":204,"pr_number":str(p["number"]),"author":p["user"]["login"],"head_repository":fork,"head_sha":sha}))
