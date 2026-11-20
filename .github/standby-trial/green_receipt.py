"""Trial only: import this exact successful worker after validating its original grant."""
import os
import coverage_standby as s

parent, child, job_id = 44680679514, 44680711177, 136103157187
revision = "3771e8369c41370e025d7eeb75883eafc9b5fc16"
head, src = os.environ["HEAD"], os.environ["SOURCE"]
s.check(os.environ["GITHUB_RUN_ATTEMPT"] == "1", "successful group unexpectedly selected for retry")
s.check(head == "f2d6e753ddc739e63f3cc3f9d13eda06bcd605b4" and src == "Ariznawlll/matrixone", "wrong PR head")
p = s.one(f"/actions/runs/{parent}/attempts/1")
s.check((p["id"],p["head_branch"],p["head_sha"],p["event"],p["path"],p["repository"]["full_name"],p["actor"]["login"])
        == (parent,s.WORKER_REF,revision,"push",s.ENTRY,s.REPO,"Ariznawlll"), "wrong source trial")
g = s.artifact(parent,f"coverage-admit-{child}","grant.json")
t = g["ticket"]
s.check((g["child"],g["pool"],t["group"],t["pr"],t["parent"],t["attempt"],t["source"],t["head"],t["revision"],t["key"])
        == (child,"cs",0,32922,parent,1,src,head,revision,s.digest(g["nonce"])), "wrong original grant")
m = s.child_meta(child,t,"cs",revision)
j = s.one(f"/actions/jobs/{job_id}")
s.check(m["status"] == "completed" and m["conclusion"] == "success" and j["conclusion"] == "success"
        and j["run_id"] == child and j["run_attempt"] == 1 and s.assigned(j,"cs")
        and any(v["name"] == "Run coverage unit tests" and v["conclusion"] == "success" for v in j["steps"]),
        "original group 0 UT did not pass")
expected = {14606165715:f"ut-coverage-skips-group-0-{child}-1",14605672411:f"ut-coverage-group-0-generation-{child}-1"}
for i,name in expected.items():
    a = s.one(f"/actions/artifacts/{i}")
    s.check(a["name"] == name and a["workflow_run"]["id"] == child and not a["expired"], "wrong original evidence")
print("Reusing verified original group 0: " + j["html_url"])
s.output(child=child)
