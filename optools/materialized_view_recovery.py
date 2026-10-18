#!/usr/bin/env python3
"""Opt-in native upgrade/crash validation; owns every service it starts.

Full mode requires the pinned pre-feature mo-service (default: main 4.0.16),
a current mo-service built with the matrixone_mv_faults tag, and
test/materialized_view/native_probe. --refresh-only needs the current production
service and probe; it skips startup migration injection. Uses only the Python
standard library. Run from the repository root; see the validation
document for build commands and the tested old revision.
"""

import argparse
import hashlib
import json
import os
import pathlib
import select
import socket
import subprocess
import time
import urllib.request


def wait_for(description, check, seconds=180):
    deadline = time.monotonic() + seconds
    while True:
        value = check()
        if value:
            return value
        if time.monotonic() >= deadline:
            raise TimeoutError(description)
        time.sleep(0.25)  # Poll an observable phase, never assume elapsed readiness.


class Fixture:
    def __init__(self, args):
        self.args = args
        self.root = pathlib.Path(args.work_dir).resolve()
        self.root.mkdir()  # Refuse residue; never delete another fixture's data.
        self.processes = []
        self.logs = []
        self.started = time.monotonic()
        self.report = {"passed_cases": [], "observations": {}, "binaries": {}, "sql_observations": {}}
        for key in ("service", "probe") + (("old_service",) if args.old_service else ()):
            path = pathlib.Path(getattr(args, key)).resolve()
            with path.open("rb") as binary:
                digest = hashlib.sha256()
                for chunk in iter(lambda: binary.read(1 << 20), b""):
                    digest.update(chunk)
            self.report["binaries"][key] = {
                "path": str(path), "sha256": digest.hexdigest()
            }
        self.configure()
        probe_log = (self.root / "probe.log").open("w")
        self.logs.append(probe_log)
        self.probe = subprocess.Popen(
            [args.probe], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=probe_log, text=True,
        )

    def sql(self, statement, old=False, query=False, allow_error=False):
        port = 6102 if old else 6101
        dsn = f"dump:111@tcp(127.0.0.1:{port})/?timeout=2s&readTimeout=35s&writeTimeout=35s&time_zone=%27%2B00%3A00%27"
        self.probe.stdin.write(json.dumps({"dsn": dsn, "sql": statement, "query": query}) + "\n")
        self.probe.stdin.flush()
        if not select.select([self.probe.stdout], [], [], 40)[0]:
            raise TimeoutError("SQL probe failed to honor its operation deadline")
        line = self.probe.stdout.readline()
        if not line:
            raise RuntimeError("SQL probe exited")
        response = json.loads(line)
        key = ("old: " if old else "current: ") + statement
        observation = self.report["sql_observations"].setdefault(key, {"attempts": 0, "changes": []})
        observation["attempts"] += 1
        if not observation["changes"] or observation["changes"][-1] != response:
            observation["changes"].append(response)
        if response["error"] and not allow_error:
            raise RuntimeError(statement + ": " + response["error"])
        return response if allow_error else response["rows"]

    def q(self, statement, **kwargs):
        return self.sql(statement, query=True, **kwargs)

    def passed(self, name, observation=None):
        self.report["passed_cases"].append(name)
        if observation is not None:
            self.report["observations"][name] = observation
        print("PASS", name, flush=True)

    def configure(self):
        template = pathlib.Path("etc/launch")
        for name in ("log", "tn", "cn"):
            text = (template / (name + ".toml")).read_text()
            text = text.replace("./mo-data", str(self.root / "data"))
            text = text.replace("disableMetric = false", "disableMetric = true")
            text = text.replace("enable-auto-id-cache = true", "enable-auto-id-cache = false")
            text = text.replace("status-port = 7001", "status-port = " + {"log": "7301", "tn": "7302", "cn": "7303"}[name])
            if name == "cn":
                text = text.replace("[cn]\n", "[cn]\nauto-upgrade = true\n")
                (self.root / "cn.toml").write_text(text + "\n[cn.frontend]\nport=6101\n")
                old = text.replace("dd1dccb4", "ee1dccb4").replace("18000", "20000")
                old = old.replace("status-port = 7303", "status-port = 7304")
                (self.root / "old.toml").write_text(old + "\n[cn.frontend]\nport=6102\n")
            else:
                (self.root / (name + ".toml")).write_text(text)
        (self.root / "storage.toml").write_text(
            f'logservices=["{self.root}/log.toml"]\ntnservices=["{self.root}/tn.toml"]\n'
        )

    def start(self, label, old=False, storage=False, fault="", role=None):
        if storage:
            # The regular -launch entry requires at least one CN. Separate
            # native LOG/TN processes let a CN crash leave storage running.
            processes = []
            for service, port in (("log", 32001), ("tn", 19000)):
                p = self.start(label + "-" + service, old=old, role=service)
                processes.append(p)

                def listening():
                    if p.poll() is not None:
                        raise RuntimeError(f"{service} exited: {p.returncode}")
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=1):
                            return True
                    except OSError:
                        return False
                wait_for(service + " TCP readiness", listening)
            return processes
        log = (self.root / (label + ".log")).open("w")
        self.logs.append(log)
        binary = self.args.old_service if old else self.args.service
        env = dict(os.environ)
        env.pop("MO_MV_STARTUP_FAULT", None)
        if fault:
            env["MO_MV_STARTUP_FAULT"] = fault
        config = role + ".toml" if role else "old.toml" if old else "cn.toml"
        args = [binary, "-cfg", str(self.root / config)]
        if role is None and not old:
            args += ["-debug-http", "127.0.0.1:7603"]
        p = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT, env=env)
        p.fixture_label = label
        self.processes.append(p)
        return p

    @staticmethod
    def stop(p, crash=False):
        if isinstance(p, list):
            for child in reversed(p):
                Fixture.stop(child, crash=crash)
            return
        if p.poll() is None:
            if crash:
                p.kill()
            else:
                p.terminate()
            try:
                p.wait(timeout=45)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
                raise
        if crash and p.returncode != -9:
            raise RuntimeError(f"expected SIGKILL, got {p.returncode}")

    def ready(self, p, old=False):
        def check():
            if p.poll() is not None:
                raise RuntimeError(f"server exited: {p.returncode}")
            result = self.q("select 1", old=old, allow_error=True)
            return result["rows"] == [["1"]] and not result["error"]
        wait_for("normal authenticated SQL readiness", check)

    def await_log(self, label, needle, p):
        def check():
            if p.poll() is not None:
                raise RuntimeError(f"{label} exited: {p.returncode}")
            return needle in (self.root / (label + ".log")).read_text(errors="replace")
        wait_for(label + ": " + needle, check)

    def native_upgrade(self):
        storage = self.start("old-storage", old=True, storage=True)
        old = self.start("old-bootstrap", old=True)
        self.ready(old, old=True)
        assert self.q("select version,state from mo_catalog.mo_version order by create_at desc limit 1", old=True) == [[self.args.old_version, "2"]]
        inventory = self.q("select mo_ctl('cn','GetProtocolVersion','')", old=True)
        versions = json.loads(inventory[0][0])["result"]
        assert versions and all(int(entry.rsplit(":", 1)[1]) == self.args.old_protocol for entry in versions.split(",")), inventory
        self.report["old_protocol"] = self.args.old_protocol
        self.sql("create database mv_recovery", old=True)
        self.sql("create table mv_recovery.facts(id int primary key,v bigint)", old=True)
        self.sql("insert into mv_recovery.facts values(1,10),(2,20)", old=True)
        assert self.q("select count(*) from mo_catalog.mo_tables where reldatabase='mo_catalog' and relname in ('mo_materialized_views','mo_mv_dependencies','mo_mv_refresh_runs','mo_mv_account_state')", old=True) == [["0"]]
        self.passed(f"ordinary authenticated {self.args.old_version} deployment", self.q("select sum(v) from mv_recovery.facts", old=True))
        # Retain the real old CN and LOG/TN while starting the new CN. Catalog
        # installation must wait for the protocol barrier across this topology.
        current = self.start("new-mixed", fault="mv_catalog_upgrade_after_ddl#2:2::#wait#0##false")
        self.await_log("new-mixed", "requires all CNs to support protocol version 120", current)
        self.await_log("new-mixed", "materialized view catalogs are not installed", current)
        control = self.q("select count(*) from mo_catalog.mo_tables where reldatabase='mo_catalog' and relname in ('mo_materialized_views','mo_mv_dependencies','mo_mv_refresh_runs','mo_mv_account_state')", old=True, allow_error=True)
        if not control["error"]:
            assert control["rows"] == [["0"]], control
        rejected = self.q("select 1", allow_error=True)
        # Login waits for the target upgrade state (bounded by the probe's
        # context), or reports the explicit earlier tenant-version fence.
        assert any(message in rejected["error"] for message in ("tenant upgrade requires", "context deadline exceeded")), rejected
        # New executor metadata itself must not enter the old scheduler, even
        # though no MV can yet be created. Unknown-executor failures on old CNs
        # are not an acceptable way to implement capability exclusion.
        for table in ("sys_cron_task", "sys_async_task"):
            assert self.q(f"select count(*) from mo_task.{table} where task_metadata_executor=17", old=True) == [["0"]]
        assert "executor with code 17 not exists" not in (self.root / "old-bootstrap.log").read_text(errors="replace")
        self.passed("old nodes block MV catalog migration and new-CN login remains pending", {"new_login": rejected, "old_catalog_control": control})
        self.passed("mixed old nodes never receive MV cron or asynchronous task metadata")
        self.stop(current)
        self.stop(old)
        self.stop(storage)
        storage = self.start("new-storage", storage=True)
        current = self.start("new-staged-migration", fault="mv_catalog_upgrade_after_ddl#2:2::#wait#0##false")

        # The startup-only tagged hook waits after the second real DDL. SQL
        # login correctly cannot happen yet; inspect the existing debug stack.
        def staged():
            if current.poll() is not None:
                raise RuntimeError("upgrader exited")
            try:
                with urllib.request.urlopen("http://127.0.0.1:7603/debug/pprof/goroutine?debug=2", timeout=3) as response:
                    stack = response.read().decode()
                if "v4_0_17.(*versionHandle).HandleClusterUpgrade" in stack and "fault.(*faultEntry).doWithContext" in stack:
                    (self.root / "interrupted-upgrade.stack").write_text(stack)
                    return True
            except OSError:
                return False
            return False
        wait_for("two catalog DDLs staged before commit", staged, seconds=240)
        self.stop(current, crash=True)
        self.passed("SIGKILL after two catalog DDLs before migration commit")
        current = self.start("new-migration-recovery")
        self.ready(current)
        self.await_log("new-migration-recovery", "ISCP-Task Start", current)
        self.assert_catalogs()
        assert self.q("select sum(v) from mv_recovery.facts") == [["30"]]
        self.passed("interrupted migration recovers atomically with ordinary data intact")
        self.stop(current)
        current = self.start("new-migration-repeat")
        self.ready(current)
        self.assert_catalogs()
        self.passed("repeated automatic upgrade has exactly four catalogs and one ready target")
        return current

    def assert_catalogs(self):
        assert self.q("select version,version_offset,state from mo_catalog.mo_version where version='4.0.17'") == [["4.0.17", "5", "2"]]
        assert self.q("select relname,count(*) from mo_catalog.mo_tables where account_id=0 and reldatabase='mo_catalog' and relname in ('mo_materialized_views','mo_mv_dependencies','mo_mv_refresh_runs','mo_mv_account_state') group by relname order by relname") == [
            ["mo_materialized_views", "1"], ["mo_mv_account_state", "1"], ["mo_mv_dependencies", "1"], ["mo_mv_refresh_runs", "1"]]

    def snapshot(self):
        # One snapshot includes result rows, source provenance and run status.
        return self.q("select m.n,m.s,v.last_run,v.successful_sequence,v.source_physical,v.source_logical,r.status,r.source_physical,r.source_logical from mv_recovery.result m,mo_catalog.mo_materialized_views v,mo_catalog.mo_mv_refresh_runs r where v.account_id=0 and v.database_name='mv_recovery' and v.name='result' and r.account_id=v.account_id and r.mv_id=v.mv_id and r.run_id=v.last_run")

    def enable(self, point, action="wait", argument=0):
        self.q("select enable_fault_injection()")
        self.q(f"select add_fault_point('{point}',':::','{action}',{argument},'')")
        self.q(f"select add_fault_point('mv_probe_waiters',':::','getwaiters',0,'{point}')")
        self.q(f"select add_fault_point('mv_probe_count',':::','getcount',0,'{point}')")

    def crash_refresh(self, current, phase, value):
        before = self.snapshot()
        self.sql(f"update mv_recovery.facts set v={value}")
        self.enable("mv_refresh_" + phase)
        self.sql("update mo_catalog.mo_materialized_views set next_due=current_timestamp() where account_id=0 and database_name='mv_recovery' and name='result'")
        wait_for("refresh " + phase + " barrier", lambda: self.q("select trigger_fault_point('mv_probe_waiters')") == [["1"]], seconds=120)
        assert self.q("select trigger_fault_point('mv_probe_count')") == [["1"]]
        run = self.q("select run_id,sequence,status from mo_catalog.mo_mv_refresh_runs where account_id=0 and mv_id=(select mv_id from mo_catalog.mo_materialized_views where account_id=0 and database_name='mv_recovery' and name='result') order by sequence desc limit 1")[0]
        assert len(run[0]) == 64, run  # Actual periodic task, not a manual UUID.
        queue = self.q(f"select task_status,task_runner from mo_task.sys_async_task where task_metadata_id='mv:{run[0][:40]}'")
        assert len(queue) == 1 and queue[0][0] == "1", queue
        committed = self.snapshot()
        if phase == "before_commit":
            assert committed == before and run[2] == "RUNNING", (before, committed, run)
        else:
            assert committed[0][:2] == ["2", str(2 * value)] and run[2] == "SUCCEEDED", (committed, run)
            assert committed[0][2] == run[0] and committed[0][4:6] == committed[0][7:9], committed
        self.stop(current, crash=True)
        self.passed("SIGKILL periodic refresh " + phase, {"before": before, "at_barrier": committed, "run": run, "queue": queue})
        current = self.start("recovery-" + phase)
        self.ready(current)
        recovered = self.snapshot()
        if phase == "after_commit":
            assert recovered == committed, (committed, recovered)
        elif recovered != before:
            assert len(recovered) == 1 and recovered[0][:4] == ["2", str(2 * value), run[0], run[1]], recovered
            assert recovered[0][6] == "SUCCEEDED" and recovered[0][4:6] == recovered[0][7:9], recovered
        # Observe the same durable queue identity being acknowledged (or GC'd)
        # after restart. No hand-written task rows or run-state repair.
        def replayed():
            result = self.q(f"select task_status from mo_task.sys_async_task where task_metadata_id='mv:{run[0][:40]}'")
            return result in ([], [["2"]])
        wait_for("actual task replay acknowledgement", replayed, seconds=240)
        after = self.snapshot()
        assert len(after) == 1 and after[0][:2] == ["2", str(2 * value)], after
        assert after[0][2] == run[0] and after[0][3] == run[1], (run, after)
        assert after[0][6] == "SUCCEEDED" and after[0][4:6] == after[0][7:9], after
        assert tuple(map(int, after[0][4:6])) > tuple(map(int, before[0][4:6])), (before, after)
        if phase == "after_commit":
            assert after == committed, (committed, after)  # No second publication.
        assert self.q("select count(*) from mo_catalog.mo_mv_refresh_runs where run_id='" + run[0] + "'") == [["1"]]
        self.passed("restart and same-run replay " + phase, {"after_restart": recovered, "after_ack": after})
        return current

    def run(self):
        if self.args.refresh_only:
            self.start("storage", storage=True)
            current = self.start("initial")
            self.ready(current)
            self.sql("create database mv_recovery")
            self.sql("create table mv_recovery.facts(id int primary key,v bigint)")
            self.sql("insert into mv_recovery.facts values(1,10),(2,20)")
        else:
            current = self.native_upgrade()
        self.sql("set experimental_materialized_view=1")
        self.sql("create materialized view mv_recovery.result refresh every interval 1 day as select count(*) as n,sum(v) as s from mv_recovery.facts")
        self.sql("refresh materialized view mv_recovery.result")
        initial = self.snapshot()
        assert initial[0][:2] == ["2", "30"], initial
        if not self.args.refresh_only:
            # Normal automatic-upgrade startup of an actual old binary must refuse
            # the persisted newer cluster before admitting SQL or scheduled work.
            old = self.start("unsafe-downgrade", old=True)
            wait_for("unsafe old-binary startup refused", lambda: old.poll() is not None, seconds=120)
            log = (self.root / "unsafe-downgrade.log").read_text(errors="replace")
            fences = (f"cannot startup a old version {self.args.old_version} to join cluster, current version is 4.0.17",
                      f"requires persisted expression protocol version 120 (local version {self.args.old_protocol})")
            reason = next((fence for fence in fences if fence in log), None)
            assert old.returncode != 0 and reason, log[-3000:]
            assert self.snapshot() == initial
            assert self.q("select count(*) from mo_task.sys_async_task where task_metadata_executor=17 and task_runner='ee1dccb4-4d3c-41f8-b482-5251dc7a41bf'") == [["0"]]
            self.passed("old binary refuses downgrade with populated periodic MV before SQL/task admission", {"exit_code": old.returncode, "fence": reason})
            # Wait for failed old-node liveness to leave the protocol inventory.
            wait_for("old node removed from protocol inventory", lambda: not self.q("select mo_ctl('cn','GetProtocolVersion','')", allow_error=True)["error"], seconds=240)
        self.sql("update mv_recovery.facts set v=25")
        self.enable("mv_refresh_after_commit", action="echo", argument=1)
        self.sql("refresh materialized view mv_recovery.result")
        assert self.q("select trigger_fault_point('mv_probe_count')") == [["1"]]
        lost = self.snapshot()
        assert lost[0][:2] == ["2", "50"] and lost[0][6] == "SUCCEEDED" and lost[0][4:6] == lost[0][7:9], lost
        assert self.q("select count(*) from mo_catalog.mo_mv_refresh_runs where status='FAILED'") == [["0"]]
        self.q("select disable_fault_injection()")
        self.passed("native durable commit response loss resolves to success", lost)
        current = self.crash_refresh(current, "before_commit", 40)
        current = self.crash_refresh(current, "after_commit", 60)
        self.sql("drop materialized view mv_recovery.result")
        self.sql("drop database mv_recovery")
        assert self.q("select count(*) from mo_catalog.mo_database where datname='mv_recovery'") == [["0"]]
        for table in ("mo_materialized_views", "mo_mv_dependencies", "mo_mv_refresh_runs"):
            assert self.q("select count(*) from mo_catalog." + table) == [["0"]]
        # Queue completion precedes HAKeeper's periodic GC. A completed row is
        # not an active task leak; require terminal status, then actual removal.
        assert self.q("select count(*) from mo_task.sys_async_task where task_metadata_id like 'mv:%' and task_status<>2") == [["0"]]
        wait_for("completed per-view task GC", lambda: self.q("select count(*) from mo_task.sys_async_task where task_metadata_id like 'mv:%'") == [["0"]], seconds=60)
        self.passed("native fixture removes MV rows, dependencies, history and per-view tasks")

    def close(self):
        errors = []
        for process in reversed(self.processes):
            try:
                self.stop(process)
            except Exception as error:
                errors.append(str(error))
        self.probe.stdin.close()
        try:
            self.probe.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.probe.kill()
            self.probe.wait()
        for log in self.logs:
            log.close()
        if errors:
            self.report["cleanup_errors"] = errors
        self.report["process_exits"] = {p.fixture_label: p.returncode for p in self.processes}
        self.report["elapsed_seconds"] = time.monotonic() - self.started
        (self.root / "results.json").write_text(json.dumps(self.report, indent=2) + "\n")
        if errors:
            raise RuntimeError(errors)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh-only", action="store_true",
                        help="fresh current cluster: run commit-loss and refresh crash/replay cases without upgrade/downgrade")
    parser.add_argument("--service", required=True)
    parser.add_argument("--old-service", help="required unless --refresh-only is selected")
    parser.add_argument("--old-version", default="4.0.16", help="catalog version of the pinned old binary")
    parser.add_argument("--old-protocol", type=int, default=118,
                        help="expected protocol of the pinned old binary (118 by default)")
    parser.add_argument("--probe", required=True)
    parser.add_argument("--work-dir", required=True)
    args = parser.parse_args()
    if not args.refresh_only and not args.old_service:
        parser.error("--old-service is required for upgrade/downgrade validation")
    fixture = Fixture(args)
    try:
        fixture.run()
    except BaseException as error:
        fixture.report["error"] = repr(error)
        raise
    finally:
        fixture.close()


if __name__ == "__main__":
    main()
