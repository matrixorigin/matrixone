"""Bounded P1 Linux qualification; never a performance or Rebase executor.

The caller stages source.tar.gz and the single Go probe after reserve. Immutable
manifest binds all three inputs. One supervisor owns the cooperative lease and
one systemd-confined worker. Failures retain evidence; no foreign cleanup.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import time


PARENT = Path("/mnt/data/native-rebase/runs")
LOCK = Path("/mnt/data/issue29322/active.lock")
COMPILER = Path("/mnt/data/pr29798/runs/pr29798-20261018141822-6ee67900")
BASE = "b01e952ad1021ef8f188641f3a11a1360c001218"
GIB = 2 ** 30


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 ** 2), b""):
            h.update(block)
    return h.hexdigest()


def save(path, data):
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(data, indent=2) + "\n")
    temp.replace(path)


def call(args, check=True):
    return subprocess.run(args, capture_output=True, text=True, timeout=30, check=check)


def props(unit):
    out = call(["systemctl", "--user", "show", unit, "-p", "ActiveState", "-p", "MainPID",
                "-p", "ControlGroup", "-p", "Result", "-p", "ExecMainStatus"], False).stdout
    return dict(line.split("=", 1) for line in out.splitlines() if "=" in line)


def available():
    return next(int(x.split()[1]) * 1024 for x in Path("/proc/meminfo").read_text().splitlines()
                if x.startswith("MemAvailable:"))


def verify(root):
    if root.parent != PARENT or root.resolve() != root or root.is_symlink():
        raise ValueError("unexpected root")
    manifest = json.loads((root / "manifest.json").read_text())
    if manifest["base"] != BASE:
        raise ValueError("base changed")
    for name, sha in manifest["inputs"].items():
        if name not in {"native_rebase_probe_linux.py", "source.tar.gz", "rebase_transaction_prerequisites_test.go",
                        "usearch.zip", "usearch.mod", "usearch.info", "rebase_transaction_restart_test.go",
                        "rebase_transaction_cancellation_test.go", "startup_restart_control_test.go",
                        "rebase_source_drop_test.go", "rebase_retained_base_gc_test.go",
                        "rebase_crash_recovery_test.go"}:
            raise ValueError("unexpected input")
        if (root / name).is_symlink() or digest(root / name) != sha:
            raise ValueError("input changed: " + name)
    return manifest


def owned(root):
    identity = json.loads((root / "identity.json").read_text())
    if json.loads((LOCK / "owner.json").read_text()) != identity:
        raise ValueError("lease is not owned")
    return identity


def reserve(root):
    if root.parent != PARENT or root.resolve() != root or root.is_symlink():
        raise ValueError("unexpected root")
    manifest = json.loads((root / "manifest.json").read_text())
    if manifest["base"] != BASE or digest(root / "native_rebase_probe_linux.py") != manifest["inputs"]["native_rebase_probe_linux.py"]:
        raise ValueError("unfrozen supervisor")
    if (root / "identity.json").exists() or LOCK.exists():
        raise RuntimeError("existing identity or owner")
    mode = manifest.get("mode", "full")
    if mode not in {"full", "startup_control", "p1_lifecycle", "p1_recovery", "p1_crash", "p1_bwrap", "p1_bwrap_build"}:
        raise ValueError("unknown probe mode")
    # Startup control reuses a verified closed tree and native outputs; its
    # budget is one compiler-cache copy plus a linked test, not a native build.
    minimum_disk = 40 if mode == "full" else 12
    if available() < 22 * GIB or shutil.disk_usage(root).free < minimum_disk * GIB:
        raise RuntimeError("insufficient build headroom")
    for proc in Path("/proc").iterdir():
        try:
            if proc.name.isdigit() and (proc / "comm").read_text().strip() in {
                    "mo-service", "dolt", "go", "compile", "link", "make", "cmake"}:
                raise RuntimeError("foreign workload; no takeover")
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
    identity = {"root": str(root), "phase": "NATIVE_REBASE_P1_QUALIFICATION",
                "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
                "unit": root.name + "-worker", "supervisor": root.name + "-supervisor"}
    LOCK.mkdir(mode=0o700)
    try:
        save(LOCK / "owner.json", identity)
        save(root / "identity.json", identity)
    except BaseException:
        # Do not delete a replaced or foreign lease on a partial failure.
        if (LOCK / "owner.json").exists() and json.loads((LOCK / "owner.json").read_text()) == identity:
            (LOCK / "owner.json").unlink()
            LOCK.rmdir()
        raise


def validate_source_members(members):
    for member in members:
        name = member.name.rstrip("/")
        if name == "source" and member.isdir():
            continue
        if not name.startswith("source/") or ".." in Path(name).parts:
            raise ValueError("source archive must stay inside source/")


def child(root):
    manifest = verify(root)
    owned(root)
    if manifest.get("mode") == "p1_bwrap":
        return bwrap_probe(root, manifest)
    if manifest.get("mode") in {"startup_control", "p1_lifecycle", "p1_recovery", "p1_crash", "p1_bwrap_build"}:
        return startup_control(root, manifest)
    source = root / "source"
    if source.exists():
        raise ValueError("source already exists; no blind retry")
    with tarfile.open(root / "source.tar.gz") as archive:
        validate_source_members(archive.getmembers())
        archive.extractall(root, filter="data")
    shutil.copyfile(root / "rebase_transaction_prerequisites_test.go",
                    source / "pkg/tests/sqlintegration/rebase_transaction_prerequisites_test.go")
    shutil.copyfile(root / "rebase_transaction_restart_test.go",
                    source / "pkg/tests/issues/isolated/rebase_transaction_restart_test.go")
    shutil.copyfile(root / "rebase_transaction_cancellation_test.go",
                    source / "pkg/tests/sqlintegration/rebase_transaction_cancellation_test.go")
    prior = Path(manifest["compiler_cache_from"])
    if prior.parent != PARENT or prior.resolve() != prior or prior == root:
        raise ValueError("unexpected compiler cache owner")
    prior_result = json.loads((prior / "result.json").read_text())
    prior_manifest = json.loads((prior / "manifest.json").read_text())
    if prior_result["status"] != "PASS_PREREQUISITE_ONLY" or not prior_result["released"] or prior_manifest["base"] != BASE:
        raise ValueError("compiler cache owner is not successfully closed")
    (root / "cache").mkdir()
    with (root / "cache-copy.log").open("x") as cache_log:
        subprocess.run(["cp", "-a", "--reflink=auto", str(prior / "cache/go-build"),
                        str(root / "cache/go-build")], check=True, timeout=180,
                       stdout=cache_log, stderr=subprocess.STDOUT)
    proxy = root / "proxy/github.com/cpegeric/usearch/golang/@v"
    proxy.mkdir(parents=True)
    for extension in ("zip", "mod", "info"):
        shutil.copyfile(root / ("usearch." + extension),
                        proxy / ("v0.0.0-20261026183644-b1f5768c8466." + extension))
    prebuilt = root / "prebuilt"
    prebuilt.mkdir()
    onnx = Path("/mnt/data/branchbench-micro/runs/bbmicro-20261023T183251Z-be663676/onnxruntime-1.26.0.so")
    if digest(onnx) != "5bd5bedf736fc501692435d0ec4f6e8b2bdf48cd30af8e6d00d61b3ddc9a7ab8":
        raise ValueError("qualified ONNX input changed")
    shutil.copyfile(onnx, prebuilt / "onnxruntime.so")
    env = dict(os.environ, PROOT_NO_SECCOMP="1")
    shell = """unset TMPDIR
export PATH=/compiler/go/bin:/usr/bin:/bin GOENV=off GOWORK=off GOTOOLCHAIN=local
export GOMODCACHE=/compiler/cache/go-mod GOCACHE=/experiment/cache/go-build GOPATH=/experiment/cache/gopath
export GOPROXY=file:///experiment/proxy,https://proxy.golang.org GOSUMDB=sum.golang.org CGO_ENABLED=1
export GOMAXPROCS=2 GOMEMLIMIT=12GiB GOFLAGS=-p=2 GOGC=100
make -j2 cgo ONNX_PREBUILT_DIR=/experiment/prebuilt
export CGO_CFLAGS="-I$PWD/cgo -I$PWD/thirdparties/install/include"
export CGO_LDFLAGS="-L$PWD/thirdparties/install/lib"
export LD_LIBRARY_PATH="$PWD/cgo:$PWD/thirdparties/install/lib"
deps=$(go list -mod=readonly -f '{{join .CgoLDFLAGS " "}}' ./cgo)
go test -mod=readonly -ldflags="-extldflags '-fopenmp -L$PWD/cgo -lmo $deps -Wl,-rpath,$PWD/cgo -Wl,-rpath,$PWD/thirdparties/install/lib'" -count=1 -v -timeout=240s -run '^TestRebaseTransaction(Prerequisites|Cancellation)$' ./pkg/tests/sqlintegration
go test -mod=readonly -ldflags="-extldflags '-fopenmp -L$PWD/cgo -lmo $deps -Wl,-rpath,$PWD/cgo -Wl,-rpath,$PWD/thirdparties/install/lib'" -count=1 -v -timeout=240s -run '^TestRebaseTransactionRestart$' ./pkg/tests/issues/isolated
"""
    args = [str(COMPILER / "bootstrap/usr/bin/proot"), "-0", "-r", str(COMPILER / "rootfs"),
            "-b", "/proc", "-b", "/dev", "-b", "/etc/resolv.conf:/etc/resolv.conf",
            "-b", str(root) + ":/experiment", "-b", str(COMPILER) + ":/compiler",
            "-w", "/experiment/source", "/bin/bash", "-euc", shell]
    with (root / "build-test.log").open("xb") as log:
        result = subprocess.run(args, stdout=log, stderr=subprocess.STDOUT, env=env, timeout=1100)
    output = (root / "build-test.log").read_text(errors="replace")
    selection = all(marker in output for marker in (
        "--- PASS: TestRebaseTransactionPrerequisites",
        "--- PASS: TestRebaseTransactionPrerequisites/late_table_failure_rolls_back_data_edge_and_pin",
        "--- PASS: TestRebaseTransactionPrerequisites/commit_retains_upstream_base_before_replay",
        "--- PASS: TestRebaseTransactionCancellation",
        "--- PASS: TestRebaseTransactionRestart"))
    save(root / "test.json", {"returncode": result.returncode, "selected_tests_pass": selection, "base": BASE,
                              "probe_sha256": digest(root / "rebase_transaction_prerequisites_test.go")})
    if result.returncode or not selection:
        raise RuntimeError("native build or SQL prerequisite test failed; inspect retained log")


def native_fingerprint(source):
    paths = [source / "cgo/libmo.so", source / "cgo/libmo.a"]
    paths += sorted(p for p in (source / "thirdparties/install").rglob("*") if p.is_file())
    return {str(p.relative_to(source)): digest(p) for p in paths}


def native_abort_locations(root, output):
    locations = ["build-test.log"] if "double free or corruption" in output else []
    for path in sorted((root / "cases").glob("mo-rebase-crash-*/*.log")):
        if path.is_symlink() or path.parent.is_symlink():
            raise ValueError("unexpected child log link")
        if "double free or corruption" in path.read_text(errors="replace"):
            locations.append(str(path.relative_to(root)))
    return locations


def closed_probe(path):
    if path.parent != PARENT or path.resolve() != path or path.is_symlink():
        raise ValueError("unexpected closed probe")
    identity = json.loads((path / "identity.json").read_text())
    if identity["root"] != str(path):
        raise ValueError("closed probe identity mismatch")
    for unit in (identity["unit"], identity["supervisor"]):
        state = props(unit)
        if state.get("MainPID") != "0" or state.get("ControlGroup") or state.get("ActiveState") not in {"inactive", "failed"}:
            raise ValueError("probe owner still active")
    if not json.loads((path / "result.json").read_text())["released"]:
        raise ValueError("probe not released")


def namespace_prefix(root, prior):
    return ["/usr/bin/bwrap", "--die-with-parent", "--unshare-pid",
            "--ro-bind", str(COMPILER / "rootfs"), "/", "--proc", "/proc", "--dev-bind", "/dev", "/dev",
            "--bind", str(root), "/experiment", "--bind", str(root / "tmp"), "/tmp",
            "--ro-bind", str(prior / "source"), "/experiment/source", "--chdir", "/experiment/source",
            "--setenv", "GOMAXPROCS", "2", "--setenv", "GOMEMLIMIT", "12GiB",
            "--setenv", "GOGC", "100", "--setenv", "TMPDIR", "/experiment/cases",
            "--setenv", "LD_LIBRARY_PATH", "/experiment/source/cgo:/experiment/source/thirdparties/install/lib"]


def bwrap_probe(root, manifest):
    # One diagnostic variable: replace the ptrace-based launcher. Original
    # executable bytes and the Ubuntu 22.04 native loader/libraries stay fixed.
    prior = Path(manifest["source_from"])
    closed_probe(prior)
    old_manifest = json.loads((prior / "manifest.json").read_text())
    if old_manifest["base"] != BASE or digest(prior / "source.tar.gz") != old_manifest["inputs"]["source.tar.gz"]:
        raise ValueError("source archive identity mismatch")
    checked = 0
    with tarfile.open(prior / "source.tar.gz") as archive:
        validate_source_members(archive.getmembers())
        for member in archive.getmembers():
            if member.isfile():
                if digest(prior / member.name) != hashlib.sha256(archive.extractfile(member).read()).hexdigest():
                    raise ValueError("source changed: " + member.name)
                checked += 1
    before = native_fingerprint(prior / "source")
    expected = [
        ("TestStartupRestartControl", "nrp1-20261030T030300Z-b01e952a-control", "startup-control.test", 240),
        ("TestRebaseSourceDropAdmission", "nrp1-20261030T042000Z-b01e952a-lifecycle", "probe0.test", 180),
        ("TestRebaseCrashRecovery", "nrp1-20261030T050000Z-b01e952a-crash-identity", "probe0.test", 420),
    ]
    if set(manifest["binaries"]) != {test for test, _, _, _ in expected}:
        raise ValueError("unexpected runtime probe selection")
    for test, owner, filename, _ in expected:
        previous = PARENT / owner
        closed_probe(previous)
        binary = previous / filename
        if binary.is_symlink() or digest(binary) != manifest["binaries"][test]:
            raise ValueError("binary changed: " + test)
        shutil.copyfile(binary, root / test)
        (root / test).chmod(0o700)
        if digest(root / test) != manifest["binaries"][test]:
            raise ValueError("copied binary differs")
    (root / "cases").mkdir()
    (root / "tmp").mkdir()
    prefix = namespace_prefix(root, prior)
    libc = subprocess.run(prefix + ["--", "/usr/bin/getconf", "GNU_LIBC_VERSION"],
                          capture_output=True, text=True, timeout=15, check=True).stdout.strip()
    save(root / "input-identity.json", {"base": BASE, "verified_main_files": checked, "native": before,
                                      "binaries": manifest["binaries"], "runtime": "bwrap",
                                      "bwrap_sha256": digest(Path("/usr/bin/bwrap")), "glibc": libc,
                                      "rootfs": str(COMPILER / "rootfs"), "production_changes": False})
    results = {}
    code = 0
    for test, _, _, seconds in expected:
        with (root / (test + ".log")).open("xb") as log:
            run = subprocess.run(prefix + ["--", "/experiment/" + test, "-test.v",
                                           "-test.timeout=" + str(seconds) + "s", "-test.run=^" + test + "$"],
                                 stdout=log, stderr=subprocess.STDOUT, timeout=seconds + 30)
        output = (root / (test + ".log")).read_text(errors="replace")
        passed = run.returncode == 0 and "--- PASS: " + test in output
        results[test] = {"returncode": run.returncode, "pass": passed,
                         "native_abort_logs": native_abort_locations(root, output)}
        save(root / "runtime-tests.json", results)
        code = code or run.returncode or (0 if passed else 1)
        # A failed control is not a qualified environment for subsequent SQL.
        if test == "TestStartupRestartControl" and not passed:
            break
    unchanged = before == native_fingerprint(prior / "source")
    passed = len(results) == len(expected) and all(item["pass"] for item in results.values()) and unchanged
    save(root / "test.json", {"returncode": code, "selected_tests_pass": passed,
                              "native_unchanged": unchanged, "tests": results, "runtime": "bwrap"})
    if not passed:
        raise RuntimeError("native namespace probe did not qualify all selected cases")


def startup_control(root, manifest):
    prior = Path(manifest["source_from"])
    cache_owner = Path(manifest["compiler_cache_from"])
    for candidate in (prior, cache_owner):
        if candidate.parent != PARENT or candidate.resolve() != candidate or candidate == root:
            raise ValueError("unexpected closed source/cache owner")
        identity = json.loads((candidate / "identity.json").read_text())
        for unit in (identity["unit"], identity["supervisor"]):
            state = props(unit)
            if state.get("MainPID") != "0" or state.get("ControlGroup") or state.get("ActiveState") not in {"inactive", "failed"}:
                raise ValueError("prior owner still active")
        previous = json.loads((candidate / "manifest.json").read_text())
        terminal = json.loads((candidate / "result.json").read_text())
        if previous["base"] != BASE or not terminal["released"]:
            raise ValueError("prior base or release differs")
    if json.loads((cache_owner / "result.json").read_text())["status"] != "PASS_PREREQUISITE_ONLY":
        raise ValueError("cache owner not qualified")
    previous = json.loads((prior / "manifest.json").read_text())
    if digest(prior / "source.tar.gz") != previous["inputs"]["source.tar.gz"]:
        raise ValueError("source archive differs")
    # Verify original main files, not merely the old JSON claim. Test overlays
    # from the failed probe contain no init functions and are never selected.
    checked = 0
    with tarfile.open(prior / "source.tar.gz") as archive:
        validate_source_members(archive.getmembers())
        for member in archive.getmembers():
            if member.isfile():
                expected = hashlib.sha256(archive.extractfile(member).read()).hexdigest()
                if digest(prior / member.name) != expected:
                    raise ValueError("main source differs: " + member.name)
                checked += 1
    before = native_fingerprint(prior / "source")
    save(root / "input-identity.json", {"base": BASE, "verified_main_files": checked,
                                      "native": before, "source_from": str(prior),
                                      "go_binary_sha256": digest(COMPILER / "go/bin/go"),
                                      "proot_sha256": digest(COMPILER / "bootstrap/usr/bin/proot"),
                                      "historical_toolchain_binary_digest": "NOT_RECORDED"})
    (root / "cache").mkdir()
    with (root / "cache-copy.log").open("x") as log:
        subprocess.run(["cp", "-a", "--reflink=auto", str(cache_owner / "cache/go-build"),
                        str(root / "cache/go-build")], check=True, timeout=180, stdout=log, stderr=subprocess.STDOUT)
    namespace = manifest.get("mode") == "p1_bwrap_build"
    recovery = manifest.get("mode") in {"p1_recovery", "p1_crash", "p1_bwrap_build"}
    lifecycle = manifest.get("mode") in {"p1_lifecycle", "p1_recovery", "p1_crash", "p1_bwrap_build"}
    selected = [("pkg/tests/sqlintegration", "TestRebaseSourceDropAdmission"),
                ("pkg/embed", "TestRebaseRetainedBaseGC")] if lifecycle else [
                    ("pkg/tests/issues/isolated", "TestStartupRestartControl")]
    overlay_paths = ["pkg/tests/sqlintegration/rebase_source_drop_test.go",
                     "pkg/embed/rebase_retained_base_gc_test.go"] if lifecycle else [
                         "pkg/tests/issues/isolated/startup_restart_control_test.go"]
    if recovery:
        selected = [("pkg/embed", "TestRebaseRetainedBaseGC|TestRebaseCrashRecovery")]
        overlay_paths = ["pkg/embed/rebase_retained_base_gc_test.go", "pkg/embed/rebase_crash_recovery_test.go"]
        if manifest.get("mode") == "p1_crash":
            selected = [("pkg/embed", "TestRebaseCrashRecovery")]
            overlay_paths = ["pkg/embed/rebase_crash_recovery_test.go"]
    if namespace:
        selected = [("pkg/tests/sqlintegration", "TestRebaseSourceDropAdmission"),
                    ("pkg/embed", "TestRebaseCrashRecovery")]
        overlay_paths = ["pkg/tests/sqlintegration/rebase_source_drop_test.go", "pkg/embed/rebase_crash_recovery_test.go"]
    save(root / "overlay.json", {"Replace": {
        "/experiment/source/" + path: "/experiment/" + Path(path).name for path in overlay_paths}})
    shell = """unset TMPDIR
export PATH=/compiler/go/bin:/usr/bin:/bin GOENV=off GOWORK=off GOTOOLCHAIN=local
export GOMODCACHE=/compiler/cache/go-mod GOCACHE=/experiment/cache/go-build GOPATH=/experiment/cache/gopath
export GOPROXY=https://proxy.golang.org GOSUMDB=sum.golang.org CGO_ENABLED=1
export GOMAXPROCS=2 GOMEMLIMIT=12GiB GOFLAGS=-p=2 GOGC=100
export CGO_CFLAGS="-I$PWD/cgo -I$PWD/thirdparties/install/include"
export CGO_LDFLAGS="-L$PWD/thirdparties/install/lib"
export LD_LIBRARY_PATH="$PWD/cgo:$PWD/thirdparties/install/lib"
deps=$(go list -mod=readonly -f '{{join .CgoLDFLAGS " "}}' ./cgo)
"""
    if recovery:
        shell += "mkdir /experiment/cases\nexport TMPDIR=/experiment/cases\n"
    for index, (package, test) in enumerate(selected):
        shell += f"go test -c -mod=readonly -overlay=/experiment/overlay.json -o /experiment/probe{index}.test -ldflags=\"-extldflags '-fopenmp -L$PWD/cgo -lmo $deps -Wl,-rpath,$PWD/cgo -Wl,-rpath,$PWD/thirdparties/install/lib'\" ./{package}\n"
        if not namespace:
            shell += f"/experiment/probe{index}.test -test.v -test.timeout={'900' if recovery else '600' if lifecycle else '240'}s -test.run='^({test})$'\n"
    command = [str(COMPILER / "bootstrap/usr/bin/proot"), "-0", "-r", str(COMPILER / "rootfs"),
               "-b", "/proc", "-b", "/dev", "-b", "/etc/resolv.conf:/etc/resolv.conf",
               "-b", str(root) + ":/experiment", "-b", str(prior / "source") + ":/experiment/source",
               "-b", str(COMPILER) + ":/compiler", "-w", "/experiment/source", "/bin/bash", "-euc", shell]
    with (root / "build-test.log").open("xb") as log:
        result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT,
                                env=dict(os.environ, PROOT_NO_SECCOMP="1"), timeout=1100 if lifecycle else 600)
    returncode = result.returncode
    if namespace and returncode == 0:
        (root / "tmp").mkdir()
        prefix = namespace_prefix(root, prior)
        runtime = {}
        for index, (_, test) in enumerate(selected):
            with (root / "build-test.log").open("ab") as log:
                run = subprocess.run(prefix + ["--", "/experiment/probe" + str(index) + ".test", "-test.v",
                                               "-test.timeout=420s", "-test.run=^" + test + "$"],
                                     stdout=log, stderr=subprocess.STDOUT, timeout=450)
            runtime[test] = {"returncode": run.returncode, "runtime": "bwrap"}
            save(root / "runtime-tests.json", runtime)
            returncode = returncode or run.returncode
    unchanged = before == native_fingerprint(prior / "source")
    output = (root / "build-test.log").read_text(errors="replace")
    per_test = {test: "--- PASS: " + test in output for _, tests in selected for test in tests.split("|")}
    selection = all(per_test.values())
    abort_locations = native_abort_locations(root, output)
    save(root / "test.json", {"returncode": returncode, "selected_tests_pass": selection,
                              "native_unchanged": unchanged, "native_heap_abort": bool(abort_locations),
                              "native_heap_abort_logs": abort_locations,
                              "base": BASE, "tests": per_test,
                              "binary_sha256": {str(i): digest(root / f"probe{i}.test")
                                                for i in range(len(selected)) if (root / f"probe{i}.test").exists()}})
    if returncode or not selection or not unchanged:
        raise RuntimeError("incremental probe did not pass; retain original failure")


def supervise(root):
    manifest = verify(root)
    identity = owned(root)
    if (root / "dispatch.json").exists():
        raise ValueError("already dispatched")
    unit = identity["unit"]
    save(root / "dispatch.json", {"unit": unit, "epoch": time.time()})
    error = None
    try:
        call(["systemd-run", "--user", "--quiet", "--unit", unit,
              "-p", "CPUQuota=400%", "-p", "MemoryMax=16G", "-p", "MemorySwapMax=0",
              "-p", "RuntimeMaxSec=1200", "-p", "TimeoutStopSec=30", "-p", "KillMode=control-group",
              "/usr/bin/python3", str(root / "native_rebase_probe_linux.py"), "child", str(root)])
        started = time.monotonic()
        pressure = 0
        with (root / "resources.jsonl").open("x") as log:
            while True:
                state = props(unit)
                mem = available()
                free = shutil.disk_usage(root).free
                log.write(json.dumps({"epoch": time.time(), "available": mem, "disk_free": free, **state}) + "\n")
                log.flush()
                if state.get("ActiveState") in {"inactive", "failed"}:
                    break
                pressure = pressure + 1 if mem < 4 * GIB else 0
                if pressure >= 3 or free < 12 * GIB or time.monotonic() - started > 1250:
                    raise RuntimeError("resource or deadline gate")
                time.sleep(5)
        result = json.loads((root / "test.json").read_text())
        if result["returncode"] or not result["selected_tests_pass"] or state.get("ExecMainStatus") != "0" or state.get("Result") != "success":
            raise RuntimeError("worker did not pass")
    except BaseException as exc:
        error = type(exc).__name__ + ": " + str(exc)
    finally:
        call(["systemctl", "--user", "stop", unit], False)
        terminal = props(unit)
        quiescent = terminal.get("MainPID") == "0" and terminal.get("ActiveState") in {"inactive", "failed"}
        if quiescent:
            owned(root)
            (LOCK / "owner.json").unlink()
            LOCK.rmdir()
        success = {"startup_control": "PASS_STARTUP_CONTROL_ONLY", "p1_lifecycle": "PASS_LIFECYCLE_PROBES_ONLY", "p1_recovery": "PASS_RECOVERY_PROBES_ONLY", "p1_crash": "PASS_CRASH_PROBE_ONLY", "p1_bwrap": "PASS_BWRAP_PROBES_ONLY", "p1_bwrap_build": "PASS_CORRECTED_P1_PROBES_ONLY"}.get(manifest.get("mode"), "PASS_PREREQUISITE_ONLY")
        save(root / "result.json", {"status": "FAIL" if error else success,
                                    "error": error, "terminal": terminal, "released": quiescent})
    if error:
        raise RuntimeError("P1 probe failed; retained evidence")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("reserve", "supervise", "child"))
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    {"reserve": reserve, "supervise": supervise, "child": child}[args.action](args.root)
