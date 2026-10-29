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
                        "rebase_transaction_cancellation_test.go"}:
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
    if available() < 22 * GIB or shutil.disk_usage(root).free < 40 * GIB:
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


def supervise(root):
    verify(root)
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
        save(root / "result.json", {"status": "FAIL" if error else "PASS_PREREQUISITE_ONLY",
                                    "error": error, "terminal": terminal, "released": quiescent})
    if error:
        raise RuntimeError("P1 probe failed; retained evidence")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("reserve", "supervise", "child"))
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    {"reserve": reserve, "supervise": supervise, "child": child}[args.action](args.root)
