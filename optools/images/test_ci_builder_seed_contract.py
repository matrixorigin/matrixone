"""Producer contract tests: python3 -m unittest discover -s optools/images -p test_ci_builder_seed_contract.py -v.

Execute the Dockerfile shell with tiny owned fixtures and mocked Go/toolchain
commands; no image build, downloads, native compilation, or performance claims.
The consumer's payload validation and measured Linux canary live in CI.
"""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


HERE = Path(__file__).resolve().parent
CHECKOUT = "/home/runner/_work/matrixone/matrixone"
MODULES = "/home/runner/go/pkg/mod"
ORIGINAL = "/go/src/github.com/matrixorigin/matrixone"
RACE = "readonly-short-matrixone_test-vetoff-race-v1"
COVERAGE = "short-matrixone_test-vetoff-covermode-set-filter-driver-aoe-memEngine-catalog-v1"


def instructions():
    return [line.strip() for line in
            (HERE / "Dockerfile.ci-builder").read_text().replace("\\\n", "").splitlines()
            if line.strip() and not line.lstrip().startswith("#")]


class SeedContractTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="seed-contract-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.env = dict(os.environ, PATH=str(self.bin) + os.pathsep + os.environ["PATH"])
        self.lines = instructions()

    def tool(self, name, body):
        path = self.bin / name
        path.write_text("#!/bin/sh\nset -eu\n" + body + "\n")
        path.chmod(0o755)

    def run_shell(self, command, cwd=None, check=True):
        return subprocess.run(["sh", "-eu", "-c", command], cwd=cwd or self.root,
                              env=self.env, text=True, capture_output=True,
                              timeout=10, check=check)

    def run_instruction(self, fragment):
        matches = [line[4:] for line in self.lines
                   if line.startswith("RUN ") and fragment in line]
        self.assertEqual(len(matches), 1)
        return matches[0]

    def test_stage_order_and_final_export_layout(self):
        warm, final = [], []
        target = warm
        for line in self.lines:
            if line.startswith("FROM ") and " AS warm" not in line:
                target = final
            target.append(line)
        relocation = next(i for i, line in enumerate(warm) if "mv " + ORIGINAL in line)
        self.assertLess(warm.index("RUN make build GOBUILD_OPT=-cover"), relocation)
        self.assertLess(warm.index("RUN make build"), relocation)
        self.assertIn("WORKDIR " + ORIGINAL, warm[:relocation])
        self.assertIn("WORKDIR " + CHECKOUT, warm[relocation:])
        self.assertIn("ENV GOMODCACHE=" + MODULES, warm[relocation:])
        for fragment in ("go test", "install_go_ut_analysis", "go_env=$(go env"):
            self.assertTrue(all(i > relocation for i, line in enumerate(warm)
                                if fragment in line))
        self.assertFalse(any(line.startswith(("WORKDIR ", "ENV GOMODCACHE=")) for line in final))
        for source, destination in (
                (MODULES, "/go/pkg/mod"),
                ("/root/.cache/go-build", "/root/.cache/go-build"),
                (CHECKOUT + "/thirdparties/install", "/mo-prebuilt/thirdparties/install"),
                ("/mo-prebuilt/go-cache-manifest.json", "/mo-prebuilt/go-cache-manifest.json")):
            self.assertIn(f"COPY --from=warm {source} {destination}", final)
        self.assertFalse(any("-trimpath" in line for line in self.lines))

    def test_warm_flavors_and_manifest_execute_with_real_shell(self):
        metadata = self.root / "metadata"
        metadata.mkdir()
        self.env.update(META=str(metadata), GOMODCACHE=MODULES)
        expected_env = dict(GOMODCACHE=MODULES, GOVERSION="go1.26.4", GOOS="linux",
                            GOARCH="amd64", GOAMD64="v1", GOEXPERIMENT="")
        self.env["GO_ENV_JSON"] = json.dumps(expected_env)
        self.tool("go", r'''
printf '%s\n' "$*" >> "$META/commands"
case "$1" in
  env) printf '%s\n' "$GO_ENV_JSON"; exit "${ENV_EXIT:-0}" ;;
  list) printf '%s\n' example/pkg example/driver example/engine/aoe example/engine/memEngine example/pkg/catalog ;;
  test) case " $* " in *" -race "*) exit "$RACE_EXIT" ;; *) exit "$COVERAGE_EXIT" ;; esac ;;
  *) exit 99 ;;
esac''')
        race = self.run_instruction("echo \"warm-race=ok\"")
        coverage = self.run_instruction("echo \"warm-coverage=ok\"")
        manifest = self.run_instruction("go_env=$(go env")
        def local(command):
            return command.replace("/mo-prebuilt", str(metadata))
        for race_exit, coverage_exit in ((0, 0), (1, 0), (0, 1), (1, 1)):
            with self.subTest(race=race_exit, coverage=coverage_exit):
                (metadata / "warm-status").write_text("")
                (metadata / "commands").write_text("")
                self.env.update(RACE_EXIT=str(race_exit), COVERAGE_EXIT=str(coverage_exit))
                self.run_shell(local(race))
                self.run_shell(local(coverage))
                self.run_shell(local(manifest))
                result = json.loads((metadata / "go-cache-manifest.json").read_text())
                self.assertEqual(result, dict(schema=2, profile="host-ut-v1", checkout=CHECKOUT,
                    go_env=expected_env, flavors=dict(race="FAILED" if race_exit else "ok",
                    coverage="FAILED" if coverage_exit else "ok"),
                    race_contract=RACE, coverage_contract=COVERAGE))
                commands = (metadata / "commands").read_text().splitlines()
                self.assertEqual(commands, [
                    "test -mod=readonly -short -race -tags matrixone_test -vet=off -exec /bin/true ./...",
                    "list -mod=readonly ./...",
                    "test -mod=readonly -short -tags matrixone_test -vet=off -exec /bin/true -covermode=set -coverpkg=example/pkg example/pkg",
                    "env -json GOMODCACHE GOVERSION GOOS GOARCH GOAMD64 GOEXPERIMENT"])
        for ledger in ("", "warm-race=unknown\nwarm-coverage=ok\n",
                       "warm-race=ok\nwarm-race=ok\nwarm-coverage=ok\n"):
            with self.subTest(invalid_ledger=ledger):
                (metadata / "warm-status").write_text(ledger)
                (metadata / "go-cache-manifest.json").unlink(missing_ok=True)
                self.assertNotEqual(self.run_shell(local(manifest), check=False).returncode, 0)
                self.assertFalse((metadata / "go-cache-manifest.json").exists())
        (metadata / "warm-status").write_text("warm-race=ok\nwarm-coverage=ok\n")
        self.env["ENV_EXIT"] = "1"
        self.assertNotEqual(self.run_shell(local(manifest), check=False).returncode, 0)
        self.assertFalse((metadata / "go-cache-manifest.json").exists())

    def test_relocation_preserves_native_fingerprint_and_moves_modules(self):
        source = self.root / ORIGINAL.lstrip("/")
        for name, contents in (("Makefile", "native-build-contract\n"),
                               ("cgo/mo.h", "native header\n"),
                               ("thirdparties/Makefile", "native inputs\n"),
                               ("thirdparties/install/lib/mock.a", "built output\n")):
            path = source / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(contents)
        modules = self.root / "go/pkg/mod"
        modules.mkdir(parents=True)
        (modules / "module.txt").write_text("module bytes")
        self.tool("go", "printf 'go version go1.26.4 linux/amd64\\n'")
        self.tool("cmake", "printf 'cmake version fixture\\n'")
        for compiler in ("cc", "c++"):
            self.tool(compiler, "printf 'fixture-compiler\\n'")
        # Use real file hashing; only compiler/toolchain discovery is mocked.
        self.assertIsNotNone(shutil.which("sha256sum"))
        self.env["CI_BUILDER_TOOL_PATH"] = self.env["PATH"]
        def fingerprint(root):
            self.env["CI_BUILDER_SOURCE_ROOT"] = str(root)
            return subprocess.run(["sh", str(HERE / "ci-builder-fingerprint.sh")],
                                  env=self.env, text=True, capture_output=True,
                                  timeout=10, check=True).stdout
        before = fingerprint(source)
        move = self.run_instruction("mv " + ORIGINAL)
        # Rebase absolute fixture paths, leaving the actual mkdir/mv commands intact.
        move = move.replace("/home/runner", str(self.root / "home/runner"))
        move = move.replace("/go/src", str(self.root / "go/src"))
        move = move.replace("mv /go/pkg/mod", "mv " + str(modules))
        self.run_shell(move)
        relocated = self.root / CHECKOUT.lstrip("/")
        self.assertFalse(source.exists())
        self.assertFalse(modules.exists())
        self.assertEqual((self.root / MODULES.lstrip("/") / "module.txt").read_text(), "module bytes")
        self.assertEqual((relocated / "thirdparties/install/lib/mock.a").read_text(), "built output\n")
        self.assertEqual(before, fingerprint(relocated))
        (relocated / "cgo/mo.h").write_text("changed native input\n")
        self.assertNotEqual(before, fingerprint(relocated))


if __name__ == "__main__":
    unittest.main()
