# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import copy
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import mo_gpu_toolchain as toolchain


REPO = Path(__file__).resolve().parent.parent


class GPUContractTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="mo-gpu-contract-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.prefix = self.root / "pixi"
        self.lockfile = self.root / "pixi.lock"
        self.lockfile.write_text("test lock\n")
        paths = ("bin/cc", "bin/cxx", "bin/nvcc", "targets/x86_64-linux/include/cuda.h",
                 "targets/x86_64-linux/lib/libcudart.so", "targets/x86_64-linux/lib/stubs/libcuda.so",
                 "include/cuvs/core/c_api.h", "include/rmm/cuda_stream_view.hpp",
                 "lib/libcuvs.so", "lib/libcuvs_c.so", "lib/librmm.so")
        self.artifacts = {}
        for name in paths:
            path = self.prefix / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("#!/bin/sh\nexit 0\n" if name.startswith("bin/") else name)
            if name.startswith("bin/"):
                path.chmod(0o755)
            self.artifacts[str(path)] = toolchain.digest(path)
        cuda = self.prefix / "targets/x86_64-linux"
        self.data = {
            "schema_version": 1, "provider": "pixi", "platform": "linux-64", "prefix": str(self.prefix),
            "compilers": {name: {"path": str(self.prefix / "bin" / name), "version": "test"}
                          for name in ("cc", "cxx", "nvcc")},
            "cuda": {"version": "13.3", "include_dirs": [str(cuda / "include")],
                     "library_dirs": [str(cuda / "lib")], "stub_library_dirs": [str(cuda / "lib/stubs")]},
            "rapids": {"include_dirs": [str(self.prefix / "include")], "library_dirs": [str(self.prefix / "lib")],
                       "versions": {"libcuvs": "26.08.01", "libcuvs_c": "26.08.01", "librmm": "26.08.00"}},
            "pixi": {"environment": "default", "lockfile": str(self.lockfile),
                     "lockfile_sha256": toolchain.digest(self.lockfile)},
            "packages": [{"name": name, "version": version, "build": "0", "url": "https://example.com/" + name,
                          "sha256": "a" * 64} for name, version in
                         (("cuda-version", "13.3"), ("libcuvs", "26.08.01"), ("librmm", "26.08.00"))],
            "runtime_roots": [str(self.prefix / "lib" / name) for name in ("libcuvs.so", "libcuvs_c.so")],
            "artifact_sha256": self.artifacts,
        }
        self.manifest = self.root / "toolchain.json"
        self.write_manifest()
        self.env = dict(os.environ, GPU_TOOLCHAIN_MANIFEST=str(self.manifest), MO_CL_CUDA="1")
        for name in ("CONDA_PREFIX", "CUDA_PATH", "CC", "CXX", "MAKEFLAGS", "MFLAGS", "MAKELEVEL"):
            self.env.pop(name, None)

        # The GPU fixtures describe the shipped Linux/x86_64 build contract,
        # independently of the host running these Python tests. Keep ordinary
        # CPU Make checks on the real host below.
        shim = self.root / "host-bin"
        shim.mkdir()
        uname = shim / "uname"
        uname.write_text(
            "#!/bin/sh\n"
            "case \"$1\" in\n"
            "  -m) echo x86_64 ;;\n"
            "  -s) echo Linux ;;\n"
            "  *) echo 'unsupported fixture uname arguments' >&2; exit 1 ;;\n"
            "esac\n"
        )
        uname.chmod(0o755)
        self.gpu_env = dict(self.env, PATH=str(shim) + os.pathsep + self.env.get("PATH", ""))

    def write_manifest(self, data=None):
        self.manifest.write_text(json.dumps(self.data if data is None else data, sort_keys=True))

    def make(self, directory, *args, env=None):
        return subprocess.run(["make", "--no-print-directory", "-n", *args], cwd=REPO / directory,
                              env=self.env if env is None else env, text=True, capture_output=True, timeout=30)

    def make_gpu(self, directory, *args, env=None):
        selected = self.gpu_env if env is None else dict(
            env, PATH=self.gpu_env["PATH"]
        )
        return self.make(directory, *args, env=selected)

    def test_resolves_disjoint_target_and_compiler_roots_without_conda(self):
        values = toolchain.resolve(self.env)
        self.assertEqual(values["MO_GPU_NVCC"], str(self.prefix / "bin/nvcc"))
        self.assertIn(str(self.prefix / "targets/x86_64-linux/include"), values["MO_GPU_CFLAGS"])
        self.assertIn("stubs", values["MO_GPU_LDFLAGS"])
        self.assertNotIn("stubs", values["MO_GPU_RUNTIME_PATH"])
        self.assertNotIn("/usr/local/cuda", " ".join(values.values()))

    def test_all_make_consumers_select_manifest_nvcc_and_host_compiler(self):
        for directory, target in (("cgo", "mo.o"), ("cgo/cuda", "cuda.o"),
                                  ("cgo/cuvs", "helper.o"), ("cgo/test", "test_add.exe")):
            with self.subTest(directory=directory):
                result = self.make_gpu(directory, "-B", target)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(str(self.prefix / "bin/nvcc") + " -ccbin " + str(self.prefix / "bin/cxx"), result.stdout)
                self.assertNotIn("/usr/local/cuda", result.stdout)

    def test_legacy_provider_keeps_system_and_conda_paths(self):
        env = {"CONDA_PREFIX": "/opt/conda/gpu"}
        values = toolchain.resolve(env)
        self.assertEqual(values["MO_GPU_NVCC"], "/usr/local/cuda/bin/nvcc")
        self.assertIn("-L/opt/conda/gpu/lib", values["MO_GPU_LDFLAGS"])
        env["CUDA_PATH"] = "/opt/cuda"
        self.assertEqual(toolchain.resolve(env)["MO_GPU_NVCC"], "/opt/cuda/bin/nvcc")

    def test_explicit_invalid_manifest_never_falls_back(self):
        env = dict(self.env, GPU_TOOLCHAIN_MANIFEST="/missing/toolchain.json", CONDA_PREFIX="/valid/legacy")
        with self.assertRaises(toolchain.ToolchainError):
            toolchain.resolve(env)
        result = self.make_gpu("cgo", "mo.o", env=env)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("GPU toolchain", result.stderr)

    def test_manifest_remains_authoritative_over_ambient_compiler_selectors(self):
        result = self.make_gpu("cgo/cuvs", "-B", "helper.o", "NVCC=/wrong/nvcc", "HOST_COMPILER=/wrong/cxx")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("/wrong", result.stdout)
        self.assertIn(str(self.prefix / "bin/nvcc"), result.stdout)

    def test_make_command_line_manifest_is_rejected_without_fallback(self):
        env = dict(self.gpu_env, CONDA_PREFIX="/opt/legacy/gpu")
        env.pop("GPU_TOOLCHAIN_MANIFEST")
        result = self.make_gpu("cgo", "-B", "mo.o", f"GPU_TOOLCHAIN_MANIFEST={self.manifest}", env=env)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must be set in the environment", result.stderr)
        self.assertNotIn("/usr/local/cuda", result.stdout)

        missing = self.root / "missing-toolchain.json"
        result = self.make_gpu("cgo", "-B", "mo.o", f"GPU_TOOLCHAIN_MANIFEST={missing}", env=env)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must be set in the environment", result.stderr)

    def test_lock_and_artifact_drift_fail_closed(self):
        old = toolchain.resolve(self.env)["MO_GPU_TOOLCHAIN_FINGERPRINT"]
        self.data["pixi"]["environment"] = "renamed"
        self.write_manifest()
        self.assertNotEqual(old, toolchain.resolve(self.env)["MO_GPU_TOOLCHAIN_FINGERPRINT"])
        self.lockfile.write_text("changed lock\n")
        with self.assertRaisesRegex(toolchain.ToolchainError, "lockfile changed"):
            toolchain.resolve(self.env)
        self.lockfile.write_text("test lock\n")
        (self.prefix / "lib/libcuvs_c.so").write_text("changed artifact")
        with self.assertRaisesRegex(toolchain.ToolchainError, "artifact changed"):
            toolchain.resolve(self.env)

    def test_rejects_version_conflicts_unhashed_inputs_and_driver_runtime(self):
        mutations = (
            lambda data: data["cuda"].update(version="13.2"),
            lambda data: data["rapids"]["versions"].update(libcuvs="26.08.02"),
            lambda data: data["artifact_sha256"].pop(str(self.prefix / "bin/nvcc")),
            lambda data: data["runtime_roots"].append(str(self.prefix / "targets/x86_64-linux/lib/stubs/libcuda.so")),
            lambda data: data["compilers"]["cc"].update(path="/tmp/$(touch-pwned)"),
            lambda data: data["packages"].append(data["packages"][0]),
        )
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                data = copy.deepcopy(self.data)
                mutate(data)
                self.write_manifest(data)
                with self.assertRaises(toolchain.ToolchainError):
                    toolchain.resolve(self.env)

    def test_rejects_runtime_root_symlink_to_driver_stub(self):
        alias = self.prefix / "lib/runtime-alias.so"
        alias.symlink_to(self.prefix / "targets/x86_64-linux/lib/stubs/libcuda.so")
        self.data["runtime_roots"].append(str(alias))
        self.data["artifact_sha256"][str(alias)] = toolchain.digest(alias)
        self.write_manifest()
        with self.assertRaisesRegex(toolchain.ToolchainError, "driver libraries/stubs"):
            toolchain.resolve(self.env)

    def test_rejects_stub_aliases_in_every_runtime_library_directory(self):
        stub = str(self.prefix / "targets/x86_64-linux/lib/stubs")
        alias = self.prefix / "lib/runtime_alias"
        alias.symlink_to(stub, target_is_directory=True)
        for section in ("cuda", "rapids"):
            for path in (stub, str(alias)):
                with self.subTest(section=section, path=path):
                    data = copy.deepcopy(self.data)
                    data[section]["library_dirs"].append(path)
                    self.write_manifest(data)
                    with self.assertRaisesRegex(toolchain.ToolchainError, "runtime directories"):
                        toolchain.resolve(self.env)

        link_only = self.prefix / "link_only"
        link_only.mkdir()
        (link_only / "libcuda.so").write_text("injected link-only driver")
        unmarked_alias = self.prefix / "lib/link_only_alias"
        unmarked_alias.symlink_to(link_only, target_is_directory=True)
        data = copy.deepcopy(self.data)
        data["cuda"]["stub_library_dirs"] = [str(link_only)]
        data["rapids"]["library_dirs"].append(str(unmarked_alias))
        self.write_manifest(data)
        with self.assertRaisesRegex(toolchain.ToolchainError, "runtime directories"):
            toolchain.resolve(self.env)

        driver = self.prefix / "lib/libcuda.so"
        driver.write_text("injected driver stub")
        self.write_manifest()
        with self.assertRaisesRegex(toolchain.ToolchainError, "runtime directories"):
            toolchain.resolve(self.env)

    def test_cpu_and_clean_ignore_invalid_manifest_without_python(self):
        env = dict(self.env, GPU_TOOLCHAIN_MANIFEST="/missing/toolchain.json", MO_CL_CUDA="0")
        result = self.make("cgo", "-B", "mo.o", env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("nvcc", result.stdout)
        self.assertNotIn("python", result.stdout)
        env["MO_CL_CUDA"] = "1"
        for directory in ("cgo", "cgo/cuvs", "cgo/cuda", "cgo/test"):
            with self.subTest(directory=directory):
                result = self.make_gpu(directory, "clean", env=env)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertNotIn("GPU toolchain", result.stderr)


if __name__ == "__main__":
    unittest.main()
