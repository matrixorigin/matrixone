#!/usr/bin/env python3
# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Contract tests for the Pixi-only GPU Make and shell consumers."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


REPO = Path(__file__).resolve().parent.parent
GPU_ENV = REPO / "cgo/mo-gpu-env"


class GPUContractTest(unittest.TestCase):
    host_arch = "x86_64"
    target_arch = "x86_64"
    cuda_target = "x86_64"

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="mo-gpu-contract-")
        self.addCleanup(temporary.cleanup)
        self.project = Path(temporary.name) / "project"
        self.prefix = self.project / ".pixi/envs/mo"
        self.project.mkdir()
        (self.project / "pixi.lock").write_text("test lock\n")
        (self.prefix / "conda-meta").mkdir(parents=True)
        paths = (
            f"bin/{self.target_arch}-conda-linux-gnu-cc",
            f"bin/{self.target_arch}-conda-linux-gnu-c++",
            "bin/nvcc",
            f"targets/{self.cuda_target}-linux/include/cuda.h",
            f"targets/{self.cuda_target}-linux/lib/libcudart.so",
            f"targets/{self.cuda_target}-linux/lib/stubs/libcuda.so",
            "include/cuvs/core/c_api.h",
            "lib/libcuvs.so",
            "lib/libcuvs_c.so",
            "lib/librmm.so",
        )
        for name in paths:
            path = self.prefix / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(name)
            if name.startswith("bin/"):
                path.chmod(0o755)
        self.env = dict(os.environ)
        for name in (
            "PIXI_PROJECT_ROOT", "PIXI_ENVIRONMENT_NAME", "CONDA_PREFIX",
            "CC", "CXX", "MAKEFLAGS", "MFLAGS", "MAKELEVEL",
        ):
            self.env.pop(name, None)
        self.env.update(
            PIXI_PROJECT_ROOT=str(self.project),
            PIXI_ENVIRONMENT_NAME="mo",
            CONDA_PREFIX=str(self.prefix),
            MO_CL_CUDA="1",
        )
        shim = self.project / "host-bin"
        shim.mkdir()
        uname = shim / "uname"
        uname.write_text(
            "#!/bin/sh\n"
            "case \"$1\" in\n"
            f"  -m) echo {self.host_arch} ;;\n"
            "  -s) echo Linux ;;\n"
            "  *) exit 1 ;;\n"
            "esac\n"
        )
        uname.chmod(0o755)
        self.env["PATH"] = str(shim) + os.pathsep + self.env.get("PATH", "")

    def make(self, directory, *args, env=None):
        return subprocess.run(
            ["make", "--no-print-directory", "-n", *args],
            cwd=REPO / directory,
            env=self.env if env is None else env,
            text=True,
            capture_output=True,
            timeout=30,
        )

    def shell(self, env=None):
        return subprocess.run(
            ["sh", "-c", '. "$1" || exit 1; printf "%s|%s|%s\\n" "$MO_GPU_NVCC" "$MO_GPU_CFLAGS" "$MO_GPU_RUNTIME_PATH"',
             "sh", str(GPU_ENV)],
            env=self.env if env is None else env,
            text=True,
            capture_output=True,
            timeout=30,
        )

    def test_all_gpu_make_consumers_use_one_pixi_prefix(self):
        for directory, target in (
            ("cgo", "mo.o"), ("cgo/cuda", "cuda.o"),
            ("cgo/cuvs", "helper.o"), ("cgo/test", "test_add.exe"),
        ):
            with self.subTest(directory=directory):
                result = self.make(directory, "-B", target)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(str(self.prefix / "bin/nvcc"), result.stdout)
                self.assertIn(str(self.prefix / f"bin/{self.target_arch}-conda-linux-gnu-c++"), result.stdout)
                self.assertNotIn("/usr/local/cuda", result.stdout)
                if directory == "cgo/cuvs":
                    isa = "armv8-a" if self.target_arch == "aarch64" else "haswell"
                    self.assertIn(f"-march={isa}", result.stdout)

    def test_shell_consumer_uses_link_only_stubs(self):
        result = self.shell()
        self.assertEqual(result.returncode, 0, result.stderr)
        nvcc, flags, runtime = result.stdout.strip().split("|")
        # The shell helper resolves the prefix physically; macOS maps /var to
        # /private/var, unlike the lexical TemporaryDirectory path.
        canonical = self.prefix.resolve()
        self.assertEqual(nvcc, str(canonical / "bin/nvcc"))
        self.assertIn(str(canonical / f"targets/{self.cuda_target}-linux/include"), flags)
        self.assertNotIn("stubs", runtime)
        self.assertEqual(
            runtime,
            f"{canonical}/targets/{self.cuda_target}-linux/lib:{canonical}/lib",
        )

    def test_shell_consumer_canonicalizes_project_symlink(self):
        alias = self.project.parent / "project-alias"
        alias.symlink_to(self.project, target_is_directory=True)
        env = dict(
            self.env,
            PIXI_PROJECT_ROOT=str(alias),
            CONDA_PREFIX=str(alias / ".pixi/envs/mo"),
        )
        result = self.shell(env)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.split("|", 1)[0], str(self.prefix.resolve() / "bin/nvcc"))

    def test_missing_or_mismatched_activation_fails_without_legacy_fallback(self):
        cases = (
            dict(PIXI_PROJECT_ROOT="", PIXI_ENVIRONMENT_NAME="", CONDA_PREFIX="/opt/legacy"),
            dict(CONDA_PREFIX="/opt/legacy"),
            dict(PIXI_ENVIRONMENT_NAME="wrong"),
        )
        for changes in cases:
            with self.subTest(changes=changes):
                env = dict(self.env, **changes)
                make = self.make("cgo", "mo.o", env=env)
                self.assertNotEqual(make.returncode, 0)
                self.assertNotIn("/usr/local/cuda", make.stdout)
                shell = self.shell(env)
                self.assertNotEqual(shell.returncode, 0)

    def test_missing_input_fails_closed(self):
        missing = self.prefix / "lib/libcuvs_c.so"
        missing.unlink()
        make = self.make("cgo/cuvs", "helper.o")
        self.assertNotEqual(make.returncode, 0)
        self.assertIn("incomplete Pixi", make.stderr)
        shell = self.shell()
        self.assertNotEqual(shell.returncode, 0)
        self.assertIn("incomplete Pixi", shell.stderr)

    def test_other_architecture_compiler_cannot_satisfy_native_profile(self):
        compiler = self.prefix / f"bin/{self.target_arch}-conda-linux-gnu-c++"
        other = "x86_64" if self.target_arch == "aarch64" else "aarch64"
        compiler.rename(self.prefix / f"bin/{other}-conda-linux-gnu-c++")
        self.assertNotEqual(self.make("cgo/cuvs", "helper.o").returncode, 0)
        self.assertNotEqual(self.shell().returncode, 0)

    def test_unsupported_host_fails_before_compilation(self):
        uname = self.project / "host-bin/uname"
        uname.write_text('#!/bin/sh\ncase "$1" in -m) echo ppc64le;; -s) echo Linux;; esac\n')
        for result in (self.make("cgo/cuvs", "helper.o"), self.shell()):
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("require Linux amd64 or arm64", result.stderr)

    def test_cpu_and_clean_do_not_require_pixi(self):
        env = dict(self.env, MO_CL_CUDA="0")
        for name in ("PIXI_PROJECT_ROOT", "PIXI_ENVIRONMENT_NAME", "CONDA_PREFIX"):
            env.pop(name)
        cpu = self.make("cgo", "-B", "mo.o", env=env)
        self.assertEqual(cpu.returncode, 0, cpu.stderr)
        self.assertNotIn("nvcc", cpu.stdout)
        env["MO_CL_CUDA"] = "1"
        for directory in ("cgo", "cgo/cuvs", "cgo/cuda", "cgo/test"):
            with self.subTest(directory=directory):
                clean = self.make(directory, "clean", env=env)
                self.assertEqual(clean.returncode, 0, clean.stderr)


class ARMGPUContractTest(GPUContractTest):
    host_arch = "aarch64"
    target_arch = "aarch64"
    cuda_target = "sbsa"


if __name__ == "__main__":
    unittest.main()
