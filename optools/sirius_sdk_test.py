# Copyright 2026 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os
from pathlib import Path
import shlex
import stat
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import sirius_sdk


class SDKTest(unittest.TestCase):
    def fixture(self, root):
        artifact = root / "device-link.o"
        artifact.write_bytes(b"native object")
        consumer = root / "sirius_c_smoke"
        consumer.write_bytes(b"consumer")
        for name in ("cc", "c++"):
            compiler = root / name
            compiler.write_bytes(b"compiler fixture")
            compiler.chmod(0o755)
        flags = [str(artifact), "-Wl,--start-group", "-lfrom-sdk", "-Wl,--end-group"]
        manifest = {
            "schema_version": 1,
            "abi_version": 1,
            "compiler": str(root / "c++"),
            "c_compiler": str(root / "cc"),
            "build_directory": str(root),
            "source_directory": str(root),
            "source_revision": "a" * 40,
            "source_dirty": False,
            "consumer": str(consumer),
            "link_arguments": flags,
            "artifact_sha256": {
                str(p): sirius_sdk.digest(p) for p in (artifact, consumer)
            },
        }
        (root / "link.json").write_text(json.dumps(manifest))
        (root / "link.rsp").write_text(sirius_sdk.response(flags))
        (root / "sirius_c.h").write_text("#define SIRIUS_ABI_VERSION 1u\n")
        return manifest

    def test_release_requires_merged_clean_revision_and_exact_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            with patch.object(
                sirius_sdk, "run", side_effect=["a" * 40, "", ""]
            ) as command:
                sirius_sdk.validate(root, "release", "origin/integration")
                self.assertEqual(
                    command.call_args.args[-4:],
                    ("merge-base", "--is-ancestor", "a" * 40, "origin/integration"),
                )
            with patch.object(sirius_sdk, "run", side_effect=["a" * 40, ""]):
                with self.assertRaisesRegex(ValueError, "SIRIUS_MERGED_REF"):
                    sirius_sdk.validate(root, "release", "")
            with patch.object(
                sirius_sdk, "run", side_effect=["a" * 40, " M native.cc"]
            ):
                with self.assertRaisesRegex(ValueError, "clean SDK"):
                    sirius_sdk.validate(root, "release", "origin/integration")
            (root / "device-link.o").write_bytes(b"replaced object")
            with patch.object(sirius_sdk, "run", side_effect=["a" * 40, ""]):
                with self.assertRaisesRegex(ValueError, "artifact changed"):
                    sirius_sdk.validate(root, "development", "")

    def test_sdk_must_match_matrixone_submodule_pin(self):
        with tempfile.TemporaryDirectory() as directory:
            mo_root = Path(directory)
            source = mo_root / "third_party" / "sirius"
            source.mkdir(parents=True)
            self.fixture(source)
            with patch.object(
                sirius_sdk, "run", side_effect=["a" * 40, "a" * 40, ""]
            ) as command:
                sirius_sdk.validate(source, "development", "", mo_root)
                self.assertEqual(
                    command.call_args_list[0].args,
                    ("git", "-C", str(mo_root), "rev-parse", "HEAD:third_party/sirius"),
                )
            with patch.object(sirius_sdk, "run", return_value="b" * 40):
                with self.assertRaisesRegex(ValueError, "submodule pin"):
                    sirius_sdk.validate(source, "development", "", mo_root)
            manifest = json.loads((source / "link.json").read_text())
            manifest["source_directory"] = str(mo_root / "other-sirius")
            (source / "link.json").write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, "MatrixOne submodule"):
                sirius_sdk.validate(source, "development", "", mo_root)

    def test_manifest_response_mismatch_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            (root / "link.rsp").write_text("-lother")
            with self.assertRaisesRegex(ValueError, "does not match"):
                sirius_sdk.validate(root, "development", "")

    def test_sdk_requires_the_verified_absolute_c_compiler(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for value in (None, "cc", str(root / "missing"), str(root / "link.rsp")):
                with self.subTest(compiler=value):
                    manifest = self.fixture(root)
                    if value is None:
                        del manifest["c_compiler"]
                    else:
                        manifest["c_compiler"] = value
                    (root / "link.json").write_text(json.dumps(manifest))
                    with self.assertRaisesRegex(
                        ValueError, "absolute executable c_compiler"
                    ):
                        sirius_sdk.validate(root, "development", "")

    def test_complete_closure_keeps_device_link_and_relocates_rpath(self):
        flags = [
            "/sdk/device-link.o",
            "-Wl,--start-group",
            "/sdk/lib.a",
            "-Wl,--end-group",
            "-Wl,-rpath,/build/tree",
            "-L/sdk/lib",
            "-lgpu",
        ]
        result = sirius_sdk.relocatable_flags(flags)
        self.assertEqual(result, flags[:4] + flags[5:] + ["-Wl,-rpath,$ORIGIN/lib"])
        self.assertEqual(shlex.split(sirius_sdk.response(result)), result)

    def test_runtime_closure_excludes_driver_not_cuda_runtime(self):
        libraries = sirius_sdk.runtime_libraries(
            """
            libcuda.so.1 => /host/libcuda.so.1 (0x1)
            libc.so.6 => /host/libc.so.6 (0x1)
            libmvec.so.1 => /host/libmvec.so.1 (0x1)
            libcudart.so.13 => /sdk/libcudart.so.13 (0x1)
            libcustom.so => /sdk/libcustom.so (0x1)
            /lib64/ld-linux-x86-64.so.2 => /lib/x86_64-linux-gnu/ld-linux-x86-64.so.2 (0x1)
            /lib64/ld-linux-x86-64.so.2 (0x1)
        """
        )
        self.assertEqual(set(libraries), {"libcudart.so.13", "libcustom.so"})
        self.assertEqual(
            sirius_sdk.runtime_libraries(
                "libcuda.so.1 => not found\nlibnvidia-ptxjitcompiler.so.1 => not found"
            ),
            {},
        )
        with self.assertRaisesRegex(ValueError, "unresolved"):
            sirius_sdk.runtime_libraries("libgpu.so => not found")
        with self.assertRaisesRegex(ValueError, "SONAME"):
            sirius_sdk.runtime_libraries("/build/libwithout-soname.so (0x1)")
        with self.assertRaisesRegex(ValueError, "NVIDIA driver resolved from the Pixi"):
            sirius_sdk.runtime_libraries(
                "libcuda.so.1 => /pixi/targets/x86_64-linux/lib/stubs/libcuda.so.1 (0x1)",
                provider_prefix=Path("/pixi"),
            )

    def test_pixi_provider_binds_sdk_compilers_to_one_prefix(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project = root / "sirius"
            prefix = project / ".pixi/envs/mo"
            (prefix / "bin").mkdir(parents=True)
            (project / "pixi.lock").write_text("locked\n")
            for name in ("cc", "c++"):
                compiler = prefix / "bin" / name
                compiler.write_bytes(b"compiler")
                compiler.chmod(0o755)
            manifest = {
                "source_directory": str(project),
                "compiler": str(prefix / "bin/c++"),
                "c_compiler": str(prefix / "bin/cc"),
            }
            env = {
                "PIXI_PROJECT_ROOT": str(project),
                "PIXI_ENVIRONMENT_NAME": "mo",
                "CONDA_PREFIX": str(prefix),
            }
            got = sirius_sdk.pixi_provider(manifest, env)
            self.assertEqual(got["prefix"], str(prefix))
            self.assertEqual(got["lock_sha256"], sirius_sdk.digest(project / "pixi.lock"))
            with self.assertRaisesRegex(ValueError, "one Pixi project and prefix"):
                sirius_sdk.pixi_provider(manifest, dict(env, CONDA_PREFIX=str(root)))
            manifest["compiler"] = str(root / "outside-c++")
            (root / "outside-c++").write_bytes(b"compiler")
            with self.assertRaisesRegex(ValueError, "outside the activated Pixi prefix"):
                sirius_sdk.pixi_provider(manifest, env)

    def test_runtime_source_rejects_driver_stubs_and_symlink_aliases(self):
        with tempfile.TemporaryDirectory() as directory:
            prefix = Path(directory) / "pixi"
            stubs = prefix / "targets/x86_64-linux/lib/stubs"
            stubs.mkdir(parents=True)
            stub = stubs / "libcudart.so.13"
            stub.write_bytes(b"stub")
            alias = prefix / "lib/libcudart.so.13"
            alias.parent.mkdir()
            alias.symlink_to(stub)
            with self.assertRaisesRegex(ValueError, "unsafe Pixi runtime"):
                sirius_sdk.runtime_source(alias, prefix)
            driver = prefix / "lib/libcuda.so.1"
            driver.write_bytes(b"driver")
            with self.assertRaisesRegex(ValueError, "unsafe Pixi runtime"):
                sirius_sdk.runtime_source(driver, prefix)

    def test_mo_gpu_runtime_records_only_same_prefix_elf_closure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repo = root / "mo"
            prefix = root / "sirius/.pixi/envs/mo"
            (repo / "cgo").mkdir(parents=True)
            (repo / "lib").mkdir()
            (repo / "thirdparties/install/lib").mkdir(parents=True)
            (prefix / "lib").mkdir(parents=True)
            (repo / "cgo/libmo.so").write_bytes(b"libmo")
            (repo / "lib/libmo.so").write_bytes(b"libmo")
            names = ("libcuvs.so", "libcuvs_c.so", "libcudart.so.13")
            for name in names:
                (prefix / "lib" / name).write_bytes(name.encode())
            baseline = repo / "thirdparties/install/lib/libusearch_c.so"
            baseline.write_bytes(b"baseline")
            lines = "\n".join(
                f"{name} => {prefix / 'lib' / name} (0x1)" for name in names
            ) + f"\nlibusearch_c.so => {baseline} (0x1)\nlibcuda.so.1 => not found\n"
            result = subprocess.CompletedProcess([], 0, lines, "")
            with patch.object(sirius_sdk.subprocess, "run", return_value=result), \
                 patch.object(sirius_sdk, "elf_soname", side_effect=lambda path: path.name):
                got = sirius_sdk.mo_gpu_runtime(
                    {"prefix": str(prefix)}, {"libcuvs.so": prefix / "lib/libcuvs.so"}, repo
                )
            self.assertEqual(set(got["runtime_libraries"]), set(names))
            self.assertEqual(got["libmo_sha256"], sirius_sdk.digest(repo / "cgo/libmo.so"))

            outside = root / "external/libcuvs_c.so"
            outside.parent.mkdir()
            outside.write_bytes(b"external")
            unsafe = lines.replace(str(prefix / "lib/libcuvs_c.so"), str(outside))
            with patch.object(
                sirius_sdk.subprocess,
                "run",
                return_value=subprocess.CompletedProcess([], 0, unsafe, ""),
            ), patch.object(sirius_sdk, "elf_soname", side_effect=lambda path: path.name):
                with self.assertRaisesRegex(ValueError, "escapes Pixi"):
                    sirius_sdk.mo_gpu_runtime({"prefix": str(prefix)}, {}, repo)

    def test_release_mo_gpu_requires_verified_native_generation(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            (repo / "cgo").mkdir()
            stamp = repo / "cgo/.mo-native-provenance"
            stamp.write_text(
                "accelerator=gpu\ngoos=linux\ngoarch=amd64\n"
                "optimization=release\nsimsimd=0\n"
            )
            mo_gpu = {"libmo": str(repo / "cgo/libmo.so")}
            with patch.object(sirius_sdk, "run", return_value="") as command:
                sirius_sdk.verify_mo_native_generation("release", mo_gpu, repo)
            self.assertEqual(command.call_args.args[-3:], ("gpu", "release", "0"))
            stamp.write_text(stamp.read_text().replace("accelerator=gpu", "accelerator=cpu"))
            with self.assertRaisesRegex(ValueError, "wrong build key"):
                sirius_sdk.verify_mo_native_generation("release", mo_gpu, repo)
            sirius_sdk.verify_mo_native_generation("development", mo_gpu, repo)

    def package_fixture(self, root):
        project = root / "pixi-project"
        prefix = project / ".pixi/envs/mo"
        prefix.mkdir(parents=True)
        lockfile = project / "pixi.lock"
        lockfile.write_text("locked\n")
        environment = patch.dict(
            os.environ,
            {
                "PIXI_PROJECT_ROOT": str(project),
                "PIXI_ENVIRONMENT_NAME": "mo",
                "CONDA_PREFIX": str(prefix),
            },
        )
        environment.start()
        self.addCleanup(environment.stop)
        sdk = root / "sdk"
        sdk.mkdir()
        (sdk / "link.json").write_text("{}\n")
        bundle = root / "bundle"
        output = bundle / "lib"
        output.mkdir(parents=True)
        source = root / "original"
        source.mkdir()
        prepared = root / "prepared"
        prepared.mkdir()
        originals = {}
        for name, mode in (
            ("mo-service", 0o755),
            ("libmo.so", 0o751),
            ("libusearch.so", 0o755),
            ("libgomp.so.1", 0o555),
        ):
            path = source / name
            path.write_bytes(b"\x7fELF " + name.encode())
            path.chmod(mode)
            originals[name] = path.read_bytes()
        # Packaging must detach both hardlinks and symlinks before patchelf.
        os.link(source / "mo-service", bundle / "mo-service")
        os.link(source / "libmo.so", output / "libmo.so")
        (output / "libusearch.so").symlink_to(source / "libusearch.so")
        provenance = {
            "sdk": str(sdk),
            "mode": "development",
            "merged_ref": "",
            "sdk_manifest_sha256": sirius_sdk.digest(sdk / "link.json"),
            "artifact_sha256": {},
            "pixi_provider": {
                "project": str(project),
                "environment": "mo",
                "prefix": str(prefix),
                "lockfile": str(lockfile),
                "lock_sha256": sirius_sdk.digest(lockfile),
            },
            "mo_gpu": None,
            "mo_baseline": {
                name: {"source": str(source / name), "sha256": sirius_sdk.digest(source / name)}
                for name in ("libmo.so", "libusearch.so")
            },
            "runtime_libraries": {
                "libgomp.so.1": {
                    "source": str(source / "libgomp.so.1"),
                    "sha256": sirius_sdk.digest(source / "libgomp.so.1"),
                }
            },
        }
        (prepared / "provenance.json").write_text(json.dumps(provenance))
        args = SimpleNamespace(
            prepared=prepared, binary=bundle / "mo-service", output=output
        )
        return args, source, originals

    def package_tools(self, args, source, force_external=False, gpu_source=None):
        def command(*words):
            if words[0] == "patchelf":
                self.assertEqual(words[1], "--set-rpath")
                temporary = Path(words[3])
                self.assertTrue(temporary.name.startswith(".sirius-"))
                self.assertNotEqual(temporary.parent, source)
                temporary.write_bytes(
                    temporary.read_bytes() + b"|rpath=" + words[2].encode()
                )
                return ""
            self.assertEqual(words, ("ldd", str(args.binary)))
            self.assertIn(b"|rpath=$ORIGIN/lib", args.binary.read_bytes())
            local = all(
                (args.output / name).is_file()
                and b"|rpath=$ORIGIN" in (args.output / name).read_bytes()
                for name in ("libmo.so", "libusearch.so")
            )
            paths = {
                "libmo.so": args.output / "libmo.so",
                "libusearch.so": args.output / "libusearch.so" if local else None,
                "libgomp.so.1": (args.output if local else source) / "libgomp.so.1",
            }
            if gpu_source is not None:
                gpu_staged = args.output / "libcudart.so.13"
                paths["libcudart.so.13"] = (
                    gpu_staged if gpu_staged.is_file() else gpu_source
                )
            if force_external:
                paths["libmo.so"] = source / "libmo.so"
            return "\n".join(
                (
                    f"{name} => {path} (0x1)"
                    if path is not None
                    else f"{name} => not found"
                )
                for name, path in paths.items()
            )

        return command

    def test_package_retargets_binary_and_baseline_without_mutating_originals(self):
        with tempfile.TemporaryDirectory() as directory:
            args, source, originals = self.package_fixture(Path(directory))
            sdk = Path(directory) / "sdk"
            with patch.object(
                sirius_sdk, "run", side_effect=self.package_tools(args, source)
            ):
                sirius_sdk.package(args)
            self.assertIn(b"|rpath=$ORIGIN/lib", args.binary.read_bytes())
            for name, original in originals.items():
                self.assertEqual((source / name).read_bytes(), original)
                staged = args.binary if name == "mo-service" else args.output / name
                self.assertEqual(
                    stat.S_IMODE(staged.stat().st_mode),
                    stat.S_IMODE((source / name).stat().st_mode),
                )
                self.assertFalse(staged.is_symlink())
            report = json.loads((args.output / "sirius-provenance.json").read_text())
            self.assertEqual(
                set(report["baseline_input_sha256"]), {"libmo.so", "libusearch.so"}
            )
            self.assertEqual(
                set(report["packaged_sha256"]),
                {"libmo.so", "libusearch.so", "libgomp.so.1"},
            )
            self.assertEqual(report["binary_sha256"], sirius_sdk.digest(args.binary))
            self.assertNotEqual(report["linked_binary_sha256"], report["binary_sha256"])
            with patch.object(sirius_sdk, "validate", return_value={}):
                sirius_sdk.verify_package(
                    SimpleNamespace(prepared=args.prepared, output=args.output, sdk=sdk)
                )
            changed = args.output / "libgomp.so.1"
            changed.chmod(0o755)
            changed.write_bytes(b"changed after packaging")
            with patch.object(sirius_sdk, "validate", return_value={}):
                with self.assertRaisesRegex(ValueError, "runtime changed"):
                    sirius_sdk.verify_package(
                        SimpleNamespace(prepared=args.prepared, output=args.output, sdk=sdk)
                    )

    def test_package_rejects_changed_submodule_pin(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args, _, _ = self.package_fixture(root)
            sdk = root / "sdk"
            (sdk / "link.json").write_text(
                json.dumps(
                    {
                        "source_directory": str(root / "third_party" / "sirius"),
                        "source_revision": "a" * 40,
                    }
                )
            )
            provenance_path = args.prepared / "provenance.json"
            provenance = json.loads(provenance_path.read_text())
            provenance["mo_root"] = str(root)
            provenance["source_revision"] = "a" * 40
            provenance["sdk_manifest_sha256"] = sirius_sdk.digest(sdk / "link.json")
            provenance_path.write_text(json.dumps(provenance))
            with patch.object(sirius_sdk, "run", return_value="b" * 40):
                with self.assertRaisesRegex(ValueError, "submodule pin"):
                    sirius_sdk.package(args)
            with patch.object(
                sirius_sdk, "run", side_effect=["a" * 40, "b" * 40]
            ):
                with self.assertRaisesRegex(ValueError, "source SHA is stale"):
                    sirius_sdk.package(args)
            provenance["mode"] = "release"
            provenance_path.write_text(json.dumps(provenance))
            with patch.object(
                sirius_sdk, "run", side_effect=["a" * 40, "a" * 40, " M source.cc"]
            ):
                with self.assertRaisesRegex(ValueError, "became dirty"):
                    sirius_sdk.package(args)

    def test_package_stages_only_prepared_pixi_gpu_runtime(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args, source, _ = self.package_fixture(root)
            provenance = json.loads((args.prepared / "provenance.json").read_text())
            prefix = Path(provenance["pixi_provider"]["prefix"])
            stubs = prefix / "lib/stubs"
            stubs.mkdir(parents=True)
            cudart = prefix / "lib/libcudart.so.13.3"
            cudart.write_bytes(b"\x7fELF verified cudart")
            cudart.chmod(0o755)
            staged = args.output / "libcudart.so.13"
            staged.write_bytes(b"\x7fELF stale prior provider")
            provenance["mo_gpu"] = {
                "libmo": str(source / "libmo.so"),
                "libmo_sha256": sirius_sdk.digest(source / "libmo.so"),
                "runtime_libraries": {
                    "libcudart.so.13": {
                        "source": str(cudart),
                        "sha256": sirius_sdk.digest(cudart),
                    }
                },
            }
            (args.prepared / "provenance.json").write_text(json.dumps(provenance))
            with patch.object(
                sirius_sdk,
                "run",
                side_effect=self.package_tools(args, source, gpu_source=cudart),
            ):
                sirius_sdk.package(args)
            self.assertIn(b"|rpath=$ORIGIN", staged.read_bytes())
            self.assertNotIn(b"stale prior provider", staged.read_bytes())
            self.assertEqual(cudart.read_bytes(), b"\x7fELF verified cudart")
            report = json.loads((args.output / "sirius-provenance.json").read_text())
            self.assertEqual(
                report["gpu_runtime_input_sha256"],
                {"libcudart.so.13": sirius_sdk.digest(cudart)},
            )

    def test_package_rejects_gpu_runtime_from_stub_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args, source, _ = self.package_fixture(root)
            provenance = json.loads((args.prepared / "provenance.json").read_text())
            prefix = Path(provenance["pixi_provider"]["prefix"])
            stubs = prefix / "lib/stubs"
            stubs.mkdir(parents=True)
            cudart = stubs / "libcudart.so.13"
            cudart.write_bytes(b"\x7fELF forbidden stub")
            provenance["mo_gpu"] = {
                "libmo": str(source / "libmo.so"),
                "libmo_sha256": sirius_sdk.digest(source / "libmo.so"),
                "runtime_libraries": {
                    "libcudart.so.13": {
                        "source": str(cudart),
                        "sha256": sirius_sdk.digest(cudart),
                    }
                },
            }
            (args.prepared / "provenance.json").write_text(json.dumps(provenance))
            with patch.object(
                sirius_sdk,
                "run",
                side_effect=self.package_tools(args, source, gpu_source=cudart),
            ):
                with self.assertRaisesRegex(ValueError, "unsafe Pixi runtime"):
                    sirius_sdk.package(args)
            self.assertFalse((args.output / "sirius-provenance.json").exists())

    def test_package_requires_local_baseline_closure_and_consistent_layout(self):
        for failure in (
            "missing baseline",
            "external final resolution",
            "wrong output",
        ):
            with self.subTest(
                failure=failure
            ), tempfile.TemporaryDirectory() as directory:
                args, source, _ = self.package_fixture(Path(directory))
                if failure == "missing baseline":
                    (args.output / "libusearch.so").unlink()
                elif failure == "wrong output":
                    args.output = args.output.parent / "other-lib"
                with patch.object(
                    sirius_sdk,
                    "run",
                    side_effect=self.package_tools(
                        args, source, failure == "external final resolution"
                    ),
                ):
                    with self.assertRaises(ValueError):
                        sirius_sdk.package(args)
                self.assertFalse((args.output / "sirius-provenance.json").exists())

    def test_package_rejects_pixi_lock_drift(self):
        with tempfile.TemporaryDirectory() as directory:
            args, _, _ = self.package_fixture(Path(directory))
            provenance = json.loads((args.prepared / "provenance.json").read_text())
            Path(provenance["pixi_provider"]["lockfile"]).write_text("changed\n")
            with self.assertRaisesRegex(ValueError, "Pixi provider changed"):
                sirius_sdk.package(args)


if __name__ == "__main__":
    unittest.main()
