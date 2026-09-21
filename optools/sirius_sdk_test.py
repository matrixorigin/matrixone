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
        with self.assertRaisesRegex(ValueError, "unresolved"):
            sirius_sdk.runtime_libraries("libgpu.so => not found")
        with self.assertRaisesRegex(ValueError, "SONAME"):
            sirius_sdk.runtime_libraries("/build/libwithout-soname.so (0x1)")

    def package_fixture(self, root):
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
            "artifact_sha256": {},
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

    def package_tools(self, args, source, force_external=False):
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


if __name__ == "__main__":
    unittest.main()
