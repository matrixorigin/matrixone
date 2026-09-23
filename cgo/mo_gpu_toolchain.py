#!/usr/bin/env python3
# Copyright 2026 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Resolve the optional GPU build contract without invoking Pixi or a compiler.

CPU consumers must not invoke this module. An explicit manifest is authoritative;
the legacy system CUDA + Conda layout is selected only when none was supplied.
The manifest describes build inputs, never a distributable runtime search path.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shlex
import subprocess
import sys
import tempfile


class ToolchainError(ValueError):
    pass


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def safe_path(value, kind=None):
    # Paths enter Make recipes as well as shell exports. Reject Make/shell
    # metacharacters instead of trying to quote differently in each consumer.
    if not isinstance(value, str) or not re.fullmatch(r"/[A-Za-z0-9_./+@-]+", value):
        raise ToolchainError(f"expected an absolute path without whitespace/metacharacters: {value!r}")
    result = Path(value)
    if kind == "dir" and not result.is_dir():
        raise ToolchainError(f"missing toolchain directory: {value}")
    if kind == "file" and not result.is_file():
        raise ToolchainError(f"missing toolchain file: {value}")
    if kind == "exe" and (not result.is_file() or not os.access(result, os.X_OK)):
        raise ToolchainError(f"missing executable toolchain compiler: {value}")
    return str(result)


def directories(obj, key):
    values = obj.get(key)
    if not isinstance(values, list) or not values:
        raise ToolchainError(f"{key} must be a nonempty array")
    return [safe_path(value, "dir") for value in values]


def contained(path, prefix):
    try:
        Path(path).resolve().relative_to(Path(prefix).resolve())
    except ValueError as error:
        raise ToolchainError(f"toolchain input escapes provider prefix: {path}") from error


def forbidden_runtime_path(path, stub_roots):
    lexical = Path(path)
    resolved = lexical.resolve()
    return (
        "stubs" in lexical.parts
        or "stubs" in resolved.parts
        or resolved.name.startswith(("libcuda.so", "libnvidia-"))
        or any(resolved.is_relative_to(stub) for stub in stub_roots)
    )


def load_manifest(filename):
    safe_path(filename, "file")
    manifest = json.loads(Path(filename).read_text())
    if manifest.get("schema_version") != 1 or manifest.get("provider") != "pixi":
        raise ToolchainError("unsupported GPU toolchain schema/provider")
    if manifest.get("platform") != "linux-64":
        raise ToolchainError("GPU toolchain must target linux-64")
    prefix = safe_path(manifest["prefix"], "dir")
    compilers = manifest["compilers"]
    for name in ("cc", "cxx", "nvcc"):
        item = compilers[name]
        safe_path(item["path"], "exe")
        contained(item["path"], prefix)
        if not isinstance(item.get("version"), str) or not item["version"]:
            raise ToolchainError(f"missing compiler version: {name}")
    cuda, rapids = manifest["cuda"], manifest["rapids"]
    if not re.fullmatch(r"13\.3(?:\.[0-9]+)*", cuda["version"]):
        raise ToolchainError("MO's Pixi GPU profile requires CUDA 13.3")
    for section, keys in ((cuda, ("include_dirs", "library_dirs", "stub_library_dirs")),
                          (rapids, ("include_dirs", "library_dirs"))):
        for key in keys:
            for path in directories(section, key):
                contained(path, prefix)
    stub_roots = [Path(path).resolve() for path in cuda["stub_library_dirs"]]
    for path in cuda["library_dirs"] + rapids["library_dirs"]:
        directory = Path(path)
        if forbidden_runtime_path(path, stub_roots) or any(
            candidate.is_file()
            for pattern in ("libcuda.so*", "libnvidia-*.so*")
            for candidate in directory.glob(pattern)
        ):
            raise ToolchainError("driver libraries/stubs cannot be GPU runtime directories")
    for name in ("libcuvs", "libcuvs_c", "librmm"):
        version = rapids["versions"][name]
        if not isinstance(version, str) or not re.fullmatch(r"26\.0?8(?:\.[0-9]+)*", version):
            raise ToolchainError(f"MO's Pixi GPU profile requires RAPIDS 26.08: {name}")
    pixi = manifest["pixi"]
    lockfile = safe_path(pixi["lockfile"], "file")
    if digest(lockfile) != pixi["lockfile_sha256"]:
        raise ToolchainError("Pixi lockfile changed; export a new GPU toolchain manifest")
    packages = manifest["packages"]
    if not isinstance(packages, list) or not packages:
        raise ToolchainError("GPU package provenance is empty")
    package_versions = {}
    for item in packages:
        for key in ("name", "version", "build", "url", "sha256"):
            if not isinstance(item.get(key), str) or not item[key]:
                raise ToolchainError(f"incomplete package provenance: {key}")
        if not re.fullmatch(r"[0-9a-f]{64}", item["sha256"]):
            raise ToolchainError("invalid package SHA256")
        if item["name"] in package_versions:
            raise ToolchainError(f"duplicate package identity: {item['name']}")
        package_versions[item["name"]] = item["version"]
    if package_versions.get("cuda-version") != cuda["version"]:
        raise ToolchainError("CUDA version disagrees with package provenance")
    for name, version in rapids["versions"].items():
        package_version = package_versions.get(name)
        if name == "libcuvs_c" and package_version is None:
            package_version = package_versions.get("libcuvs")
        if package_version != version:
            raise ToolchainError(f"RAPIDS version disagrees with package provenance: {name}")
    artifacts = manifest["artifact_sha256"]
    if not isinstance(artifacts, dict) or not artifacts:
        raise ToolchainError("GPU artifact provenance is empty")
    for path, expected in artifacts.items():
        safe_path(path, "file")
        if Path(path).resolve() != Path(lockfile).resolve():
            contained(path, prefix)
        if not isinstance(expected, str) or digest(path) != expected:
            raise ToolchainError(f"GPU toolchain artifact changed: {path}")
    required = [item["path"] for item in compilers.values()]
    for dirs, name in ((cuda["include_dirs"], "cuda.h"),
                       (rapids["include_dirs"], "cuvs/core/c_api.h"),
                       (rapids["include_dirs"], "rmm/cuda_stream_view.hpp"),
                       (cuda["library_dirs"], "libcudart.so"),
                       (cuda["stub_library_dirs"], "libcuda.so"),
                       (rapids["library_dirs"], "libcuvs.so"),
                       (rapids["library_dirs"], "libcuvs_c.so"),
                       (rapids["library_dirs"], "librmm.so")):
        matches = [str(Path(path) / name) for path in dirs if (Path(path) / name).is_file()]
        if not matches:
            raise ToolchainError(f"missing GPU toolchain input: {name}")
        required.extend(matches)
    roots = manifest["runtime_roots"]
    if not isinstance(roots, list):
        raise ToolchainError("runtime_roots must be an array")
    for path in roots:
        safe_path(path, "file")
        contained(path, prefix)
        if forbidden_runtime_path(path, stub_roots):
            raise ToolchainError("driver libraries/stubs cannot be GPU runtime roots")
    root_paths = {str(Path(path).resolve()) for path in roots}
    for name in ("libcuvs.so", "libcuvs_c.so"):
        if not any(str((Path(path) / name).resolve()) in root_paths for path in rapids["library_dirs"]):
            raise ToolchainError(f"missing GPU runtime root: {name}")
    recorded = {str(Path(path).resolve()) for path in artifacts}
    for path in required + roots:
        if str(Path(path).resolve()) not in recorded:
            raise ToolchainError(f"unhashed GPU toolchain input: {path}")
    return manifest


def resolve(env=None):
    env = os.environ if env is None else env
    filename = env.get("GPU_TOOLCHAIN_MANIFEST", "")
    if filename:
        manifest = load_manifest(filename)
        cuda, rapids = manifest["cuda"], manifest["rapids"]
        cc, cxx, nvcc = (manifest["compilers"][name]["path"] for name in ("cc", "cxx", "nvcc"))
        fingerprint = digest(filename)
        provider = "pixi"
    else:
        prefix = env.get("CONDA_PREFIX", "")
        if not prefix:
            raise ToolchainError("CONDA_PREFIX is required by the legacy GPU provider")
        safe_path(prefix)
        cuda_root = safe_path(env.get("CUDA_PATH", "/usr/local/cuda"))
        cuda = {"include_dirs": [cuda_root + "/include"],
                "library_dirs": [cuda_root + "/lib64"],
                "stub_library_dirs": [cuda_root + "/lib64/stubs"]}
        rapids = {"include_dirs": [prefix + "/include"], "library_dirs": [prefix + "/lib"]}
        # Preserve the supported legacy override contract. Resolve executables
        # when available, but allow make -n on a host without the legacy SDK.
        cc = env.get("CC", "cc")
        cxx = env.get("CXX", env.get("HOST_COMPILER", "g++"))
        for value in (cc, cxx):
            if not re.fullmatch(r"[A-Za-z0-9_./+@-]+", value):
                raise ToolchainError("GPU compiler overrides must name one executable")
        nvcc = cuda_root + "/bin/nvcc"
        fingerprint, provider = "legacy", "legacy"
    includes = list(dict.fromkeys(cuda["include_dirs"] + rapids["include_dirs"]))
    libraries = list(dict.fromkeys(cuda["library_dirs"] + rapids["library_dirs"]))
    # Stubs are link-only inputs. They must never enter LD_LIBRARY_PATH/rpath.
    link_dirs = cuda["stub_library_dirs"] + libraries
    return {
        "MO_GPU_PROVIDER": provider,
        "MO_GPU_CC": cc,
        "MO_GPU_CXX": cxx,
        "MO_GPU_NVCC": nvcc,
        "MO_GPU_CFLAGS": " ".join("-I" + path for path in includes),
        "MO_GPU_LDFLAGS": " ".join("-L" + path for path in link_dirs) + " -lcuda -lcudart -lcuvs -lcuvs_c -lstdc++",
        "MO_GPU_RUNTIME_PATH": ":".join(libraries),
        "MO_GPU_CUDA_INCLUDE_DIRS": " ".join(cuda["include_dirs"]),
        "MO_GPU_CUDA_LIBRARY_DIRS": " ".join(cuda["library_dirs"]),
        "MO_GPU_CUDA_STUB_DIRS": " ".join(cuda["stub_library_dirs"]),
        "MO_GPU_RAPIDS_INCLUDE_DIRS": " ".join(rapids["include_dirs"]),
        "MO_GPU_TOOLCHAIN_FINGERPRINT": fingerprint,
    }


def export_manifest(args):
    if platform.system() != "Linux" or platform.machine() != "x86_64":
        raise ToolchainError("MO GPU toolchain export requires Linux x86_64")
    prefix = Path(safe_path(str(Path(args.prefix).resolve()), "dir"))
    lockfile = safe_path(str(Path(args.lockfile).resolve()), "file")
    metadata = sorted((prefix / "conda-meta").glob("*.json"))
    packages = []
    for path in metadata:
        item = json.loads(path.read_text())
        packages.append({key: item.get(key, "") for key in ("name", "version", "build", "url", "sha256")})
    versions = {item["name"]: item["version"] for item in packages}
    versions.setdefault("libcuvs_c", versions.get("libcuvs", ""))
    # Export binds the installed packages to the selected frozen environment.
    # PyYAML is a build-profile dependency, never needed by CPU/resolve paths.
    import yaml
    lock = yaml.safe_load(Path(lockfile).read_text())
    platforms = lock["environments"][args.environment]["packages"]
    selected = platforms.get("linux-64", platforms.get("linux-64-cuda13"))
    if selected is None:
        raise ToolchainError("Pixi environment has no Linux x86_64 package set")
    urls = {item["conda"] for item in selected if "conda" in item}
    locked = {item["conda"]: item["sha256"] for item in lock["packages"] if "conda" in item}
    if {item["url"] for item in packages} != urls:
        raise ToolchainError("installed package set does not match the selected Pixi environment")
    if any(locked.get(item["url"]) != item["sha256"] for item in packages):
        raise ToolchainError("installed package hashes do not match the Pixi lockfile")
    compilers = {}
    for name, candidates in (("cc", ("x86_64-conda-linux-gnu-cc", "x86_64-conda-linux-gnu-gcc")),
                             ("cxx", ("x86_64-conda-linux-gnu-c++", "x86_64-conda-linux-gnu-g++")),
                             ("nvcc", ("nvcc",))):
        compiler = next((prefix / "bin" / value for value in candidates if (prefix / "bin" / value).is_file()), None)
        if compiler is None:
            raise ToolchainError(f"missing Pixi compiler: {name}")
        version = subprocess.check_output([str(compiler), "--version"], text=True, timeout=15).strip()
        if name == "nvcc" and "release 13.3," not in version:
            raise ToolchainError("NVCC does not match CUDA 13.3")
        compilers[name] = {"path": str(compiler), "version": version}
    cuda = prefix / "targets/x86_64-linux"
    if not re.search(r"^#define\s+CUDA_VERSION\s+13030\b", (cuda / "include/cuda.h").read_text(), re.M):
        raise ToolchainError("CUDA headers do not match CUDA 13.3")
    for name in ("gcc_linux-64", "gxx_linux-64"):
        if not versions.get(name, "").startswith("14."):
            raise ToolchainError(f"MO's Pixi GPU profile requires GCC 14: {name}")
    artifact_paths = [item["path"] for item in compilers.values()]
    artifact_paths += [str(cuda / name) for name in ("include/cuda.h", "lib/libcudart.so", "lib/stubs/libcuda.so")]
    artifact_paths += [str(prefix / "lib" / name) for name in ("libcuvs.so", "libcuvs_c.so", "librmm.so")]
    artifact_paths += [str(prefix / "include" / name) for name in ("cuvs/core/c_api.h", "rmm/cuda_stream_view.hpp")]
    artifact_paths += [str(path) for path in metadata]
    manifest = {
        "schema_version": 1, "provider": "pixi", "platform": "linux-64", "prefix": str(prefix),
        "compilers": compilers,
        "cuda": {"version": versions.get("cuda-version", ""), "include_dirs": [str(cuda / "include")],
                 "library_dirs": [str(cuda / "lib")], "stub_library_dirs": [str(cuda / "lib/stubs")]},
        "rapids": {"include_dirs": [str(prefix / "include")], "library_dirs": [str(prefix / "lib")],
                   "versions": {name: versions[name] for name in ("libcuvs", "libcuvs_c", "librmm") if name in versions}},
        "pixi": {"environment": args.environment, "lockfile": lockfile, "lockfile_sha256": digest(lockfile)},
        "packages": packages,
        "runtime_roots": [str(prefix / "lib" / name) for name in ("libcuvs.so", "libcuvs_c.so")],
        "artifact_sha256": {path: digest(path) for path in artifact_paths},
    }
    output = Path(safe_path(str(Path(args.output).absolute())))
    # A failed export never replaces the last valid manifest.
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", prefix=".gpu-toolchain-", dir=output.parent,
                                         delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(manifest, stream, indent=2, sort_keys=True)
            stream.write("\n")
        load_manifest(str(temporary))
        os.replace(temporary, output)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    resolver = commands.add_parser("resolve")
    resolver.add_argument("--format", choices=("json", "shell", "make", "fingerprint"), default="json")
    exporter = commands.add_parser("export")
    exporter.add_argument("--prefix", required=True)
    exporter.add_argument("--lockfile", required=True)
    exporter.add_argument("--output", required=True)
    exporter.add_argument("--environment", default="default")
    args = parser.parse_args()
    try:
        if args.command == "export":
            export_manifest(args)
            return
        values = resolve()
        if args.format == "json":
            print(json.dumps(values, sort_keys=True))
        elif args.format == "shell":
            for key, value in values.items():
                print(f"export {key}={shlex.quote(value)}")
        elif args.format == "make":
            print("|".join(f"override {key} := {value}" for key, value in values.items()))
        else:
            print(values["MO_GPU_TOOLCHAIN_FINGERPRINT"])
    except (ToolchainError, OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        print(f"GPU toolchain: {error}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
