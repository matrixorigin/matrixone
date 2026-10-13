#!/usr/bin/env python3
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

"""Validate the generated Sirius SDK and stage its actual ELF dependency closure.

No native library list is duplicated here. link.json and its verified C
consumer are authoritative; release provenance must name a merged source SHA.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import stat
import subprocess
import tempfile


def run(*args):
    return subprocess.run(args, check=True, capture_output=True, text=True).stdout


def digest(path):
    with open(path, "rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def verify_source_pin(source, revision, mo_root):
    mo_root = Path(mo_root).resolve()
    pinned_source = mo_root / "third_party" / "sirius"
    if source.resolve() != pinned_source.resolve():
        raise ValueError("Sirius SDK was not built from the MatrixOne submodule")
    pinned_revision = run(
        "git", "-C", str(mo_root), "rev-parse", "HEAD:third_party/sirius"
    ).strip()
    if revision != pinned_revision:
        raise ValueError("Sirius SDK does not match the MatrixOne submodule pin")
    if run("git", "-C", str(source), "rev-parse", "HEAD").strip() != revision:
        raise ValueError("Sirius SDK source SHA is stale")


def validate(sdk, mode, merged_ref, mo_root=None):
    manifest = json.loads((sdk / "link.json").read_text())
    if manifest.get("schema_version") != 1 or manifest.get("abi_version") != 1:
        raise ValueError("Sirius SDK requires schema 1 and ABI 1")
    for field in ("compiler", "c_compiler"):
        value = manifest.get(field)
        if (
            not isinstance(value, str)
            or not Path(value).is_absolute()
            or not Path(value).is_file()
            or not os.access(value, os.X_OK)
        ):
            raise ValueError("Sirius SDK requires an absolute executable " + field)
    flags = manifest["link_arguments"]
    if shlex.split((sdk / "link.rsp").read_text()) != flags:
        raise ValueError("Sirius response file does not match link.json")
    if not flags or any(
        not isinstance(flag, str) or flag.startswith("@") for flag in flags
    ):
        raise ValueError("Sirius SDK has an opaque or empty link closure")
    if not re.search(
        r"#define\s+SIRIUS_ABI_VERSION\s+1[uU]?\b", (sdk / "sirius_c.h").read_text()
    ):
        raise ValueError("Sirius SDK header ABI mismatch")
    source = Path(manifest["source_directory"])
    revision = manifest["source_revision"]
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("Sirius SDK lacks an exact source SHA")
    if mo_root is not None:
        verify_source_pin(source, revision, mo_root)
    elif run("git", "-C", str(source), "rev-parse", "HEAD").strip() != revision:
        raise ValueError("Sirius SDK source SHA is stale")
    dirty = bool(
        run(
            "git",
            "-C",
            str(source),
            "status",
            "--porcelain",
            "--untracked-files=normal",
        ).strip()
    )
    if mode == "release":
        if manifest.get("source_dirty", True) or dirty:
            raise ValueError(
                "release requires a clean SDK built from a merged Sirius SHA"
            )
        if not merged_ref:
            raise ValueError(
                "release requires SIRIUS_MERGED_REF naming the verified merged branch"
            )
        run(
            "git",
            "-C",
            str(source),
            "merge-base",
            "--is-ancestor",
            revision,
            merged_ref,
        )
    hashes = manifest.get("artifact_sha256", {})
    if not hashes:
        raise ValueError("Sirius SDK lacks build-time artifact hashes")
    for name, expected in hashes.items():
        if digest(name) != expected:
            raise ValueError("Sirius SDK artifact changed: " + name)
    consumer = Path(manifest["consumer"])
    if str(consumer) not in hashes:
        raise ValueError("Sirius SDK consumer is not covered by build provenance")
    for flag in flags:
        if (
            not flag.startswith("-")
            and Path(flag).suffix in (".a", ".o")
            and flag not in hashes
        ):
            raise ValueError("unhashed static/device-link artifact: " + flag)
    return manifest


def response(flags):
    return (
        "\n".join(
            '"' + f.replace("\\", "\\\\").replace('"', '\\"') + '"' for f in flags
        )
        + "\n"
    )


def relocatable_flags(flags):
    result = []
    for flag in flags:
        if flag.startswith(("-Wl,-rpath,", "-Wl,-rpath=")):
            continue
        if flag in ("-rpath", "-R") or flag.startswith("-Wl,-R"):
            raise ValueError("unsupported SDK runtime-path spelling: " + flag)
        result.append(flag)
    result.append("-Wl,-rpath,$ORIGIN/lib")
    return result


def elf_soname(path):
    output = run("readelf", "-d", str(path))
    match = re.search(r"\(SONAME\).*\[([^]]+)\]", output)
    if match is None or Path(match[1]).name != match[1]:
        raise ValueError("GPU runtime dependency lacks a relocatable SONAME: " + str(path))
    return match[1]


def pixi_provider(manifest, env=None):
    """Bind the SDK's verified compilers to the one active Sirius Pixi prefix."""
    if "gpu_toolchain_manifest" in manifest:
        raise ValueError("SDK still exports the retired GPU toolchain manifest")
    env = os.environ if env is None else env
    project_name, prefix_name = env.get("PIXI_PROJECT_ROOT"), env.get("CONDA_PREFIX")
    if not project_name or not prefix_name or env.get("PIXI_ENVIRONMENT_NAME") != "mo":
        raise ValueError("embedded Sirius requires pixi run --frozen -e mo")
    project, prefix = Path(project_name), Path(prefix_name)
    if not project.is_absolute() or not prefix.is_absolute():
        raise ValueError("Pixi project and prefix must be absolute")
    project, prefix = project.resolve(), prefix.resolve()
    if (
        not prefix.is_dir()
        or prefix != (project / ".pixi/envs/mo").resolve()
        or project != Path(manifest["source_directory"]).resolve()
    ):
        raise ValueError("Sirius SDK and MatrixOne must use one Pixi project and prefix")
    for field in ("compiler", "c_compiler"):
        compiler = Path(manifest[field]).resolve()
        if not compiler.is_file() or not compiler.is_relative_to(prefix / "bin"):
            raise ValueError("Sirius SDK compiler is outside the activated Pixi prefix")
    lockfile = project / "pixi.lock"
    if not lockfile.is_file():
        raise ValueError("Sirius Pixi lockfile is missing")
    return {
        "project": str(project),
        "environment": "mo",
        "prefix": str(prefix),
        "lockfile": str(lockfile),
        "lock_sha256": digest(lockfile),
    }


def runtime_source(source, prefix):
    """Accept only user-space ELF under the active provider, never stubs/drivers."""
    lexical = Path(source)
    resolved = lexical.resolve()
    if (
        not resolved.is_file()
        or not resolved.is_relative_to(prefix)
        or "stubs" in lexical.parts
        or "stubs" in resolved.parts
        or resolved.name.startswith(("libcuda.so", "libnvidia-"))
    ):
        raise ValueError("unsafe Pixi runtime dependency: " + str(source))
    return resolved


def mo_gpu_runtime(provider, sdk_libraries, repo=None):
    """Record the actual GPU dependency closure selected by the built libmo."""
    repo = Path(__file__).resolve().parents[1] if repo is None else Path(repo)
    libmo = repo / "cgo/libmo.so"
    staged = repo / "lib/libmo.so"
    if not libmo.is_file() or not staged.is_file() or digest(libmo) != digest(staged):
        raise ValueError("MO GPU native library is missing or not staged")
    prefix = Path(provider["prefix"])
    search = [
        str(prefix / "lib"),
        str(prefix / "targets/x86_64-linux/lib"),
        str(repo / "thirdparties/install/lib"),
        str(repo / "cgo"),
        str(repo / "lib"),
    ]
    environment = dict(os.environ)
    environment["LD_LIBRARY_PATH"] = ":".join(
        search + ([environment["LD_LIBRARY_PATH"]] if environment.get("LD_LIBRARY_PATH") else [])
    )
    output = subprocess.run(
        ["ldd", str(libmo)], check=True, capture_output=True, text=True, env=environment
    ).stdout
    libraries = runtime_libraries(output, provider_prefix=prefix)
    gpu = {}
    baseline_roots = (repo / "thirdparties/install/lib", repo / "cgo", repo / "lib")
    for name, path in libraries.items():
        if path.is_relative_to(prefix):
            source = runtime_source(path, prefix)
            if elf_soname(source) != name:
                raise ValueError("MO GPU runtime SONAME mismatch: " + name)
            item = {"source": str(source), "sha256": digest(source)}
            sdk_source = sdk_libraries.get(name)
            if sdk_source is not None and sdk_source != source:
                raise ValueError("MO and Sirius select different runtime libraries: " + name)
            gpu[name] = item
        elif not any(path.is_relative_to(root) for root in baseline_roots):
            raise ValueError("MO GPU runtime dependency escapes Pixi and MO: " + name)
    for required in ("libcuvs.so", "libcuvs_c.so", "libcudart.so.13"):
        if required not in gpu and required not in sdk_libraries:
            raise ValueError("MO GPU runtime dependency is missing: " + required)
    return {"libmo": str(libmo), "libmo_sha256": digest(libmo), "runtime_libraries": gpu}


def verify_mo_native_generation(mode, mo_gpu, repo=None):
    if mode != "release" or mo_gpu is None:
        return
    repo = Path(__file__).resolve().parents[1] if repo is None else Path(repo)
    stamp = repo / "cgo/.mo-native-provenance"
    if not stamp.is_file():
        raise ValueError("release MO GPU native provenance is missing")
    values = dict(
        line.split("=", 1) for line in stamp.read_text().splitlines() if "=" in line
    )
    if (
        values.get("accelerator") != "gpu"
        or values.get("goos") != "linux"
        or values.get("goarch") != "amd64"
        or values.get("optimization") not in ("release", "debug")
        or values.get("simsimd") not in ("0", "1")
    ):
        raise ValueError("release MO GPU native provenance has the wrong build key")
    run(
        str(repo / "cgo/mo-native-provenance"),
        "verify",
        str(repo),
        mo_gpu["libmo"],
        "linux",
        "amd64",
        "gpu",
        values["optimization"],
        values["simsimd"],
    )


def verify_pixi_provider(provider):
    project = Path(provider["project"])
    prefix = Path(provider["prefix"])
    lockfile = Path(provider["lockfile"])
    if (
        not project.is_dir()
        or not prefix.is_dir()
        or not lockfile.is_file()
        or os.environ.get("PIXI_ENVIRONMENT_NAME") != provider["environment"]
        or not os.environ.get("PIXI_PROJECT_ROOT")
        or Path(os.environ["PIXI_PROJECT_ROOT"]).resolve() != project
        or not os.environ.get("CONDA_PREFIX")
        or Path(os.environ["CONDA_PREFIX"]).resolve() != prefix
        or lockfile.resolve() != project / "pixi.lock"
        or digest(lockfile) != provider["lock_sha256"]
    ):
        raise ValueError("Pixi provider changed between SDK preparation and packaging")
    return prefix


def mo_baseline_libraries(repo=None):
    """Bind only native libraries freshly staged by MO's build owner."""
    repo = Path(__file__).resolve().parents[1] if repo is None else Path(repo)
    sources = [repo / "cgo/libmo.so"]
    sources.extend((repo / "thirdparties/install/lib").glob("*.so*"))
    baseline = {}
    for source in sources:
        staged = repo / "lib" / source.name
        if source.is_file() and staged.is_file():
            source_hash = digest(source)
            if source_hash == digest(staged):
                baseline[source.name] = {"source": str(source), "sha256": source_hash}
    return baseline


def runtime_libraries(text, allow_missing=False, provider_prefix=None):
    libraries = {}
    # glibc and the NVIDIA driver belong to the host ABI, never the SDK bundle.
    host = re.compile(
        r"^(?:libcuda\.so(?:\..*)?|libnvidia-[A-Za-z0-9_-]+\.so(?:\..*)?|lib(?:c|m|mvec|dl|rt|pthread|resolv|util)\.so(?:\..*)?|ld-linux.*)$"
    )
    for line in text.splitlines():
        if "not found" in line:
            missing = re.match(r"\s*(\S+)\s+=>\s+not found\s*$", line)
            if missing and host.fullmatch(missing[1]):
                continue
            if (
                allow_missing
                and missing
                and Path(missing[1]).name == missing[1]
                and not host.fullmatch(missing[1])
            ):
                libraries[missing[1]] = None
                continue
            raise ValueError("unresolved Sirius runtime dependency: " + line.strip())
        match = re.match(r"\s*(\S+)\s+=>\s+(/\S+)\s+\(", line)
        if not match:
            absolute = re.match(r"\s*(/\S+)\s+\(", line)
            if absolute and not host.fullmatch(Path(absolute[1]).name):
                raise ValueError(
                    "dependency lacks a relocatable SONAME: " + absolute[1]
                )
            continue
        name, path = match.groups()
        if host.fullmatch(Path(name).name):
            if (
                provider_prefix is not None
                and name.startswith(("libcuda.so", "libnvidia-"))
                and (
                    Path(path).resolve().is_relative_to(provider_prefix)
                    or "stubs" in Path(path).parts
                )
            ):
                raise ValueError("NVIDIA driver resolved from the Pixi build provider")
            continue
        if Path(name).name != name:
            raise ValueError("dependency SONAME is not relocatable: " + name)
        libraries[name] = Path(path).resolve()
    return libraries


def prepare(args):
    manifest = validate(args.sdk, args.mode, args.merged_ref, args.mo_root)
    provider = pixi_provider(manifest)
    prefix = Path(provider["prefix"])
    libraries = runtime_libraries(run("ldd", manifest["consumer"]), provider_prefix=prefix)
    libraries = {
        name: runtime_source(path, prefix) for name, path in libraries.items()
    }
    mo_gpu = mo_gpu_runtime(provider, libraries) if os.environ.get("MO_CL_CUDA") == "1" else None
    verify_mo_native_generation(args.mode, mo_gpu)
    args.output.mkdir(parents=True, exist_ok=True)
    flags = relocatable_flags(manifest["link_arguments"])
    (args.output / "link.rsp").write_text(response(flags))
    provenance = {
        "schema_version": 1,
        "sdk": str(args.sdk.resolve()),
        "mode": args.mode,
        "source_revision": manifest["source_revision"],
        "compiler": manifest["compiler"],
        "c_compiler": manifest["c_compiler"],
        "merged_ref": args.merged_ref,
        "mo_root": str(args.mo_root.resolve()) if args.mo_root is not None else None,
        "sdk_manifest_sha256": digest(args.sdk / "link.json"),
        "artifact_sha256": manifest["artifact_sha256"],
        "runtime_libraries": {
            name: {"source": str(path), "sha256": digest(path)}
            for name, path in libraries.items()
        },
        "pixi_provider": provider,
        "mo_gpu": mo_gpu,
        "mo_baseline": mo_baseline_libraries(),
    }
    (args.output / "provenance.json").write_text(
        json.dumps(provenance, indent=2) + "\n"
    )


def stage_rpath(source, target, rpath):
    # Copy first even for an already staged file: baseline artifacts or the
    # binary may be hardlinks/symlinks to originals owned by another build.
    # copy2 also restores source permissions instead of publishing tempfile0600.
    with tempfile.NamedTemporaryFile(
        dir=target.parent, prefix=".sirius-", delete=False
    ) as temp:
        temporary = Path(temp.name)
    try:
        shutil.copy2(source, temporary)
        mode = stat.S_IMODE(temporary.stat().st_mode)
        temporary.chmod(mode | stat.S_IWUSR)
        run("patchelf", "--set-rpath", rpath, str(temporary))
        temporary.chmod(mode)
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)


def package(args):
    # Resolve the containing directory, not a possibly linked binary itself.
    binary = args.binary.parent.resolve() / args.binary.name
    output = args.output.resolve()
    if output != binary.parent / "lib":
        raise ValueError("Sirius package output must be binary.parent/lib")
    provenance = json.loads((args.prepared / "provenance.json").read_text())
    if provenance.get("mo_root") is not None:
        sdk = Path(provenance["sdk"])
        if digest(sdk / "link.json") != provenance["sdk_manifest_sha256"]:
            raise ValueError("Sirius SDK changed while linking")
        manifest = json.loads((sdk / "link.json").read_text())
        verify_source_pin(
            Path(manifest["source_directory"]),
            manifest["source_revision"],
            provenance["mo_root"],
        )
        if manifest["source_revision"] != provenance["source_revision"]:
            raise ValueError("Sirius source revision changed while linking")
        if provenance["mode"] == "release" and run(
            "git",
            "-C",
            manifest["source_directory"],
            "status",
            "--porcelain",
            "--untracked-files=normal",
        ).strip():
            raise ValueError("Sirius source became dirty while linking")
    prefix = verify_pixi_provider(provenance["pixi_provider"])
    mo_gpu = provenance.get("mo_gpu")
    gpu_libraries = mo_gpu["runtime_libraries"] if mo_gpu is not None else {}
    if mo_gpu is not None:
        expected = mo_gpu["libmo_sha256"]
        if digest(mo_gpu["libmo"]) != expected or digest(output / "libmo.so") != expected:
            raise ValueError("MO GPU native library changed while linking")
        for name, item in gpu_libraries.items():
            source = runtime_source(item["source"], prefix)
            if Path(name).name != name or digest(source) != item["sha256"]:
                raise ValueError("MO GPU runtime library changed while linking: " + name)
    # Recheck every artifact after Go linking, closing the prepare/link/stage gap.
    for name, expected in provenance["artifact_sha256"].items():
        if digest(name) != expected:
            raise ValueError("Sirius SDK changed while linking: " + name)
    for name, item in provenance["runtime_libraries"].items():
        if Path(name).name != name or Path(item["source"]).absolute() == output / name:
            raise ValueError(
                "SDK source overlaps or escapes package destination: " + name
            )
        if digest(item["source"]) != item["sha256"]:
            raise ValueError("Sirius runtime library changed while linking: " + name)
    for name, item in provenance["mo_baseline"].items():
        staged = output / name
        if (
            Path(name).name != name
            or digest(item["source"]) != item["sha256"]
            or not staged.is_file()
            or digest(staged) != item["sha256"]
        ):
            raise ValueError("MO baseline native library changed while linking: " + name)
    output.mkdir(parents=True, exist_ok=True)
    provenance["linked_binary_sha256"] = digest(binary)
    for name, item in provenance["runtime_libraries"].items():
        stage_rpath(item["source"], output / name, "$ORIGIN")
    stage_rpath(binary, binary, "$ORIGIN/lib")

    patched = set(provenance["runtime_libraries"])
    baseline_hashes = {}
    gpu_runtime_hashes = {}
    # Re-evaluate after patching each newly reachable baseline ELF. Its former
    # absolute RPATH may have selected an external library (or hidden a staged
    # transitive dependency). Never copy such baseline inputs from that path:
    # mo-stage-native-libs must already have supplied their local counterparts.
    while True:
        resolved = runtime_libraries(
            run("ldd", str(binary)), allow_missing=True, provider_prefix=prefix
        )
        pending = set(resolved) - patched
        if not pending:
            break
        for name in sorted(pending):
            staged = output / name
            gpu = gpu_libraries.get(name)
            if gpu is not None:
                source = runtime_source(gpu["source"], prefix)
                if digest(source) != gpu["sha256"]:
                    raise ValueError("MO GPU runtime library changed while packaging: " + name)
                gpu_runtime_hashes[name] = gpu["sha256"]
                stage_rpath(source, staged, "$ORIGIN")
            elif not staged.is_file():
                raise ValueError("missing staged MO runtime dependency: " + name)
            else:
                if name not in provenance["mo_baseline"]:
                    raise ValueError("unverified staged MO runtime dependency: " + name)
                baseline_hashes[name] = digest(staged)
                stage_rpath(staged, staged, "$ORIGIN")
            patched.add(name)

    resolved = runtime_libraries(run("ldd", str(binary)), provider_prefix=prefix)
    for name, actual in resolved.items():
        if actual.parent != output or actual != (output / name).resolve():
            raise ValueError("MO resolved a non-local packaged dependency: " + name)
    provenance["binary_sha256"] = digest(binary)
    provenance["baseline_input_sha256"] = baseline_hashes
    provenance["gpu_runtime_input_sha256"] = gpu_runtime_hashes
    provenance["packaged_sha256"] = {
        name: digest(output / name) for name in sorted(patched)
    }
    (output / "sirius-provenance.json").write_text(
        json.dumps(provenance, indent=2) + "\n"
    )


def verify_package(args):
    prepared = json.loads((args.prepared / "provenance.json").read_text())
    sdk = Path(prepared["sdk"])
    if getattr(args, "sdk", None) is not None and args.sdk.resolve() != sdk:
        raise ValueError("selected Sirius SDK differs from the packaged SDK")
    validate(sdk, prepared["mode"], prepared["merged_ref"], prepared.get("mo_root"))
    if digest(sdk / "link.json") != prepared["sdk_manifest_sha256"]:
        raise ValueError("Sirius SDK changed after package preparation")
    verify_pixi_provider(prepared["pixi_provider"])
    for name, expected in prepared["artifact_sha256"].items():
        if digest(name) != expected:
            raise ValueError("Sirius SDK artifact changed after packaging: " + name)
    for name, item in prepared["mo_baseline"].items():
        if digest(item["source"]) != item["sha256"]:
            raise ValueError("MO baseline native library changed after packaging: " + name)
    mo_gpu = prepared.get("mo_gpu")
    if mo_gpu is not None:
        if digest(mo_gpu["libmo"]) != mo_gpu["libmo_sha256"]:
            raise ValueError("MO GPU native library changed after packaging")
        prefix = Path(prepared["pixi_provider"]["prefix"])
        for name, item in mo_gpu["runtime_libraries"].items():
            if digest(runtime_source(item["source"], prefix)) != item["sha256"]:
                raise ValueError("MO GPU runtime library changed after packaging: " + name)
    packaged = json.loads((args.output / "sirius-provenance.json").read_text())
    for key, value in prepared.items():
        if packaged.get(key) != value:
            raise ValueError("packaged Sirius runtime is stale: " + key)
    hashes = packaged.get("packaged_sha256")
    if not isinstance(hashes, dict) or not hashes:
        raise ValueError("packaged Sirius runtime has no dependency closure")
    for name, expected in hashes.items():
        path = args.output / name
        if Path(name).name != name or not path.is_file() or digest(path) != expected:
            raise ValueError("packaged Sirius runtime changed: " + name)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("prepare")
    p.add_argument("--sdk", type=Path, required=True)
    p.add_argument("--mode", choices=("release", "development"), default="release")
    p.add_argument("--merged-ref", default="")
    p.add_argument("--mo-root", type=Path)
    p.add_argument("--output", type=Path, required=True)
    p.set_defaults(action=prepare)
    p = commands.add_parser("package")
    p.add_argument("--prepared", type=Path, required=True)
    p.add_argument("--binary", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.set_defaults(action=package)
    p = commands.add_parser("verify-package")
    p.add_argument("--prepared", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--sdk", type=Path)
    p.set_defaults(action=verify_package)
    args = parser.parse_args()
    args.action(args)


if __name__ == "__main__":
    main()
