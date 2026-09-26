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


def validate(sdk, mode, merged_ref):
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
    if run("git", "-C", str(source), "rev-parse", "HEAD").strip() != revision:
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


def runtime_libraries(text, allow_missing=False):
    libraries = {}
    # glibc and the NVIDIA driver belong to the host ABI, never the SDK bundle.
    host = re.compile(
        r"^(?:lib(?:cuda|nvidia-ml)\.so(?:\..*)?|lib(?:c|m|mvec|dl|rt|pthread|resolv|util)\.so(?:\..*)?|ld-linux.*)$"
    )
    for line in text.splitlines():
        if "not found" in line:
            missing = re.match(r"\s*(\S+)\s+=>\s+not found\s*$", line)
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
            continue
        if Path(name).name != name:
            raise ValueError("dependency SONAME is not relocatable: " + name)
        libraries[name] = Path(path).resolve()
    return libraries


def prepare(args):
    manifest = validate(args.sdk, args.mode, args.merged_ref)
    libraries = runtime_libraries(run("ldd", manifest["consumer"]))
    args.output.mkdir(parents=True, exist_ok=True)
    flags = relocatable_flags(manifest["link_arguments"])
    (args.output / "link.rsp").write_text(response(flags))
    provenance = {
        "schema_version": 1,
        "mode": args.mode,
        "source_revision": manifest["source_revision"],
        "compiler": manifest["compiler"],
        "c_compiler": manifest["c_compiler"],
        "merged_ref": args.merged_ref,
        "sdk_manifest_sha256": digest(args.sdk / "link.json"),
        "artifact_sha256": manifest["artifact_sha256"],
        "runtime_libraries": {
            name: {"source": str(path), "sha256": digest(path)}
            for name, path in libraries.items()
        },
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
    output.mkdir(parents=True, exist_ok=True)
    provenance["linked_binary_sha256"] = digest(binary)
    for name, item in provenance["runtime_libraries"].items():
        stage_rpath(item["source"], output / name, "$ORIGIN")
    stage_rpath(binary, binary, "$ORIGIN/lib")

    patched = set(provenance["runtime_libraries"])
    baseline_hashes = {}
    # Re-evaluate after patching each newly reachable baseline ELF. Its former
    # absolute RPATH may have selected an external library (or hidden a staged
    # transitive dependency). Never copy such baseline inputs from that path:
    # mo-stage-native-libs must already have supplied their local counterparts.
    while True:
        resolved = runtime_libraries(run("ldd", str(binary)), allow_missing=True)
        pending = set(resolved) - patched
        if not pending:
            break
        for name in sorted(pending):
            staged = output / name
            if not staged.is_file():
                raise ValueError("missing staged MO runtime dependency: " + name)
            baseline_hashes[name] = digest(staged)
            stage_rpath(staged, staged, "$ORIGIN")
            patched.add(name)

    resolved = runtime_libraries(run("ldd", str(binary)))
    for name, actual in resolved.items():
        if actual.parent != output or actual != (output / name).resolve():
            raise ValueError("MO resolved a non-local packaged dependency: " + name)
    provenance["binary_sha256"] = digest(binary)
    provenance["baseline_input_sha256"] = baseline_hashes
    provenance["packaged_sha256"] = {
        name: digest(output / name) for name in sorted(patched)
    }
    (output / "sirius-provenance.json").write_text(
        json.dumps(provenance, indent=2) + "\n"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("prepare")
    p.add_argument("--sdk", type=Path, required=True)
    p.add_argument("--mode", choices=("release", "development"), default="release")
    p.add_argument("--merged-ref", default="")
    p.add_argument("--output", type=Path, required=True)
    p.set_defaults(action=prepare)
    p = commands.add_parser("package")
    p.add_argument("--prepared", type=Path, required=True)
    p.add_argument("--binary", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.set_defaults(action=package)
    args = parser.parse_args()
    args.action(args)


if __name__ == "__main__":
    main()
