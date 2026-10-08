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

"""Align MO's small GPU profile with Sirius's `mo` feature, then re-lock MO.

The Sirius lock contains many unrelated environments and is not copied into
MO's standalone profile. Pixi owns the complete transitive resolution here.
"""

import argparse
from pathlib import Path
import re
import subprocess
import sys


MO_MANIFEST = Path(__file__).resolve().with_name("pixi.toml")
SHARED = (
    "cuda-version",
    "libcuvs",
    "librmm",
    "gcc_linux-64",
    "gxx_linux-64",
    "patchelf",
)
ASSIGNMENT = re.compile(r'^([A-Za-z0-9_-]+)\s*=\s*"([^"\n]+)"\s*(?:#.*)?$')
SECTION = re.compile(r"^\[([^]]+)\]$")
CUDA_VIRTUAL = re.compile(r'\bcuda\s*=\s*"([0-9]+(?:\.[0-9]+)?)"')


def read_dependencies(manifest: Path, section: str) -> dict[str, str]:
    """Read the direct string dependencies used by these two Pixi manifests."""
    found = False
    in_section = False
    values = {}
    for number, line in enumerate(manifest.read_text().splitlines(), 1):
        stripped = line.strip()
        header = SECTION.fullmatch(stripped)
        if header:
            in_section = header.group(1) == section
            found = found or in_section
            continue
        if not in_section or not stripped or stripped.startswith("#"):
            continue
        assignment = ASSIGNMENT.fullmatch(stripped)
        if not assignment:
            raise ValueError(f"{manifest}:{number}: unsupported dependency syntax")
        name, constraint = assignment.groups()
        if name in values:
            raise ValueError(f"{manifest}:{number}: duplicate dependency {name}")
        values[name] = constraint
    if not found:
        raise ValueError(f"{manifest}: missing [{section}]")
    return values


def desired_constraints(sirius_manifest: Path) -> dict[str, str]:
    sirius = read_dependencies(sirius_manifest, "feature.mo.dependencies")
    missing = [name for name in SHARED if name not in sirius]
    if missing:
        raise ValueError(f"{sirius_manifest}: missing shared MO dependencies: {missing}")
    desired = {name: sirius[name] for name in SHARED}
    desired["cuda-nvcc"] = sirius["cuda-version"]
    cuvs = re.fullmatch(r"==([0-9]+)\.([0-9]+)\.[0-9]+", sirius["libcuvs"])
    if not cuvs:
        raise ValueError("Sirius's MO libcuvs version must be an exact major.minor.patch")
    desired["libraft"] = f"{cuvs.group(1)}.{cuvs.group(2)}.*"
    return desired


def pixi_spec(name: str, constraint: str) -> str:
    return name + (constraint if constraint[0] in "=<>~!" else "=" + constraint)


def declared_cuda(manifest: Path) -> str:
    workspace = manifest.read_text().split("[dependencies]", 1)[0]
    versions = CUDA_VIRTUAL.findall(workspace)
    if len(versions) != 1:
        raise ValueError(f"{manifest}: expected one MO GPU __cuda virtual package")
    return versions[0]


def main(argv=None, mo_manifest: Path = MO_MANIFEST) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sirius-manifest", required=True, type=Path)
    parser.add_argument("--check", action="store_true", help="report drift without modifying MO")
    args = parser.parse_args(argv)
    # The manifest is an upgrade source only when Sirius has locked it. Avoid
    # copying an unverified proposal into MO's standalone dependency graph.
    subprocess.run(
        ["pixi", "lock", "--check", "--offline", "--manifest-path",
         str(args.sirius_manifest)],
        check=True,
    )
    desired = desired_constraints(args.sirius_manifest)
    cuda = re.fullmatch(r"([0-9]+\.[0-9]+)\.\*", desired["cuda-version"])
    if not cuda:
        raise ValueError("Sirius's MO cuda-version must pin a major.minor series")
    desired_cuda = cuda.group(1)
    current_cuda = declared_cuda(mo_manifest)
    current = read_dependencies(mo_manifest, "dependencies")
    missing = [name for name in desired if name not in current]
    if missing:
        raise ValueError(f"{mo_manifest}: missing MO GPU dependencies: {missing}")
    changes = {name: value for name, value in desired.items() if current[name] != value}
    if not changes and current_cuda == desired_cuda:
        print("MO GPU constraints already match Sirius's mo profile")
        return 0
    if current_cuda != desired_cuda:
        print(f"MO platform __cuda: {current_cuda} -> {desired_cuda}")
    for name, value in changes.items():
        print(f"{name}: {current[name]} -> {value}")
    if args.check:
        return 1
    if current_cuda != desired_cuda:
        print(
            "Update MO's virtual CUDA version first with: pixi workspace platform "
            f"edit --no-install --manifest-path {mo_manifest} "
            f"--cuda {desired_cuda} linux-64-cuda13",
            file=sys.stderr,
        )
        return 2
    subprocess.run(
        ["pixi", "add", "--no-install", "--manifest-path", str(mo_manifest),
         *(pixi_spec(name, value) for name, value in changes.items())],
        check=True,
    )
    print("MO pixi.toml and pixi.lock updated; verify the GPU-only and combined builds")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
