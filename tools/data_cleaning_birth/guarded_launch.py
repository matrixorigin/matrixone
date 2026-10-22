#!/usr/bin/env python3
"""One-shot launcher for isolated same-binary M0 birth-fill experiments.

This is not a controller, cluster upgrader, or protection against a same-user
manual bypass. Every launch/restart/recovery in a qualified run must use it.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import tomllib

MARKER = ".m0-birth-format-v1.json"
LOCK = ".m0-birth.lock"
MARKER_KEYS = {"format_version", "binary_sha256", "source_sha", "native_sha256"}
MANIFEST_KEYS = MARKER_KEYS | {"run_id", "root", "args", "files"}


def sha256(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def inside(root, path):
    path = Path(path)
    if not path.is_absolute() or str(path) != os.path.normpath(str(path)):
        raise ValueError("paths must be absolute and clean")
    if not path.resolve().is_relative_to(root):
        raise ValueError("path escapes managed root")
    return path


def read_json(path, keys):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON field")
            result[key] = value
        return result
    value = json.loads(Path(path).read_text(), object_pairs_hook=unique)
    if set(value) != keys:
        raise ValueError("unknown or missing identity fields")
    return value


def write_new(path, value):
    with Path(path).open("x") as stream:
        os.chmod(path, 0o600)
        json.dump(value, stream, sort_keys=True, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def configs(root, launch):
    launch = inside(root, launch)
    with launch.open("rb") as stream:
        settings = tomllib.load(stream)
    if settings.get("dynamic", {}).get("enable") or settings.get("proxy-services") or settings.get("python-udf-services"):
        raise ValueError("only a static in-process LOG/TN/CN deployment is supported")
    files = {str(launch): sha256(launch)}
    data_paths = []
    for key, role in (("logservices", "LOG"), ("tnservices", "TN"), ("cnservices", "CN")):
        members = settings.get(key, [])
        if not members or (role != "CN" and len(members) != 1):
            raise ValueError("one LOG, one TN and at least one CN are required")
        for item in members:
            path = inside(root, item)
            if str(path) in files:
                raise ValueError("duplicate role/configuration")
            with path.open("rb") as stream:
                config = tomllib.load(stream)
            if config.get("service-type") != role:
                raise ValueError("role differs from configuration")
            data_paths.append(inside(root, config["data-dir"]))
            for service in config.get("fileservice", []):
                if service["backend"] not in {"DISK", "DISK-V2", "DISK-ETL", "DISK-TMP"}:
                    raise ValueError("remote/object storage is outside this deployment contract")
                data_paths.append(inside(root, service["data-dir"]))
                cache = service.get("cache", {}).get("disk-path")
                if cache:
                    data_paths.append(inside(root, cache))
            files[str(path)] = sha256(path)
    return files, data_paths


def identity(binary, source, native):
    if not re.fullmatch(r"[0-9a-f]{40}", source):
        raise ValueError("source must be a full lowercase commit SHA")
    return {"format_version": 1, "binary_sha256": sha256(binary),
            "source_sha": source, "native_sha256": sha256(native)}


def prepare(root, binary, source, native, launch, run_id, processors=8):
    root = Path(root).resolve(strict=True)
    files, data_paths = configs(root, Path(launch))
    marker = identity(binary, source, native)
    marker_path = root / MARKER
    manifest_path = root / "birth-manifest.json"
    if marker_path.exists() or manifest_path.exists():
        raise ValueError("prepare never replaces an existing format identity")
    if any(path.exists() and (not path.is_dir() or any(path.iterdir())) for path in data_paths):
        raise ValueError("install the guard before populating any data directory")
    if not run_id or processors < 1:
        raise ValueError("run identity and CPU count are required")
    manifest = dict(marker, run_id=run_id, root=str(root),
                    args=["-launch", str(launch), "-max-processor", str(processors)], files=files)
    # Failure leaves a conservative marker; there is no automatic rollback or
    # replacement that could accidentally reopen candidate data with old code.
    write_new(marker_path, marker)
    write_new(manifest_path, manifest)
    return manifest_path


def verify(manifest_path, binary):
    manifest_path = Path(manifest_path).resolve(strict=True)
    manifest = read_json(manifest_path, MANIFEST_KEYS)
    root = Path(manifest["root"])
    if not root.is_absolute() or root.resolve(strict=True) != root:
        raise ValueError("managed root identity differs")
    inside(root, manifest_path)
    marker = read_json(root / MARKER, MARKER_KEYS)
    if marker != {key: manifest[key] for key in MARKER_KEYS} or marker["format_version"] != 1:
        raise ValueError("candidate format/binary binding differs")
    if sha256(binary) != marker["binary_sha256"]:
        raise ValueError("binary differs; candidate data cannot be opened by an old binary")
    args = manifest["args"]
    if len(args) != 4 or args[0] != "-launch" or args[2] != "-max-processor" or int(args[3]) < 1:
        raise ValueError("only the frozen launch command is allowed")
    actual, _ = configs(root, args[1])
    if actual != manifest["files"]:
        raise ValueError("configuration content or membership changed")
    return manifest


def run(manifest_path, binary):
    manifest = verify(manifest_path, binary)
    root = Path(manifest["root"])
    lock_path = inside(root, root / LOCK)
    lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    # Check again while holding the lifetime lock. The controller separately
    # supplies exclusive host ownership and the resource contract.
    verify(manifest_path, binary)
    os.set_inheritable(lock_fd, True)
    os.environ["MO_M0_BIRTH_MANIFEST"] = str(Path(manifest_path).resolve())
    os.environ["MO_M0_BIRTH_LOCK_FD"] = str(lock_fd)
    os.chdir(root)
    os.execv(str(Path(binary).resolve()), [str(binary), *manifest["args"]])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    create = sub.add_parser("prepare")
    for name in ("root", "binary", "source", "native", "launch", "run-id"):
        create.add_argument("--" + name, required=True)
    create.add_argument("--processors", type=int, default=8)
    for command in ("verify", "run"):
        child = sub.add_parser(command)
        child.add_argument("--manifest", required=True)
        child.add_argument("--binary", required=True)
    args = parser.parse_args()
    if args.command == "prepare":
        print(prepare(args.root, args.binary, args.source, args.native, args.launch, args.run_id, args.processors))
    elif args.command == "verify":
        print(json.dumps(verify(args.manifest, args.binary), sort_keys=True))
    else:
        run(args.manifest, args.binary)


if __name__ == "__main__":
    main()
