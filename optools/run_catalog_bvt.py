"""Run SHOW ACCOUNTS scripts on the full CN, preserving group ordering and sessions."""

import itertools
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

import yaml


SHOW = re.compile(r"^\s*show\s+accounts\b", re.IGNORECASE | re.MULTILINE)


def needs_full(script):
    return bool(SHOW.search(Path(script).read_text(encoding="utf-8")))


def main():
    tester, cases, resources, addr, debug_port = sys.argv[1:]
    tester = Path(tester)
    scripts = [line.rstrip("\n") for line in sys.stdin]
    with tempfile.TemporaryDirectory(prefix="bvt-full-") as tmp:
        full = Path(tmp)
        conf = yaml.safe_load((tester / "mo.yml").read_text())
        conf["jdbc"]["server"] = [{"addr": addr}]
        conf["debug"].update(serverIP=addr.rsplit(":", 1)[0], port=int(debug_port))
        (full / "mo.yml").write_text(yaml.safe_dump(conf))
        for name in ("run.yml", "pprof.sh", "log4j.properties"):
            if (tester / name).exists():
                (full / name).symlink_to(tester / name)
        (full / "log").mkdir()
        classes = full / "classes"
        classes.mkdir()
        jars = sorted((tester / "target").glob("mo-tester-*.jar"))
        cp = os.pathsep.join(map(str, [classes, *(j for j in jars if not j.name.endswith("-test.jar")), tester / "lib/*"]))
        subprocess.run(["javac", "-source", "8", "-target", "8", "-cp", cp, "-d", str(classes),
                        str(Path(__file__).with_name("CatalogBvtBatch.java"))], check=True)
        # mo-tester traverses child directories before files in each directory.
        def traversal_key(script):
            parts = Path(script).relative_to(cases).parts
            return tuple((i == len(parts) - 1, part) for i, part in enumerate(parts))

        groups = [(key, list(batch)) for key, batch in itertools.groupby(sorted(scripts, key=traversal_key), needs_full)]
        for index, (dedicated, selected) in enumerate(groups):
            where = full if dedicated else tester
            if dedicated:
                print(f"Full catalog CN {addr}: {', '.join(selected)}", flush=True)
            cmd = ["java", "-Xms128m", "-Xmx1024m", "-cp", str(where) + os.pathsep + cp, "CatalogBvtBatch", cases, resources,
                   str(index == 0).lower(), str(index == len(groups) - 1).lower(), *selected]
            try:
                subprocess.run(cmd, cwd=where, check=True)
            finally:
                for report in (where / "report").glob("*"):
                    if report.is_file():
                        dest = tester / "report" / f"batch-{index}"
                        dest.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(report, dest)


if __name__ == "__main__":
    main()
