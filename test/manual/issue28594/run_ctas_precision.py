#!/usr/bin/env python3
# Copyright 2026 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run the CTAS contract probes against an already running isolated MO service."""

import os
from pathlib import Path
import subprocess
import sys
import uuid


def main():
    # Arguments are passed directly to mysql (e.g. --defaults-extra-file=...).
    # Use its option file or MYSQL_PWD for credentials, not a shell command.
    command = [
        os.environ.get("MYSQL_CLIENT", "mysql"),
        *sys.argv[1:],
        "--batch", "--raw", "--skip-column-names", "--connect-timeout=10",
    ]

    def execute(sql):
        result = subprocess.run(
            command, input=sql, text=True, capture_output=True, timeout=120,
            check=True,
        )
        return result.stdout

    database = "qa_div_precision_" + uuid.uuid4().hex
    execute(f"create database {database};")
    try:
        sql = Path(__file__).with_name("ctas_precision.sql").read_text()
        output = execute(f"use {database};\n{sql}")
        expected_ids = {f"C{i:02d}" for i in range(1, 18)}
        seen = set()
        failed = []
        print("case\texpected\tactual\tmatches")
        for line in output.splitlines():
            fields = line.split("\t")
            if len(fields) != 4 or fields[0] not in expected_ids or fields[0] in seen:
                raise ValueError(f"Unexpected or duplicate result: {line!r}")
            seen.add(fields[0])
            print(line)
            if fields[3] != "1" or fields[1] != fields[2]:
                failed.append(fields[0])
        if seen != expected_ids:
            raise ValueError(f"Missing results: {sorted(expected_ids - seen)}")
        print(f"{len(seen) - len(failed)}/{len(seen)} passed; failures: {','.join(failed) or 'none'}")
        return bool(failed)
    finally:
        execute(f"drop database {database};")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except subprocess.CalledProcessError as error:
        print(error.stderr, file=sys.stderr)
        sys.exit(2)
