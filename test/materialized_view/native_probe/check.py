#!/usr/bin/env python3
"""Exercise the recovery SQL bridge against an existing native BVT endpoint."""

import json
import pathlib
import subprocess
import sys


def check(binary, dsn):
    def request(sql, query=True):
        return json.dumps({"dsn": dsn, "sql": sql, "query": query})

    statements = [
        "{",  # Malformed JSON must not terminate the following requests.
        json.dumps({"dsn": "invalid-dsn", "sql": "select 1", "query": True}),
        request("select 1,cast(null as char),cast('bridge' as char) union all select 2,'value','tail'"),
        request("set @mv_native_probe_value=23", query=False),
        request("select @mv_native_probe_value"),
        request("select 1 where false"),
        request("select * from mo_catalog.__mv_native_probe_missing__"),
        request("insert into mo_catalog.__mv_native_probe_missing__ values(1)", query=False),
        request("select @mv_native_probe_value+1"),
    ]
    result = subprocess.run(
        [binary], input="\n".join(statements) + "\n", text=True,
        capture_output=True, timeout=90,
    )
    if result.returncode:
        raise RuntimeError("SQL bridge failed: " + result.stderr)
    responses = [json.loads(line) for line in result.stdout.splitlines()]
    assert len(responses) == len(statements), responses
    assert responses[0]["error"].startswith("unexpected end of JSON input"), responses[0]
    assert "invalid DSN" in responses[1]["error"], responses[1]
    expected = {
        2: [["1", "<NULL>", "bridge"], ["2", "value", "tail"]],
        3: None,
        4: [["23"]],  # Same DSN reuses its session across Exec and Query.
        5: [],  # Zero rows and SQL NULL remain distinct protocol values.
        8: [["24"]],  # SQL errors must not poison the stream/session.
    }
    for index, rows in expected.items():
        assert responses[index] == {"rows": rows, "error": ""}, (index, responses[index])
    for index in (6, 7):
        assert responses[index]["rows"] is None and "Error 1146" in responses[index]["error"], (index, responses[index])
    return {"passed_requests": len(responses), "process_exit": result.returncode}


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("Usage: check.py <native-probe-binary> <BVT-DSN>")
    # Two independent client lifetimes exercise the same native instance. Only
    # connection-local state is changed; EOF closes the bridge's SQL resources.
    print(json.dumps({"runs": [check(str(pathlib.Path(sys.argv[1]).resolve()), sys.argv[2]) for _ in range(2)]}))
