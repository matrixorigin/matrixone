#!/usr/bin/env python3
"""Exercise PR #30071 review boundaries on a built, isolated local MO service."""

import argparse
import json
import uuid

import pymysql


def scalar_json(value):
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    return json.loads(value) if isinstance(value, str) else value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=16001)
    args = parser.parse_args()
    connection = pymysql.connect(
        host="127.0.0.1", port=args.port, user="root", password="111",
        autocommit=True, connect_timeout=10, read_timeout=120,
    )
    database = "review_json_30071_" + uuid.uuid4().hex[:10]
    created = False
    checks = 0
    try:
        with connection.cursor() as cur:
            cur.execute("create database " + database)
            created = True
            cur.execute("use " + database)
            cur.execute("set sql_mode = ''")
            keys = ["", "a.b", '"', "\\", "<>&", "\u2028", "\u2029", "中文🙂"]
            keys += [chr(c) for c in range(32)]
            keys += [chr(c) for c in range(0x7F, 0xA0)]
            cur.execute("create table controls(id int primary key, j json)")
            cur.executemany(
                "insert into controls values(%s,json_parse_exact(%s))",
                [(i, json.dumps({key: 1000 + i}, ensure_ascii=False)) for i, key in enumerate(keys)],
            )
            cur.execute("select mo_ctl('dn','flush',%s)", (database + ".controls",))
            cur.fetchall()
            for i, key in enumerate(keys):
                path = "$." + json.dumps(key, ensure_ascii=False)
                cur.execute(
                    "select json_extract(j," + connection.escape(path) + ") from controls where id=%s", (i,),
                )
                assert scalar_json(cur.fetchone()[0]) == 1000 + i, repr(key)
                checks += 1
            cur.execute("select id,j from controls order by id")
            for i, value in cur.fetchall():
                assert scalar_json(value) == {keys[i]: 1000 + i}, repr(keys[i])
                checks += 1

            cur.execute(
                "create table wide(id int primary key," +
                ",".join("j%d json" % i for i in range(65)) + ")"
            )
            cur.execute(
                "insert into wide values(0," + ",".join("json_parse_exact(%s)" for _ in range(65)) + ")",
                tuple(json.dumps({"x": i}) for i in range(65)),
            )
            cur.execute("select mo_ctl('dn','flush',%s)", (database + ".wide",))
            cur.fetchall()
            for columns in (63, 64, 65):
                cur.execute(
                    "select " + ",".join("json_extract(j%d,'$.x')" % i for i in range(columns)) + " from wide"
                )
                assert [scalar_json(v) for v in cur.fetchone()] == list(range(columns))
                checks += 1

            prefix = "mo-json-subcolumns-v1:"
            base_size = len(prefix) + len(json.dumps({"1": ["$.x"]}, separators=(",", ":")))
            path_cases = []
            for size in (65535, 65536, 65537):
                key = "x" * (size - base_size + 1)
                path = "$." + key
                encoded_size = len(prefix) + len(json.dumps({"1": [path]}, separators=(",", ":")))
                assert encoded_size == size
                path_cases.append((key, path, encoded_size))
            key = '"' * 20000
            path = "$." + json.dumps(key)
            encoded_size = len(prefix) + len(json.dumps({"1": [path]}, separators=(",", ":")))
            assert len(path) < 65536 < encoded_size
            path_cases.append((key, path, encoded_size))
            cur.execute("create table longpaths(id int primary key,j json)")
            cur.executemany(
                "insert into longpaths values(%s,json_parse_exact(%s))",
                [(i, json.dumps({key: 2000 + i})) for i, (key, _, _) in enumerate(path_cases)],
            )
            cur.execute("select mo_ctl('dn','flush',%s)", (database + ".longpaths",))
            cur.fetchall()
            for i, (_, path, _) in enumerate(path_cases):
                cur.execute(
                    "select json_extract(j," + connection.escape(path) + ") from longpaths where id=%s", (i,),
                )
                assert scalar_json(cur.fetchone()[0]) == 2000 + i, i
                checks += 1
            print(json.dumps({
                "result": "PASS", "sql_checks": checks, "key_cases": len(keys),
                "wide_columns": [63, 64, 65], "contract_bytes": [v[2] for v in path_cases],
                "flush_tables": 3,
            }), flush=True)
    finally:
        if created:
            with connection.cursor() as cur:
                cur.execute("drop database " + database)
        connection.close()


if __name__ == "__main__":
    main()
