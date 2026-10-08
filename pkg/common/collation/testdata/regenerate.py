#!/usr/bin/env python3
"""Regenerate the independent oracle using an explicitly selected MySQL container.

The container must be task-owned: this creates/replaces mo28164_oracle tables.
It does not start a server, expose ports, or change the default Docker context.
"""
import argparse
import base64
import gzip
import hashlib
import json
from pathlib import Path
import subprocess

ap = argparse.ArgumentParser()
ap.add_argument("--context", required=True)
ap.add_argument("--container", required=True)
ap.add_argument("--fixture", type=Path, default=Path(__file__).with_name("mysql_8_0_45.json.gz"))
ap.add_argument("--evidence-dir", type=Path, required=True)
args = ap.parse_args()
args.evidence_dir.mkdir(parents=True, exist_ok=True)
docker = ["docker", "--context", args.context]
image = "mysql@sha256:4af1f8815716546f5b12410f7621f37f93db8dd11a184706ef59111930b8c2ff"
container_image = subprocess.check_output(docker + ["inspect", "--format", "{{.Image}}", args.container], text=True).strip()
digests = json.loads(subprocess.check_output(docker + ["image", "inspect", "--format", "{{json .RepoDigests}}", container_image], text=True))
if image not in digests:
    raise SystemExit("unexpected MySQL image digest")
mysql = docker + ["exec", "-i", args.container, "mysql", "-uroot", "-N", "-B", "--binary-mode"]
def query(name, sql):
    (args.evidence_dir / (name + ".sql")).write_text(sql)
    result = subprocess.run(mysql, input=sql.encode(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    (args.evidence_dir / (name + ".tsv")).write_bytes(result.stdout)
    return result.stdout.decode()
server = query("server", "SELECT VERSION(),@@version_compile_machine;").strip()
if server != "8.0.45\taarch64":
    raise SystemExit("unexpected server build: " + server)
# Existing corpus supplies inputs only. Every expected answer below is freshly
# obtained from the server, never from the Go implementation or old answers.
values = json.loads(gzip.decompress(args.fixture.read_bytes()))["values"]
sql = "CREATE DATABASE IF NOT EXISTS mo28164_oracle; USE mo28164_oracle; DROP TABLE IF EXISTS samples; CREATE TABLE samples(id INT PRIMARY KEY,v VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin);\n"
sql += "INSERT INTO samples VALUES " + ",".join("(%d,CONVERT(X'%s' USING utf8mb4))" % (i, v.encode().hex()) for i, v in enumerate(values)) + ";\n"
collations = ["utf8mb4_bin", "utf8mb4_general_ci", "utf8mb4_0900_ai_ci"]
for c in collations:
    a, b = "a.v COLLATE " + c, "b.v COLLATE " + c
    sql += "SELECT '%s',a.id,b.id,CASE WHEN %s=%s THEN 0 WHEN %s<%s THEN -1 ELSE 1 END FROM samples a CROSS JOIN samples b ORDER BY a.id,b.id;\n" % (c, a, b, a, b)
order = {c: bytearray() for c in collations}
for row in query("order", sql).splitlines():
    c, a, b, sign = row.split("\t")
    assert int(a)*len(values)+int(b) == len(order[c])
    order[c].append(int(sign)+1)
points = [r for r in range(0x10000) if not 0xd800 <= r <= 0xdfff] + [0x10000+i*509 for i in range(2048)]
sql = "USE mo28164_oracle; DROP TABLE IF EXISTS codepoints; CREATE TABLE codepoints(id INT PRIMARY KEY,v VARCHAR(1) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin);\n"
for pos in range(0, len(points), 512):
    sql += "INSERT INTO codepoints VALUES " + ",".join("(%d,CONVERT(X'%s' USING utf8mb4))" % (r, chr(r).encode().hex()) for r in points[pos:pos+512]) + ";\n"
sql += "SELECT id,HEX(WEIGHT_STRING(v COLLATE utf8mb4_general_ci)),HEX(WEIGHT_STRING(v COLLATE utf8mb4_bin)) FROM codepoints ORDER BY id;\n"
general, binary = hashlib.sha256(), hashlib.sha256()
rows = query("weights", sql).splitlines()
assert len(rows) == len(points)
for expected, row in zip(points, rows):
    r, g, b = row.split("\t")
    assert int(r) == expected
    general.update(bytes.fromhex(g))
    binary.update(bytes.fromhex(b))
fixture = {"server": server, "image": image, "values": values, "order": {c:base64.b64encode(v).decode() for c,v in order.items()}, "general_weight_sha256":general.hexdigest(), "bin_weight_sha256":binary.hexdigest()}
args.fixture.write_bytes(gzip.compress(json.dumps(fixture,ensure_ascii=True).encode(),mtime=0))
print("PASS: 126075 comparison results and 65536 character weight pairs")
