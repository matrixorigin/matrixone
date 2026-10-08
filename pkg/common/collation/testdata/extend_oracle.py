#!/usr/bin/env python3
"""Collect only missing/extended MySQL answers; never overwrite the old oracle."""
import argparse
import base64
import gzip
import hashlib
import json
from pathlib import Path
import subprocess

IMAGE = 'mysql@sha256:4af1f8815716546f5b12410f7621f37f93db8dd11a184706ef59111930b8c2ff'
p = argparse.ArgumentParser()
p.add_argument('--context', required=True)
p.add_argument('--container', required=True)
p.add_argument('--evidence-dir', type=Path, required=True)
a = p.parse_args()
a.evidence_dir.mkdir(parents=True, exist_ok=True)
root = Path(__file__).parent
docker = ['docker', '--context', a.context]
image = subprocess.check_output(docker + ['inspect', '--format', '{{.Image}}', a.container], text=True).strip()
digests = json.loads(subprocess.check_output(docker + ['image', 'inspect', '--format', '{{json .RepoDigests}}', image], text=True))
if IMAGE not in digests:
    raise SystemExit('unexpected image')
def query(name, sql):
    (a.evidence_dir / (name + '.sql')).write_text(sql)
    result = subprocess.run(docker + ['exec', '-i', a.container, 'mysql', '-uroot', '-N', '-B', '--binary-mode'], input=sql.encode(), capture_output=True, check=True)
    (a.evidence_dir / (name + '.tsv')).write_bytes(result.stdout)
    return result.stdout.decode().splitlines()
server = query('server', 'SELECT VERSION(),@@version_compile_machine;')[0]
if server != '8.0.45\taarch64':
    raise SystemExit('unexpected server build')
old_bytes = (root / 'mysql_8_0_45.json.gz').read_bytes()
old = json.loads(gzip.decompress(old_bytes))
values = old['values'].copy()
extra = ['\u00ad', '\u034f', '\u200b', '\ufeff', 'a\u00adb', 'ab', '\ufb03', 'ffi', '\u0130', 'i\u0307', '\u212b', 'A\u030a', '\u1e9e', 'ss', '\ufdfa', 'ا', '\uff21', 'A', '\U00010400', '\U00010428', '\U0010ffff', '\u0378', '\u0000\u0000', 'a\u0000\u0000', 'a\u0301\u0327', 'a\u0327\u0301', 'x'*4096, 'x'*4096+' ', 'x'*4096+'\x00', 'é'*1024, 'e\u0301'*1024, '😀'*1024]
# Prefix inputs are obtained by character slicing, never by slicing weights.
for s in ['aé😀z', '\ufb03x', 'e\u0301x', 'a \x00b']:
    extra.extend(s[:n] for n in range(len(s)+1))
for v in extra:
    if v not in values:
        values.append(v)
n0, n = len(old['values']), len(values)
collations = ['utf8mb4_bin', 'utf8mb4_general_ci', 'utf8mb4_0900_ai_ci', 'utf8mb4_0900_bin']
setup = 'CREATE DATABASE IF NOT EXISTS mo28164_freeze; USE mo28164_freeze; DROP TABLE IF EXISTS samples; CREATE TABLE samples(id INT PRIMARY KEY,v LONGTEXT CHARACTER SET utf8mb4);\n'
setup += 'INSERT INTO samples VALUES ' + ','.join("(%d,CONVERT(X'%s' USING utf8mb4))" % (i, v.encode().hex()) for i,v in enumerate(values)) + ';\n'
query('setup', setup)
orders = {}
for c in collations:
    # Reuse old comparison answers, querying only missing pairs.
    result = bytearray(n*n)
    if c in old['order']:
        prior = base64.b64decode(old['order'][c])
        assert len(prior) == n0*n0
        for i in range(n0):
            result[i*n:i*n+n0] = prior[i*n0:(i+1)*n0]
    where = '' if c not in old['order'] else f' WHERE a.id >= {n0} OR b.id >= {n0}'
    left, right = 'a.v COLLATE '+c, 'b.v COLLATE '+c
    sql = f'USE mo28164_freeze; SELECT a.id,b.id,CASE WHEN {left}={right} THEN 1 WHEN {left}<{right} THEN 0 ELSE 2 END FROM samples a CROSS JOIN samples b{where} ORDER BY a.id,b.id;'
    rows = query('order_'+c, sql)
    assert len(rows) == n*n - (n0*n0 if c in old['order'] else 0)
    for row in rows:
        i,j,sign = map(int,row.split('\t'))
        result[i*n+j] = sign
    orders[c] = base64.b64encode(result).decode()
weights = {}
for c in collations[2:]:
    rows = query('sample_weights_'+c, f'USE mo28164_freeze; SELECT id,HEX(WEIGHT_STRING(v COLLATE {c})) FROM samples ORDER BY id;')
    assert len(rows) == n
    weights[c] = [r.split('\t')[1].lower() for r in rows]
points = [r for r in range(0x10000) if not 0xd800 <= r <= 0xdfff] + [0x10000+i*509 for i in range(2048)]
sql = 'USE mo28164_freeze; DROP TABLE IF EXISTS codepoints; CREATE TABLE codepoints(id INT PRIMARY KEY,v VARCHAR(1) CHARACTER SET utf8mb4);\n'
for pos in range(0,len(points),512):
    sql += 'INSERT INTO codepoints VALUES '+','.join("(%d,CONVERT(X'%s' USING utf8mb4))" % (r,chr(r).encode().hex()) for r in points[pos:pos+512])+';\n'
query('codepoints',sql)
digests = {}
for c in collations[2:]:
    rows = query('mapping_'+c, f'USE mo28164_freeze; SELECT id,HEX(WEIGHT_STRING(v COLLATE {c})) FROM codepoints ORDER BY id;')
    assert len(rows) == len(points)
    h = hashlib.sha256()
    for point,row in zip(points,rows):
        r,w = row.split('\t')
        assert int(r) == point
        # Length delimiters prevent concatenation ambiguity in mapping evidence.
        payload = bytes.fromhex(w)
        h.update(point.to_bytes(4,'big')+len(payload).to_bytes(4,'big')+payload)
    digests[c] = h.hexdigest()
fixture = dict(server=server,image=IMAGE,parent_sha256=hashlib.sha256(old_bytes).hexdigest(),parent_count=n0,values=values,order=orders,native_weights=weights,native_mapping_sha256=digests)
(root/'mysql_8_0_45_extended.json.gz').write_bytes(gzip.compress(json.dumps(fixture,ensure_ascii=True).encode(),mtime=0))
print(json.dumps(dict(status='PASS',values=n,pairs_per_collation=n*n,reused_pairs=3*n0*n0,new_pairs=4*n*n-3*n0*n0,native_mapping_samples=len(points))))
