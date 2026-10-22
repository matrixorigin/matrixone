#!/usr/bin/env python3
# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Generate independent DuckDB VARIANT files and verify MO's exported file."""
import argparse,json,pathlib
from decimal import Decimal
import duckdb
p=argparse.ArgumentParser();p.add_argument('mode',choices=['generate','verify']);p.add_argument('--dir',required=True);p.add_argument('--file',default='mo-variant.parquet');a=p.parse_args();root=pathlib.Path(a.dir);root.mkdir(parents=True,exist_ok=True);c=duckdb.connect()
print('DuckDB',duckdb.__version__,c.execute('pragma version').fetchall())
if a.mode=='generate':
 c.execute("create table traces as select i::integer a_unused_000, {'trace_id':'trace-'||i,'cost':('9007199254740993.'||lpad((i%1000)::varchar,3,'0'))::decimal(38,3),'latency':i%997,'drift':case when i%11=0 then NULL else i end,'nested':{'ok':i%2=0,'tags':['agent','tool']}}::variant z_nested from range(10000)t(i)")
 for name,option in [('automatic',''),('shredded',", SHREDDING {'z_nested':'STRUCT(trace_id VARCHAR,cost DECIMAL(38,3),latency BIGINT,nested STRUCT(ok BOOLEAN,tags VARCHAR[]))'}")]:
  c.execute(f"copy traces to '{root}/duckdb-{name}.parquet' (format parquet,row_group_size 2048{option})")
  rows=c.execute(f"select a_unused_000,z_nested from read_parquet('{root}/duckdb-{name}.parquet') order by a_unused_000").fetchall();assert len(rows)==10000
  print(name,'10000 rows generated')
else:
 rows=c.execute(f"select a_unused_000,z_nested from read_parquet('{root}/{a.file}') order by a_unused_000").fetchall();assert len(rows)==10000
 for i,v in rows:
  assert v['cost']==Decimal(f'9007199254740993.{i%1000:03d}'),(i,v)
  assert type(v['cost']) is Decimal and type(v['nested']['ok']) is bool
  assert v['trace_id']==f'trace-{i}' and v['latency']==i%997
  assert v['nested']=={'ok':i%2==0,'tags':['agent','tool']}
  assert 'drift' in v and v['drift']==(None if i%11==0 else i)
 print(json.dumps({'direction':'MO -> DuckDB','rows':len(rows),'result':'PASS'}))
