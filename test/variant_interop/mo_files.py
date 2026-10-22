#!/usr/bin/env python3
# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Load independent files through a real MO SQL service and export native VARIANT."""
import argparse,json,pathlib,pymysql
from decimal import Decimal
p=argparse.ArgumentParser();p.add_argument('--dir',required=True);p.add_argument('--port',type=int,default=16001);a=p.parse_args();root=pathlib.Path(a.dir)
c=pymysql.connect(host='127.0.0.1',port=a.port,user='root',password='111',autocommit=True,read_timeout=120,connect_timeout=5);x=c.cursor();x.execute('create database if not exists variant_file_local');x.execute('use variant_file_local')
for kind in ['automatic','shredded','doris']:
 if kind=='doris':
  files=list(root.glob('doris-bridge-*.parquet'));assert len(files)==1;file=files[0];expected=json.loads((root/'doris-bridge-actual.json').read_text())
 else:file=root/f'duckdb-{kind}.parquet'
 table='from_'+kind;x.execute(f'drop table if exists {table}');x.execute(f'create table {table}(a_unused_000 int,z_nested json)')
 x.execute("load data infile {'filepath'=%s,'format'='parquet'} into table "+table,(str(file),))
 x.execute(f"select mo_ctl('dn','flush','variant_file_local.{table}')");x.fetchall()
 x.execute(f'select a_unused_000,z_nested from {table} order by a_unused_000');rows=x.fetchall();assert len(rows)==10000
 for i,s in rows:
  got=json.loads(s,parse_float=Decimal)
  if kind=='doris':assert got==expected[i]['z_nested'],(i,got,expected[i])
  else:
   assert got['cost']==Decimal(f'9007199254740993.{i%1000:03d}') and type(got['cost']) is Decimal,(i,got)
   assert got['trace_id']==f'trace-{i}' and got['latency']==i%997
   assert type(got['nested']['ok']) is bool and got['nested']=={'ok':i%2==0,'tags':['agent','tool']}
   assert 'drift' in got and got['drift']==(None if i%11==0 else i)
 if kind!='doris':
  x.execute(f"select a_unused_000,cast(json_extract(z_nested,'$.cost') as decimal(38,3)),json_extract_string(z_nested,'$.trace_id') from {table} order by a_unused_000")
  for i,cost,trace in x.fetchall():assert cost==Decimal(f'9007199254740993.{i%1000:03d}') and trace==f'trace-{i}'
 print(json.dumps({'MO_SQL_LOAD':kind,'rows':len(rows),'result':'PASS'}),flush=True)
out=root/'mo-sql-variant.parquet';assert not out.exists(),'use a fresh destination for OUTFILE'
x.execute('select a_unused_000,z_nested from from_automatic order by a_unused_000 into outfile %s format \'parquet_variant\'',(str(out),))
print(json.dumps({'MO_SQL_EXPORT':str(out),'result':'WRITTEN; verify with DuckDB'}),flush=True)
c.close()
