# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
import pathlib,pymysql,http.client,json,base64,uuid
import argparse
p=argparse.ArgumentParser();p.add_argument('--dir',required=True);a=p.parse_args();root=pathlib.Path(a.dir)
c=pymysql.connect(host='127.0.0.1',port=19030,user='root',use_unicode=False,autocommit=True,connect_timeout=5)
results={}
with c.cursor() as cur:
 cur.execute('create database if not exists mo_variant_interop')
 cur.execute('use mo_variant_interop')
 cur.execute('show backends');results['backends']=cur.fetchall()
 for kind in ['mo-variant','duckdb-automatic','duckdb-shredded']:
  table=kind.replace('-','_')
  cur.execute(f'DROP TABLE IF EXISTS {table}')
  cur.execute(f'CREATE TABLE IF NOT EXISTS {table} (a_unused_000 INT,z_nested VARIANT) DUPLICATE KEY(a_unused_000) DISTRIBUTED BY HASH(a_unused_000) BUCKETS 1 PROPERTIES("replication_num"="1")')
  conn=http.client.HTTPConnection('127.0.0.1',18040,timeout=90)
  conn.request('PUT',f'/api/mo_variant_interop/{table}/_stream_load',body=(root/(kind+'.parquet')).read_bytes(),headers={'Authorization':'Basic '+base64.b64encode(b'root:').decode(),'format':'parquet','label':uuid.uuid4().hex,'strict_mode':'true','max_filter_ratio':'0','Content-Type':'application/octet-stream'})
  response=conn.getresponse();raw=response.read().decode();results[kind]={'http':response.status,'load':json.loads(raw)};conn.close();print(kind,raw,flush=True)
  if results[kind]['load'].get('Status')=='Success':
   cur.execute(f'SELECT a_unused_000, hex(cast(z_nested as string)),cast(z_nested["cost"] as decimal(38,3)),cast(z_nested["trace_id"] as string) FROM {table} ORDER BY a_unused_000 LIMIT 3');results[kind]['sample']=cur.fetchall();print('sample',results[kind]['sample'],flush=True)
   cur.execute(f'SELECT count(*),sum(z_nested["trace_id"] is null),sum(z_nested["cost"] is null) FROM {table}');results[kind]['count_and_missing_fields']=cur.fetchall();results[kind]['semantic_result']='FAIL: document identity must be verified, not just the loaded row count'
 try:
  cur.execute('ADMIN SET FRONTEND CONFIG ("enable_outfile_to_local"="true")');results['export_config']='enabled'
 except Exception as e:results['export_config']=str(e)
 try:
  cur.execute('SELECT * FROM mo_variant INTO OUTFILE "file:///interop/doris-native-negative-" FORMAT AS PARQUET');results['export']=cur.fetchall();print('export',results['export'],flush=True)
 except Exception as e:results['export']=str(e);print('export error',e,flush=True)
(root/'doris-results.json').write_text(json.dumps(results,default=str,indent=2))

# This is a compatibility test, not a success test for the loading API.
raise SystemExit(1 if any(v.get("semantic_result", "").startswith("FAIL") for v in results.values() if isinstance(v,dict)) else 0)
