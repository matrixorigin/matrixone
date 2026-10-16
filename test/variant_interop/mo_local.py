#!/usr/bin/env python3
"""Run against the built local MO service; --verify repeats after restart."""
import argparse,json,pathlib,pymysql
from decimal import Decimal
p=argparse.ArgumentParser();p.add_argument('--port',type=int,default=16001);p.add_argument('--verify',action='store_true');p.add_argument('--state',default='/workspace/variant-interop/mo-expected.json');a=p.parse_args()
def connect():return pymysql.connect(host='127.0.0.1',port=a.port,user='root',password='111',autocommit=True,read_timeout=90,connect_timeout=5)
c=connect();x=c.cursor();x.execute('create database if not exists variant_local');x.execute('use variant_local')
state=pathlib.Path(a.state)
if not a.verify:
 x.execute('drop table if exists traces');x.execute('create table traces(id int primary key,doc json)')
 expected={}
 for start in range(0,10000,500):
  vals=[]
  for i in range(start,start+500):
   if i%23==0:s=None
   elif i%23==1:s='null'
   elif i%23==2:s='{}'
   else:s='{"trace_id":'+json.dumps('trace-'+str(i))+',"cost":9007199254740993.'+f'{i%1000:03d}'+',"latency":'+str(i%997)+',"nested":{"tags":["agent","tool"]},"tail":'+json.dumps('unused'*100)+'}'
   vals.append((i,s));expected[str(i)]=s
  x.executemany('insert into traces values(%s,json_parse_exact(%s))',vals)
 # Isolation and rollback around a persisted object.
 x.execute("select mo_ctl('dn','flush','variant_local.traces')");print('initial flush',x.fetchall(),flush=True)
 x.execute('begin');x.execute("update traces set doc=json_parse_exact('{\"trace_id\":\"rollback\"}') where id=7");x.execute('rollback')
 other=connect();y=other.cursor();y.execute('use variant_local')
 x.execute('select @@transaction_isolation');isolation=x.fetchone()[0];print('transaction isolation',isolation,flush=True)
 x.execute('begin');x.execute("select json_extract_string(doc,'$.trace_id') from traces where id=7");assert x.fetchone()[0]=='trace-7'
 new='{"trace_id":"updated","cost":9007199254740993.007,"latency":7}'
 y.execute('update traces set doc=json_parse_exact(%s) where id=7',(new,))
 x.execute("select json_extract_string(doc,'$.trace_id') from traces where id=7");observed=x.fetchone()[0];assert observed==('updated' if 'COMMITTED' in isolation.upper().replace('-',' ') else 'trace-7'),(isolation,observed);x.execute('commit');expected['7']=new
 x.execute('delete from traces where id%31=0');expected={k:v for k,v in expected.items() if int(k)%31}
 x.execute("select mo_ctl('dn','flush','variant_local.traces')");print('update/delete flush',x.fetchall(),flush=True)
 state.write_text(json.dumps(expected,ensure_ascii=False));other.close()
else:expected=json.loads(state.read_text())
# Compare projection and full-document consumers on the same persisted rows.
x.execute("select id,json_extract_string(doc,'$.trace_id'),cast(json_extract(doc,'$.cost') as decimal(38,3)),json_extract_float64(doc,'$.latency') from traces order by id")
rows=x.fetchall();assert len(rows)==len(expected)
for i,trace,cost,latency in rows:
 raw=expected[str(i)];v=None if raw is None else json.loads(raw,parse_float=Decimal)
 obj=v if isinstance(v,dict) else {}
 assert trace==obj.get('trace_id'),(i,trace,obj)
 assert cost==obj.get('cost'),(i,cost,obj)
 assert latency==obj.get('latency'),(i,latency,obj)
x.execute('select id,doc from traces order by id')
for i,raw in x.fetchall():
 want=expected[str(i)];assert (raw is None)==(want is None)
 if want is not None:assert json.loads(raw,parse_float=Decimal)==json.loads(want,parse_float=Decimal),(i,raw,want)
print(json.dumps({'mode':'restart verification' if a.verify else 'persisted MVCC/delete/rollback','rows':len(rows),'checks':'full documents, path projections, exact DECIMAL, SQL/JSON null','result':'PASS'}),flush=True)
c.close()
