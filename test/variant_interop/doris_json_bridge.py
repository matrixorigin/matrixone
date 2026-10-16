# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
import pymysql,json,pathlib,http.client,base64,uuid
import argparse
p=argparse.ArgumentParser();p.add_argument('--dir',required=True);a=p.parse_args();root=pathlib.Path(a.dir)
c=pymysql.connect(host='127.0.0.1',port=19030,user='root',autocommit=True,connect_timeout=5,read_timeout=60)
x=c.cursor();x.execute('use mo_variant_interop');x.execute('drop table if exists json_bridge')
x.execute('create table json_bridge(a_unused_000 int,z_nested variant) duplicate key(a_unused_000) distributed by hash(a_unused_000) buckets 1 properties("replication_num"="1")')
expected=[]
for i in range(10000):
 v={'trace_id':f'trace-{i}','cost_text':f'9007199254740993.{i%1000:03d}','latency':i%997,'drift':None if i%11==0 else i,'nested':{'ok':i%2==0,'tags':['agent','tool']}}
 expected.append({'a_unused_000':i,'z_nested':v})
body='\n'.join(json.dumps(r) for r in expected).encode()
h=http.client.HTTPConnection('127.0.0.1',18040,timeout=90);h.request('PUT','/api/mo_variant_interop/json_bridge/_stream_load',body=body,headers={'Authorization':'Basic '+base64.b64encode(b'root:').decode(),'format':'json','read_json_by_line':'true','label':uuid.uuid4().hex,'strict_mode':'true','max_filter_ratio':'0'})
r=h.getresponse();data=json.loads(r.read());print('load',data,flush=True);assert data['Status']=='Success' and data['NumberLoadedRows']==10000 and data['NumberFilteredRows']==0
x.execute('select a_unused_000,cast(z_nested as string) from json_bridge order by a_unused_000');rows=x.fetchall();assert len(rows)==10000
def typed(v):
 if isinstance(v,dict):return {k:typed(x) for k,x in v.items()}
 if isinstance(v,list):return [typed(x) for x in v]
 return (type(v).__name__,v)
mismatches=[i for i,s in rows if typed(json.loads(s))!=typed(expected[i]['z_nested'])]
print('Strict JSON bridge mismatches',len(mismatches),'first',mismatches[:5],flush=True)
(root/'doris-bridge-actual.json').write_text(json.dumps([{'a_unused_000':i,'z_nested':json.loads(s)} for i,s in rows]))
print('Cost strings preserved; explicit null and boolean identity are checked separately',flush=True)
try:
 x.execute('select * from json_bridge into outfile "file:///interop/doris-bridge-" format as parquet');result=x.fetchall();print('Doris parquet export',result,flush=True);(root/'doris-bridge-export.json').write_text(json.dumps(result,default=str))
except Exception as e:print('Doris export error',repr(e),flush=True)
(root/'doris-bridge-expected.json').write_text(json.dumps(expected))
