#!/usr/bin/env python3
"""Sequential MO ABBA / source-built DuckDB, identical local ClickBench projection."""
import argparse,ctypes as C,decimal,hashlib,json,os,pathlib,signal,subprocess,time
import pymysql
P=pathlib.Path(os.environ['BENCH_OUTPUT'])
P.mkdir(parents=True,exist_ok=True)
ROOT=pathlib.Path(os.environ['BENCH_ROOT'])
QUERIES={
'Q5':'SELECT COUNT(DISTINCT UserID) FROM hits',
'Q6':'SELECT COUNT(DISTINCT SearchPhrase) FROM hits',
'Q21':"SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase<>'' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
'Q29':r"SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '$1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer<>'' GROUP BY k HAVING COUNT(*)>100000 ORDER BY l DESC LIMIT 25"}

def dump(name,x):
 (P/name).write_text(json.dumps(x,indent=2,default=str,ensure_ascii=False)+'\n')
def mo_connect():return pymysql.connect(host='127.0.0.1',port=16879,user='root',password='111',autocommit=True,read_timeout=600,write_timeout=600,charset='utf8mb4')
def moquery(c,q):
 with c.cursor() as cur:
  cur.execute(q);return cur.fetchall()
def mo_sql(q):
 # MySQL ordinary SQL literals consume backslashes. The intended regex has one backslash.
 return q.replace('\\','\\\\')
def setup(c):
 moquery(c,'USE regexp31079')
 for v in ['max_dop=4','agg_spill_mem=4294967296','join_spill_mem=4294967296','sort_spill_mem=4294967296']:moquery(c,'SET SESSION '+v)
 return moquery(c,'SELECT @@max_dop,@@agg_spill_mem,@@join_spill_mem,@@sort_spill_mem,VERSION()')
def start(variant,phase):
 log=open(P/(phase+'-service.log'),'wb');env=os.environ.copy();env.update(GOMAXPROCS='4',GOMEMLIMIT='3GiB',OMP_NUM_THREADS='4',LD_LIBRARY_PATH=os.environ['MO_LIBRARY_PATH'])
 proc=subprocess.Popen([str(ROOT/('mo-service-'+variant)),'-launch',str(ROOT/'cluster/launch.toml'),'-debug-http=:17879'],cwd=ROOT/'cluster',env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
 try:
  until=time.monotonic()+180
  while time.monotonic()<until:
   if proc.poll() is not None:raise RuntimeError('service exit '+str(proc.returncode))
   try:
    c=mo_connect();moquery(c,'SELECT 1')
    if 'ISCP-Task Start' in (P/(phase+'-service.log')).read_text(errors='replace'):return proc,log,c
    c.close()
   except pymysql.Error:pass
   time.sleep(1)
  raise TimeoutError('readiness')
 except:
  stop(proc,log);raise
def stop(proc,log):
 os.killpg(proc.pid,signal.SIGTERM)
 try:proc.wait(timeout=45)
 except subprocess.TimeoutExpired:
  os.killpg(proc.pid,signal.SIGKILL);proc.wait()
 log.close()
 return proc.returncode

def corpus(c):
 # SQL entry-point parity, including LF, UTF-8, offsets, occurrence, NULL and capture replacement.
 qs=["SELECT REGEXP_REPLACE('https://www.example.com/a','^https?://(?:www\\\\.)?([^/]+)/.*$','$1')", "SELECT REGEXP_REPLACE('abxyz','(a|ab).*\\\\z','x')", "SELECT REGEXP_REPLACE('a\\nb','a.*\\\\z','x')", "SELECT REGEXP_REPLACE('a\\nb','(?s)a.*\\\\z','x')", "SELECT REGEXP_INSTR('bbb','a.*b')", "SELECT REGEXP_INSTR('aba','a.*\\\\z',2)", "SELECT REGEXP_SUBSTR('éabxyz','(a|ab).*\\\\z',2)", "SELECT REGEXP_REPLACE('a-b-a','a','x',1,2)", "SELECT REGEXP_REPLACE(NULL,'a.*\\\\z','x')", "SELECT REGEXP_REPLACE('','a.*\\\\z','x')", "SELECT REGEXP_REPLACE('a\\0b','a.*\\\\z','x')", "SELECT REGEXP_REPLACE('abc','[','x')", "SELECT REGEXP_INSTR('abc','a',0)"]
 qs += [r"SELECT HEX(REGEXP_REPLACE(CAST(UNHEX('61ff62') AS BINARY), CAST('a.*\\z' AS BINARY), CAST('x' AS BINARY)))",r"SELECT HEX(REGEXP_REPLACE(CAST(UNHEX('610062') AS BINARY), CAST('a.*\\z' AS BINARY), CAST('x' AS BINARY)))"]
 qs.append(r"SELECT REGEXP_REPLACE(CAST(UNHEX('61ff62') AS BINARY), 'a.*\\z', 'x')")
 out=[]
 for q in qs:
  try:out.append({'sql':q,'result':moquery(c,q)})
  except pymysql.Error as e:out.append({'sql':q,'error':list(e.args)})
 expected=['example.com','x','a\nb','x',0,3,'abxyz','a-b-x',None,'','x',20203,20301,'78','78',3995]
 assert len(out)==len(expected)
 for i,(actual,want) in enumerate(zip(out,expected)):
  if i in (11,12,15):assert actual.get('error',[None])[0]==want,(i,actual,want)
  else:assert actual.get('result')==((want,),),(i,actual,want)
 return out

def run_mo():
 for phase,variant in [('A1','baseline'),('B1','candidate'),('B2','candidate'),('A2','baseline')]:
  proc,log,c=start(variant,phase);record={'phase':phase,'variant':variant,'queries':{},'binary_sha256':hashlib.file_digest(open(ROOT/('mo-service-'+variant),'rb'),'sha256').hexdigest()}
  try:
   if phase=='A1':
    moquery(c,'CREATE DATABASE IF NOT EXISTS regexp31079');moquery(c,'USE regexp31079')
    moquery(c,'CREATE TABLE IF NOT EXISTS hits (UserID BIGINT, SearchPhrase TEXT, URL TEXT, Referer TEXT)')
    count=moquery(c,'SELECT COUNT(*) FROM hits')[0][0]
    if count==0:
     t=time.monotonic();res=moquery(c,"LOAD DATA INFILE {'filepath'='"+str(ROOT/'data/hits-10m.parquet')+"','format'='parquet'} INTO TABLE hits")
     record['load']={'seconds':time.monotonic()-t,'result':res}
    elif count!=10000000:raise RuntimeError('unexpected existing count '+str(count))
   record['settings']=setup(c);record['count']=moquery(c,'SELECT COUNT(*) FROM hits');assert record['count'][0][0]==10000000
   record['corpus']=corpus(c)
   record['Q21_deterministic_parity']=moquery(c,QUERIES['Q21'].replace('ORDER BY c DESC LIMIT','ORDER BY c DESC, SearchPhrase ASC LIMIT'))
   for name,q in QUERIES.items():
    q=mo_sql(q);sample={'sql':q,'warmups':[],'seconds':[],'results':[]};record['queries'][name]=sample
    for n in range(7):
     t=time.perf_counter();result=moquery(c,q);elapsed=time.perf_counter()-t
     if n<2:sample['warmups'].append(elapsed)
     else:sample['seconds'].append(elapsed);sample['results'].append(result)
     print(phase,name,n,elapsed,flush=True)
     dump(phase+'-results.json',record)
    plan=moquery(c,'EXPLAIN ANALYZE VERBOSE '+q)
    text='\n'.join(str(r[0]) for r in plan)+'\n';(P/(phase+'-'+name+'-plan.txt')).write_text(text)
    assert 'SpillSize=' not in text,'MO spilled '+phase+name
    assert 'Actual' in text or 'Analyze: timeConsumed=' in text, 'missing analyze metrics'
    sample['no_spill']='No SpillSize= emitted by complete EXPLAIN ANALYZE VERBOSE'
   dump(phase+'-results.json',record)
  finally:
   c.close();code=stop(proc,log);print(phase,'service stop',code,flush=True)
   record['service_stop']=code;dump(phase+'-results.json',record)

class Result(C.Structure):
 _fields_=[('columns',C.c_uint64),('rows',C.c_uint64),('changed',C.c_uint64),('data',C.c_void_p),('error',C.c_void_p),('internal',C.c_void_p)]
class Duck:
 def __init__(self):
  self.lib=C.CDLL(os.environ['DUCKDB_LIBRARY']);l=self.lib
  for name,args,ret in [('duckdb_open',[C.c_char_p,C.POINTER(C.c_void_p)],C.c_int),('duckdb_connect',[C.c_void_p,C.POINTER(C.c_void_p)],C.c_int),('duckdb_query',[C.c_void_p,C.c_char_p,C.POINTER(Result)],C.c_int),('duckdb_column_count',[C.POINTER(Result)],C.c_uint64),('duckdb_row_count',[C.POINTER(Result)],C.c_uint64),('duckdb_value_is_null',[C.POINTER(Result),C.c_uint64,C.c_uint64],C.c_bool),('duckdb_value_varchar',[C.POINTER(Result),C.c_uint64,C.c_uint64],C.c_void_p),('duckdb_result_error',[C.POINTER(Result)],C.c_char_p),('duckdb_free',[C.c_void_p],None),('duckdb_destroy_result',[C.POINTER(Result)],None),('duckdb_disconnect',[C.POINTER(C.c_void_p)],None),('duckdb_close',[C.POINTER(C.c_void_p)],None)]:
   f=getattr(l,name);f.argtypes=args;f.restype=ret
  self.db=C.c_void_p();self.conn=C.c_void_p();assert l.duckdb_open(str(ROOT/'duckdb.db').encode(),C.byref(self.db))==0;assert l.duckdb_connect(self.db,C.byref(self.conn))==0
 def query(self,q):
  l=self.lib;r=Result()
  try:
   if l.duckdb_query(self.conn,q.encode(),C.byref(r)):raise RuntimeError(l.duckdb_result_error(C.byref(r)).decode())
   result=[]
   for row in range(l.duckdb_row_count(C.byref(r))):
    values=[]
    for col in range(l.duckdb_column_count(C.byref(r))):
     if l.duckdb_value_is_null(C.byref(r),col,row):values.append(None)
     else:
      ptr=l.duckdb_value_varchar(C.byref(r),col,row)
      try:values.append(C.string_at(ptr).decode())
      finally:l.duckdb_free(ptr)
    result.append(values)
   return result
  finally:l.duckdb_destroy_result(C.byref(r))
 def close(self):self.lib.duckdb_disconnect(C.byref(self.conn));self.lib.duckdb_close(C.byref(self.db))
def run_duck():
 d=Duck();record=json.loads((P/'duckdb-results.json').read_text()) if args.only and (P/'duckdb-results.json').exists() else {'source':'cc9648d736fd7e4fb2fc813cba6f6d34932c3386','queries':{}}
 try:
  d.query('SET threads=4');d.query("SET memory_limit='4GiB'")
  d.query("CREATE TABLE IF NOT EXISTS hits AS SELECT * FROM read_parquet('"+str(ROOT/'data/hits-10m.parquet')+"')")
  record['settings']=d.query("SELECT version(), current_setting('threads'), current_setting('memory_limit')");record['count']=d.query('SELECT COUNT(*) FROM hits');assert record['count']==[['10000000']]
  record['Q21_deterministic_parity']=d.query(QUERIES['Q21'].replace('ORDER BY c DESC LIMIT','ORDER BY c DESC, SearchPhrase ASC LIMIT'))
  for name,q in QUERIES.items():
   if args.only and name not in args.only:continue
   if name=='Q29':q=q.replace("'$1'",r"'\1'").replace('AVG(length(Referer))','AVG(octet_length(encode(Referer)))')
   sample={'sql':q,'warmups':[],'seconds':[],'results':[]};record['queries'][name]=sample
   for n in range(7):
    t=time.perf_counter();result=d.query(q);elapsed=time.perf_counter()-t
    if n<2:sample['warmups'].append(elapsed)
    else:sample['seconds'].append(elapsed);sample['results'].append(result)
    print('DuckDB',name,n,elapsed,flush=True)
   profile=P/('duckdb-'+name+'-profile.json')
   d.query("SET enable_profiling='json'");d.query("SET profiling_output='"+str(profile)+"'");d.query(q);d.query('PRAGMA disable_profiling')
   prof=json.loads(profile.read_text());sample['peak_temp_dir_size']=prof.get('system',{}).get('peak_temp_dir_size',prof.get('system_peak_temp_dir_size'));sample['total_bytes_spilled']=prof.get('system',{}).get('total_bytes_spilled');assert sample['peak_temp_dir_size']==0 and sample['total_bytes_spilled']==0,'DuckDB spilled'
   dump('duckdb-results.json',record)
 finally:d.close()
if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('engine',choices=['mo','duckdb']);parser.add_argument('--only',nargs='+',choices=list(QUERIES));args=parser.parse_args()
 (run_mo if args.engine=='mo' else run_duck)()
