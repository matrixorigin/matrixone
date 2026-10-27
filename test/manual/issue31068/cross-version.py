#!/usr/bin/env python3
"""Two isolated local clusters, real unedited pre-fix archive, explicit oracles."""
from pathlib import Path
import argparse,hashlib,json,os,re,signal,subprocess,time
import pymysql
P=Path(os.environ['PR31069_EVIDENCE_DIR']);P.mkdir(parents=True,exist_ok=True)
ROOT=Path(os.environ['PR31069_RUNTIME_ROOT']);W=Path(os.environ['PR31069_REPO'])
DDL=r"CREATE TABLE {name} (id int primary key, v varchar(32) default 'd\\ef', c int comment 'C:\\dir\\new it''s', key idx_v(v) comment 'idx\\q''s') comment 'x\\ty''z'"
ROWS=((1,'one',10),(2,'two',20))
TRACE=[]
def dump(name,x):(P/name).write_text(json.dumps(x,indent=2,default=str,ensure_ascii=False)+'\n')
def sql(c,q):
 with c.cursor() as cur:
  t=time.monotonic()
  try:
   cur.execute(q);r=cur.fetchall();TRACE.append({'sql':q,'result':r,'seconds':time.monotonic()-t});return r
  except pymysql.Error as e:
   TRACE.append({'sql':q,'error':list(e.args),'seconds':time.monotonic()-t});raise
  finally:dump(ARGS.phase+'-sql.json',TRACE)
def connect():return pymysql.connect(host='127.0.0.1',port=16869,user='root',password='111',autocommit=True,charset='utf8mb4',read_timeout=180)
def start(binary,cluster):
 log=open(P/(ARGS.phase+'-service.log'),'wb');env=dict(os.environ,GOMAXPROCS='4',GOMEMLIMIT='3GiB',OMP_NUM_THREADS='4',LD_LIBRARY_PATH=str(W/'lib')+':'+str(W/'cgo')+':'+str(W/'thirdparties/install/lib'))
 proc=subprocess.Popen([str(ROOT/('mo-service-'+binary)),'-launch',str(ROOT/cluster/'launch.toml'),'-debug-http=:17869'],cwd=ROOT/cluster,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
 dump(ARGS.phase+'-process.json',{'pid':proc.pid,'binary':str(ROOT/('mo-service-'+binary)),'binary_sha256':hashlib.file_digest(open(ROOT/('mo-service-'+binary),'rb'),'sha256').hexdigest(),'cluster':str(ROOT/cluster)})
 try:
  until=time.monotonic()+180
  while time.monotonic()<until:
   if proc.poll() is not None:raise RuntimeError('service exit '+str(proc.returncode))
   try:
    c=connect();sql(c,'SELECT 1')
    if 'ISCP-Task Start' in (P/(ARGS.phase+'-service.log')).read_text(errors='replace'):return proc,log,c
    c.close()
   except pymysql.Error:pass
   time.sleep(1)
  raise TimeoutError('readiness')
 except:
  stop(proc,log);raise

def stop(proc,log):
 if proc.poll() is None:
  try:os.killpg(proc.pid,signal.SIGTERM)
  except ProcessLookupError:pass
 try:proc.wait(timeout=45)
 except subprocess.TimeoutExpired:os.killpg(proc.pid,signal.SIGKILL);proc.wait()
 log.close();dump(ARGS.phase+'-stop.json',{'returncode':proc.returncode})

def comments(c,name):
 return {'columns':sql(c,"SELECT column_name,hex(column_comment) FROM information_schema.columns WHERE table_schema='pr31069_compat' AND table_name='"+name+"' ORDER BY ordinal_position"),'table':sql(c,"SELECT hex(table_comment) FROM information_schema.tables WHERE table_schema='pr31069_compat' AND table_name='"+name+"'")}
def create(c,name,ddl=DDL):sql(c,ddl.format(name=name))
def load(c,name,archive):return sql(c,"LOAD TABLE "+name+" FROM 'stage://pr31069_compat_stage/"+archive+"'")
def archive(c,name,archive,metadata=False):
 if not metadata:
  indexes=sql(c,"SELECT DISTINCT index_table_name FROM mo_catalog.mo_indexes WHERE table_id=(SELECT rel_id FROM mo_catalog.mo_tables WHERE reldatabase='pr31069_compat' AND relname='"+name+"') AND index_table_name IS NOT NULL AND index_table_name<>''")
  for (index,) in indexes:sql(c,"SELECT mo_ctl('dn','flush','pr31069_compat."+index+"')")
 sql(c,"DUMP TABLE "+name+" TO 'stage://pr31069_compat_stage/"+archive+"'"+(' METADATA ONLY' if metadata else ''))
 path=ROOT/'archives'/archive/'manifest.json';data=path.read_bytes();m=json.loads(data);dump(archive+'-manifest.json',m);dump(archive+'-identity.json',{'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data),'path':str(path)})
 return m

def main():
 baseline=ARGS.phase in ['produce','reverse'];cluster='producer' if baseline else os.environ.get('CONSUMER_CLUSTER','consumer');proc,log,c=start('baseline' if baseline else 'candidate',cluster)
 try:

  if ARGS.phase=='produce':
   sql(c,'DROP DATABASE IF EXISTS pr31069_compat');sql(c,'DROP STAGE IF EXISTS pr31069_compat_stage')
  sql(c,'CREATE DATABASE IF NOT EXISTS pr31069_compat');sql(c,'USE pr31069_compat')
  sql(c,"CREATE STAGE IF NOT EXISTS pr31069_compat_stage URL='file://"+str(ROOT/'archives')+"/'")
  if ARGS.phase=='produce':
   create(c,'src');sql(c,"INSERT INTO src VALUES (1,'one',10),(2,'two',20)");sql(c,"SELECT mo_ctl('dn','flush','pr31069_compat.src')")
   source=comments(c,'src');dump('source-comments.json',source)
   show=sql(c,'SHOW CREATE TABLE src')[0][1];dump('baseline-show.json',show)
   replay=re.sub(r'CREATE TABLE `src`','CREATE TABLE `replay_old`',show,count=1);assert replay!=show;sql(c,replay)
   assert comments(c,'replay_old')!=source,'old SHOW unexpectedly roundtrips'
   archive(c,'src','legacy');archive(c,'src','legacy_metadata',True)
  elif ARGS.phase=='consume':
   expected=json.loads((P/'source-comments.json').read_text())
   for name,arc in [('dst','legacy'),('meta_dst','legacy_metadata')]:
    create(c,name);load(c,name,arc);assert sql(c,'SELECT * FROM '+name+' ORDER BY id')==ROWS
    assert json.loads(json.dumps(comments(c,name)))==expected
    assert sql(c,"SELECT id FROM "+name+" FORCE INDEX(idx_v) WHERE v='one'")==((1,),)
   show=sql(c,'SHOW CREATE TABLE dst')[0][1];dump('candidate-show.json',show)
   replay=re.sub(r'CREATE TABLE `dst`','CREATE TABLE `replay_new`',show,count=1);assert replay!=show;sql(c,replay)
   assert json.loads(json.dumps(comments(c,'replay_new')))==expected
   changes=[('bad_column',DDL.replace(r'C:\\dir\\new',r'C:\\other\\new')),('bad_table',DDL.replace(r'x\\ty',r'x\\other')),('bad_index',DDL.replace(r'idx\\q',r'idx\\other')),('bad_type',DDL.replace('c int comment','c bigint comment')),('bad_default',DDL.replace(r'd\\ef',r'd\\other'))]
   for name,ddl in changes:
    assert ddl!=DDL;create(c,name,ddl)
    try:load(c,name,'legacy')
    except pymysql.Error as e:assert e.args[0]==20301 and 'schema does not match' in e.args[1],e.args
    else:raise AssertionError('schema mismatch accepted: '+name)
    assert sql(c,'SELECT COUNT(*) FROM '+name)==((0,),)
   sql(c,"SELECT mo_ctl('dn','flush','pr31069_compat.dst')")
   new=archive(c,'dst','current');old=json.loads((P/'legacy-manifest.json').read_text());assert new['schema_hash']==old['schema_hash']
   effective=lambda m:[(x['role'],x.get('index_name'),x.get('index_algo_table_type'),x.get('logical_schema_hash') or x['schema_hash']) for x in m['relations']]
   assert effective(new)==effective(old)
   # Preserve the archive produced by the old server byte for byte.
   for arc in ['legacy','legacy_metadata']:
    expected_id=json.loads((P/(arc+'-identity.json')).read_text());assert hashlib.sha256((ROOT/'archives'/arc/'manifest.json').read_bytes()).hexdigest()==expected_id['sha256']
  else:
   create(c,'reverse_dst');load(c,'reverse_dst','current');assert sql(c,'SELECT * FROM reverse_dst ORDER BY id')==ROWS
   assert json.loads(json.dumps(comments(c,'reverse_dst')))==json.loads((P/'source-comments.json').read_text())
   assert sql(c,"SELECT id FROM reverse_dst FORCE INDEX(idx_v) WHERE v='one'")==((1,),)
  print('PASS',ARGS.phase,flush=True)
 finally:c.close();stop(proc,log)
if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('phase',choices=['produce','consume','reverse']);ARGS=parser.parse_args();main()
