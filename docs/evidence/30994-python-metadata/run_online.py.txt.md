# `run_online.py.txt`

Artifact: {"name":"run_online.py.txt","bytes":2957,"sha256":"2ae379a7316ca5e1834f1c2e2849e9c9e01a9be79bd3cbde90141b4ccd2ef9a0","encoding":"utf-8","stored_bytes":2957}

````python
from pathlib import Path
import subprocess,os,time,json,hashlib,socket,signal
import pymysql
root=Path(__file__).parent;work=Path('/private/tmp/mo-pr30994');server=Path('/Users/xupeng/.codex/issue-to-pr/matrixone/30995/rebase-94faf020/mo-service-baseline');native=Path('/private/tmp/mo-issue30995')
for port in [56411,56460,56200,56301,56400]:
 with socket.socket() as sock:sock.bind(('127.0.0.1',port))
serverenv=os.environ.copy();serverenv.update(GOMAXPROCS='4',GOMEMLIMIT='3GiB',DYLD_LIBRARY_PATH=f'{native}/cgo:{native}/thirdparties/install/lib:{native}/lib')
env=os.environ.copy();env.update(MATRIXONE_HOST='127.0.0.1',MATRIXONE_PORT='56411',MATRIXONE_USER='root',MATRIXONE_PASSWORD='111',MATRIXONE_DATABASE='test',PYTHONPATH=str(work/'clients/python'))
cmd=[str(server),'-launch',str(root/'launch.toml'),'-debug-http','127.0.0.1:56460'];started=time.time();log=(root/'frontend-service.log').open('wb');p=subprocess.Popen(cmd,cwd=work,env=serverenv,stdout=log,stderr=subprocess.STDOUT);receipts=[];c=None
try:
 deadline=time.monotonic()+90
 while c is None:
  if p.poll() is not None:raise RuntimeError(('frontend exit',p.returncode))
  try:c=pymysql.connect(host='127.0.0.1',port=56411,user='root',password='111',autocommit=True,read_timeout=15)
  except pymysql.MySQLError:
   assert time.monotonic()<deadline,'SQL readiness timeout';time.sleep(.1)
 with c.cursor() as s:s.execute('SELECT 1');assert s.fetchall()==((1,),);s.execute('CREATE DATABASE test')
 c.close();c=None
 tests=[('online',[str(root/'venv/bin/python'),'-m','pytest','tests/online/test_metadata_online.py','-o','addopts=','-vv','--tb=short','--junitxml='+str(root/'online.xml')]),('async-challenge',[str(root/'venv/bin/python'),str(root/'async_challenge.py')])]
 for name,testcmd in tests:
  t=time.time()
  with (root/(name+'.log')).open('wb') as f:r=subprocess.run(testcmd,cwd=work/'clients/python',env=env,stdout=f,stderr=subprocess.STDOUT,timeout=300)
  receipts.append({'name':name,'command':testcmd,'cwd':str(work/'clients/python'),'started_epoch':t,'finished_epoch':time.time(),'exit':r.returncode,'source_head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=work,text=True).strip(),'log_sha256':hashlib.sha256((root/(name+'.log')).read_bytes()).hexdigest()});(root/'online-receipts.json').write_text(json.dumps(receipts,indent=2));print(name,r.returncode,flush=True)
  if r.returncode:raise SystemExit(r.returncode)
finally:
 if c:c.close()
 if p.poll() is None:p.send_signal(signal.SIGTERM);p.wait(timeout=30)
 log.close()
(root/'frontend-receipt.json').write_text(json.dumps({'command':cmd,'started_epoch':started,'finished_epoch':time.time(),'exit':p.returncode,'server_binary_sha256':hashlib.sha256(server.read_bytes()).hexdigest(),'server_commit':'94faf0209b9ac3254d31906f92e34405f1988203','runtime':str(root/'runtime'),'ports':[56411,56460,56200,56301,56400],'shutdown':'owned process exited after SIGTERM'},indent=2));print('LIVE_VALIDATION_PASS',flush=True)

````
