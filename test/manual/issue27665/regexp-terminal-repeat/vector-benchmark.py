#!/usr/bin/env python3
import os,pathlib,subprocess,json,time
P=pathlib.Path(os.environ['BENCH_OUTPUT'])
P.mkdir(parents=True,exist_ok=True)
root=pathlib.Path(os.environ['MICRO_BIN_ROOT'])
records=[]
# Predeclared four ABBA cycles: eight samples/variant/pattern, 300ms/sample.
for i,v in enumerate(['baseline','candidate','candidate','baseline']*4):
 command=['taskset','-c',os.environ.get('BENCH_CPU','12'),str(root/('vector-'+v+'.test')),'-test.run=^$','-test.bench=^BenchmarkRegexpTerminalPrefixRejection$','-test.benchtime=300ms','-test.count=1']
 t=time.monotonic();r=subprocess.run(command,env=dict(os.environ,GOMAXPROCS='1',LD_LIBRARY_PATH=os.environ['MO_LIBRARY_PATH']),text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
 (P/('vector-%02d-%s.txt'%(i,v))).write_text(r.stdout);records.append({'phase':i,'variant':v,'command':command,'exit':r.returncode,'elapsed':time.monotonic()-t})
 (P/'vector-commands.json').write_text(json.dumps(records,indent=2)+'\n');assert r.returncode==0
 print(i,v,'PASS',flush=True)
