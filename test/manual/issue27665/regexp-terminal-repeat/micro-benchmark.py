#!/usr/bin/env python3
import os,pathlib,subprocess,json,time
P=pathlib.Path(os.environ['BENCH_OUTPUT'])
P.mkdir(parents=True,exist_ok=True)
root=pathlib.Path(os.environ['MICRO_BIN_ROOT'])
# Predeclared: 4 repetitions of B/O/C/C/O/B, 8 samples per binary/control, 200ms/sample.
order=['baseline','original','candidate','candidate','original','baseline']*4
records=[]
for i,v in enumerate(order):
 command=['taskset','-c',os.environ.get('BENCH_CPU','12'),str(root/(v+'.test')),'-test.run=^$','-test.bench=Benchmark(TerminalRepeat|BitStatePrefixRejection)$','-test.benchtime=200ms','-test.count=1']
 t=time.monotonic();r=subprocess.run(command,env=dict(os.environ,GOMAXPROCS='1'),text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
 (P/('micro-%02d-%s.txt'%(i,v))).write_text(r.stdout);records.append({'phase':i,'variant':v,'command':command,'exit':r.returncode,'elapsed':time.monotonic()-t})
 (P/'micro-commands.json').write_text(json.dumps(records,indent=2)+'\n');assert r.returncode==0
 print(i,v,'PASS',flush=True)
