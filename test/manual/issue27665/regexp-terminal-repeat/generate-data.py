import json,math,hashlib,time,os
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import fsspec,aiohttp
p=Path(os.environ['BENCH_OUTPUT']);p.mkdir(parents=True,exist_ok=True)
root=Path(os.environ['BENCH_ROOT'])/'data';root.mkdir(parents=True,exist_ok=True)
url='https://datasets.clickhouse.com/hits_compatible/hits.parquet'
fs=fsspec.filesystem('https',client_kwargs={'timeout':aiohttp.ClientTimeout(total=120,connect=15,sock_read=60),'trust_env':True,'auto_decompress':False,'headers':{'Accept-Encoding':'identity'}})
start=time.monotonic();info=fs.info(url);print('Source',json.dumps(info,default=str),flush=True)
columns=['UserID','SearchPhrase','URL','Referer'];count=10_000_000
with fs.open(url,'rb',block_size=1<<20,cache_type='blockcache',cache_options={'maxblocks':16}) as remote:
    source=pq.ParquetFile(remote);metadata=source.metadata
    sizes=sorted(metadata.row_group(i).num_rows for i in range(metadata.num_row_groups));median=sizes[len(sizes)//2]
    full=[i for i in range(metadata.num_row_groups) if metadata.row_group(i).num_rows>=median]
    n=math.ceil(count/median);assert n<=len(full)
    selected=[full[round(j*(len(full)-1)/(n-1))] for j in range(n)] if n>1 else [full[0]]
    assert len(set(selected))==n
    quotas=[count//n+(j<count%n) for j in range(n)]
    schema=pa.schema([source.schema_arrow.field(c) for c in columns]);output=root/'hits-10m.parquet'
    with pq.ParquetWriter(output,schema,compression='zstd') as writer:
        for k,(rg,quota) in enumerate(zip(selected,quotas)):
            table=source.read_row_group(rg,columns=columns,use_threads=False).slice(0,quota)
            assert len(table)==quota
            writer.write_table(table,row_group_size=131072)
            print(json.dumps({'group':rg,'rows':quota,'done_groups':k+1,'groups':n,'elapsed':time.monotonic()-start}),flush=True)
    manifest={'source_url':url,'source_info':{k:str(v) for k,v in info.items() if k.lower() not in ['cookies']},'source_rows':metadata.num_rows,'source_groups':metadata.num_row_groups,'source_schema':str(source.schema_arrow),'schema':str(schema),'columns':columns,'algorithm':'Choose full row groups with at least median row count, evenly spaced inclusive quantile indexes; take leading quotas whose sum is exactly10000000. Regenerated Linux dataset, not original macOS projection.','selected_groups':selected,'rows_per_group':quotas,'rows':count,'pyarrow':pa.__version__,'output':str(output),'elapsed':time.monotonic()-start}
assert pq.ParquetFile(output).metadata.num_rows==count
h=hashlib.sha256()
with output.open('rb') as f:
    for b in iter(lambda:f.read(4<<20),b''):h.update(b)
manifest['sha256']=h.hexdigest();manifest['size']=output.stat().st_size
(p/'dataset-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
print('PASS',json.dumps(manifest),flush=True)
