# `async_challenge.py.txt`

Artifact: {"name":"async_challenge.py.txt","bytes":2005,"sha256":"032ead5905ebfca1e936bb753732947e157ab939a59dd35356b4edd77e54dda9","encoding":"utf-8","stored_bytes":2005}

````python
import asyncio,json,os
from pathlib import Path
from matrixone import AsyncClient
from matrixone.config import get_connection_kwargs
root=Path(__file__).parent
async def main():
 client=AsyncClient();await client.connect(**get_connection_kwargs());ready=asyncio.Event();records=[]
 async def read(index,session):
  await ready.wait()
  if session:
   async with client.session() as tx:stats=await tx.metadata.get_table_brief_stats('test_metadata_db','test_users')
  else:stats=await client.metadata.get_table_brief_stats('test_metadata_db','test_users')
  assert stats['test_users']['row_cnt']==50,(index,stats)
  return {'call':index,'context':'independent-session' if session else 'client','row_cnt':stats['test_users']['row_cnt']}
 try:
  tasks=[asyncio.create_task(read(i,bool(i%2))) for i in range(8)]
  ready.set();records=await asyncio.wait_for(asyncio.gather(*tasks),30)
  assert client._engine.pool.checkedout()==0
  errors=[]
  for session in [False,True]:
   try:
    if session:
     async with client.session() as tx:await tx.metadata.get_table_brief_stats('test_metadata_db','missing30994')
    else:await client.metadata.get_table_brief_stats('test_metadata_db','missing30994')
   except Exception as e:errors.append({'context':'session' if session else 'client','type':type(e).__name__,'message':str(e)})
   else:raise AssertionError('missing table error did not propagate')
  assert client._engine.pool.checkedout()==0
  stats=await client.metadata.get_table_brief_stats('test_metadata_db','test_users');assert stats['test_users']['row_cnt']==50
  result={'overlapping_calls':records,'propagated_errors':errors,'checked_out_connections_after_calls':client._engine.pool.checkedout(),'post_error_row_cnt':stats['test_users']['row_cnt']}
 finally:await client.disconnect()
 assert client._engine is None
 result['disconnected']=True;(root/'async-challenge-results.json').write_text(json.dumps(result,indent=2));print('ASYNC_CHALLENGE_PASS',json.dumps(result),flush=True)
asyncio.run(main())

````
