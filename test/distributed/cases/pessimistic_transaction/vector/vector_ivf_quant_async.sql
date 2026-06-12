-- ivfflat int8 QUANTIZATION over the ASYNC (ISCP/CDC) maintenance path.
-- An async index builds and updates entirely through the CDC consumer:
--   * the first CDC iteration runs ALTER ... REINDEX ... FORCE_SYNC, which trains
--     the int8 [min,max] bounds and builds the initial scaled entries;
--   * later inserts/updates ride the CDC delta path (toIvfflatUpsert), which must
--     re-apply the trained quantizer instead of an identity vecf32->vecint8 cast.
-- Two well-separated clusters [1..] and [50..]; each side is queried after the
-- async consumer settles. sleep(30) lets the 10s-tick consumer catch up.
SET probe_limit=10;

create table q(a int primary key, v vecf32(4));
insert into q values (1,'[1,1,1,1]'),(2,'[3,3,3,3]'),(3,'[5,5,5,5]'),(4,'[50,50,50,50]'),(5,'[52,52,52,52]'),(6,'[54,54,54,54]');

-- async int8 index: built by the CDC consumer (reindex InitSQL), not synchronously.
create index qi8 using ivfflat on q(v) lists=2 op_type 'vector_l2_ops' quantization 'int8' ASYNC;

-- wait for the first CDC iteration to build the index.
select sleep(30);

-- initial async build: both clusters resolve correctly.
select a from q order by l2_distance(v,'[1,1,1,1]'), a limit 3;
select a from q order by l2_distance(v,'[54,54,54,54]'), a limit 3;

-- incremental rows ride the CDC delta path (toIvfflatUpsert + trained bounds).
insert into q values (7,'[2,2,2,2]'),(8,'[53,53,53,53]');
select sleep(30);

-- new rows are indexed and rank correctly against each cluster.
select a from q order by l2_distance(v,'[2,2,2,2]'), a limit 3;
select a from q order by l2_distance(v,'[53,53,53,53]'), a limit 3;

-- update an existing row across to the other cluster; the delta path must
-- re-quantize it under the trained bounds.
update q set v = '[55,55,55,55]' where a = 1;
select sleep(30);
select a from q order by l2_distance(v,'[54,54,54,54]'), a limit 3;

drop table q;
