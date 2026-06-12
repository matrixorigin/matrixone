-- ivfflat QUANTIZATION over the ASYNC (ISCP/CDC) maintenance path, for all three
-- narrow entry types:
--   * int8   — the scaled quantizer: the CDC delta path (toIvfflatUpsert) must
--              re-apply the trained [min,max] q(x)=x*mul+add, not an identity cast;
--   * bf16 / float16 — lossless narrowing cast on the entry projection.
-- Every async index is built and maintained entirely by the CDC consumer: the
-- first iteration runs ALTER ... REINDEX ... FORCE_SYNC, later inserts/updates
-- ride the delta path. Three shared sleep(30) windows let the 10s-tick consumer
-- settle for all three indexes at once. Two well-separated clusters [1..]/[50..];
-- each side is queried after each settle.
SET probe_limit=10;

create table qi8(a int primary key, v vecf32(4));
insert into qi8 values (1,'[1,1,1,1]'),(2,'[3,3,3,3]'),(3,'[5,5,5,5]'),(4,'[50,50,50,50]'),(5,'[52,52,52,52]'),(6,'[54,54,54,54]');
create index xi8 using ivfflat on qi8(v) lists=2 op_type 'vector_l2_ops' quantization 'int8' ASYNC;

create table qbf(a int primary key, v vecf32(4));
insert into qbf values (1,'[1,1,1,1]'),(2,'[3,3,3,3]'),(3,'[5,5,5,5]'),(4,'[50,50,50,50]'),(5,'[52,52,52,52]'),(6,'[54,54,54,54]');
create index xbf using ivfflat on qbf(v) lists=2 op_type 'vector_l2_ops' quantization 'bf16' ASYNC;

create table qf(a int primary key, v vecf32(4));
insert into qf values (1,'[1,1,1,1]'),(2,'[3,3,3,3]'),(3,'[5,5,5,5]'),(4,'[50,50,50,50]'),(5,'[52,52,52,52]'),(6,'[54,54,54,54]');
create index xf using ivfflat on qf(v) lists=2 op_type 'vector_l2_ops' quantization 'float16' ASYNC;

-- 1) initial async build (CDC reindex InitSQL): both clusters resolve for each type.
select sleep(30);
select a from qi8 order by l2_distance(v,'[1,1,1,1]'), a limit 3;
select a from qi8 order by l2_distance(v,'[54,54,54,54]'), a limit 3;
select a from qbf order by l2_distance(v,'[1,1,1,1]'), a limit 3;
select a from qbf order by l2_distance(v,'[54,54,54,54]'), a limit 3;
select a from qf order by l2_distance(v,'[1,1,1,1]'), a limit 3;
select a from qf order by l2_distance(v,'[54,54,54,54]'), a limit 3;

-- 2) incremental rows ride the CDC delta path (toIvfflatUpsert).
insert into qi8 values (7,'[2,2,2,2]'),(8,'[53,53,53,53]');
insert into qbf values (7,'[2,2,2,2]'),(8,'[53,53,53,53]');
insert into qf values (7,'[2,2,2,2]'),(8,'[53,53,53,53]');
select sleep(30);
select a from qi8 order by l2_distance(v,'[2,2,2,2]'), a limit 3;
select a from qi8 order by l2_distance(v,'[53,53,53,53]'), a limit 3;
select a from qbf order by l2_distance(v,'[2,2,2,2]'), a limit 3;
select a from qbf order by l2_distance(v,'[53,53,53,53]'), a limit 3;
select a from qf order by l2_distance(v,'[2,2,2,2]'), a limit 3;
select a from qf order by l2_distance(v,'[53,53,53,53]'), a limit 3;

-- 3) update an existing row across to the other cluster; the delta path must
--    re-narrow/re-quantize it under the trained model.
update qi8 set v = '[55,55,55,55]' where a = 1;
update qbf set v = '[55,55,55,55]' where a = 1;
update qf set v = '[55,55,55,55]' where a = 1;
select sleep(30);
select a from qi8 order by l2_distance(v,'[54,54,54,54]'), a limit 3;
select a from qbf order by l2_distance(v,'[54,54,54,54]'), a limit 3;
select a from qf order by l2_distance(v,'[54,54,54,54]'), a limit 3;

drop table qi8;
drop table qbf;
drop table qf;
