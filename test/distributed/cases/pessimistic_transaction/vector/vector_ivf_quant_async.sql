-- ivfflat QUANTIZATION over the ASYNC (ISCP/CDC) maintenance path, for all four
-- narrow entry types:
--   * int8 / uint8 — the scaled quantizer: the CDC delta path (toIvfflatUpsert)
--                    must re-apply the trained [min,max] q(x)=x*mul+add (int8 ->
--                    [-128,127], uint8 -> [0,255]), not an identity cast;
--   * bf16 / float16 — lossless narrowing cast on the entry projection.
-- Every async index is built and maintained entirely by the CDC consumer (the
-- first iteration runs ALTER ... REINDEX ... FORCE_SYNC, later inserts/updates
-- ride the delta path). Three shared sleep(30) windows let the 10s-tick consumer
-- settle for all indexes at once.
--
-- The queries are `ORDER BY l2_distance(v, q) LIMIT k` with NO secondary sort key,
-- so the ivfflat index pushdown fires (the ivf_search table function), actually
-- exercising the quantized re-rank. Two well-separated clusters [1..5]/[50..54]
-- and cluster-center query points keep every top-k distance distinct (no ties),
-- so the result is deterministic without a tiebreaker.
SET probe_limit=10;

create table qi8(a int primary key, v vecf32(4));
insert into qi8 values (1,'[1,1,1,1]'),(2,'[3,3,3,3]'),(3,'[5,5,5,5]'),(4,'[50,50,50,50]'),(5,'[52,52,52,52]'),(6,'[54,54,54,54]');
create index xi8 using ivfflat on qi8(v) lists=2 op_type 'vector_l2_ops' quantization 'int8' ASYNC;

create table qu8(a int primary key, v vecf32(4));
insert into qu8 values (1,'[1,1,1,1]'),(2,'[3,3,3,3]'),(3,'[5,5,5,5]'),(4,'[50,50,50,50]'),(5,'[52,52,52,52]'),(6,'[54,54,54,54]');
create index xu8 using ivfflat on qu8(v) lists=2 op_type 'vector_l2_ops' quantization 'uint8' ASYNC;

create table qbf(a int primary key, v vecf32(4));
insert into qbf values (1,'[1,1,1,1]'),(2,'[3,3,3,3]'),(3,'[5,5,5,5]'),(4,'[50,50,50,50]'),(5,'[52,52,52,52]'),(6,'[54,54,54,54]');
create index xbf using ivfflat on qbf(v) lists=2 op_type 'vector_l2_ops' quantization 'bf16' ASYNC;

create table qf(a int primary key, v vecf32(4));
insert into qf values (1,'[1,1,1,1]'),(2,'[3,3,3,3]'),(3,'[5,5,5,5]'),(4,'[50,50,50,50]'),(5,'[52,52,52,52]'),(6,'[54,54,54,54]');
create index xf using ivfflat on qf(v) lists=2 op_type 'vector_l2_ops' quantization 'float16' ASYNC;

-- 1) initial async build (CDC reindex InitSQL). Low cluster -> 1,2,3 ; high -> 6,5,4.
select sleep(30);
select a from qi8 order by l2_distance(v,'[1,1,1,1]') limit 3;
select a from qi8 order by l2_distance(v,'[54,54,54,54]') limit 3;
select a from qu8 order by l2_distance(v,'[1,1,1,1]') limit 3;
select a from qu8 order by l2_distance(v,'[54,54,54,54]') limit 3;
select a from qbf order by l2_distance(v,'[1,1,1,1]') limit 3;
select a from qbf order by l2_distance(v,'[54,54,54,54]') limit 3;
select a from qf order by l2_distance(v,'[1,1,1,1]') limit 3;
select a from qf order by l2_distance(v,'[54,54,54,54]') limit 3;

-- 2) incremental rows ride the CDC delta path (toIvfflatUpsert). Row 7=[2,2,2,2]
--    joins the low cluster (now 1,7,2) and row 8=[53,53,53,53] the high cluster
--    (now 6,8,5) -- their appearance proves the delta path indexed them.
insert into qi8 values (7,'[2,2,2,2]'),(8,'[53,53,53,53]');
insert into qu8 values (7,'[2,2,2,2]'),(8,'[53,53,53,53]');
insert into qbf values (7,'[2,2,2,2]'),(8,'[53,53,53,53]');
insert into qf values (7,'[2,2,2,2]'),(8,'[53,53,53,53]');
select sleep(30);
select a from qi8 order by l2_distance(v,'[1,1,1,1]') limit 3;
select a from qi8 order by l2_distance(v,'[54,54,54,54]') limit 3;
select a from qu8 order by l2_distance(v,'[1,1,1,1]') limit 3;
select a from qu8 order by l2_distance(v,'[54,54,54,54]') limit 3;
select a from qbf order by l2_distance(v,'[1,1,1,1]') limit 3;
select a from qbf order by l2_distance(v,'[54,54,54,54]') limit 3;
select a from qf order by l2_distance(v,'[1,1,1,1]') limit 3;
select a from qf order by l2_distance(v,'[54,54,54,54]') limit 3;

-- 3) update row 1 across to the other cluster ([1,1,1,1] -> [55,55,55,55]); the
--    delta path must re-quantize + re-bucket it. Query the LOW cluster center
--    [1,1,1,1]: row 1 has LEFT it, so the top-3 is now 7,2,3 (row 1 absent) --
--    proving the delta UPDATE moved it. (Querying the high side instead would tie
--    under quantization: 55 clamps to the same code as 54.)
update qi8 set v = '[55,55,55,55]' where a = 1;
update qu8 set v = '[55,55,55,55]' where a = 1;
update qbf set v = '[55,55,55,55]' where a = 1;
update qf set v = '[55,55,55,55]' where a = 1;
select sleep(30);
select a from qi8 order by l2_distance(v,'[1,1,1,1]') limit 3;
select a from qu8 order by l2_distance(v,'[1,1,1,1]') limit 3;
select a from qbf order by l2_distance(v,'[1,1,1,1]') limit 3;
select a from qf order by l2_distance(v,'[1,1,1,1]') limit 3;

drop table qi8;
drop table qu8;
drop table qbf;
drop table qf;
