-- ivfflat NATIVE narrow base columns (vecbf16 / vecf16 / vecint8 / vecuint8) over the
-- ASYNC (ISCP/CDC) maintenance path. The synchronous CREATE INDEX on a narrow base
-- already works (vector_ivf_quantization.sql); this proves the ONGOING CDC delta path
-- carries a narrow base column too.
--
-- The fix this guards: the ISCP row pipeline (pkg/iscp/util.go) extracts a narrow base
-- column to its native Go slice ([]types.Float16 / []types.BF16 / []int8 / []uint8) and
-- serializes it back into the IvfflatSqlWriter VALUES tuple as CAST('[...]' as vecXXX(n)).
-- Before the fix only vecf32/vecf64 had extract + serialize cases, so any DML on a
-- narrow-base ivfflat index errored ("extractRowFromVector: unsupported type") and the
-- CDC consumer could never apply the delta.
--
-- Each index is built and maintained entirely by the CDC consumer (first iteration runs
-- the InitSQL build, later inserts/updates ride the delta path). Shared sleep(30) windows
-- let the 10s-tick consumer settle for all four indexes at once.
--
-- Two well-separated clusters [1..5]/[50..54] and cluster-center query points keep every
-- top-k distance distinct (no ties) so the result is deterministic for every narrow type;
-- integer values 1..55 are exact in bf16/f16 and in int8/uint8 range, so the narrow base
-- stores them without ambiguity. Queries are `ORDER BY l2_distance LIMIT k` with no
-- secondary sort key so the ivfflat index pushdown (ivf_search) actually fires.
SET probe_limit=10;

create table nbf(a int primary key, v vecbf16(4));
insert into nbf values (1,'[1,1,1,1]'),(2,'[3,3,3,3]'),(3,'[5,5,5,5]'),(4,'[50,50,50,50]'),(5,'[52,52,52,52]'),(6,'[54,54,54,54]');
create index xbf using ivfflat on nbf(v) lists=2 op_type 'vector_l2_ops' ASYNC;

create table nhf(a int primary key, v vecf16(4));
insert into nhf values (1,'[1,1,1,1]'),(2,'[3,3,3,3]'),(3,'[5,5,5,5]'),(4,'[50,50,50,50]'),(5,'[52,52,52,52]'),(6,'[54,54,54,54]');
create index xhf using ivfflat on nhf(v) lists=2 op_type 'vector_l2_ops' ASYNC;

create table ni8(a int primary key, v vecint8(4));
insert into ni8 values (1,'[1,1,1,1]'),(2,'[3,3,3,3]'),(3,'[5,5,5,5]'),(4,'[50,50,50,50]'),(5,'[52,52,52,52]'),(6,'[54,54,54,54]');
create index xi8 using ivfflat on ni8(v) lists=2 op_type 'vector_l2_ops' ASYNC;

create table nu8(a int primary key, v vecuint8(4));
insert into nu8 values (1,'[1,1,1,1]'),(2,'[3,3,3,3]'),(3,'[5,5,5,5]'),(4,'[50,50,50,50]'),(5,'[52,52,52,52]'),(6,'[54,54,54,54]');
create index xu8 using ivfflat on nu8(v) lists=2 op_type 'vector_l2_ops' ASYNC;

-- 1) initial async build (CDC reindex InitSQL). Low cluster -> 1,2,3 ; high -> 6,5,4.
select sleep(30);
select a from nbf order by l2_distance(v,'[1,1,1,1]') limit 3;
select a from nbf order by l2_distance(v,'[54,54,54,54]') limit 3;
select a from nhf order by l2_distance(v,'[1,1,1,1]') limit 3;
select a from nhf order by l2_distance(v,'[54,54,54,54]') limit 3;
select a from ni8 order by l2_distance(v,'[1,1,1,1]') limit 3;
select a from ni8 order by l2_distance(v,'[54,54,54,54]') limit 3;
select a from nu8 order by l2_distance(v,'[1,1,1,1]') limit 3;
select a from nu8 order by l2_distance(v,'[54,54,54,54]') limit 3;

-- 2) incremental rows ride the CDC delta path through the narrow-base extract +
--    serialize. Row 7=[2,2,2,2] joins the low cluster (now 1,7,2) and row 8=[53,53,53,53]
--    the high cluster (now 6,8,5) -- their appearance proves the delta path indexed them.
insert into nbf values (7,'[2,2,2,2]'),(8,'[53,53,53,53]');
insert into nhf values (7,'[2,2,2,2]'),(8,'[53,53,53,53]');
insert into ni8 values (7,'[2,2,2,2]'),(8,'[53,53,53,53]');
insert into nu8 values (7,'[2,2,2,2]'),(8,'[53,53,53,53]');
select sleep(30);
select a from nbf order by l2_distance(v,'[1,1,1,1]') limit 3;
select a from nbf order by l2_distance(v,'[54,54,54,54]') limit 3;
select a from nhf order by l2_distance(v,'[1,1,1,1]') limit 3;
select a from nhf order by l2_distance(v,'[54,54,54,54]') limit 3;
select a from ni8 order by l2_distance(v,'[1,1,1,1]') limit 3;
select a from ni8 order by l2_distance(v,'[54,54,54,54]') limit 3;
select a from nu8 order by l2_distance(v,'[1,1,1,1]') limit 3;
select a from nu8 order by l2_distance(v,'[54,54,54,54]') limit 3;

-- 3) update row 1 across to the other cluster ([1,1,1,1] -> [55,55,55,55]); the delta path
--    must re-extract + re-bucket the narrow vector. Query the LOW cluster center [1,1,1,1]:
--    row 1 has LEFT it, so the top-3 is now 7,2,3 (row 1 absent) -- proving the delta UPDATE
--    moved it.
update nbf set v = '[55,55,55,55]' where a = 1;
update nhf set v = '[55,55,55,55]' where a = 1;
update ni8 set v = '[55,55,55,55]' where a = 1;
update nu8 set v = '[55,55,55,55]' where a = 1;
select sleep(30);
select a from nbf order by l2_distance(v,'[1,1,1,1]') limit 3;
select a from nhf order by l2_distance(v,'[1,1,1,1]') limit 3;
select a from ni8 order by l2_distance(v,'[1,1,1,1]') limit 3;
select a from nu8 order by l2_distance(v,'[1,1,1,1]') limit 3;

drop table nbf;
drop table nhf;
drop table ni8;
drop table nu8;
