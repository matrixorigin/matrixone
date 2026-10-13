-- Preserve the large-plan threshold with six input pairs. The regression
-- checks DEDUP shuffle selection; estimates are not a capacity measurement.
drop database if exists cross_join_cardinality;
create database cross_join_cardinality;
use cross_join_cardinality;
set @saved_max_dop=@@max_dop;
set @saved_join_spill_mem=@@join_spill_mem;
set max_dop=2;
set join_spill_mem=1000;
create table a(n int primary key);
create table b(n int primary key);
create table t(a int,b int,primary key(a,b));
insert into a values(0),(1);
insert into b values(0),(1),(2);
select table_cnt from table_stats('cross_join_cardinality.a','patch','{"table_cnt":1000,"block_number":1,"accurate_object_number":1,"ndv_map":{"n":1000}}') g;
select table_cnt from table_stats('cross_join_cardinality.b','patch','{"table_cnt":1000,"block_number":1,"accurate_object_number":1,"ndv_map":{"n":1000}}') g;
select table_cnt from table_stats('cross_join_cardinality.t','patch','{"table_cnt":1000,"block_number":1,"accurate_object_number":1,"ndv_map":{"a":1000,"b":1000}}') g;
-- @ignore:0
explain (check '["Join Type: DEDUP", "shuffle: hash"]') insert into t select a.n,b.n from a cross join b;
insert into t select a.n,b.n from a cross join b;
select count(*) as rows_written,count(distinct 100*a+b) as distinct_keys,min(100*a+b) as minimum,max(100*a+b) as maximum,sum(100*a+b) as checksum from t;
-- @regex("Duplicate entry",true)
insert into t select a.n,b.n from a cross join b;
select count(*) as rows_written,sum(100*a+b) as checksum from t;
select count(*) as pairs,sum(100*a.n+b.n) as checksum from a inner join b on a.n=b.n;
select count(*) as pairs,sum(100*a.n+b.n) as checksum from a inner join b on a.n<b.n;
insert into t select a.n+10,b.n from a cross join b where a.n<0;
select count(*) as rows_written,sum(100*a+b) as checksum from t;
begin;
insert into t select a.n+10,b.n from a cross join b;
select count(*) as rows_written,sum(100*a+b) as checksum from t;
rollback;
select count(*) as rows_written,sum(100*a+b) as checksum from t;
insert into t select a.n+20,b.n from a cross join b;
select count(*) as rows_written,sum(100*a+b) as checksum from t;
-- Repeated matches must delete each preserved row once; rollback restores it.
-- @ignore:0
explain (check '["Join Type: RIGHT SEMI"]') delete from t where a in (select a.n from a cross join b);
begin;
delete from t where a in (select a.n from a cross join b);
select count(*) as rows_remaining,sum(100*a+b) as checksum from t;
rollback;
select count(*) as rows_remaining,sum(100*a+b) as checksum from t;
delete from t where a in (select a.n from a cross join b);
select count(*) as rows_remaining,min(a) as minimum,max(a) as maximum,sum(100*a+b) as checksum from t;
set join_spill_mem=@saved_join_spill_mem;
set max_dop=@saved_max_dop;
drop database cross_join_cardinality;
