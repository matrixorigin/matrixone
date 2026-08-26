drop database if exists d1;
create database d1;
use d1;
drop table if exists t1;
drop table if exists t2;
create table t1(c1 int not null, c2 int not null, c3 int not null) cluster by c1;
create table t2(c1 int not null, c2 int not null, c3 int not null) cluster by c1;
-- Sample the original 5M/4M key domains. Patched statistics below preserve
-- the original planner thresholds while the persisted test data stays small.
insert into t1
select result * 50 + result % 50, result * 50 + result % 50, result * 50 + result % 50
from generate_series(100000) g;
insert into t2
select result * 50 + result % 50, result * 50 + result % 50, result * 50 + result % 50
from generate_series(80000) g;
set @@join_spill_mem = 1000;
-- @separator:table
select mo_ctl('dn', 'flush', 'd1.t1');
-- @separator:table
select mo_ctl('dn', 'flush', 'd1.t2');
-- Keep the production planner's original 5M/4M estimates. Unlike forcing an
-- exec type, patched table statistics still exercise shuffle selection.
set @spill_t1_stats = '{"table_cnt":5000000,"block_number":640,"accurate_object_number":40,"approx_object_number":40,"ndv_map":{"c1":5000000,"c2":5000000,"c3":5000000},"min_val_map":{"c1":1,"c2":1,"c3":1},"max_val_map":{"c1":5000000,"c2":5000000,"c3":5000000},"shuffle_range_map":{"c1":{"overlap":0.1,"uniform":1,"result":[1,1250000,2500000,3750000,5000000]},"c2":{"overlap":0.1,"uniform":1,"result":[1,1250000,2500000,3750000,5000000]},"c3":{"overlap":0.1,"uniform":1,"result":[1,1250000,2500000,3750000,5000000]}}}';
set @spill_t2_stats = '{"table_cnt":4000000,"block_number":520,"accurate_object_number":33,"approx_object_number":33,"ndv_map":{"c1":4000000,"c2":4000000,"c3":4000000},"min_val_map":{"c1":1,"c2":1,"c3":1},"max_val_map":{"c1":4000000,"c2":4000000,"c3":4000000},"shuffle_range_map":{"c1":{"overlap":0.1,"uniform":1,"result":[1,1000000,2000000,3000000,4000000]},"c2":{"overlap":0.1,"uniform":1,"result":[1,1000000,2000000,3000000,4000000]},"c3":{"overlap":0.1,"uniform":1,"result":[1,1000000,2000000,3000000,4000000]}}}';
select table_cnt from table_stats(
    'd1.t1',
    'patch',
    @spill_t1_stats
) g;
select table_cnt from table_stats(
    'd1.t2',
    'patch',
    @spill_t2_stats
) g;
-- @separator:table
-- @regex("(?i)ap query plan on multicn", true)
-- @ignore:0
explain (check '["Join Type: INNER", "shuffle: range"]')
select count(*) from t1,t2 where t1.c1=t2.c1;
select count(*) from t1,t2 where t1.c1=t2.c1;
-- Prove that the forced-spill setting reaches the join spill path, not merely
-- a shuffle-capable logical plan. The following count remains the independent
-- semantic oracle.
-- @ignore:0
-- @regex("SpillRows=[1-9][0-9]*", true)
-- @regex("SpillSize=[1-9][0-9.]* [KMGT]iB", true)
explain (analyze true, check '["Join Type: INNER", "shuffle: range"]')
select count(*) from t1,t2 where t1.c1=t2.c1;
-- @separator:table
-- @ignore:0
explain (check '["Group Key: t1.c1 shuffle: REUSE", "Join Type: INNER", "shuffle: range"]')
select count(*) as cnt from t1,t2 where t1.c1=t2.c1 group by t1.c1 having cnt>1;
select count(*) as cnt from t1,t2 where t1.c1=t2.c1 group by t1.c1 having cnt>1;
-- @separator:table
-- @ignore:0
explain (check '["Join Type: INNER", "shuffle: range"]')
select count(*) from t1,t2 where t1.c2=t2.c2;
select count(*) from t1,t2 where t1.c2=t2.c2;
select count(*) from t1,t2 where t1.c2=t2.c2 and t2.c3<500000;
select count(*) from t1,t2 where t1.c2=t2.c2 and t2.c3<1500000;
select table_cnt from table_stats('d1.t1', 'patch', @spill_t1_stats) g;
select table_cnt from table_stats('d1.t2', 'patch', @spill_t2_stats) g;
-- @ignore:0
explain (check '["Join Type: SEMI", "shuffle: range"]')
select count(*) from t1 where t1.c2 in ( select c2 from t2 where t2.c3>100000 );
select count(*) from t1 where t1.c2 in ( select c2 from t2 where t2.c3>100000 );
select table_cnt from table_stats('d1.t1', 'patch', @spill_t1_stats) g;
select table_cnt from table_stats('d1.t2', 'patch', @spill_t2_stats) g;
-- @ignore:0
explain (check '["Join Type: ANTI", "shuffle: range"]')
select count(*) from t1 where t1.c2 not in ( select c3 from t2 where t2.c3 between 100 and 700000 );
select count(*) from t1 where t1.c2 not in ( select c3 from t2 where t2.c3 between 100 and 700000 );
select table_cnt from table_stats('d1.t1', 'patch', @spill_t1_stats) g;
select table_cnt from table_stats('d1.t2', 'patch', @spill_t2_stats) g;
-- @ignore:0
explain (check '["Join Type: ANTI", "shuffle: range"]')
select count(*) from t1 where t1.c3<800000 and t1.c2 not in ( select c3 from t2 where t2.c3 between 10000 and 600000 );
select count(*) from t1 where  t1.c3<800000 and t1.c2 not in ( select c3 from t2 where t2.c3 between 10000 and 600000 );
select count(*) from t1 where t1.c1 <300000 and  t1.c2 in ( select c2 from t2 where t2.c3>100000 );
select count(*) from t1 left join t2 on t1.c1=t2.c1 where t1.c3 >5000000;
-- @separator:table
-- @ignore:0
explain (check '["Join Type: LEFT", "shuffle: range"]')
select count(*) from t1 left join t2 on t1.c1=t2.c1 and t1.c3 >t2.c3;
select count(*) from t1 left join t2 on t1.c1=t2.c1 and t1.c3 >t2.c3;
-- right outer join (preserves all t1 rows)
select count(*) from t2 right join t1 on t1.c1=t2.c1;
-- multi-column equi-join
select count(*) from t1, t2 where t1.c1=t2.c1 and t1.c2=t2.c2;

-- Keep plan output compact while retaining cross-CN shuffle.
set @@max_dop = 1;

-- Projected IN is planned as MARK. Both keys are NOT NULL, so every exact
-- match is co-located and bucket-local state is sufficient for FALSE misses.
-- @ignore:0
explain (check '["Join Type: MARK", "shuffle: range"]')
select sum(case when marker is true then 1 else 0 end) as true_count,
       sum(case when marker is false then 1 else 0 end) as false_count,
       sum(case when marker is null then 1 else 0 end) as null_count
from (
    select t1.c1 in (select c1 from t2) as marker
    from t1
) s;

select sum(case when marker is true then 1 else 0 end) as true_count,
       sum(case when marker is false then 1 else 0 end) as false_count,
       sum(case when marker is null then 1 else 0 end) as null_count
from (
    select t1.c1 in (select c1 from t2) as marker
    from t1
) s;

set @@max_dop = 0;

create table t3(c1 int not null, c2 int not null)cluster by c1;
insert into t3
select result * 5 + result % 5, result * 5 + result % 5
from generate_series(1,200000)g;
-- @separator:table
select mo_ctl('dn', 'flush', 'd1.t3');
select count(*) from t3 where t3.c2 in (select c3 from t1 where t1.c2!=20000 and  t1.c2 not in ( select c2 from t2 where t2.c3>150000 ));
select count(*) from t3 where t3.c1<100000 and t3.c2 not in (select c3 from t1 where t1.c2!=30000 and  t1.c2  in ( select c2 from t2 where t2.c3<850000 ));
select count(*) from t1,t2,t3 where t1.c1=t2.c1 and t1.c2=t3.c2 and t2.c2<900000 and t3.c1<500000;
select table_cnt from table_stats('d1.t1', 'patch', @spill_t1_stats) g;
select table_cnt from table_stats('d1.t2', 'patch', @spill_t2_stats) g;
-- @ignore:0
explain (check '["Join Type: INNER", "shuffle: REUSE", "Group Key: t1.c1 shuffle: range"]')
select count(*) from (select c1 from t1 group by c1) s1, t2 where s1.c1=t2.c1 and t2.c2<1000000;
select count(*) from (select c1 from t1 group by c1) s1, t2 where s1.c1=t2.c1 and t2.c2<1000000;
delete from t1 where c3%5=1;
insert into t1 values(-1,-2,-3);
insert into t1 values(10,11,12);
select count(*) from t1 where c3!=0;
drop table t3;
create table t4(c1 int not null, c2  int unsigned) cluster by c1;
insert into t4
select result * 10 + result % 10, result * 10 + result % 10
from generate_series(100000) g;
insert into t4
select result * 10 + result % 10 + 10000000, result * 10 + result % 10 + 10000000
from generate_series(100000) g;
-- @separator:table
select mo_ctl('dn', 'flush', 'd1.t4');
-- @separator:table
-- @ignore:0
explain (check '["Group Key: t4.c1 shuffle: range"]')
select count(*) as cnt from t4 group by c1 having cnt>1;
select count(*) as cnt from t4 group by c1 having cnt>1;
-- @separator:table
-- @ignore:0
explain (check '["Group Key: t4.c2 shuffle: range"]')
select count(*) as cnt from t4 group by c2 having cnt>1;
select count(*) as cnt from t4 group by c2 having cnt>1;
-- dedup join spill: large ODKU with low join_spill_mem
drop table if exists t_dedup_spill;
create table t_dedup_spill (id int primary key, val int);
insert into t_dedup_spill select *,* from generate_series(400000) g;
set @@join_spill_mem = 1000;
-- @ignore:0
explain (check '["Join Type: DEDUP", "shuffle: hash"]')
insert into t_dedup_spill select *, 0 from generate_series(200000, 600000) g
on duplicate key update val = val + 1;
insert into t_dedup_spill select *, 0 from generate_series(200000, 600000) g
on duplicate key update val = val + 1;
select count(*) from t_dedup_spill;
drop table if exists t_dedup_spill;

drop database if exists d1;
drop database if exists test;
create database test;
use test;
create table t1(a int primary key, b int);
select enable_fault_injection();
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'test.t1');
-- One hundred thousand rows still creates multiple fault-forced small objects
-- and exercises the same update path without making a spill regression own a
-- 10M-row DML.
insert into t1 select *, 1 from generate_series(1, 100*1000)g;
set @before_object_count = (
    select count(distinct object_name) from metadata_scan('test.t1', 'a') m
);
select @before_object_count > 1 as multiple_objects,
       sum(rows_cnt) = 100000 as all_rows_flushed
from metadata_scan('test.t1', 'a') m;
select sum(b) from t1;
-- Updating 20% keeps the small dataset above the fault-injected S3 threshold.
update t1 set b = b + 1 where a mod 5 = 0;
-- The injected update is complete; release global FI state before assertions.
select disable_fault_injection();
select count(distinct object_name) > @before_object_count as update_created_object,
       sum(rows_cnt) = 120000 as update_rows_flushed
from metadata_scan('test.t1', 'a') m;
select sum(b) from t1;
drop database if exists test;
