-- Four persisted blocks put repeated keys on separate scan workers. Injected
-- statistics reach the shuffle threshold without a multi-million-row fixture.
drop database if exists adjacent_aggregate_reuse;
create database adjacent_aggregate_reuse;
use adjacent_aggregate_reuse;
set @saved_max_dop=@@max_dop;
set max_dop=4;
create table t(k int);
insert into t select case when result%3=0 then null else result%2 end from generate_series(32768) g;
-- @separator:table
select mo_ctl('dn','flush','adjacent_aggregate_reuse.t');
select table_cnt from table_stats('adjacent_aggregate_reuse.t','refresh','full') g;
select table_cnt from table_stats('adjacent_aggregate_reuse.t','patch','{"table_cnt":5000000,"block_number":640,"ndv_map":{"k":5000000}}') g;
select count(*) from (select distinct k from (select distinct k from t) a) b;
select distinct k from (select distinct k from t) a order by k;
select count(*) from (select distinct k from (select distinct k from t where k>1) a) b;
prepare nested_distinct from 'select count(*) from (select distinct k from (select distinct k from t) a) b';
execute nested_distinct;
execute nested_distinct;
deallocate prepare nested_distinct;
set max_dop=@saved_max_dop;
drop database adjacent_aggregate_reuse;
