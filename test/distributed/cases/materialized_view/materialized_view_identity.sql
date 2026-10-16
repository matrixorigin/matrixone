drop database if exists mv_identity;
create database mv_identity;
use mv_identity;
-- The typed VALUES transport used by delta SQL must retain earlier NULLs.
select count(*)=2 and count(column_0)=1 as nullable_values from (values row(cast(null as int)),row(cast(1 as int))) v;
select count(*)=2 as nullable_groups from (select column_0,count(*) c from (values row(cast(null as int)),row(cast(1 as int))) v group by column_0) g;
-- Comments are user text, never object identity.
create table ordinary(a int primary key) comment='matrixone materialized view state';
insert into ordinary values(1);
update ordinary set a=2;
select a=2 as ok from ordinary;
delete from ordinary;
drop table ordinary;
create table spoof(a int) properties('mv_materialized'='true');
create table __mo_mv_state_spoof(a int);
create table src(k int primary key,v int);
insert into src values(7,5);
-- Every refresh mode must have complete tracked inputs. Ordinary views retain subqueries.
create materialized view hidden_input refresh complete on change as select k,sum(v) s from src where exists(select 1 from src s2 where s2.k=src.k) group by k;
create materialized view volatile_input refresh fast on change as select k,count(*) c from src group by k having rand()>0.5;
create materialized view session_input refresh complete on demand as select k,v+@source_id s from src;
select count(*)=0 as rejected_inputs from mo_catalog.mo_tables where reldatabase='mv_identity' and relname in ('hidden_input','volatile_input','session_input');
create view ordinary_query as select k,v from src where exists(select 1 from src s2 where s2.k=src.k);
select sum(v)=5 as ordinary_query_works from ordinary_query;
drop view ordinary_query;
create materialized view mv refresh complete on demand as select k,sum(v) s from src group by k;
refresh materialized view mv;
select sum(s)=5 as ok from mv;
prepare read_mv from 'select sum(s)=5 as ok from mv';
execute read_mv;
-- Refresh shares the caller transaction; source and target both roll back.
begin;
insert into src values(8,11);
refresh materialized view mv;
select sum(s)=16 as ok from mv;
rollback;
select sum(s)=5 as ok from mv;
select sum(v)=5 as ok from src;
-- Unrestricted DELETE preserves source identity and manual refresh semantics.
set @source_id=(select rel_id from mo_catalog.mo_tables where reldatabase='mv_identity' and relname='src');
delete from src;
select rel_id=@source_id as ok from mo_catalog.mo_tables where reldatabase='mv_identity' and relname='src';
select sum(s)=5 as ok from mv;
refresh materialized view mv;
select count(*)=0 as ok from mv;
insert into src values(7,5);
refresh materialized view mv;
execute read_mv;
insert into mv values(1,1);
update mv set s=0;
delete from mv;
truncate table mv;
alter table mv add column x int;
alter table mv rename to other;
-- Replacing a same-name source invalidates new reads, cached reads and refresh.
drop table src;
create table src(k int primary key,v int);
insert into src values(99,99);
select * from mv;
execute read_mv;
refresh materialized view mv;
deallocate prepare read_mv;
drop materialized view mv;
create materialized view mv refresh complete on demand as select k,count(*) c from src group by k;
refresh materialized view mv;
truncate table src;
select * from mv;
refresh materialized view mv;
drop materialized view mv;
create materialized view mv refresh complete on demand as select k,count(*) c from src group by k;
refresh materialized view mv;
alter table src add column x int;
select * from mv;
refresh materialized view mv;
drop materialized view mv;
insert into src(k,v) values(1,10);
create materialized view fast refresh fast on change as select v%2 k,count(*) c,sum(v) s,count(distinct v) d from src group by v%2;
-- @wait_expect(2, 30)
select sum(s)=10 as ok from fast;
-- Source rename keeps the target identity and DROP retires all owned resources.
alter table src rename to renamed;
select * from fast;
drop materialized view fast;
select count(*)=0 as ok from mo_catalog.mo_iscp_log where json_extract(job_spec,'$.DBName')='mv_identity' and drop_at is null;
select count(*)=0 as ok from mo_catalog.mo_tables where reldatabase='mv_identity' and relname like '__mo_mv_state_%';
drop database mv_identity;
