drop user if exists perform_user;
drop role if exists perform_role;
drop database if exists perform_db;

create database perform_db;
create table perform_db.perform (perform int);
insert into perform_db.perform values (3), (1), (2);

perform select perform from perform_db.perform order by perform;
select row_count();
perform select perform from perform_db.perform where perform > 100;
perform with cte as (select perform from perform_db.perform where perform > 1) select * from cte order by perform;
perform select perform from perform_db.perform where perform = 1 union all select perform from perform_db.perform where perform = 2 order by perform;

perform select 1; select 'after-perform' as marker;
select 'before-perform' as marker; perform select 1;

set save_query_result = on;
perform select perform as value from perform_db.perform order by perform;
set @perform_qid = last_query_id();
select * from result_scan(@perform_qid) as u order by value;
select statement, tables, ColumnMap from meta_scan(@perform_qid) as u;
set save_query_result = off;

set @perform_min = 1;
prepare perform_stmt from perform select perform from perform_db.perform where perform > ? order by perform;
execute perform_stmt using @perform_min;
select row_count();
deallocate prepare perform_stmt;

create procedure perform_db.perform_proc() 'begin perform select perform from perform_db.perform order by perform; end';
call perform_db.perform_proc();
select row_count();
drop procedure perform_db.perform_proc;

perform 1;
perform * from perform_db.perform;
perfrom select 1;
perform values row(1);
perform insert into perform_db.perform values (4);
perform select 1 into outfile 'perform.csv';
perform with outfile_cte as (select 1 into outfile 'perform-cte.csv') select * from outfile_cte;
perform select (select 1 into outfile 'perform-subquery.csv');
perform select * from perform_db.perform_missing;

create role perform_role;
grant connect on account * to perform_role;
create user perform_user identified by '111';
grant perform_role to perform_user;
-- @session:id=1&user=sys:perform_user:perform_role&password=111
perform select * from perform_db.perform;
-- @session
grant select on table perform_db.perform to perform_role;
-- @session:id=2&user=sys:perform_user:perform_role&password=111
perform select * from perform_db.perform;
select row_count();
-- @session

drop user perform_user;
drop role perform_role;
drop database perform_db;
