set global save_query_result = on;
drop table if exists tt;
create table tt (a int);
insert into tt values(1), (2);
/* save_result */select * from tt;
select * from result_scan(last_query_id()) as u;
/* save_result */select * from tt;
select count(*) from meta_scan(last_query_id()) as u;
set global save_query_result = off;

select * from tt;
-- @bvt:issue#9886
select * from result_scan(last_query_id()) as u;
-- @bvt:issue
set global save_query_result = on;
drop table if exists t2;
create table t2 (a int, b int, c int);
insert into t2 values(1, 2, 3), (1, 2, 3);
/* save_result */select c from tt, t2 where tt.a = t2.a;
select * from result_scan(last_query_id()) as u;
/* save_result */select c from tt, t2 where tt.a = t2.a;
/* save_result */select t2.b from result_scan(last_query_id()) as u, t2 where u.c = t2.c;
select * from result_scan(last_query_id()) as u;
/* save_result */select c from tt, t2 where tt.a = t2.a;
select * from result_scan(last_query_id()) as u, result_scan(last_query_id()) as v limit 1;
set global save_query_result = off;

set global save_query_result = on;
/* save_result */select tt.a from tt, t2;
select tables from meta_scan(last_query_id()) as u;
set global query_result_maxsize = 0;
/* save_result */select tt.a from tt, t2;
select char_length(result_path) from meta_scan(last_query_id()) as u;
/* save_result */select tt.a from tt, t2;
select result_size = 0 from meta_scan(last_query_id()) as u;
set global save_query_result = off;

set global save_query_result = on;
set global query_result_maxsize = 100;
create role rrrqqq;
grant rrrqqq to dump;
/* save_result */select * from tt;
set role rrrqqq;
select * from meta_scan(last_query_id(-2)) as u;
set role moadmin;
create database db111;
create table db111.tt1 (a int);
insert into db111.tt1 values(1), (2);
create table db111.tt2 (a int);
insert into db111.tt2 values(1), (2);
grant select on table db111.tt1 to rrrqqq;
/* save_result */select * from db111.tt1;
/* save_result */select * from db111.tt2;
set role rrrqqq;
select * from result_scan(last_query_id(-3)) as u;
select * from meta_scan(last_query_id(-3)) as u;
set role moadmin;
drop role rrrqqq;
select * from result_scan('d8fb97e7-e30e-11ed-8d80-d6aeb943c8b4') as u;
--need to clean database db111
drop database if exists db111;
set global save_query_result = off;

create account abc ADMIN_NAME 'admin' IDENTIFIED BY '123456';
-- @session:id=2&user=abc:admin&password=123456
set global save_query_result = on;
create database test;
use test;
drop table if exists tt;
create table tt (a int);
insert into tt values(1), (2);
/* save_result */select * from tt;
select * from result_scan(last_query_id()) as u;
/* save_result */select * from tt;
select count(*) from meta_scan(last_query_id()) as u;
set global save_query_result = off;
-- @session
drop account abc;