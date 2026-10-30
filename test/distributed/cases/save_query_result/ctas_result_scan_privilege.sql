-- issue #31536 follow-up: CREATE TABLE ... AS SELECT ... FROM result_scan(...)
-- must re-check the saved query's source privileges, like a direct RESULT_SCAN.
drop database if exists ctas_rs_priv;
create database ctas_rs_priv;
create table ctas_rs_priv.src (id int primary key, s varchar(10));
insert into ctas_rs_priv.src values (1, 'secret'), (2, 'x');
drop user if exists ctas_rs_u1;
drop role if exists ctas_rs_r1;
create role ctas_rs_r1;
create user ctas_rs_u1 identified by '123456' default role ctas_rs_r1;
grant connect on account * to ctas_rs_r1;
grant create table on database ctas_rs_priv to ctas_rs_r1;
grant show tables on database ctas_rs_priv to ctas_rs_r1;
grant insert on table ctas_rs_priv.* to ctas_rs_r1;
grant select on table ctas_rs_priv.src to ctas_rs_r1;
-- @session:id=1&user=sys:ctas_rs_u1:ctas_rs_r1&password=123456
set save_query_result = on;
use ctas_rs_priv;
/* save_result */select * from src order by id;
set @qid = last_query_id();
-- @session
revoke select on table ctas_rs_priv.src from ctas_rs_r1;
-- @session:id=1&user=sys:ctas_rs_u1:ctas_rs_r1&password=123456
select * from result_scan(@qid) as r;
create table stolen as select * from result_scan(@qid) as r;
show tables;
-- @session
grant select on table ctas_rs_priv.src to ctas_rs_r1;
-- @session:id=1&user=sys:ctas_rs_u1:ctas_rs_r1&password=123456
create table copied as select * from result_scan(@qid) as r;
show tables;
set save_query_result = off;
-- @session
select * from ctas_rs_priv.copied order by id;
drop database ctas_rs_priv;
drop user if exists ctas_rs_u1;
drop role if exists ctas_rs_r1;
