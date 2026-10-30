-- issue #31536: CREATE TABLE ... AS SELECT from a saved RESULT_SCAN
drop database if exists ctas_result_scan;
create database ctas_result_scan;
use ctas_result_scan;
create table original (id int primary key, name varchar(30));
insert into original values (1, 'hello'), (2, '中文'), (3, null);
set save_query_result = on;
/* save_result */select id, name from original order by id;
set @qid = last_query_id();
create table scan_copy as select * from result_scan(@qid) as r;
select * from scan_copy order by id;
create table scan_filtered as select id, concat(name, '!') as n from result_scan(@qid) as r where id > 1;
select * from scan_filtered order by id;
create table scan_count as select count(*) as c from result_scan(@qid) as r;
select * from scan_count;
set save_query_result = off;
drop database ctas_result_scan;
