-- @suite
-- @case
-- @desc: Domainless NULL value windows materialize safely; finite RANGE excludes NULL tails
-- @label:bvt

drop database if exists window_null_domain_range;
create database window_null_domain_range;
use window_null_domain_range;
create table src(id int primary key,k int,v int);
insert into src values(1,NULL,100),(2,0,10),(3,1,20);
select id,first_value(NULL) over(order by id) as fv,
last_value(NULL) over(order by id) as lv,nth_value(NULL,2) over(order by id) as nv,
lag(NULL,1,7) over(order by id) as lagv,lead(NULL,1,7) over(order by id) as leadv
from src order by id;
select id,count(*) over(order by k desc range between 1 following and 2 following) as n,
sum(v) over(order by k desc range between 1 following and 2 following) as total,
first_value(v) over(order by k desc range between 1 following and 2 following) as fv
from src order by id;
create table materialized as select id,first_value(NULL) over(order by id) as fv from src;
select data_type from information_schema.columns
where table_schema='window_null_domain_range' and table_name='materialized' and column_name='fv';
select id,fv from materialized order by id;
drop database window_null_domain_range;
