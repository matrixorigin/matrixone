drop database if exists cte_test;
create database cte_test;
use cte_test;
create table t1(a int);
insert into t1 values (1),(2);

-- view v1
create view v1 as
with
    c as (
        select * from  t1
    )
select
    *
from
    c;

select * from v1;

-- query 1
with
    c as (
        select * from t1
    )
select
    *
from
    (
        select * from c
        union all
        select * from v1
    )
;


--view v2
create view v2 as
with
    v2 as (
        select a from t1
    )
select distinct
    *
from
    (
        select * from v2
    )
;

select * from v2;



--view v3
create view v3 as
with
    v3 as (
        select a from t1
    )
select distinct
    *
from
    v3;

select * from v3;

drop database if exists cte_test;