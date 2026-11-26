drop database if exists jdbc_enum_set_fields;
create database jdbc_enum_set_fields;
use jdbc_enum_set_fields;
create table t (
    id int primary key,
    e enum('a','甲乙',''),
    s set('x','😀'),
    c char(2),
    v varchar(3)
);
insert into t values (1,'甲乙','x,😀','甲乙','x,😀'),(2,'','','',''),(3,NULL,NULL,NULL,NULL);

-- Declared fields retain CHAR metadata and label values, including empty/NULL.
-- @metacmp(true)
select e,s from t order by id;
-- @metacmp(true)
select s as set_label,v,e as enum_label from t order by id;
-- @metacmp(true)
select e,s from t where false;

-- Identity survives transparent query boundaries, not just a direct scan.
-- @metacmp(true)
-- @sortkey:0,1
select distinct e,s from t;
-- @metacmp(true)
-- @sortkey:0,1
select e,s from t group by e,s;
-- @metacmp(true)
-- @sortkey:0,1
select e,s from t group by id;
-- @metacmp(true)
-- @sortkey:0,1
select e,s from (select e,s from t group by id) d;
-- @metacmp(true)
-- @sortkey:0,1
select e,s from (select e,s from t order by id limit 3) d;
-- @metacmp(true)
-- @sortkey:0,1
with d as (select e,s from t) select e,s from d;
create view labels as select e,s from t;
-- @metacmp(true)
-- @sortkey:0,1
select e,s from labels;
-- @metacmp(true)
select a.e,b.s from t a left join t b on a.id=b.id order by a.id;

-- String expressions/UNION must not inherit declaration flags or identity.
-- @metacmp(true)
select concat(e) as e,cast(s as char) as s from t order by id;
-- @metacmp(true)
-- @sortkey:0,1
select e,s from t union all select e,s from t;
-- @metacmp(true)
select c,v from t order by id;

-- Reusing a parameterized handle must not make field metadata value-dependent.
prepare fields from 'select e,s from t where id>=? order by id';
set @n=1;
-- @metacmp(true)
execute fields using @n;
set @n=4;
-- @metacmp(true)
execute fields using @n;
set @n=1;
-- @metacmp(true)
execute fields using @n;
deallocate prepare fields;

-- Ordinary IF over explicitly typed enum ordinals still materializes a
-- numeric column; the internal UPDATE guard must not change CTAS schema.
create table if_input (k int primary key);
insert into if_input values (1),(2);
create table if_ctas as select k,if(k=1,cast(1 as enum('blue','red')),cast(2 as enum('blue','red'))) as v from if_input;
select k from if_ctas where v=k and v+0=k and exists (select 1 from information_schema.columns where table_schema='jdbc_enum_set_fields' and table_name='if_ctas' and column_name='v' and column_type='smallint unsigned') order by k;
drop database jdbc_enum_set_fields;
