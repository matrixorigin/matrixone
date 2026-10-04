
create table t1(a BOOL, b bool, c boolean);
insert into t1 values(true, true, true);
insert into t1 values(false, false, false);

-- @meta_cmp(true)
select * from t1;

-- JDBC result metadata must retain the declared TEXT family capacity and
-- temporal display precision, including for an empty result set.
drop table if exists jdbc_result_metadata;
create table jdbc_result_metadata (
    tt tinytext,
    t text,
    mt mediumtext,
    lt longtext,
    dt date,
    tm time,
    tm6 time(6),
    dtt datetime,
    dtt6 datetime(6),
    ts timestamp,
    ts6 timestamp(6),
    yr year
);

-- @meta_cmp(true)
select * from jdbc_result_metadata where 1 = 0;
drop table jdbc_result_metadata;

-- Conditional expressions must retain MEDIUMTEXT/LONGTEXT protocol lengths
-- instead of collapsing their TEXT-family markers to ordinary TEXT.
drop table if exists jdbc_conditional_text_metadata;
create table jdbc_conditional_text_metadata (mt mediumtext, lt longtext);
-- @meta_cmp(true)
select case when true then mt else mt end as case_mt,
       if(true, lt, lt) as if_lt,
       coalesce(mt, mt) as coalesce_mt
from jdbc_conditional_text_metadata where 1 = 0;
drop table jdbc_conditional_text_metadata;
