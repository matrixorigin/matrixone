drop database if exists udf_sql_namespace_bvt;
create database udf_sql_namespace_bvt;
use udf_sql_namespace_bvt;
create table namespace_values(x smallint);
insert into namespace_values values (1),(2);
create function overload(x bigint) returns bigint language sql as '$1 + 100';
select overload(x) as result from namespace_values order by x;
prepare namespace_plan from 'select overload(x) as result from namespace_values order by x';
execute namespace_plan;
-- @session:id=1
use udf_sql_namespace_bvt;
create function overload(x smallint) returns bigint language sql as '$1 + 200';
-- @session
execute namespace_plan;
select overload(x) as result from namespace_values order by x;
-- @session:id=1
use udf_sql_namespace_bvt;
create or replace function overload(x smallint) returns bigint language sql as '$1 + 300';
-- @session
execute namespace_plan;
select overload(x) as result from namespace_values order by x;
-- @session:id=1
use udf_sql_namespace_bvt;
drop function overload(smallint);
-- @session
execute namespace_plan;
select overload(x) as result from namespace_values order by x;
-- @session:id=1
use udf_sql_namespace_bvt;
create function overload(x smallint) returns bigint language sql as '$1 + 400';
-- @session
execute namespace_plan;
select overload(x) as result from namespace_values order by x;
deallocate prepare namespace_plan;
drop database udf_sql_namespace_bvt;
