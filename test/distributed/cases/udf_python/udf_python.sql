drop database if exists udf_python_bvt;
create database udf_python_bvt;
use udf_python_bvt;

create function python_bvt_add (x int) returns int language python as 'def python_bvt_add(ctx, x): return None if x is None else x + 1' handler 'python_bvt_add';
create table input_values (v int);
insert into input_values values (1), (null), (3);

select v, python_bvt_add(v) as plus_one from input_values order by v is null, v;
select case when v is null then 99 else python_bvt_add(v) end as guarded from input_values order by v is null, v;

-- Python source is compiled by the current worker before a Catalog identity
-- or revision is published.
-- @regex("Python syntax error at line",true)
create function python_bvt_bad_syntax (x int) returns int language python as 'def python_bvt_bad_syntax(ctx, x) return x' handler 'python_bvt_bad_syntax';

-- A failed replacement keeps the previous immutable active revision usable.
create function python_bvt_replace (x int) returns int language python as 'def python_bvt_replace(ctx, x): return x + 10' handler 'python_bvt_replace';
select python_bvt_replace(1);
-- @regex("Python syntax error at line",true)
create or replace function python_bvt_replace (x int) returns int language python as 'def python_bvt_replace(ctx, x) return x + 20' handler 'python_bvt_replace';
select python_bvt_replace(1);

drop function python_bvt_add(int);
drop function python_bvt_replace(int);
drop table input_values;
drop database udf_python_bvt;
