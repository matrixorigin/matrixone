drop database if exists udf_python_bvt;
create database udf_python_bvt;
use udf_python_bvt;

create function python_bvt_add (x int) returns int language python as 'def python_bvt_add(ctx, x): return None if x is None else x + 1' handler 'python_bvt_add';
create table input_values (v int);
insert into input_values values (1), (null), (3);

select v, python_bvt_add(v) as plus_one from input_values order by v is null, v;
select case when v is null then 99 else python_bvt_add(v) end as guarded from input_values order by v is null, v;

drop function python_bvt_add(int);
drop table input_values;
drop database udf_python_bvt;
