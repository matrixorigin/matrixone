-- Exercise Python UDFs in INSERT values, UPDATE assignments, and DELETE
-- predicates. Every mutation is observed by the final SELECT.
drop database if exists udf_python_dml_bvt;
create database udf_python_dml_bvt;
use udf_python_dml_bvt;

create function python_bvt_dml_add (x int) returns int language python as 'def python_bvt_dml_add(ctx, x): return None if x is None else x + 1' handler 'python_bvt_dml_add';
create table dml_values (id int primary key, value int);
insert into dml_values values (1, 1), (2, 2), (3, null);

-- INSERT must evaluate the routine for the incoming row.
insert into dml_values values (4, python_bvt_dml_add(3));

-- UPDATE must evaluate the routine against the target row image.
update dml_values set value = python_bvt_dml_add(value) where id = 1;

-- DELETE must keep the routine in its target predicate. NULL remains unknown
-- and therefore must not be deleted.
delete from dml_values where python_bvt_dml_add(value) >= 4;

select id, value from dml_values order by id;

drop function python_bvt_dml_add(int);
drop table dml_values;
drop database udf_python_dml_bvt;
