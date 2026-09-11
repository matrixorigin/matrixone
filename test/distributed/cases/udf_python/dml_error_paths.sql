-- A Python failure is a statement failure.  The DML operator must not publish
-- rows that were evaluated before the handler raised an error.
drop database if exists udf_python_dml_errors_bvt;
create database udf_python_dml_errors_bvt;
use udf_python_dml_errors_bvt;

create function python_bvt_dml_fail (x int) returns int language python as 'def python_bvt_dml_fail(ctx, x): return x if x != 2 else 1 / 0' handler 'python_bvt_dml_fail';
create table source_values (id int primary key, value int);
insert into source_values values (1, 1), (2, 2), (3, 3);

-- UPDATE evaluates row 1 before row 2 raises.  The original table must remain
-- unchanged after the failed statement.
--error
update source_values set value = python_bvt_dml_fail(value);
select id, value from source_values order by id;

create table inserted_values (id int primary key, value int);
-- INSERT ... SELECT must publish no row after the same handler failure.
--error
insert into inserted_values select id, python_bvt_dml_fail(value) from source_values;
select count(*) as inserted_rows from inserted_values;

-- DELETE must also leave all source rows intact when its predicate fails.
--error
delete from source_values where python_bvt_dml_fail(value) > 0;
select id, value from source_values order by id;

drop function python_bvt_dml_fail(int);
drop table inserted_values;
drop table source_values;
drop database udf_python_dml_errors_bvt;
