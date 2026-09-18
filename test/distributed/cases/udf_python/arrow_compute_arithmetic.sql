-- Arrow compute arithmetic and validity kernels through the SQL UDF boundary.
drop database if exists udf_python_arrow_compute_arithmetic_bvt;
create database udf_python_arrow_compute_arithmetic_bvt;
use udf_python_arrow_compute_arithmetic_bvt;

create function python_bvt_pc_add (x int) returns int language python as 'python_bvt_pc_add = lambda ctx, values: __import__("pyarrow.compute", fromlist=["add"]).add(values, __import__("pyarrow").scalar(10, type=__import__("pyarrow").int32()))' handler 'python_bvt_pc_add' mode vector;
create function python_bvt_pc_multiply (x int, y int) returns int language python as 'python_bvt_pc_multiply = lambda ctx, left, right: __import__("pyarrow.compute", fromlist=["multiply"]).multiply(left, right)' handler 'python_bvt_pc_multiply' mode vector;
create function python_bvt_pc_if_else (flag bool, x int, y int) returns int language python as 'python_bvt_pc_if_else = lambda ctx, flag, left, right: __import__("pyarrow.compute", fromlist=["if_else"]).if_else(flag, left, right)' handler 'python_bvt_pc_if_else' mode vector;
create function python_bvt_pc_fill_null (x int) returns int language python as 'python_bvt_pc_fill_null = lambda ctx, values: __import__("pyarrow.compute", fromlist=["fill_null"]).fill_null(values, __import__("pyarrow").scalar(-7, type=__import__("pyarrow").int32()))' handler 'python_bvt_pc_fill_null' mode vector;
create function python_bvt_pc_is_null (x int) returns bool language python as 'python_bvt_pc_is_null = lambda ctx, values: __import__("pyarrow.compute", fromlist=["is_null"]).is_null(values)' handler 'python_bvt_pc_is_null' mode vector;

create table arithmetic_values (id int, flag bool, left_value int, right_value int);
insert into arithmetic_values values
    (1, true, 1, 10),
    (2, false, null, 20),
    (3, null, 3, null),
    (4, true, null, null);

-- These kernels must preserve Arrow validity instead of converting NULL to a
-- value or changing the batch length.
select id,
       python_bvt_pc_add(left_value) as added,
       python_bvt_pc_multiply(left_value, right_value) as multiplied,
       python_bvt_pc_if_else(flag, left_value, right_value) as selected,
       python_bvt_pc_fill_null(left_value) as filled,
       python_bvt_pc_is_null(left_value) as is_null
from arithmetic_values order by id;

drop function python_bvt_pc_add(int);
drop function python_bvt_pc_multiply(int, int);
drop function python_bvt_pc_if_else(bool, int, int);
drop function python_bvt_pc_fill_null(int);
drop function python_bvt_pc_is_null(int);
drop table arithmetic_values;
drop database udf_python_arrow_compute_arithmetic_bvt;
