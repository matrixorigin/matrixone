-- Vector handlers receive one Arrow array per argument and return one Arrow
-- array with the same number of rows. Both NULL policies are exercised here.
drop database if exists udf_python_vector_bvt;
create database udf_python_vector_bvt;
use udf_python_vector_bvt;

create function python_bvt_vector (x int) returns int language python as 'python_bvt_vector = lambda ctx, values: __import__("pyarrow").array([-99 if value.as_py() is None else value.as_py() + 10 for value in values], type=__import__("pyarrow").int32())' handler 'python_bvt_vector' mode vector;
create function python_bvt_vector_strict (x int) returns int language python as 'python_bvt_vector_strict = lambda ctx, values: __import__("pyarrow").array([value.as_py() + 10 for value in values], type=__import__("pyarrow").int32())' handler 'python_bvt_vector_strict' mode vector returns null on null input;
create function python_bvt_zero_vector () returns int language python as 'python_bvt_zero_vector = lambda ctx: __import__("pyarrow").array([7] * ctx.num_rows, type=__import__("pyarrow").int32())' handler 'python_bvt_zero_vector' mode vector;

create table vector_values (id int, value int);
insert into vector_values values (1, 1), (2, null), (3, 2);

-- CALLED ON NULL INPUT reaches the vector handler for the NULL row.
select id, python_bvt_vector(value) as called_result
from vector_values order by id;

-- RETURNS NULL ON NULL INPUT compacts the selected rows and scatters NULL
-- back to the original row positions.
select id, python_bvt_vector_strict(value) as strict_result
from vector_values order by id;

-- A zero-argument VECTOR routine gets its result shape from ctx.num_rows.
select id, python_bvt_zero_vector() as zero_result
from vector_values order by id;

drop function python_bvt_vector(int);
drop function python_bvt_vector_strict(int);
drop function python_bvt_zero_vector();
drop table vector_values;
drop database udf_python_vector_bvt;
