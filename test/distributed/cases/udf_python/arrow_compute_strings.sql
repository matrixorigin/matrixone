-- Arrow UTF-8 compute kernels with empty, Unicode, and NULL values.
drop database if exists udf_python_arrow_compute_strings_bvt;
create database udf_python_arrow_compute_strings_bvt;
use udf_python_arrow_compute_strings_bvt;

create function python_bvt_pc_upper (x varchar(64)) returns varchar(64) language python as 'python_bvt_pc_upper = lambda ctx, values: __import__("pyarrow.compute", fromlist=["utf8_upper"]).utf8_upper(values)' handler 'python_bvt_pc_upper' mode vector;
create function python_bvt_pc_contains (x varchar(64)) returns bool language python as 'python_bvt_pc_contains = lambda ctx, values: __import__("pyarrow.compute", fromlist=["match_substring"]).match_substring(values, "a", ignore_case=False)' handler 'python_bvt_pc_contains' mode vector;
create function python_bvt_pc_replace (x varchar(64)) returns varchar(64) language python as 'python_bvt_pc_replace = lambda ctx, values: __import__("pyarrow.compute", fromlist=["replace_substring"]).replace_substring(values, "a", "X", max_replacements=-1)' handler 'python_bvt_pc_replace' mode vector;
create function python_bvt_pc_length (x varchar(64)) returns int language python as 'python_bvt_pc_length = lambda ctx, values: __import__("pyarrow.compute", fromlist=["utf8_length"]).utf8_length(values)' handler 'python_bvt_pc_length' mode vector;

create table string_values (id int, value varchar(64));
insert into string_values values
    (1, ''),
    (2, 'Alpha'),
    (3, '中a😀'),
    (4, null);

-- The kernels must preserve empty strings and NULL validity. utf8_length
-- counts Unicode code points, so the final non-NULL value has length three.
select id,
       python_bvt_pc_upper(value) as upper_value,
       python_bvt_pc_contains(value) as contains_a,
       python_bvt_pc_replace(value) as replaced_value,
       python_bvt_pc_length(value) as codepoint_length
from string_values order by id;

drop function python_bvt_pc_upper(varchar(64));
drop function python_bvt_pc_contains(varchar(64));
drop function python_bvt_pc_replace(varchar(64));
drop function python_bvt_pc_length(varchar(64));
drop table string_values;
drop database udf_python_arrow_compute_strings_bvt;
