create function sql_sum (a int, b int) returns int language sql as '$1 + $2';
select sql_sum(1, 1) as ret;
drop function sql_sum(int, int);
drop database if exists db1;
create database db1;
use db1;
select name, db from mo_catalog.mo_user_defined_function where db = 'db1';
create function helloworld () returns int language sql as 'select id from tbl1 limit 1';
select name, db from mo_catalog.mo_user_defined_function where db = 'db1';
drop database if exists db1;
select name, db from mo_catalog.mo_user_defined_function where db = 'db1';
create function db1.helloworld5 () returns int language sql as 'select id from tbl1 limit 1';
create database db1;
use db1;
create function db1.helloworld5 () returns int language sql as 'select id from tbl1 limit 1';
drop function db1.helloworld5();
drop function db2.helloworld5();
create database if not exists udf_bvt;
use udf_bvt;
drop database if exists db1;

-- bvt: preserve backslash in $$...$$ string for UDF body
create function sql_nl() returns varchar language sql as $$select '\\n'$$;
select hex(body) as body_hex from mo_catalog.mo_user_defined_function where name = 'sql_nl' and db = 'udf_bvt';
drop database udf_bvt;
