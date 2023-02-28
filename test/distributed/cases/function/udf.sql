-- -- @suite
-- -- @setup
-- create database udf_first;
-- create database udf_second;
-- use udf_first;
-- create table tbl1 (
-- id int,
-- test_val int
-- );
-- create table tbl2 (
-- id int,
-- test_val int
-- );

-- insert into tbl1(id, test_val) values (1, 10);
-- insert into tbl1(id, test_val) values (2, 11);
-- insert into tbl1(id, test_val) values (3, 12);
-- insert into tbl1(id, test_val) values (4, 13);

-- insert into tbl2(id, test_val) values (1, 100);
-- insert into tbl2(id, test_val) values (2, 200);
-- insert into tbl2(id, test_val) values (3, 300);
-- insert into tbl2(id, test_val) values (4, 400);

-- -- @case
-- -- @desc:test for create function (no arg)
-- -- @label:bvt
-- use udf_first;
-- create function helloworld () returns int language sql as 'select id from tbl1 limit 1';
-- select args, rettype, language, db from mo_catalog.mo_user_defined_function where name = 'helloworld';

-- -- @case
-- -- @desc:test for create duplicated function
-- -- @label:bvt
-- use udf_first;
-- create function helloworld () returns int language sql as 'select id from tbl1 limit 1';

-- -- @case
-- -- @desc:test for create function (multiple args)
-- -- @label:bvt
-- use udf_first;
-- create function twosum (x int, y int) returns int language sql as 'select $1 + $2' ;
-- select args, rettype, language, db from mo_catalog.mo_user_defined_function where name = 'twosum';

-- -- @case
-- -- @desc:test for create function (duplicate function name with different argument types)
-- -- @label:bvt
-- create function twosum (x float, y float) returns float language sql as 'select $1 + $2' ;
-- select args, rettype, language, db from mo_catalog.mo_user_defined_function where name = 'twosum';

-- -- @case
-- -- @desc:test for create function (duplicate function name with different argument numbers)
-- -- @label:bvt
-- create function twosum (x int) returns int language sql as 'select $1 + 10' ;
-- select args, rettype, language, db from mo_catalog.mo_user_defined_function where name = 'twosum';

-- -- @case
-- -- @desc:test for create function in another database
-- -- @label:bvt
-- use udf_first;
-- create function udf_second.helloworld () returns int language sql as 'select id from test_table limit 1';
-- select args, rettype, language, db from mo_catalog.mo_user_defined_function where name = 'helloworld' and db = 'udf_second';

-- -- @case:
-- -- @desc:
-- -- @label:
-- use udf_first;
-- create function mysum(s1 int, s2 int) returns int language sql as '$1 + $2';
-- select args, rettype, language, db from mo_catalog.mo_user_defined_function where name = 'mysum' and db = 'udf_first';


-- -- @desc: test udf function inside another udf function
-- use udf_first;
-- create function mysumtable(x int) returns int language sql as 'select mysum(test_val, id) from tbl1 where id = $1';
-- select args, rettype, language, db from mo_catalog.mo_user_defined_function where name = 'mysumtable' and db = 'udf_first';

-- -- @case
-- -- @desc: test for standalone udf execution none arg
-- -- @label:bvt
-- use udf_first;
-- select helloworld();

-- -- @case
-- -- @desc: test for standalone udf execution invalid arg
-- -- @label:bvt
-- use udf_first;
-- select helloworld(1);

-- -- @case
-- -- @desc: test for standalone udf execution with args
-- -- @label:bvt
-- use udf_first;
-- select twosum(1, 2);

-- -- @case
-- -- @desc: test for standalone udf execution with override
-- -- @label:bvt
-- use udf_first;
-- select twosum(1.1, 2.2);

-- -- @case
-- -- @desc: test for non-existing udf
-- -- @label:bvt
-- use udf_first;
-- select twomult(1.1, 2.2);

-- -- @case
-- -- @desc: test for complex udf usage 1
-- -- @label: bvt
-- use udf_first;
-- select mysum(id, test_val) from tbl1 where id > 1;

-- -- @case
-- -- @desc: test for complex udf usage 2
-- -- @label: bvt
-- use udf_first;
-- select mysum(tbl1.test_val, tbl2.test_val) from tbl1, tbl2 where tbl1.id = tbl2.id;

-- -- @case
-- -- @desc: test for complex udf usage 3
-- -- @label: bvt
-- use udf_first;
-- select mysumtable(id) from tbl2;

-- -- @case
-- -- @desc:test for drop function (no arg)
-- -- @label:bvt
-- use udf_first;
-- drop function helloworld ();
-- select args, rettype, language, db from mo_catalog.mo_user_defined_function where name = 'helloworld' and db = 'udf_first';

-- -- @case
-- -- @desc:test for drop function (multiple args)
-- -- @label:bvt
-- drop function twosum (int, int);
-- select args, rettype, language, db from mo_catalog.mo_user_defined_function where name = 'twosum';

-- -- @case
-- -- @desc:test for drop function (duplicate function name with different argument numbers)
-- -- @label:bvt
-- drop function twosum (int);
-- select args, rettype, language, db from mo_catalog.mo_user_defined_function where name = 'twosum';

-- -- @case
-- -- @desc:test for drop function (duplicate function name with different argument types)
-- -- @label:bvt
-- drop function twosum (float, float);
-- select args, rettype, language, db from mo_catalog.mo_user_defined_function where name = 'twosum';


-- -- @case
-- -- @desc:test for drop function from another database
-- -- @label:bvt
-- use udf_first;
-- drop function udf_second.helloworld ();
-- select args, rettype, language, db from mo_catalog.mo_user_defined_function where name = 'helloworld' and db = 'udf_second';

-- -- @case
-- -- @desc:test for drop non-existing function
-- -- @label:bvt
-- use udf_second;
-- drop function helloworld();
