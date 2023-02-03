-- @suite
-- @setup
create database udf_first;
create database udf_second;

-- @case
-- @desc:test for create function (no arg)
-- @label:bvt
use udf_first;
create function helloworld () returns int language sql as 'select id from test_table limit 1';
select args, rettype, language, db from mo_catalog.mo_user_defined_function where name = 'helloworld';

-- @case
-- @desc:test for create duplicated function
-- @label:bvt
use udf_first;
create function helloworld () returns int language sql as 'select id from test_table limit 1';

-- @case
-- @desc:test for create function (multiple args)
-- @label:bvt
use udf_first;
create function twosum (x int, y int) returns int language sql as 'select $1 + $2' ;
select args, rettype, language, db from mo_catalog.mo_user_defined_function where name = 'twosum';

-- @case
-- @desc:test for create function (duplicate function name with different argument types)
-- @label:bvt
create function twosum (x float, y float) returns float language sql as 'select $1 + $2' ;
select args, rettype, language, db from mo_catalog.mo_user_defined_function where name = 'twosum';

-- @case
-- @desc:test for create function (duplicate function name with different argument numbers)
-- @label:bvt
create function twosum (x int) returns int language sql as 'select $1 + 10' ;
select args, rettype, language, db from mo_catalog.mo_user_defined_function where name = 'twosum';

-- @case
-- @desc:test for create function in another database
-- @label:bvt
use udf_first;
create function udf_second.helloworld () returns int language sql as 'select id from test_table limit 1';
select args, rettype, language, db from mo_catalog.mo_user_defined_function where name = 'helloworld' and db = 'udf_second';

-- @case
-- @desc:test for drop function (no arg)
-- @label:bvt
drop function helloworld ();
select args, rettype, language, db from mo_catalog.mo_user_defined_function where name = 'helloworld' and db = 'udf_first';

-- @case
-- @desc:test for drop function (multiple args)
-- @label:bvt
drop function twosum (int, int);
select args, rettype, language, db from mo_catalog.mo_user_defined_function where name = 'twosum';

-- @case
-- @desc:test for drop function (duplicate function name with different argument numbers)
-- @label:bvt
drop function twosum (int);
select args, rettype, language, db from mo_catalog.mo_user_defined_function where name = 'twosum';

-- @case
-- @desc:test for drop function (duplicate function name with different argument types)
-- @label:bvt
drop function twosum (float, float);
select args, rettype, language, db from mo_catalog.mo_user_defined_function where name = 'twosum';


-- @case
-- @desc:test for drop function from another database
-- @label:bvt
use udf_first;
drop function udf_second.helloworld ();
select args, rettype, language, db from mo_catalog.mo_user_defined_function where name = 'helloworld' and db = 'udf_second';

-- @case
-- @desc:test for drop non-existing function
-- @label:bvt
use udf_second;
drop function helloworld();



