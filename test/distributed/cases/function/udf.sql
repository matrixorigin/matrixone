-- @suite
-- @setup

-- @case
-- @desc:test for create function (no arg)
-- @label:bvt
create function helloworld () returns int language sql as 'select id from test_table limit 1';

-- @case
-- @desc:test for create function (multiple args)
-- @label:bvt
create function twosum (x int, y int) returns int language sql as 'select $1 + $2' ;

-- @case
-- @desc:test for create function (duplicate function name with different argument types)
-- @label:bvt
create function twosum (x float, y float) returns float language sql as 'select $1 + $2' ;

-- @case
-- @desc:test for create function (duplicate function name with different argument numbers)
-- @label:bvt
create function twosum (x int) returns int language sql as 'select $1 + 10' ;

-- @case
-- @desc:test for drop function (no arg)
-- @label:bvt
drop function helloWorld ();

-- @case
-- @desc:test for drop function (multiple args)
-- @label:bvt
drop function twosum (int, int);

-- @case
-- @desc:test for drop function (duplicate function name with different argument numbers)
-- @label:bvt
drop function twosum (int);

-- @case
-- @desc:test for drop function (duplicate function name with different argument types)
-- @label:bvt
drop function twosum (float, float);

