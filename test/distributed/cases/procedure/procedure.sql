create database procedure_test;
use procedure_test;

-- -- @case
-- -- @desc:test for create function (no arg)
-- -- @label:bvt
-- create procedure noarg () 
-- 'begin 
--     DECLARE v1 INT DEFAULT 5;
--     WHILE v1 > 0 DO 
--         SET v1 = v1 - 1;
--     END WHILE;
-- end';

-- -- @case
-- -- @desc:test for create function (only in arg)
-- -- @label:bvt
-- create procedure inarg (in v1 int, in v2 int)
-- `BEGIN
--     SET @x = 0;
--     REPEAT
--         SET @x = @x + v1;
--     UNTIL @x > v2 END REPEAT;
-- END`;

-- -- @case
-- -- @desc:test for create function (only out arg)
-- -- @label:bvt
-- create procedure outarg (out cnt int)
-- `BEGIN					
--     IF @n = @m THEN SET cnt = @n;
--     ELSE
--         IF @n > @m THEN SET s = @n;
--         ELSE SET s = @m;
--         END IF;
--     END IF;
-- END`;

-- -- @case
-- -- @desc:test for create function (inout arg)
-- -- @label:bvt
-- create procedure countarea (inout cnt int)
-- `begin
--     declare v1 int;
--     select COUNT(*) into v1 from world.city where area = cnt;
--     set cnt = v1;
-- end`;

-- -- @case
-- -- @desc:test for create function (in and out arg)
-- -- @label:bvt
-- create procedure inandoutarg (in country char(3), out cities int)
-- `begin
--     select COUNT(*) into cities from world.city where CountryCode = country;
-- end`;

-- -- -- @case
-- -- -- @desc:test for call procedure with no arg
-- -- -- @label:bvt
-- -- call noarg();

-- -- -- @case
-- -- -- @desc:test for call procedure with in arg
-- -- -- @label:bvt
-- -- set @v1 = 10;
-- -- call inarg(@v1, 2);
-- -- select @x;

-- -- -- @case
-- -- -- @desc:test for call procedure with out arg
-- -- -- @label:bvt
-- -- call outarg(@cnt);
-- -- select @cnt;

-- -- -- @case
-- -- -- @desc:test for call procedure with inout arg
-- -- -- @label:bvt
-- -- call countarea(@area_count);
-- -- select @area_count;

-- -- -- @case
-- -- -- @desc:test for call procedure with both in and out arg
-- -- -- @label:bvt
-- -- call inandoutarg('JPN', @cities);
-- -- select @cities;

-- -- @case
-- -- @desc:test for dropping procedure
-- drop procedure outarg();