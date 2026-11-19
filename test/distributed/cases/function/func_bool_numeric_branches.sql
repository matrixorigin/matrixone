-- CASE / IF / IFNULL / COALESCE with a BOOL branch mixed with numeric
-- branches: MySQL treats TRUE/FALSE as 1/0, so the result keeps the numeric
-- type instead of folding every value to 0/1.
select coalesce(2, true), coalesce(true, 2), coalesce(null, 7, false);
select coalesce(2.5e0, true), coalesce(false, 2.5e0);
select coalesce(1.5, true), coalesce(true, 1.5);
select ifnull(5, true), ifnull(null, true) + 0, ifnull(null, 7) + true;
select if(1, 2.5e0, true), if(0, 2.5e0, true), if(1, true, 3), if(0, true, 3);
select if(1, 1.5, true), if(0, 1.5, false);
select case when 1 then 9 else true end, case when 0 then 9 else true end;
select case when 0 then false when 1 then -4 else 0.5e0 end;
select case when 1 then 1.5 else true end, case when 0 then 1.5 else false end;
-- BOOL-only branches stay BOOL
select coalesce(true, false), if(1, true, false), case when 1 then false else true end;

drop database if exists func_bool_numeric_branches;
create database func_bool_numeric_branches;
use func_bool_numeric_branches;
create table t(id int, i int, d double, dc decimal(10,2), b bool);
insert into t values (1, 7, 2.5, 1.5, true), (2, null, null, null, false), (3, -3, -0.5, -1.5, null);
select id, coalesce(i, b), ifnull(d, b), if(b, i, b), case when b then d else false end from t order by id;
select id, coalesce(i, b) + 1, coalesce(d, b) * 2 from t order by id;
select id, coalesce(dc, b), ifnull(dc, b), if(b, dc, b), case when b then dc else false end from t order by id;
select sum(coalesce(i, b)), max(ifnull(d, b)), min(coalesce(dc, b)) from t;
select id, coalesce(b, i), if(b, b, i), case when 1 then b else d end from t order by id;

-- A BOOL branch is 1/0 (one integral digit) in a DECIMAL mixture, so a
-- high-scale DECIMAL keeps its declared precision instead of being widened
-- by BIGINT's 19 digits or folded back to BOOL.
select coalesce(cast('0.5' as decimal(65,58)), true), coalesce(true, cast('0.5' as decimal(65,58)));
select coalesce(cast('0.25' as decimal(65,57)), false), coalesce(false, cast('0.25' as decimal(65,57)));
select ifnull(cast('0.5' as decimal(65,58)), true), ifnull(cast(null as decimal(65,57)), false);
select if(1, cast('0.5' as decimal(65,58)), true), if(0, true, cast('0.25' as decimal(65,57)));
select case when 1 then cast('0.5' as decimal(65,58)) else true end, case when 0 then true else cast('0.25' as decimal(65,57)) end;
-- Scale 65 is the highest publicly reachable precision; with BIGINT's 19
-- digits this mixture would need 84 digits and fold to BOOL.
select coalesce(cast('0.5' as decimal(65,65)), true), coalesce(true, cast('0.5' as decimal(65,65)));
create table hs(id int, d58 decimal(65,58), d57 decimal(65,57), b bool);
insert into hs values (1, 0.5, 0.25, true), (2, null, null, false);
select id, coalesce(d58, b), coalesce(b, d58), ifnull(d57, b) from hs order by id;
select id, if(b, d58, b), case when b then d57 else true end from hs order by id;
drop table hs;

-- BOOL assigned to a DECIMAL column follows MySQL: TRUE is 1, so a target
-- without an integral digit rejects it; FALSE and NULL fit the same target.
create table bool_src(b bool);
insert into bool_src values (true), (false), (null);
create table bool_dst_wide(d decimal(2,1));
insert into bool_dst_wide select b from bool_src;
select * from bool_dst_wide;
create table bool_dst_narrow(d decimal(1,1));
insert into bool_dst_narrow values (false), (null);
select * from bool_dst_narrow;
-- TRUE needs one integral digit, so the assignment conversion into a
-- DECIMAL without one is rejected, exactly like the string path rejects '1'.
-- @regex("^invalid input: 1 beyond the range, can't be converted to Decimal64\(1,1\)\.",true)
insert into bool_dst_narrow select b from bool_src;
-- @regex("^invalid input: 1 beyond the range, can't be converted to Decimal64\(1,1\)\.",true)
insert into bool_dst_narrow values (true);
-- The explicit CAST keeps its clamp-and-warning behavior on the same target.
insert into bool_dst_narrow select cast(b as decimal(1,1)) from bool_src;
select * from bool_dst_narrow;
drop table bool_src;
drop table bool_dst_wide;
drop table bool_dst_narrow;
drop database func_bool_numeric_branches;
