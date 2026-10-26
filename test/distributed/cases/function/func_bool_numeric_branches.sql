-- CASE / IF / IFNULL / COALESCE with a BOOL branch mixed with numeric
-- branches: MySQL treats TRUE/FALSE as 1/0, so the result keeps the numeric
-- type instead of folding every value to 0/1.
select coalesce(2, true), coalesce(true, 2), coalesce(null, 7, false);
select coalesce(2.5e0, true), coalesce(false, 2.5e0);
select ifnull(5, true), ifnull(null, true) + 0, ifnull(null, 7) + true;
select if(1, 2.5e0, true), if(0, 2.5e0, true), if(1, true, 3), if(0, true, 3);
select case when 1 then 9 else true end, case when 0 then 9 else true end;
select case when 0 then false when 1 then -4 else 0.5e0 end;
-- BOOL-only branches stay BOOL
select coalesce(true, false), if(1, true, false), case when 1 then false else true end;

drop database if exists func_bool_numeric_branches;
create database func_bool_numeric_branches;
use func_bool_numeric_branches;
create table t(id int, i int, d double, b bool);
insert into t values (1, 7, 2.5, true), (2, null, null, false), (3, -3, -0.5, null);
select id, coalesce(i, b), ifnull(d, b), if(b, i, b), case when b then d else false end from t order by id;
select id, coalesce(i, b) + 1, coalesce(d, b) * 2 from t order by id;
select sum(coalesce(i, b)), max(ifnull(d, b)) from t;
drop database func_bool_numeric_branches;
