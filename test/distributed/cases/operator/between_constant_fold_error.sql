-- A constant BETWEEN whose final fold fails must return the cast error
-- (same as the >= AND <= form) instead of a planner nil-pointer panic.
select '1.5' between 1 and 20;
select 'abc' between 1 and 20;
select 1 where '1.5' between 1 and 20;
select '1.5' >= 1 and '1.5' <= 20;
select '1.5' not between 1 and 20;
-- foldable controls
select '12' between 1 and 20;
select 5 between 1 and 20, 30 between 1 and 20;
select 1 where 5 between 1 and 20;
select 1;
