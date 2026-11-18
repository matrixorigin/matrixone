-- @suite
-- @setup
drop user if exists issue_32869_reader;
drop role if exists issue_32869_role;
drop database if exists test_issue_32869;
create database test_issue_32869;
use test_issue_32869;
create table outer_rows (a int);
create table inner_rows (d int);
create table restricted_wrapper (d int);
insert into outer_rows values (1), (1), (11), (null);
insert into inner_rows values (4), (99);
insert into restricted_wrapper values (4);

-- @case
-- @desc:issue #32869 correlated scalar root LIMIT 0 returns a typed NULL per outer row
-- @label:bvt
select o.a, (select o.a from inner_rows i where i.d > o.a limit 0) as filtered_zero
from outer_rows o order by o.a;
select o.a, (select o.a from inner_rows limit 0) as unfiltered_zero
from outer_rows o order by o.a;
select o.a, (select o.a from inner_rows i where i.d > o.a limit 0 offset 1) as offset_zero
from outer_rows o order by o.a;
select o.a, (select cast(concat(o.a, char(120)) as signed) from inner_rows limit 0) as value_zero
from outer_rows o order by o.a;
select o.a, (select o.a as alias_value from inner_rows i where i.d > o.a order by i.d limit 0) as alias_order_zero
from outer_rows o order by o.a;

-- Nonzero LIMIT still evaluates the original scalar and preserves one-row behavior.
select o.a, (select o.a from inner_rows i where i.d > o.a limit 1) as positive_limit
from outer_rows o order by o.a;

-- A nested zero boundary does not erase its enclosing aggregate.
select (select count(*) from (select d from inner_rows limit 0) q) as nested_count;

-- Simplification must retain the wrapper table for authorization.
select cast(null as signed) + (select (select c.d from inner_rows r where r.d > c.d limit 0) from restricted_wrapper c limit 1) from outer_rows;

-- The discarded SQL-UDF argument must not erase a nested correlated LIMIT 0
-- table dependency before privilege checking.
create table secret_rows (s int);
insert into secret_rows values (5);
create function issue_32869_keep_second(x int, y int) returns int language sql as '$2';
create role issue_32869_role;
create user issue_32869_reader identified by '111' default role issue_32869_role;
grant connect on account * to issue_32869_role;
grant select on table test_issue_32869.outer_rows to issue_32869_role;
grant select on table test_issue_32869.inner_rows to issue_32869_role;
-- @session:id=1&user=sys:issue_32869_reader:issue_32869_role&password=111
use test_issue_32869;
select a from outer_rows order by a;
select issue_32869_keep_second(
  (select (select o.d from test_issue_32869.secret_rows s where s.s > o.d limit 0)
   from test_issue_32869.inner_rows o limit 1),
  a
) from test_issue_32869.outer_rows;
select issue_32869_keep_second(
  (select (select o.d from test_issue_32869.secret_rows s where s.s > o.d limit 0)
   from test_issue_32869.inner_rows o
   union all select 5 limit 1),
  a
) from test_issue_32869.outer_rows;
select cast(null as signed) + (select (select c.d from test_issue_32869.inner_rows r where r.d > c.d limit 0) from test_issue_32869.restricted_wrapper c limit 1) from test_issue_32869.outer_rows;
select s from test_issue_32869.secret_rows;
-- @session
drop user issue_32869_reader;
drop role issue_32869_role;

-- @teardown
drop database test_issue_32869;
