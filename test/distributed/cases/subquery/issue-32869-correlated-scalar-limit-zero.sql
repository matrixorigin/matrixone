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
create function issue_32869_identity(x int) returns int language sql as '$1';
create function issue_32869_identity_query(x int) returns int language sql as 'select $1';
create function issue_32869_add_twice(x int) returns int language sql as '$1 + $1';

-- @case
-- @desc:issue #32869 correlated scalar root LIMIT 0 returns a typed NULL per outer row
-- @label:bvt
select o.a, (select o.a from inner_rows i where i.d > o.a limit 0) as filtered_zero
from outer_rows o order by o.a;
select o.a, (select o.a from inner_rows i where i.d > o.a + 1 limit 0) as arithmetic_zero
from outer_rows o order by o.a;
select o.a, (select o.a from inner_rows limit 0) as unfiltered_zero
from outer_rows o order by o.a;
select o.a, (select o.a as inner_alias from inner_rows limit 0) as aliased_zero
from outer_rows o order by o.a;
select o.a, (select cast(o.a as signed) from inner_rows i order by i.d limit 0) as sorted_zero
from outer_rows o order by o.a;
select o.a, (select o.a from inner_rows i where i.d > o.a limit 0 offset 1) as offset_zero
from outer_rows o order by o.a;
select o.a, (select cast(concat(o.a, char(120)) as signed) from inner_rows limit 0) as value_zero
from outer_rows o order by o.a;
select o.a, (select o.a as alias_value from inner_rows i where i.d > o.a order by i.d limit 0) as alias_order_zero
from outer_rows o order by o.a;
-- CTE visibility must only exclude a source that actually resolves to a CTE.
with unused as (select 1) select o.a,
  (select o.a from inner_rows i where i.d > o.a limit 0) as cte_zero
from outer_rows o order by o.a;
with inner_rows as (select 0 as d) select o.a,
  (select o.a from test_issue_32869.inner_rows i where i.d > o.a limit 0) as qualified_zero
from outer_rows o order by o.a;
with unused as (select 1) select o.a,
  (select o.a from inner_rows i where i.d > o.a limit 1) as cte_positive
from outer_rows o order by o.a;
select o.a, issue_32869_identity((select (select o.d from inner_rows i limit 0) from inner_rows o limit 1)) as identity_zero
from outer_rows o order by o.a;
select o.a, issue_32869_identity((select o.a from inner_rows i where i.d > o.a limit 0)) as direct_identity_zero
from outer_rows o order by o.a;
select o.a, issue_32869_identity_query((select o.a from inner_rows i where i.d > o.a limit 0)) as query_identity_zero
from outer_rows o order by o.a;
select o.a, issue_32869_add_twice((select o.a from inner_rows i where i.d > o.a limit 0)) as repeated_argument_zero
from outer_rows o order by o.a;

-- Nonzero LIMIT still evaluates the original scalar and preserves one-row behavior.
select o.a, (select o.a from inner_rows i where i.d > o.a limit 1) as positive_limit
from outer_rows o order by o.a;

-- A nested zero boundary does not erase its enclosing aggregate.
select (select count(*) from (select d from inner_rows limit 0) q) as nested_count;

-- A discarded wrapper retains its authorization source, and an untyped NULL
-- remains valid when a dependent plan materializes the output.
select null + (select (select c.d from inner_rows r where r.d > c.d limit 0) from restricted_wrapper c limit 1) from outer_rows;
select null + (select null + (select c.d from inner_rows r where r.d > c.d limit 0) from restricted_wrapper c limit 1) from outer_rows;

-- The discarded SQL-UDF argument must not erase a nested correlated LIMIT 0
-- table dependency before privilege checking.
create table secret_rows (s int);
insert into secret_rows values (5);
-- View inference and named-window validation must retain folded sources.
create view issue_32869_view as
select o.a, cast(null as signed) + (select o.a from inner_rows i where i.d > o.a limit 0) as v from outer_rows o;
select * from issue_32869_view order by a;
alter view issue_32869_view as
select o.a, cast(null as signed) + (select o.a from inner_rows i where i.d > o.a limit 0) as v from outer_rows o;
select * from issue_32869_view order by a;
select o.a from outer_rows o
window unused_w as (order by null + (select o.a from inner_rows i where i.d > o.a limit 0));
create table issue_32869_dml (a int);
insert into issue_32869_dml select null + (select o.a from inner_rows i where i.d > o.a limit 0) from outer_rows o;
select count(*) as rows_inserted, count(a) as nonnull from issue_32869_dml;
update issue_32869_dml d set a = null + (select d.a from inner_rows i where i.d > d.a limit 0);
select count(*) as rows_updated, count(a) as nonnull from issue_32869_dml;
create table issue_32869_ctas as select cast(null + (select o.a from inner_rows i where i.d > o.a limit 0) as signed) as a from outer_rows o;
select count(*) as rows_copied, count(a) as nonnull from issue_32869_ctas;
-- NULL arithmetic must not fold away the eligible scalar's table source.
select null + (select o.a from secret_rows s where s.s > o.a limit 0)
from outer_rows o;
create function issue_32869_keep_second(x int, y int) returns int language sql as '$2';
select o.a, issue_32869_identity((select o.a from secret_rows s where s.s > o.a limit 0)) as identity_zero
from outer_rows o order by o.a;
select o.a, issue_32869_identity(
  (select (select i.d from secret_rows s where s.s > i.d limit 0)
   from inner_rows i limit 1)
) as nested_identity_zero from outer_rows o order by o.a;
select o.a,
  (select (select i.d from secret_rows s where s.s > i.d limit 0)
   from inner_rows i limit 1) as nested_direct_zero
from outer_rows o order by o.a;
create role issue_32869_role;
create user issue_32869_reader identified by '111' default role issue_32869_role;
grant connect on account * to issue_32869_role;
grant select on table test_issue_32869.outer_rows to issue_32869_role;
grant select on table test_issue_32869.inner_rows to issue_32869_role;
grant insert on table test_issue_32869.issue_32869_dml to issue_32869_role;
-- Saved-result authorization must survive an enclosing fold.
grant issue_32869_role to dump;
set save_query_result = on;
/* save_result */ select s as a from secret_rows;
set @issue_32869_saved = last_query_id();
select (select (select q.a from inner_rows i where i.d > q.a limit 0)
        from result_scan(@issue_32869_saved) q limit 1) as live_saved;
set role issue_32869_role;
select * from result_scan(@issue_32869_saved) q;
select null + (select (select q.a from test_issue_32869.inner_rows i where i.d > q.a limit 0)
               from result_scan(@issue_32869_saved) q limit 1) as folded_saved;
select * from meta_scan(@issue_32869_saved) q;
select null + (select (select q.account_id from test_issue_32869.inner_rows i where i.d > q.account_id limit 0)
               from meta_scan(@issue_32869_saved) q limit 1) as folded_meta;
select i.d from test_issue_32869.inner_rows i
window unused_w as (order by (select null + (select q.a from test_issue_32869.inner_rows r where r.d > q.a limit 0)
                               from result_scan(@issue_32869_saved) q limit 1));
select i.d from test_issue_32869.inner_rows i
window unused_w as (order by null + (select i.d from test_issue_32869.inner_rows r where r.d > i.d limit 0),
                    (select q.a from result_scan(@issue_32869_saved) q limit 1));
set role moadmin;
set save_query_result = off;
-- @session:id=1&user=sys:issue_32869_reader:issue_32869_role&password=111
use test_issue_32869;
select a from outer_rows order by a;
insert into test_issue_32869.issue_32869_dml
select null + (select o.a from test_issue_32869.secret_rows s where s.s > o.a limit 0)
from test_issue_32869.outer_rows o;
select o.a from test_issue_32869.outer_rows o
window unused_w as (order by null + (select o.a from test_issue_32869.secret_rows s where s.s > o.a limit 0));
select null + (select o.a from test_issue_32869.secret_rows s where s.s > o.a limit 0)
from test_issue_32869.outer_rows o;
select null + (select null + (select c.d from test_issue_32869.inner_rows r where r.d > c.d limit 0)
               from test_issue_32869.restricted_wrapper c limit 1)
from test_issue_32869.outer_rows;
select (select o.a from test_issue_32869.inner_rows i where i.d > o.a limit 0) as root_zero
from test_issue_32869.outer_rows o
window unused_w as (order by (select s from test_issue_32869.secret_rows limit 1));
select issue_32869_keep_second(
  (select (select o.d from test_issue_32869.secret_rows s where s.s > o.d limit 0)
   from test_issue_32869.inner_rows o limit 1),
  a
) from test_issue_32869.outer_rows;
select (select (select i.d from test_issue_32869.secret_rows s where s.s > i.d limit 0)
        from test_issue_32869.inner_rows i limit 1)
from test_issue_32869.outer_rows;
select (select (select i.d from test_issue_32869.inner_rows s where s.d > i.d limit 0)
        from test_issue_32869.restricted_wrapper i limit 1)
from test_issue_32869.outer_rows;
select issue_32869_keep_second(
  (select (select o.d from test_issue_32869.inner_rows s where s.d > o.d limit 0)
   from test_issue_32869.restricted_wrapper o limit 1),
  a
) from test_issue_32869.outer_rows;
select issue_32869_keep_second(
  (select (select o.d from test_issue_32869.secret_rows s where s.s > o.d limit 0)
   from test_issue_32869.inner_rows o
   union all select 5 limit 1),
  a
) from test_issue_32869.outer_rows;
select cast(null as signed) + (select (select c.d from test_issue_32869.inner_rows r where r.d > c.d limit 0) from test_issue_32869.restricted_wrapper c limit 1) from test_issue_32869.outer_rows;
select issue_32869_identity((select o.a from test_issue_32869.secret_rows s where s.s > o.a limit 0))
from test_issue_32869.outer_rows o;
select issue_32869_identity(
  (select (select o.d from test_issue_32869.secret_rows s where s.s > o.d limit 0)
   from test_issue_32869.inner_rows o limit 1)
) from test_issue_32869.outer_rows;
select issue_32869_identity(cast(null as signed) +
  (select (select i.d from test_issue_32869.secret_rows s where s.s > i.d limit 0)
   from test_issue_32869.inner_rows i limit 1)) as folded_identity;
select issue_32869_identity(cast(null as signed) +
  (select (select i.d from test_issue_32869.inner_rows s where s.d > i.d limit 0)
   from test_issue_32869.restricted_wrapper i limit 1)) as folded_identity;
select issue_32869_keep_second(cast(null as signed) +
  (select (select i.d from inner_rows s where s.d > i.d limit 0)
   from restricted_wrapper i limit 1), 42) as folded_discarded_wrapper;
select issue_32869_keep_second(cast(null as signed) +
  (select (select i.d from test_issue_32869.secret_rows s where s.s > i.d limit 0)
   from test_issue_32869.inner_rows i limit 1), 42) as folded_discarded_wrapper;
select s from test_issue_32869.secret_rows;
-- @session
drop user issue_32869_reader;
drop role issue_32869_role;

-- @case
-- @desc:discarded UDF arguments and prepared SET keep folded scalar sources
-- @label:bvt
use test_issue_32869;
create function issue_32869_constant_expr(x int) returns int language sql as '11';
create function issue_32869_constant_query(x int) returns int language sql as 'select 12';
create function issue_32869_constant_mixed(x int, y int) returns int language sql as 'select $2';
select issue_32869_constant_expr((select o.a from inner_rows i where i.d > o.a limit 0)) as expr_value,
       issue_32869_constant_query((select o.a from inner_rows i where i.d > o.a limit 0)) as query_value,
       issue_32869_constant_mixed((select o.a from inner_rows i where i.d > o.a limit 0), 13) as mixed_value
from outer_rows o where o.a = 11;
prepare issue_32869_set_lazy from 'set @answer = case when exists(select 1 from outer_rows) then 7 else (select (select o.a from inner_rows i where i.d > o.a limit 0) from outer_rows o) end';
execute issue_32869_set_lazy;
select @answer;
deallocate prepare issue_32869_set_lazy;
prepare issue_32869_set_replay from 'set @answer = case when not exists(select 1 from outer_rows where a = ?) then ? + (select (select i.d from secret_rows s where s.s > i.d limit 0) from inner_rows i limit 1) else 7 end';
set @condition = 1, @nullable = null;
execute issue_32869_set_replay using @condition, @nullable;
select @answer;
set @condition = 999, @nullable = null;
execute issue_32869_set_replay using @condition, @nullable;
select @answer;
deallocate prepare issue_32869_set_replay;
prepare issue_32869_set_decimal from 'set @decimal_answer = round((select cast(1.23 as decimal(4,2))), ?)';
set @decimal_digits = 1;
execute issue_32869_set_decimal using @decimal_digits;
select @decimal_answer;
deallocate prepare issue_32869_set_decimal;
select issue_32869_identity(cast(null as signed) +
  (select (select i.d from inner_rows s where s.d > i.d limit 0)
   from restricted_wrapper i limit 1)) as folded_identity;
prepare issue_32869_set from 'set @before = ?, @answer = 1 + (select case when o.a = 11 then o.a else cast(null as signed) + (select o.a from inner_rows i where i.d > o.a limit 0) end from outer_rows o where o.a = ? limit 1), @after = ?';
set @v_before = 21, @v_outer = 1, @v_after = 23;
execute issue_32869_set using @v_before, @v_outer, @v_after;
select @before, @answer, @after;
set @v_before = 31, @v_outer = 11, @v_after = 33;
execute issue_32869_set using @v_before, @v_outer, @v_after;
select @before, @answer, @after;
deallocate prepare issue_32869_set;
prepare issue_32869_set_simple_case from 'set @case_answer = case (select (select o.a from inner_rows i where i.d > o.a limit 0) from outer_rows o where o.a = 1 limit 1) when 1 then 11 when 2 then 12 else 13 end';
execute issue_32869_set_simple_case;
select @case_answer;
deallocate prepare issue_32869_set_simple_case;
prepare issue_32869_set_udf_null from 'set @udf_answer = (select issue_32869_identity(cast(null as signed) + (select o.a from inner_rows i where i.d > o.a limit 0)) from outer_rows o where o.a = 1 limit 1)';
execute issue_32869_set_udf_null;
select @udf_answer;
deallocate prepare issue_32869_set_udf_null;

-- @teardown
drop database test_issue_32869;
