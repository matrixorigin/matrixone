-- @suite
-- @setup
drop database if exists test_scalar_cte_non_eq_agg;
create database test_scalar_cte_non_eq_agg;
use test_scalar_cte_non_eq_agg;
create table outer_rows (id int primary key, n int);
create table inner_rows (n int, v int);
insert into outer_rows values (1, 1), (2, 2), (3, 3), (4, 0), (5, null), (6, 2);
insert into inner_rows values (1, 10), (2, null), (3, 30), (4, 40);

-- @case
-- @desc:非等值相关聚合保留完整内层关系及标量语义
-- @label:bvt
select o.id, o.n,
       (with x as (select max(i.n) as m from inner_rows i where i.n <= o.n) select m from x) as max_le,
       (with x as (select sum(i.n) as s from inner_rows i where i.n < o.n) select s from x) as sum_lt,
       (with x as (select count(*) as c from inner_rows i where i.n >= o.n) select c from x) as count_ge,
       (with x as (select count(i.v) as c from inner_rows i where i.n <= o.n) select c from x) as count_value
from outer_rows o order by o.id;
select o.id,
       (with x as (select min(i.v) as m from inner_rows i where i.n > o.n) select m from x) as min_gt,
       (with x as (select count(*) as c, sum(i.n) as s from inner_rows i where i.n <= o.n) select s from x) as second_aggregate,
       (with x as (select count(*) as c from inner_rows i where i.n = o.n) select c from x) as count_eq,
       (with x as (select count(i.v) as c from inner_rows i where i.n = o.n) select c from x) as count_eq_value
from outer_rows o order by o.id;
select o.id,
       (with x as (select count(*) as c from inner_rows i where i.n = o.n) select c from x) +
       coalesce((with x as (select sum(i.n) as s from inner_rows i where i.n <= o.n) select s from x), 0) as combined_value
from outer_rows o order by o.id;
select o.id, (select max(i.n) from inner_rows i where i.n <= o.n) as direct_max,
       (with x as (select max(i.n) as m from inner_rows i where i.n = o.n) select m from x) as cte_equal_max
from outer_rows o order by o.id;
select o.id, (select max(i.n) from inner_rows i where i.n <= o.n) as direct_max
from outer_rows o where exists (select 1 from inner_rows e where e.n = o.n) order by o.id;
select o.id, exists (select 1 from inner_rows e where e.n = o.n) as present,
       (with x as (select max(i.n) as m from inner_rows i where i.n <= o.n) select m from x) as cte_max
from outer_rows o order by o.id;
select o.id, p.id,
       (with x as (select count(*) as c from inner_rows i where i.n = o.n) select c from x) as cte_count
from outer_rows o cross join outer_rows p
where o.id in (1, 4) and p.id in (1, 4) order by o.id, p.id;

-- 嵌套聚合保留各层空集语义，不替换为单次原始行重聚合。
select o.id, (with a as (select max(i.n) as m from inner_rows i where i.n <= o.n),
              x as (select sum(m) as s from a) select s from x) as nested_value
from outer_rows o order by o.id;

-- JOIN 包装不能丢弃聚合输出。
select o.id, (with x as (select max(i.n) as m from inner_rows i where i.n <= o.n),
              y as (select 1 as k) select x.m from x cross join y) as joined_value
from outer_rows o order by o.id;

-- An empty equality-correlated COUNT is a row with zero; c+1 must not become NULL.
select o.id, (with x as (select count(*) as c from inner_rows i where i.n = o.n)
              select c + 1 from x) as computed_count
from outer_rows o order by o.id;

-- Equality GROUP BY and null-rejecting HAVING retain their existing scalar semantics.
select o.id,
       (with x as (select max(i.v) as m from inner_rows i where i.n = o.n
                   having max(i.v) > 0) select m from x) as having_max,
       (with x as (select count(*) as c from inner_rows i where i.n = o.n
                   group by i.n) select c from x) as grouped_count
from outer_rows o where o.id in (1, 2, 4) order by o.id;

-- A preceding raw scalar column must survive the range reaggregation.
select o.id, (select i.v from inner_rows i where i.n = o.n) as raw_value,
       (with x as (select max(i.n) as m from inner_rows i where i.n <= o.n)
        select m from x) as range_max
from outer_rows o where o.id in (1, 2, 4) order by o.id;

-- A WHERE disjunction consumes both old scalar values and the new aggregate.
select o.id from outer_rows o where
    (with x as (select max(i.n) as m from inner_rows i where i.n <= o.n)
     select m from x) > 0 or
    (with x as (select sum(i.n) as s from inner_rows i where i.n < o.n)
     select s from x) > 0 order by o.id;
select o.id from outer_rows o where
    exists (select 1 from inner_rows e where e.n = o.n and e.v is null) or
    (with x as (select max(i.n) as m from inner_rows i where i.n <= o.n)
     select m from x) < 0 order by o.id;

-- Every selected COUNT output needs the zero-on-empty reconstruction.
select o.id,
       (0, 0) = (with x as (select count(*) as c, count(i.v) as d
                           from inner_rows i where i.n = o.n)
                 select c, d from x) as zero_pair,
       (1, 0) = (with x as (select count(*) as c, count(i.v) as d
                           from inner_rows i where i.n = o.n)
                 select c, d from x) as null_value_pair
from outer_rows o order by o.id;

-- A lower global aggregate always emits one row, including on empty input.
select o.id, (with a as (select max(i.n) as m from inner_rows i where i.n = o.n),
              x as (select count(*) as c from a) select c from x) as nested_count
from outer_rows o order by o.id;

-- Equality reconstruction must leave the outer row identity available for a
-- following non-equality reaggregation, including unmatched and NULL keys.
select o.id,
       (with x as (select count(*) as c from inner_rows i where i.n = o.n)
        select c + 1 from x) as equality_count_plus_one,
       (with a as (select max(i.n) as m from inner_rows i where i.n = o.n),
             x as (select count(*) as c from a) select c from x) as nested_count,
       (with x as (select max(i.n) as m from inner_rows i where i.n <= o.n)
        select m from x) as range_max
from outer_rows o order by o.id;

select o.id, (with x as (select max(i.n) as m from inner_rows i
                      where i.n = o.n or i.n = o.n + 1) select m from x) as or_max
from outer_rows o order by o.id;

-- Correlation above an independent aggregate filters its completed result;
-- it must not trigger reconstruction of the aggregate's empty-input row.
select o.id, (with x as (select count(*) as c from inner_rows i)
              select c from x where c = o.id) as independent_count
from outer_rows o order by o.id;
select o.id, (with x as (select i.n, max(i.v) as m from inner_rows i group by i.n)
              select m from x where x.n < o.n) as independent_group
from outer_rows o where o.id = 2;

-- 纯聚合结果表达式复用集合式重聚合，不改变空集、多聚合和重复外行。
select o.id,
       (select max(i.v) + 1 from inner_rows i where i.n <= o.n) as inside_expression,
       (select max(i.v) from inner_rows i where i.n <= o.n) + 1 as outside_expression,
       (select count(*) + coalesce(sum(i.v), 0) from inner_rows i where i.n <= o.n) as combined
from outer_rows o order by o.id;
with q as (select n from outer_rows)
select q.n, (select count(*) + coalesce(sum(i.v), 0) from inner_rows i where i.n <= q.n) as combined
from q order by q.n;
prepare range_expression from 'select o.id, (select coalesce(max(i.v), ?) + count(*) from inner_rows i where i.n <= o.n) as v from outer_rows o order by o.id';
set @range_default = 7;
execute range_expression using @range_default;
set @range_default = null;
execute range_expression using @range_default;
set @range_default = -1;
execute range_expression using @range_default;
set @range_default = 7;
execute range_expression using @range_default;
deallocate prepare range_expression;
set @range_default = null;

-- 聚合结果表达式、HAVING 和 DISTINCT 分别保留空集与删除输出行的差异。
select o.id,
       (select max(i.v) + 1 from inner_rows i where i.n <= o.n) as plus_one,
       (select coalesce(max(i.v), -1) from inner_rows i where i.n <= o.n) as coalesced,
       (select case when max(i.v) is null then -1 else max(i.v) end from inner_rows i where i.n <= o.n) as case_value,
       (select max(i.v) from inner_rows i where i.n <= o.n having max(i.v) >= 30) as having_value,
       (select distinct max(i.v) from inner_rows i where i.n <= o.n) as distinct_value,
       (select count(*) from inner_rows i where i.n <= o.n having count(*) = 0) as empty_count
from outer_rows o order by o.id;

-- 分页属于每个外层行自己的分组结果；LIMIT 0 和 OFFSET 可删除全局聚合输出。
select o.id,
       (select max(i.v) from inner_rows i where i.n <= o.n group by i.n order by i.n desc limit 1) as top_value,
       (select max(i.v) from inner_rows i where i.n <= o.n group by i.n order by i.n desc limit 1 offset 1) as previous_value,
       (select count(*) from inner_rows i where i.n <= o.n limit 0) as deleted_value,
       (select count(*) from inner_rows i where i.n <= o.n limit 1 offset 1) as skipped_value
from outer_rows o order by o.id;

select o.id,
       (select max(i.v) from inner_rows i where i.n <= o.n) as derived_max,
       (select count(*) from (select n from inner_rows union all select n from inner_rows) i where i.n <= o.n) as union_count,
       (select count(distinct i.n) from (select n from inner_rows union all select n from inner_rows) i where i.n <= o.n) as union_distinct_count
from (select id, n from outer_rows) o order by o.id;
select o.id, x.id,
       (select max(i.v) from inner_rows i where i.n <= o.n and i.n < x.id) as two_outer_value
from outer_rows o join outer_rows x on x.id = o.id order by o.id;

-- 结果表达式的错误也必须服从外层需求；选中后报错，连接仍可复用。
select o.id, case when o.id < 0 then
       (select cast(max(i.v) + 1000 as tinyint) from inner_rows i where i.n <= o.n)
       else 7 end as skipped_expression
from outer_rows o order by o.id;
select (select cast(max(i.v) + 1000 as tinyint) from inner_rows i where i.n <= o.n)
from outer_rows o where o.id = 1;
select 1 as expression_error_recovered;

-- 多组不能静默取首行；未选 CASE 分支不能触发该错误。
select (select count(*) from inner_rows i where i.n <= o.n group by i.n having i.n > 0)
from outer_rows o where o.id = 3;
select o.id, case when o.id < 0 then
       (select count(*) from inner_rows i where i.n <= o.n group by i.n) else 7 end as guarded_value
from outer_rows o order by o.id;

-- 在不同消费入口保持相同结果，PREPARE 不跨执行保留参数和结果。
select o.id from outer_rows o
where (select max(i.v) + 1 from inner_rows i where i.n <= o.n) > 20 order by o.id;
select o.id from outer_rows o
order by (select coalesce(max(i.v), -1) from inner_rows i where i.n <= o.n), o.id;
create view range_values as select o.id,
       (select coalesce(max(i.v), -1) from inner_rows i where i.n <= o.n) as v from outer_rows o;
select * from range_values order by id;
create table range_values_copy as select * from range_values;
select * from range_values_copy order by id;
prepare range_value from 'select o.id, (select max(i.v) from inner_rows i where i.n <= o.n group by i.n order by i.n desc limit 1) as v from outer_rows o where o.id = ?';
set @range_id = 3;
execute range_value using @range_id;
set @range_id = 4;
execute range_value using @range_id;
set @range_id = 2;
execute range_value using @range_id;
deallocate prepare range_value;
set @range_id = null;

-- DISTINCT 日期 CTE 与范围过滤共同决定滚动去重计数。
with events as (
  select 1 as entity_id, cast('2025-01-01 00:00:00' as datetime) as event_time
  union all select 2, cast('2025-01-02 00:00:00' as datetime)
  union all select 1, cast('2025-01-08 00:00:00' as datetime)
), days as (select distinct cast(event_time as date) as observation_date from events)
select days.observation_date,
       (select count(distinct events.entity_id) from events
        where cast(events.event_time as date) <= days.observation_date
          and timestampdiff(day, events.event_time, cast(days.observation_date as datetime)) between 0 and 6) as visitors
from days order by days.observation_date;

-- 递归源同样需要完整分组语义；多组的负例在第一条外层行立即报错。
with recursive r(n) as (select 1 union all select n + 1 from r where n < 3)
select n, (select count(*) from r i where i.n <= r.n group by i.n having i.n > 0 order by i.n desc limit 1) as cnt
from r order by n;
with recursive r(n) as (select 1 union all select n + 1 from r where n < 3)
select (select count(*) from r i where i.n <= r.n group by i.n having i.n > 0)
from r where n = 3;
select 1 as connection_reusable;

-- 行比较两侧均属于 CASE 的求值范围；未选分支不能执行多行左操作数。
select o.id, case when o.id < 0 then
       (o.id, (select j.n from inner_rows j where j.n <= o.n)) =
       (select count(*), max(i.n) from inner_rows i where i.n <= o.n)
       else 0 end as skipped_correlated,
       case when o.id < 0 then
       (o.id, (select j.n from inner_rows j)) =
       (select count(*), max(i.n) from inner_rows i where i.n <= o.n)
       else 0 end as skipped_independent
from outer_rows o where o.id <= 2 order by o.id;
select o.id, case when o.id = 1 then
       (o.id, (select j.n from inner_rows j where j.n <= o.n)) =
       (select count(*), max(i.n) from inner_rows i where i.n <= o.n)
       else 0 end as selected_single
from outer_rows o where o.id <= 2 order by o.id;
select case when o.id > 0 then
       (o.id, (select j.n from inner_rows j where j.n <= o.n)) =
       (select count(*), max(i.n) from inner_rows i where i.n <= o.n)
       else 0 end as selected_multiple
from outer_rows o where o.id = 2;
select o.id,
       (o.id, (select j.n from inner_rows j where j.n < 0)) =
       (select count(*), max(i.n) from inner_rows i where i.n <= o.n) as empty_operand,
       (o.id, case when o.id < 0 then (select j.n from inner_rows j where j.n <= o.n) else o.id end) =
       (select count(*), max(i.n) from inner_rows i where i.n <= o.n) as nested_guard
from outer_rows o where o.id <= 2 order by o.id;

-- 深层等值关联保持集合路径；无 FROM 与多个中间绑定不改变语义。
select o.id from outer_rows o
where exists (select 1 where 1 = (select max(i.n) from inner_rows i where i.n = o.n))
order by o.id;
select o.id from outer_rows o
where exists (select 1 from inner_rows a cross join inner_rows b
              where 1 = (select max(i.n) from inner_rows i where i.n <=> o.n))
order by o.id;
select o.id from outer_rows o
where exists (select 1 where 1 = (select max(i.n) from inner_rows i where i.n < o.n))
order by o.id;

-- SQL EXECUTE 的展示转换只属于外部结果，不能改写内部标量 program。
prepare scalar_operand_params from 'select ? as exposed, (o.id,(select ?)) <=> (select count(*),max(i.n) from inner_rows i where i.n<=o.n) as compared from outer_rows o where o.id<=2 order by o.id';
set @scalar_operand_value = 1;
execute scalar_operand_params using @scalar_operand_value, @scalar_operand_value;
set @scalar_operand_value = null;
execute scalar_operand_params using @scalar_operand_value, @scalar_operand_value;
set @scalar_operand_value = 2;
execute scalar_operand_params using @scalar_operand_value, @scalar_operand_value;
set @scalar_operand_value = cast(1.00 as decimal(20,2));
execute scalar_operand_params using @scalar_operand_value, @scalar_operand_value;
set @scalar_operand_value = '2';
execute scalar_operand_params using @scalar_operand_value, @scalar_operand_value;
set @scalar_operand_value = 1;
execute scalar_operand_params using @scalar_operand_value, @scalar_operand_value;
deallocate prepare scalar_operand_params;
prepare scalar_operand_skip from 'select case when o.id<0 then (o.id,(select ?)) <=> (select count(*),max(i.n) from inner_rows i where i.n<=o.n) else 0 end as skipped from outer_rows o where o.id<=2 order by o.id';
execute scalar_operand_skip using @scalar_operand_value;
deallocate prepare scalar_operand_skip;

-- @teardown
drop database test_scalar_cte_non_eq_agg;
