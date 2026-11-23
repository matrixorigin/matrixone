--- @metacmp(true)
drop database if exists cte_scalar_union_limit;
create database cte_scalar_union_limit;
use cte_scalar_union_limit;
create table measurements(period int, amount decimal(10,2), guard_value bigint);
insert into measurements values
(1,10,-9223372036854775808),(1,20,-9223372036854775808),
(2,60,-9223372036854775808),(2,null,-9223372036854775808);

-- 无排序窗口与恰好覆盖四个标量分支的 LIMIT，不应重复执行公共聚合。
create table metrics as
select row_number() over () as rn, u.* from (
    with grouped as (
        select period, sum(amount) as total from measurements group by period
    ), totals as (
        select max(total) as mx, min(total) as mn from grouped
    )
    select 'max' as metric, mx as amount from totals
    union all select 'min', mn from totals
    union all select 'sum', mx+mn from totals
    union all select 'zero', mx/nullif(mn-mn,0) from totals
) u limit 4;
select metric, amount from metrics order by metric;
-- 无序 ROW_NUMBER 不约束指标与行号的对应关系，但必须完整且唯一。
select count(*) as n, count(distinct rn) as distinct_rn, min(rn) as first_rn, max(rn) as last_rn from metrics;
select column_name, data_type, numeric_precision, numeric_scale, is_nullable
from information_schema.columns
where table_schema='cte_scalar_union_limit' and table_name='metrics'
order by ordinal_position;

-- 重复标量结果不得被去重；OFFSET 保留重复结果的正确重数。
with totals as (select sum(amount) as total from measurements)
select total from totals union all select total from totals limit 2;
with totals as (select sum(amount) as total from measurements)
select total from totals union all select total from totals limit 2 offset 1;
with totals as (select sum(amount) as total from measurements)
select total from totals union all select total from totals limit 1;

-- 过滤只会降低保守上界；空的一个消费者不能制造额外行。
with totals as (select sum(amount) as total from measurements)
select total from totals where total<0
union all select total from totals limit 2;

-- 动态 LIMIT 可能是零，prepare/execute 不得沿用不安全的消费证明。
prepare page from 'with totals as (select sum(amount) as total from measurements) select total from totals union all select total from totals limit ?';
set @page_limit=0;
execute page using @page_limit;
set @page_limit=2;
execute page using @page_limit;
deallocate prepare page;

-- LIMIT 0 不得触发标量聚合中的错误表达式；真正消费时错误仍可见。
with risky as (select abs(max(guard_value)) as v from measurements)
select v from risky union all select v from risky limit 0;
-- @regex("out of range",true)
with risky as (select abs(max(guard_value)) as v from measurements)
select v from risky union all select v from risky limit 2;

-- 全 NULL 的输入与空输入都保留标量聚合行，COUNT 与 SUM 的值不同。
update measurements set amount=null;
with totals as (select sum(amount) as total, count(amount) as n from measurements)
select total,n from totals union all select total,n from totals limit 2;
delete from measurements;
with grouped as (select period,sum(amount) as total from measurements group by period),
     totals as (select max(total) as mx,min(total) as mn from grouped)
select mx,mn from totals union all select mx,mn from totals limit 2;
create table empty_metrics as
with totals as (select sum(amount) as total,count(*) as n from measurements)
select total,n from totals union all select total,n from totals limit 2;
select * from empty_metrics;
with totals as (select count(*) as n from measurements having count(*)>0)
select n from totals union all select n from totals limit 2;

drop database cte_scalar_union_limit;
