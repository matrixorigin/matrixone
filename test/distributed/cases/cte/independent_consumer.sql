drop database if exists cte_independent_consumer;
create database cte_independent_consumer;
use cte_independent_consumer;

create table suppliers(id int primary key);
create table sales(supplier_id int, amount decimal(10,2));
insert into suppliers values (1), (2), (3), (4);
insert into sales values (1,10), (1,20), (2,30), (3,null), (4,5);

-- 独立 CTE 的普通连接和标量消费者保留并列最大值。
with revenue as (
    select supplier_id, sum(amount) as total from sales group by supplier_id
)
select s.id, r.total
from suppliers s, revenue r
where s.id = r.supplier_id
  and r.total = (select max(total) from revenue)
order by s.id;

-- 显式等值连接是同一结果的独立对照。
with revenue as (
    select supplier_id, sum(amount) as total from sales group by supplier_id
)
select s.id, r.total
from suppliers s join revenue r on s.id = r.supplier_id
where r.total = (select max(total) from revenue)
order by s.id;

-- 过滤条件中的外部引用不等于需要按外部行重放的 CTE 投影。
select s.id from suppliers s
where s.id > 1 and (
    with local_sales as (
        select amount from sales where supplier_id = s.id
    )
    select max(amount) from local_sales
) = 30
order by s.id;

-- 全 NULL 聚合值不得与标量 MAX 的 NULL 结果匹配。
delete from sales where amount is not null;
with revenue as (
    select supplier_id, sum(amount) as total from sales group by supplier_id
)
select s.id, r.total
from suppliers s, revenue r
where s.id = r.supplier_id
  and r.total = (select max(total) from revenue)
order by s.id;

-- 空生产者不应产生连接结果。
delete from sales;
with revenue as (
    select supplier_id, sum(amount) as total from sales group by supplier_id
)
select s.id, r.total
from suppliers s, revenue r
where s.id = r.supplier_id
  and r.total = (select max(total) from revenue)
order by s.id;

drop database cte_independent_consumer;
