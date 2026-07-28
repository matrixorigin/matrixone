-- ============================================================================
-- matrixone 临时表测试用例 - 高级功能
-- 测试文件: test_cases_advanced.sql
-- ============================================================================

-- ============================================================================
-- 测试分类 1: join 操作
-- ============================================================================

-- 测试用例 1.1: 临时表与普通表 inner join
-- 预期结果: join 成功
drop database if exists temp_advanced;
create database temp_advanced;
use temp_advanced;
create table customers (
    customer_id int,
    customer_name varchar(50)
);

insert into customers values (1, 'alice');
insert into customers values (2, 'bob');
insert into customers values (3, 'charlie');

create temporary table temp_orders (
    order_id int,
    customer_id int,
    amount decimal(10,2)
);

insert into temp_orders values (101, 1, 500.00);
insert into temp_orders values (102, 2, 750.00);
insert into temp_orders values (103, 1, 300.00);

select
    c.customer_name,
    t.order_id,
    t.amount
from customers c
inner join temp_orders t on c.customer_id = t.customer_id;

-- 测试用例 1.2: left join
-- 预期结果: 返回所有客户，包括没有订单的
select
    c.customer_name,
    t.order_id,
    t.amount
from customers c
left join temp_orders t on c.customer_id = t.customer_id;

-- 测试用例 1.3: right join
-- 预期结果: 返回所有订单
select
    c.customer_name,
    t.order_id,
    t.amount
from customers c
right join temp_orders t on c.customer_id = t.customer_id;

drop table temp_orders;
drop table customers;

-- ============================================================================
-- 测试分类 2: 多个临时表 join
-- ============================================================================

-- 测试用例 2.1: 两个临时表 join
-- 预期结果: join 成功
create temporary table temp_products (
    product_id int,
    product_name varchar(50),
    price decimal(10,2)
);

create temporary table temp_inventory (
    product_id int,
    quantity int,
    warehouse varchar(50)
);

insert into temp_products values (1, 'laptop', 999.99);
insert into temp_products values (2, 'mouse', 29.99);
insert into temp_products values (3, 'keyboard', 79.99);

insert into temp_inventory values (1, 50, 'warehouse a');
insert into temp_inventory values (2, 200, 'warehouse b');
insert into temp_inventory values (3, 100, 'warehouse a');

select
    p.product_name,
    p.price,
    i.quantity,
    i.warehouse,
    (p.price * i.quantity) as total_value
from temp_products p
join temp_inventory i on p.product_id = i.product_id;

drop table temp_products;
drop table temp_inventory;

-- 测试用例 2.2: 三个临时表 join
-- 预期结果: 多表 join 成功
create temporary table temp_categories (
    cat_id int,
    cat_name varchar(50)
);

create temporary table temp_products2 (
    prod_id int,
    prod_name varchar(50),
    cat_id int,
    price decimal(10,2)
);

create temporary table temp_sales (
    sale_id int,
    prod_id int,
    quantity int
);

insert into temp_categories values (1, 'electronics');
insert into temp_categories values (2, 'books');

insert into temp_products2 values (101, 'laptop', 1, 999.99);
insert into temp_products2 values (102, 'novel', 2, 19.99);
insert into temp_products2 values (103, 'tablet', 1, 499.99);

insert into temp_sales values (1, 101, 5);
insert into temp_sales values (2, 102, 10);
insert into temp_sales values (3, 103, 3);

select
    c.cat_name,
    p.prod_name,
    s.quantity,
    p.price,
    (s.quantity * p.price) as revenue
from temp_categories c
join temp_products2 p on c.cat_id = p.cat_id
join temp_sales s on p.prod_id = s.prod_id;

drop table temp_sales;
drop table temp_products2;
drop table temp_categories;

-- ============================================================================
-- 测试分类 3: 子查询
-- ============================================================================

-- 测试用例 3.1: where 子查询
-- 预期结果: 子查询成功
create temporary table temp_employees (
    emp_id int,
    emp_name varchar(50),
    salary decimal(10,2),
    dept varchar(50)
);

insert into temp_employees values (1, 'alice', 60000, 'it');
insert into temp_employees values (2, 'bob', 55000, 'hr');
insert into temp_employees values (3, 'charlie', 70000, 'it');
insert into temp_employees values (4, 'david', 50000, 'hr');
insert into temp_employees values (5, 'eve', 80000, 'it');

-- 查询工资高于平均工资的员工
select * from temp_employees
where salary > (select avg(salary) from temp_employees);

-- 测试用例 3.2: from 子查询
-- 预期结果: 子查询成功
select
    dept,
    avg_salary,
    emp_count
from (
    select
        dept,
        avg(salary) as avg_salary,
        count(*) as emp_count
    from temp_employees
    group by dept
) dept_stats
where emp_count > 1;

-- 测试用例 3.3: in 子查询
-- 预期结果: 子查询成功
select * from temp_employees
where dept in (
    select dept from temp_employees
    group by dept
    having avg(salary) > 60000
);

-- 测试用例 3.4: exists 子查询
-- 预期结果: 子查询成功
create temporary table temp_projects (
    project_id int,
    emp_id int,
    project_name varchar(50)
);

insert into temp_projects values (1, 1, 'project a');
insert into temp_projects values (2, 3, 'project b');
insert into temp_projects values (3, 5, 'project c');

-- 查询有项目的员工
select * from temp_employees e
where exists (
    select 1 from temp_projects p
    where p.emp_id = e.emp_id
);

drop table temp_projects;
drop table temp_employees;

-- ============================================================================
-- 测试分类 4: cte (公共表表达式)
-- ============================================================================

-- 测试用例 4.1: 使用 cte 查询临时表
-- 预期结果: cte 成功
create temporary table temp_sales_data (
    sale_id int,
    product varchar(50),
    amount decimal(10,2),
    sale_date date
);

insert into temp_sales_data values (1, 'laptop', 999.99, '2024-01-15');
insert into temp_sales_data values (2, 'mouse', 29.99, '2024-01-16');
insert into temp_sales_data values (3, 'keyboard', 79.99, '2024-01-17');
insert into temp_sales_data values (4, 'monitor', 299.99, '2024-01-18');
insert into temp_sales_data values (5, 'laptop', 999.99, '2024-01-19');

with sales_summary as (
    select
        product,
        count(*) as sale_count,
        sum(amount) as total_amount
    from temp_sales_data
    group by product
)
select * from sales_summary
where total_amount > 100;

-- 测试用例 4.2: 多个 cte
-- 预期结果: 多个 cte 成功
with
high_value_sales as (
    select * from temp_sales_data where amount > 100
),
product_stats as (
    select
        product,
        count(*) as count,
        avg(amount) as avg_amount
    from high_value_sales
    group by product
)
select * from product_stats;

drop table temp_sales_data;

-- ============================================================================
-- 测试分类 5: union 操作
-- ============================================================================

-- 测试用例 5.1: 临时表与普通表 union
-- 预期结果: union 成功
create table permanent_users (
    id int,
    name varchar(50),
    source varchar(20)
);

insert into permanent_users values (1, 'alice', 'permanent');
insert into permanent_users values (2, 'bob', 'permanent');

create temporary table temp_users (
    id int,
    name varchar(50),
    source varchar(20)
);

insert into temp_users values (3, 'charlie', 'temporary');
insert into temp_users values (4, 'david', 'temporary');

select * from permanent_users
union
select * from temp_users;

-- 测试用例 5.2: union all
-- 预期结果: union all 成功
select * from permanent_users
union all
select * from temp_users;

drop table temp_users;
drop table permanent_users;

-- ============================================================================
-- 测试分类 6: 跨数据库访问
-- ============================================================================

-- 测试用例 6.1: 跨数据库访问临时表
-- 预期结果: 跨数据库访问成功
create database if not exists db1;
create database if not exists db2;

use db1;
create temporary table temp_db1 (
    id int,
    data varchar(50)
);

insert into temp_db1 values (1, 'data from db1');
insert into temp_db1 values (2, 'more data from db1');

-- 切换到 db2
use db2;

-- 通过完整名称访问 db1 的临时表
select * from db1.temp_db1;

-- 测试用例 6.2: 跨数据库 join
-- 预期结果: 跨数据库 join 成功
create temporary table temp_db2 (
    id int,
    ref_id int,
    info varchar(50)
);

insert into temp_db2 values (1, 1, 'info for id 1');
insert into temp_db2 values (2, 2, 'info for id 2');

select
    t1.id,
    t1.data,
    t2.info
from db1.temp_db1 t1
join temp_db2 t2 on t1.id = t2.ref_id;

-- 清理
drop table temp_db2;
use db1;
drop table temp_db1;

drop database db1;
drop database db2;

-- ============================================================================
-- 测试分类 7: 复杂查询
-- ============================================================================

-- 测试用例 7.1: 窗口函数（如果支持）
-- 预期结果: 窗口函数成功
use temp_advanced;
create temporary table temp_scores (
    student_id int,
    subject varchar(50),
    score int
);

insert into temp_scores values (1, 'math', 85);
insert into temp_scores values (1, 'english', 90);
insert into temp_scores values (2, 'math', 92);
insert into temp_scores values (2, 'english', 88);
insert into temp_scores values (3, 'math', 78);
insert into temp_scores values (3, 'english', 95);

-- 使用窗口函数计算排名
select
    student_id,
    subject,
    score,
    rank() over (partition by subject order by score desc) as rank_in_subject
from temp_scores;

drop table temp_scores;

-- 测试用例 7.2: 复杂的聚合和分组
-- 预期结果: 复杂查询成功
create temporary table temp_transactions (
    trans_id int,
    customer_id int,
    product_category varchar(50),
    amount decimal(10,2),
    trans_date date
);

insert into temp_transactions values (1, 1, 'electronics', 999.99, '2024-01-15');
insert into temp_transactions values (2, 1, 'books', 29.99, '2024-01-16');
insert into temp_transactions values (3, 2, 'electronics', 499.99, '2024-01-17');
insert into temp_transactions values (4, 2, 'electronics', 299.99, '2024-01-18');
insert into temp_transactions values (5, 3, 'books', 19.99, '2024-01-19');

select
    customer_id,
    product_category,
    count(*) as purchase_count,
    sum(amount) as total_spent,
    avg(amount) as avg_purchase,
    min(amount) as min_purchase,
    max(amount) as max_purchase
from temp_transactions
group by customer_id, product_category
having sum(amount) > 50
order by total_spent desc;

drop table temp_transactions;

-- ============================================================================
-- 测试分类 8: insert select
-- ============================================================================

-- 测试用例 8.1: 从普通表插入到临时表
-- 预期结果: 插入成功
create table source_data (
    id int,
    value varchar(50)
);

insert into source_data values (1, 'value1');
insert into source_data values (2, 'value2');
insert into source_data values (3, 'value3');

create temporary table temp_target (
    id int,
    value varchar(50)
);

insert into temp_target select * from source_data;

select * from temp_target;

drop table temp_target;
drop table source_data;

-- 测试用例 8.2: 从临时表插入到普通表
-- 预期结果: 插入成功
create temporary table temp_source (
    id int,
    data varchar(50)
);

insert into temp_source values (1, 'temp data 1');
insert into temp_source values (2, 'temp data 2');

create table target_table (
    id int,
    data varchar(50)
);

insert into target_table select * from temp_source;

select * from target_table;

drop table target_table;
drop table temp_source;

-- 测试用例 8.3: 临时表之间的数据复制
-- 预期结果: 复制成功
create temporary table temp_source2 (
    id int,
    name varchar(50)
);

insert into temp_source2 values (1, 'alice');
insert into temp_source2 values (2, 'bob');

create temporary table temp_target2 (
    id int,
    name varchar(50)
);

insert into temp_target2 select * from temp_source2;

select * from temp_target2;

drop table temp_target2;
drop table temp_source2;

-- ============================================================================
-- 测试分类 9: create table as select
-- ============================================================================

-- 测试用例 9.1: 从查询结果创建临时表
-- 预期结果: 创建成功
create table base_data (
    id int,
    category varchar(50),
    value decimal(10,2)
);

insert into base_data values (1, 'a', 100.00);
insert into base_data values (2, 'b', 200.00);
insert into base_data values (3, 'a', 150.00);
insert into base_data values (4, 'c', 300.00);

create temporary table temp_summary as
select
    category,
    count(*) as count,
    sum(value) as total,
    avg(value) as average
from base_data
group by category;

select * from temp_summary;

drop table temp_summary;
drop table base_data;

-- ============================================================================
-- 测试分类 10: 事务处理
-- ============================================================================

-- 测试用例 10.1: 事务中的临时表操作
-- 预期结果: 事务提交后数据保留
create temporary table temp_trans (
    id int,
    value varchar(50)
);

start transaction;

insert into temp_trans values (1, 'value1');
insert into temp_trans values (2, 'value2');

select * from temp_trans;

commit;

select * from temp_trans;  -- 预期结果: 数据仍然存在

-- 测试用例 10.2: 事务回滚
-- 预期结果: 回滚后新数据不存在
start transaction;

insert into temp_trans values (3, 'value3');
insert into temp_trans values (4, 'value4');

select * from temp_trans;  -- 预期结果: 4条记录

rollback;

select * from temp_trans;  -- 预期结果: 只有2条记录（value1, value2）

drop table temp_trans;
drop database if exists temp_advanced;

