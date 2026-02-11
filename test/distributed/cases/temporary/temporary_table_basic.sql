-- ============================================================================
-- matrixone 临时表测试用例 - 基础功能
-- 测试文件: test_cases_basic.sql
-- ============================================================================

-- ============================================================================
-- 测试分类 1: 创建和删除临时表
-- ============================================================================

-- 测试用例 1.1: 创建基本临时表
-- 预期结果: 创建成功
drop database if exists temp_db;
create database temp_db;
use temp_db;
drop table if exists temp_basic;
create temporary table temp_basic (
    id int,
    name varchar(50),
    age int
);

-- 测试用例 1.2: 验证临时表可以插入和查询数据
-- 预期结果: 插入成功，查询返回3条记录
insert into temp_basic values (1, 'alice', 25);
insert into temp_basic values (2, 'bob', 30);
insert into temp_basic values (3, 'charlie', 35);
select * from temp_basic;

-- 测试用例 1.3: 删除临时表
-- 预期结果: 删除成功
drop table temp_basic;

-- 测试用例 1.4: 验证删除后表不存在
-- 预期结果: 报错 "table does not exist"
select * from temp_basic;

-- ============================================================================
-- 测试分类 2: 数据类型支持
-- ============================================================================

-- 测试用例 2.1: 支持多种数据类型
-- 预期结果: 创建成功，数据正确存储
create temporary table temp_datatypes (
    col_int int,
    col_bigint bigint,
    col_float float,
    col_double double,
    col_decimal decimal(15,2),
    col_varchar varchar(255),
    col_text text,
    col_date date,
    col_datetime datetime,
    col_timestamp timestamp,
    col_bool boolean
);

insert into temp_datatypes values (
    100,
    9223372036854775807,
    3.14,
    2.718281828,
    12345.67,
    'test string',
    'long text content',
    '2024-01-15',
    '2024-01-15 10:30:00',
    '2024-01-15 10:30:00',
    true
);

-- @ignore:8,9
select * from temp_datatypes;

drop table temp_datatypes;

-- ============================================================================
-- 测试分类 3: 主键和约束
-- ============================================================================

-- 测试用例 3.1: 主键约束
-- 预期结果: 前两条插入成功，第三条因主键冲突失败
create temporary table temp_pk (
    id int primary key,
    name varchar(50)
);

insert into temp_pk values (1, 'alice');
insert into temp_pk values (2, 'bob');
insert into temp_pk values (1, 'charlie');  -- 应该失败

select * from temp_pk;
drop table temp_pk;

-- 测试用例 3.2: not null 约束
-- 预期结果: 前两条成功，第三条因not null约束失败
create temporary table temp_not_null (
    id int not null,
    name varchar(50) not null,
    description varchar(200)
);

insert into temp_not_null values (1, 'item1', 'description');
insert into temp_not_null values (2, 'item2', null);
insert into temp_not_null values (null, 'item3', 'description');  -- 应该失败

select * from temp_not_null;
drop table temp_not_null;

-- 测试用例 3.3: 默认值
-- 预期结果: 第一条使用默认值，第二条使用指定值
create temporary table temp_default (
    id int,
    status varchar(20) default 'active',
    created_at timestamp default current_timestamp
);

insert into temp_default (id) values (1);
insert into temp_default (id, status) values (2, 'inactive');

-- @ignore:2
select * from temp_default;
drop table temp_default;

-- ============================================================================
-- 测试分类 4: auto_increment
-- ============================================================================

-- 测试用例 4.1: auto_increment 基本功能
-- 预期结果: id 自动递增为 1, 2, 3
create temporary table temp_auto_inc (
    id bigint primary key auto_increment,
    name varchar(50)
);

insert into temp_auto_inc(name) values ('alice');
insert into temp_auto_inc(name) values ('bob');
insert into temp_auto_inc(name) values ('charlie');

select * from temp_auto_inc;
drop table temp_auto_inc;

-- 测试用例 4.2: auto_increment 指定值
-- 预期结果: 可以手动指定id，后续自动递增从最大值+1开始
create temporary table temp_auto_inc2 (
    id bigint primary key auto_increment,
    value varchar(10)
);

insert into temp_auto_inc2(value) values ('aaa');
insert into temp_auto_inc2(id, value) values (10, 'bbb');
insert into temp_auto_inc2(value) values ('ccc');  -- id应该是11

select * from temp_auto_inc2;
drop table temp_auto_inc2;

-- ============================================================================
-- 测试分类 5: dml 操作
-- ============================================================================

-- 测试用例 5.1: insert 操作
-- 预期结果: 插入成功
create temporary table temp_dml (
    id int,
    name varchar(50),
    value decimal(10,2)
);

insert into temp_dml values (1, 'item1', 100.50);
insert into temp_dml values (2, 'item2', 200.75);
insert into temp_dml (id, name) values (3, 'item3');

select * from temp_dml;

-- 测试用例 5.2: update 操作
-- 预期结果: 更新成功
update temp_dml set value = 150.00 where id = 1;
update temp_dml set name = 'updated item' where id = 2;

select * from temp_dml;

-- 测试用例 5.3: delete 操作
-- 预期结果: 删除成功
delete from temp_dml where id = 3;
select * from temp_dml;

delete from temp_dml where value > 150;
select * from temp_dml;

drop table temp_dml;

-- 测试用例 5.4: 批量插入
-- 预期结果: 批量插入成功
create temporary table temp_batch (
    id int,
    value varchar(50)
);

insert into temp_batch values
    (1, 'value1'),
    (2, 'value2'),
    (3, 'value3'),
    (4, 'value4'),
    (5, 'value5');

select count(*) from temp_batch;
drop table temp_batch;

-- ============================================================================
-- 测试分类 6: select 查询
-- ============================================================================

-- 测试用例 6.1: 基本查询
create temporary table temp_select (
    id int,
    category varchar(50),
    amount decimal(10,2)
);

insert into temp_select values
    (1, 'food', 50.00),
    (2, 'transport', 30.00),
    (3, 'food', 70.00),
    (4, 'entertainment', 100.00),
    (5, 'transport', 25.00);

-- 测试用例 6.2: where 条件查询
-- 预期结果: 返回2条food记录
select * from temp_select where category = 'food';

-- 测试用例 6.3: order by 排序
-- 预期结果: 按金额降序排列
select * from temp_select order by amount desc;

-- 测试用例 6.4: limit 限制
-- 预期结果: 返回前3条记录
select * from temp_select order by id limit 3;

-- 测试用例 6.5: distinct 去重
-- 预期结果: 返回3个不同的类别
select distinct category from temp_select;

drop table temp_select;

-- ============================================================================
-- 测试分类 7: 聚合函数
-- ============================================================================

-- 测试用例 7.1: 聚合函数测试
create temporary table temp_agg (
    id int,
    category varchar(50),
    amount decimal(10,2)
);

insert into temp_agg values
    (1, 'food', 50.00),
    (2, 'transport', 30.00),
    (3, 'food', 70.00),
    (4, 'entertainment', 100.00),
    (5, 'transport', 25.00);

-- 测试用例 7.2: count 计数
-- 预期结果: 返回5
select count(*) from temp_agg;

-- 测试用例 7.3: sum 求和
-- 预期结果: 返回275.00
select sum(amount) from temp_agg;

-- 测试用例 7.4: avg 平均值
-- 预期结果: 返回55.00
select avg(amount) from temp_agg;

-- 测试用例 7.5: min 和 max
-- 预期结果: min=25.00, max=100.00
select min(amount), max(amount) from temp_agg;

-- 测试用例 7.6: group by 分组聚合
-- 预期结果: 按类别分组统计
select
    category,
    count(*) as count,
    sum(amount) as total,
    avg(amount) as average
from temp_agg
group by category;

-- 测试用例 7.7: having 过滤
-- 预期结果: 只返回总金额大于50的类别
select
    category,
    sum(amount) as total
from temp_agg
group by category
having sum(amount) > 50;

drop table temp_agg;

-- ============================================================================
-- 测试分类 8: 子查询
-- ============================================================================

-- 测试用例 8.1: 子查询测试
create temporary table temp_subquery (
    id int,
    product varchar(50),
    price decimal(10,2)
);

insert into temp_subquery values
    (1, 'laptop', 999.99),
    (2, 'mouse', 29.99),
    (3, 'keyboard', 79.99),
    (4, 'monitor', 299.99),
    (5, 'headset', 59.99);

-- 测试用例 8.2: where 子查询
-- 预期结果: 返回价格高于平均价格的产品
select * from temp_subquery
where price > (select avg(price) from temp_subquery);

-- 测试用例 8.3: from 子查询
-- 预期结果: 返回统计信息
select
    count(*) as total_products,
    avg(avg_price) as overall_avg
from (
    select avg(price) as avg_price from temp_subquery
) sub;

-- 测试用例 8.4: in 子查询
-- 预期结果: 返回价格在指定范围内的产品
select * from temp_subquery
where price in (
    select price from temp_subquery where price < 100
);

drop table temp_subquery;

-- ============================================================================
-- 测试分类 9: show 命令
-- ============================================================================

-- 测试用例 9.1: show create table
-- 预期结果: 显示create temporary table语句
create temporary table temp_show (
    id int primary key,
    name varchar(50)
);

show create table temp_show;

drop table temp_show;

-- 测试用例 9.2: show tables 不显示临时表
-- 预期结果: show tables结果中不包含临时表
create table permanent_table (id int);
create temporary table temp_invisible (id int);

show tables;  -- 应该只显示 permanent_table

drop table temp_invisible;
drop table permanent_table;
drop database temp_db;