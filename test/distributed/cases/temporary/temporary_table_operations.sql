-- ============================================================================
-- matrixone 临时表测试用例 - 数据导入导出
-- 测试文件: test_cases_data_operations.sql
-- ============================================================================

-- ============================================================================
-- 测试分类 1: load data 导入数据
-- ============================================================================

-- 测试用例 1.1: load data 导入到临时表
-- 预期结果: 导入成功
drop database if exists temp_operations;
create database temp_operations;
use temp_operations;
drop table if exists t1;
create temporary table t1(
col1 tinyint,
col2 smallint,
col3 int,
col4 bigint,
col5 tinyint unsigned,
col6 smallint unsigned,
col7 int unsigned,
col8 bigint unsigned
);

-- load data
load data infile '$resources/load_data/integer_numbers_1.csv' into table t1 fields terminated by ',';
select * from t1;
delete from t1;

-- 测试用例 1.2: load data 指定列
-- 预期结果: 导入成功，只导入指定列
create temporary table t1(
col1 tinyint,
col2 smallint,
col3 int,
col4 bigint,
col5 tinyint unsigned,
col6 smallint unsigned,
col7 int unsigned,
col8 bigint unsigned
);
-- csv 文件只包含部分列
load data infile '$resources/load_data/integer_numbers_1.csv' into table t1 fields terminated by ',';

select * from t1;

drop table t1;

-- 测试用例 1.3: load data 跳过表头
-- 预期结果: 跳过第一行，导入数据
drop table if exists t1;
create temporary table t1(a bigint primary key, b vecf32(3), c varchar(10), d double);

insert into t1 values(1, "[-1,-1,-1]", "aaaa", 1.213);
insert into t1 values(2, "[-1, -1, 0]", "bbbb", 11.213);
insert into t1 values(3, "[1,1,1]", "cccc", 111.213);

select count(*) from t1;

select * from t1 into outfile '$resources/into_outfile/load_data/temp1.csv';
-- 不支持truncate
truncate table t1;
load data infile '$resources/into_outfile/load_data/temp1.csv' into table t1 ignore 1 lines;
select count(*) from t1;
select * from t1;

drop table t1;

-- ============================================================================
-- 测试分类 2: select into outfile 导出数据
-- ============================================================================

-- 测试用例 2.1: 导出临时表数据到文件
-- 预期结果: 导出成功
create temporary table temp_export (
    id int,
    name varchar(50),
    salary decimal(10,2)
);

insert into temp_export values (1, 'alice', 60000.00);
insert into temp_export values (2, 'bob', 55000.00);
insert into temp_export values (3, 'charlie', 70000.00);

select * from temp_export
into outfile '$resources/into_outfile/load_data/temp2.csv'
fields terminated by ','
lines terminated by '\n';

drop table temp_export;


-- 测试用例 2.2: 导出查询结果
-- 预期结果: 导出成功
create temporary table temp_sales (
    sale_id int,
    product varchar(50),
    amount decimal(10,2),
    sale_date date
);

insert into temp_sales values (1, 'laptop', 999.99, '2024-01-15');
insert into temp_sales values (2, 'mouse', 29.99, '2024-01-16');
insert into temp_sales values (3, 'keyboard', 79.99, '2024-01-17');
insert into temp_sales values (4, 'monitor', 299.99, '2024-01-18');

select
    product,
    sum(amount) as total_sales,
    count(*) as sale_count
from temp_sales
group by product
into outfile '$resources/into_outfile/load_data/sales_summary.csv'
fields terminated by ','
lines terminated by '\n';

drop table temp_sales;
create temporary table temp_sales (
    product varchar(50),
    amount decimal(10,2),
    sale_date decimal
);
load data infile '$resources/into_outfile/load_data/sales_summary.csv' into table temp_sales ignore 1 lines;
select * from temp_sales;
drop table temp_sales;

-- 测试用例 2.3: 导出时指定字段分隔符和包围符
-- 预期结果: 导出成功，使用指定格式
create temporary table temp_export_format (
    id int,
    description varchar(100),
    price decimal(10,2)
);

insert into temp_export_format values (1, 'product with, comma', 99.99);
insert into temp_export_format values (2, 'product with "quotes"', 149.99);

select * from temp_export_format
into outfile '$resources/into_outfile/load_data/export_formatted.csv'
fields terminated by ','
enclosed by '"'
lines terminated by '\n';

drop table temp_export_format;

-- ============================================================================
-- 测试分类 3: 批量数据操作
-- ============================================================================

-- 测试用例 3.1: 大批量插入
-- 预期结果: 插入成功
create temporary table temp_bulk_insert (
    id int,
    value varchar(50),
    created_at timestamp default current_timestamp
);

-- 批量插入多行
insert into temp_bulk_insert (id, value) values
    (1, 'value1'),
    (2, 'value2'),
    (3, 'value3'),
    (4, 'value4'),
    (5, 'value5'),
    (6, 'value6'),
    (7, 'value7'),
    (8, 'value8'),
    (9, 'value9'),
    (10, 'value10');

select count(*) from temp_bulk_insert;

drop table temp_bulk_insert;

-- 测试用例 3.2: 批量更新
-- 预期结果: 更新成功
create temporary table temp_bulk_update (
    id int,
    status varchar(20),
    updated_at timestamp
);

insert into temp_bulk_update (id, status) values
    (1, 'pending'),
    (2, 'pending'),
    (3, 'active'),
    (4, 'pending'),
    (5, 'active');

-- 批量更新
update temp_bulk_update
set status = 'completed', updated_at = current_timestamp
where status = 'pending';

-- @ignore:2
select * from temp_bulk_update;

drop table temp_bulk_update;

-- 测试用例 3.3: 批量删除
-- 预期结果: 删除成功
create temporary table temp_bulk_delete (
    id int,
    category varchar(50),
    value decimal(10,2)
);

insert into temp_bulk_delete values
    (1, 'a', 100.00),
    (2, 'b', 200.00),
    (3, 'a', 150.00),
    (4, 'c', 300.00),
    (5, 'b', 250.00);

-- 批量删除
delete from temp_bulk_delete where category in ('a', 'b');

select * from temp_bulk_delete;

drop table temp_bulk_delete;

-- ============================================================================
-- 测试分类 4: 数据转换和清洗
-- ============================================================================

-- 测试用例 4.1: 使用临时表进行数据清洗
-- 预期结果: 数据清洗成功
create table raw_customer_data (
    id int,
    full_name varchar(100),
    email varchar(100),
    phone varchar(20)
);

insert into raw_customer_data values
    (1, 'john doe', 'john@example.com', '123-456-7890'),
    (2, 'jane smith', 'jane@example.com', '098-765-4321'),
    (3, 'bob johnson', 'bob@example.com', '555-123-4567');

-- 创建临时表进行数据清洗
create temporary table temp_cleaned_customers as
select
    id,
    substring_index(full_name, ' ', 1) as first_name,
    substring_index(full_name, ' ', -1) as last_name,
    lower(email) as email,
    replace(phone, '-', '') as phone_clean
from raw_customer_data;

select * from temp_cleaned_customers;

drop table temp_cleaned_customers;
drop table raw_customer_data;

-- 测试用例 4.2: 数据聚合和汇总
-- 预期结果: 聚合成功
create table transaction_log (
    trans_id int,
    customer_id int,
    product_id int,
    amount decimal(10,2),
    trans_date date
);

insert into transaction_log values
    (1, 1, 101, 99.99, '2024-01-15'),
    (2, 1, 102, 149.99, '2024-01-16'),
    (3, 2, 101, 99.99, '2024-01-17'),
    (4, 2, 103, 199.99, '2024-01-18'),
    (5, 1, 101, 99.99, '2024-01-19');

-- 创建临时汇总表
create temporary table temp_customer_summary as
select
    customer_id,
    count(*) as total_transactions,
    sum(amount) as total_spent,
    avg(amount) as avg_transaction,
    min(trans_date) as first_purchase,
    max(trans_date) as last_purchase
from transaction_log
group by customer_id;

select * from temp_customer_summary;

drop table temp_customer_summary;
drop table transaction_log;

-- ============================================================================
-- 测试分类 5: etl 场景
-- ============================================================================

-- 测试用例 5.1: 使用临时表进行 etl 处理
-- 预期结果: etl 流程成功
-- step 1: extract - 从源表提取数据
create table source_orders (
    order_id int,
    customer_name varchar(50),
    product_name varchar(50),
    quantity int,
    unit_price decimal(10,2),
    order_date date
);

insert into source_orders values
    (1, 'alice', 'laptop', 2, 999.99, '2024-01-15'),
    (2, 'bob', 'mouse', 5, 29.99, '2024-01-16'),
    (3, 'alice', 'keyboard', 3, 79.99, '2024-01-17');

-- step 2: transform - 使用临时表进行转换
create temporary table temp_transformed_orders as
select
    order_id,
    customer_name,
    product_name,
    quantity,
    unit_price,
    (quantity * unit_price) as total_amount,
    order_date,
    case
        when (quantity * unit_price) > 500 then 'high value'
        when (quantity * unit_price) > 100 then 'medium value'
        else 'low value'
    end as order_category
from source_orders;

select * from temp_transformed_orders;

-- step 3: load - 加载到目标表
create table target_orders (
    order_id int,
    customer_name varchar(50),
    product_name varchar(50),
    quantity int,
    unit_price decimal(10,2),
    total_amount decimal(10,2),
    order_date date,
    order_category varchar(20)
);

insert into target_orders select * from temp_transformed_orders;

select * from target_orders;

-- 清理
drop table temp_transformed_orders;
drop table target_orders;
drop table source_orders;

-- ============================================================================
-- 测试分类 6: 数据去重
-- ============================================================================

-- 测试用例 6.1: 使用临时表去重
-- 预期结果: 去重成功
create table duplicate_data (
    id int,
    email varchar(100),
    name varchar(50)
);

insert into duplicate_data values
    (1, 'alice@example.com', 'alice'),
    (2, 'bob@example.com', 'bob'),
    (3, 'alice@example.com', 'alice smith'),
    (4, 'charlie@example.com', 'charlie'),
    (5, 'bob@example.com', 'bob johnson');

-- 使用临时表保存去重后的数据
create temporary table temp_unique_data as
select
    min(id) as id,
    email,
    max(name) as name
from duplicate_data
group by email;

select * from temp_unique_data;

drop table temp_unique_data;
drop table duplicate_data;

-- ============================================================================
-- 测试分类 7: 数据分片和采样
-- ============================================================================

-- 测试用例 7.1: 数据采样
-- 预期结果: 采样成功
create table large_dataset (
    id int,
    value varchar(50)
);

-- 插入大量数据
insert into large_dataset values
    (1, 'value1'), (2, 'value2'), (3, 'value3'), (4, 'value4'), (5, 'value5'),
    (6, 'value6'), (7, 'value7'), (8, 'value8'), (9, 'value9'), (10, 'value10'),
    (11, 'value11'), (12, 'value12'), (13, 'value13'), (14, 'value14'), (15, 'value15'),
    (16, 'value16'), (17, 'value17'), (18, 'value18'), (19, 'value19'), (20, 'value20');

-- 创建临时表保存采样数据（每5条取1条）
create temporary table temp_sample as
select * from large_dataset
where id % 5 = 0;

select * from temp_sample;

drop table temp_sample;
drop table large_dataset;

-- ============================================================================
-- 测试分类 8: 数据备份和恢复
-- ============================================================================

-- 测试用例 8.1: 使用临时表进行数据备份
-- 预期结果: 备份成功
create table important_data (
    id int,
    data varchar(100),
    last_updated timestamp
);

insert into important_data values
    (1, 'important data 1', '2024-01-15 10:00:00'),
    (2, 'important data 2', '2024-01-16 11:00:00');

-- 备份到临时表
create temporary table temp_backup as
select * from important_data;

-- 模拟数据修改
update important_data set data = 'modified data' where id = 1;
delete from important_data where id = 2;

select * from important_data;

-- 从备份恢复
delete from important_data;
insert into important_data select * from temp_backup;

select * from important_data;

drop table temp_backup;
drop table important_data;

-- ============================================================================
-- 测试分类 9: 数据合并
-- ============================================================================

-- 测试用例 9.1: 合并多个数据源
-- 预期结果: 合并成功
create table source1 (
    id int,
    name varchar(50),
    source varchar(20)
);

create table source2 (
    id int,
    name varchar(50),
    source varchar(20)
);

insert into source1 values (1, 'alice', 'source1'), (2, 'bob', 'source1');
insert into source2 values (3, 'charlie', 'source2'), (4, 'david', 'source2');

-- 使用临时表合并数据
create temporary table temp_merged as
select * from source1
union all
select * from source2;

select * from temp_merged;

drop table temp_merged;
drop table source2;
drop table source1;
drop database if exists temp_operations;
