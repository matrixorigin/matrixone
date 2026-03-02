-- ============================================================================
-- matrixone 临时表测试用例 - 会话和生命周期
-- 测试文件: test_cases_session.sql
-- 注意: 此文件中的测试需要多个数据库会话来执行
-- ============================================================================

-- ============================================================================
-- 测试分类 1: 会话隔离性
-- ============================================================================

-- 测试用例 1.1: 不同会话的临时表互不可见
-- 执行步骤:
drop database if exists temp_session;
create database temp_session;
use temp_session;
drop table if exists temp_session1;
create temporary table temp_session1 (
    id int,
    session_name varchar(50)
);

insert into temp_session1 values (1, 'session 1 data');
select * from temp_session1;  -- 预期结果: 返回 (1, 'session 1 data')

-- @session:id=1{
select * from temp_session.temp_session1;  -- 预期结果: 报错 "table does not exist"
-- @session

-- ============================================================================
-- 测试分类 2: 同名临时表在不同会话
-- ============================================================================

-- 测试用例 2.1: 不同会话可以创建同名临时表
-- 执行步骤:
create temporary table temp_shared_name (
    id int,
    data varchar(50)
);

insert into temp_shared_name values (1, 'from session 1');
select * from temp_shared_name;  -- 预期结果: 返回 'from session 1'

-- @session:id=2{
drop database if exists session2;
create database session2;
create temporary table session2.temp_shared_name (
    id int,
    data varchar(50)
);

insert into session2.temp_shared_name values (2, 'from session 2');
select * from session2.temp_shared_name;  -- 预期结果: 返回 'from session 2'
-- @session

select * from temp_shared_name;  -- 预期结果: 仍然返回 'from session 1'

-- ============================================================================
-- 测试分类 3: 会话结束后自动清理
-- ============================================================================

-- 测试用例 3.1: 会话关闭后临时表自动删除
-- 执行步骤:
create temporary table temp_session.temp_auto_cleanup (
    id int,
    value varchar(50)
);

insert into temp_session.temp_auto_cleanup values (1, 'test data');
select * from temp_session.temp_auto_cleanup;  -- 预期结果: 返回数据


-- session 2 (新建连接):
-- @session:id=2{
select * from temp_session.temp_auto_cleanup;  -- 预期结果: 报错 "table does not exist"
-- @session

-- ============================================================================
-- 测试分类 4: 同名覆盖 - 临时表与普通表
-- ============================================================================

-- 测试用例 4.1: 临时表覆盖同名普通表
-- 执行步骤:
-- 创建普通表
create table users (
    id int,
    name varchar(50),
    type varchar(20) default 'permanent'
);

insert into users values (1, 'permanent user', 'permanent');
select * from users;  -- 预期结果: 返回 (1, 'permanent user', 'permanent')

-- 创建同名临时表
create temporary table users (
    id int,
    name varchar(50),
    type varchar(20) default 'temporary'
);

insert into users values (2, 'temporary user', 'temporary');
select * from users;  -- 预期结果: 返回 (2, 'temporary user', 'temporary')

-- 测试用例 4.2: 删除临时表后普通表重新可见
-- 继续 session 1:
drop table users;  -- 删除临时表
select * from users;  -- 预期结果: 返回 (1, 'permanent user', 'permanent')

-- 清理
drop table users;  -- 删除普通表

-- ============================================================================
-- 测试分类 5: 同名覆盖 - show create table
-- ============================================================================

-- 测试用例 5.1: show create table 显示临时表定义
-- 执行步骤:
-- session 1:
create table products (
    id int,
    name varchar(50)
);

show create table products;  -- 预期结果: 显示 create table products

create temporary table products (
    id int,
    name varchar(100)
);

show create table products;  -- 预期结果: 显示 create temporary table products

drop table products;  -- 删除临时表
show create table products;  -- 预期结果: 显示 create table products

drop table products;  -- 删除普通表

-- ============================================================================
-- 测试分类 6: 同名覆盖 - dml 操作
-- ============================================================================

-- 测试用例 6.1: dml 操作作用于临时表
-- 执行步骤:
-- session 1:
create table orders (
    id int,
    amount decimal(10,2)
);

insert into orders values (1, 100.00);

create temporary table orders (
    id int,
    amount decimal(10,2)
);

insert into orders values (2, 200.00);

-- update 作用于临时表
update orders set amount = 250.00 where id = 2;

-- delete 作用于临时表
delete from orders where id = 2;

select * from orders;  -- 预期结果: 空结果（临时表为空）

drop table orders;  -- 删除临时表
select * from orders;  -- 预期结果: 返回 (1, 100.00) 普通表数据

drop table orders;  -- 删除普通表

-- ============================================================================
-- 测试分类 7: 跨会话验证普通表不受影响
-- ============================================================================
-- 测试用例 7.1: 临时表不影响其他会话对普通表的访问
-- 执行步骤:
-- @session:id=1{
create table temp_session.customers (
    id int,
    name varchar(50)
);

insert into temp_session.customers values (1, 'customer a');
insert into temp_session.customers values (2, 'customer b');
-- @session

-- @session:id=2{
select * from temp_session.customers;  -- 预期结果: 返回2条记录
-- @session

-- @session:id=1{
create temporary table temp_session.customers (
    id int,
    name varchar(50)
);

insert into temp_session.customers values (3, 'temp customer');

select * from temp_session.customers;  -- 预期结果: 返回 (3, 'temp customer')
-- @session

-- @session:id=2{
select * from temp_session.customers;  -- 预期结果: 仍然返回原来的2条记录
-- @session

-- @session:id=1{
drop table temp_session.customers;  -- 删除临时表
drop table temp_session.customers;  -- 删除普通表
-- @session



-- ============================================================================
-- 测试分类 8: 显式删除临时表
-- ============================================================================

-- 测试用例 8.1: 手动删除临时表
-- 执行步骤:
-- session 1:
create temporary table temp_manual_drop (
    id int,
    data varchar(50)
);

insert into temp_manual_drop values (1, 'test');
select * from temp_manual_drop;  -- 预期结果: 返回数据

drop table temp_manual_drop;

select * from temp_manual_drop;  -- 预期结果: 报错 "table does not exist"

-- 测试用例 8.2: 删除临时表不影响同名普通表
-- 执行步骤:
-- session 1:
create table test_table (
    id int,
    type varchar(20) default 'permanent'
);

insert into test_table values (1, 'permanent');

create temporary table test_table (
    id int,
    type varchar(20) default 'temporary'
);

insert into test_table values (2, 'temporary');

drop table test_table;  -- 删除临时表

select * from test_table;  -- 预期结果: 返回 (1, 'permanent')

drop table test_table;  -- 删除普通表

-- ============================================================================
-- 测试分类 9: 多个临时表的生命周期
-- ============================================================================

-- 测试用例 9.1: 同一会话创建多个临时表
-- 执行步骤:
-- session 1:
create temporary table temp_table1 (id int);
create temporary table temp_table2 (id int);
create temporary table temp_table3 (id int);

insert into temp_table1 values (1);
insert into temp_table2 values (2);
insert into temp_table3 values (3);

select * from temp_table1;  -- 预期结果: 返回 1
select * from temp_table2;  -- 预期结果: 返回 2
select * from temp_table3;  -- 预期结果: 返回 3

-- 删除其中一个
drop table temp_table2;

select * from temp_table1;  -- 预期结果: 仍然可访问
select * from temp_table2;  -- 预期结果: 报错
select * from temp_table3;  -- 预期结果: 仍然可访问

-- 清理
drop table temp_table1;
drop table temp_table3;

-- ============================================================================
-- 测试分类 10: 会话超时和异常断开
-- ============================================================================

-- 测试用例 10.1: 模拟会话异常断开
-- 执行步骤:
-- @session:id=1{
create temporary table temp_session.temp_timeout (
    id int,
    data varchar(50)
);

insert into temp_session.temp_timeout values (1, 'test data');
-- @session

-- 模拟会话超时或异常断开（强制关闭连接）

-- session 2 (新建连接):
select * from temp_timeout;  -- 预期结果: 报错 "table does not exist"

-- ============================================================================
-- 测试分类 11: show tables 行为
-- ============================================================================

-- 测试用例 11.1: show tables 不显示临时表
-- 执行步骤:
-- @session:id=1{
create table temp_session.permanent1 (id int);
create table temp_session.permanent2 (id int);
create temporary table temp_session.temp1 (id int);
create temporary table temp_session.temp2 (id int);

use temp_session;
show tables;
-- @session
-- 预期结果: 只显示 permanent1 和 permanent2，不显示 temp1 和 temp2

-- 清理
drop table temp1;
drop table temp2;
drop table permanent1;
drop table permanent2;

-- 测试用例 11.2: 临时表覆盖普通表时 show tables 的行为
-- 执行步骤:
create table test_visibility (id int);
show tables;  -- 预期结果: 显示 test_visibility

create temporary table test_visibility (id int);
show tables;  -- 预期结果: 仍然显示 test_visibility（普通表）

drop table test_visibility;  -- 删除临时表
show tables;  -- 预期结果: 显示 test_visibility（普通表）

drop table test_visibility;  -- 删除普通表

-- ============================================================================
-- 测试分类 12: 长时间会话
-- ============================================================================

-- 测试用例 12.1: 临时表在长时间会话中的持久性
-- 执行步骤:
create temporary table temp_long_session (
    id int,
    created_at timestamp default current_timestamp
);

insert into temp_long_session (id) values (1);

-- @ignore:1
select * from temp_long_session;  -- 预期结果: 数据仍然存在

-- 继续操作
insert into temp_long_session (id) values (2);
-- @ignore:1
select * from temp_long_session;  -- 预期结果: 返回2条记录

drop table temp_long_session;
drop database temp_session;
drop database session2;
