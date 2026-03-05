-- ============================================================================
-- matrixone 临时表测试用例 - 限制和不支持的功能
-- ============================================================================

-- ============================================================================
-- 测试分类 1: truncate table (不支持)
-- ============================================================================

-- 测试用例 1.1: truncate 临时表
-- 预期结果: 报错，不支持 truncate
drop database if exists unnormal_db;
create database unnormal_db;
use unnormal_db;
create temporary table temp_truncate_test (
    id int,
    data varchar(50)
);

insert into temp_truncate_test values (1, 'test1');
insert into temp_truncate_test values (2, 'test2');

truncate table temp_truncate_test;  -- 失败

-- 替代方案: 使用 delete
delete from temp_truncate_test;
select * from temp_truncate_test;  -- 应该返回空结果

drop table temp_truncate_test;

-- ============================================================================
-- 测试分类 2: 外键约束 (不支持)
-- ============================================================================

-- 测试用例 2.1: 临时表定义外键约束
-- 预期结果: 报错 "not supported: add foreign key for temporary table"
create table parent_table (
    id int primary key,
    name varchar(50)
);

insert into parent_table values (1, 'parent 1');

create temporary table temp_with_fk (
    id int,
    parent_id int,
    data varchar(50),
    constraint fk_parent foreign key (parent_id) references parent_table(id)
);  -- 失败

drop table parent_table;

-- 测试用例 2.2: 普通表外键引用临时表
-- 预期结果: 报错 "not supported: add foreign key for temporary table"
create temporary table temp_parent (
    id int primary key,
    name varchar(50)
);

create table child_table (
    id int,
    temp_parent_id int,
    constraint fk_temp foreign key (temp_parent_id) references temp_parent(id)
);  -- 失败

drop table temp_parent;

-- 测试用例 2.3: alter table 添加外键到临时表
-- 预期结果: 报错
create table ref_table (
    id int primary key
);

create temporary table temp_alter_fk (
    id int,
    ref_id int
);

alter table temp_alter_fk
add constraint fk_ref foreign key (ref_id) references ref_table(id);  -- 失败

drop table temp_alter_fk;
drop table ref_table;

-- ============================================================================
-- 测试分类 3: cluster by (不支持)
-- ============================================================================

-- 测试用例 3.1: 创建带 cluster by 的临时表
-- 预期结果: 报错 "temporary table does not support cluster by"
create temporary table temp_cluster (
    id int,
    category varchar(50),
    value decimal(10,2)
) cluster by (category);  -- 失败

-- ============================================================================
-- 测试分类 4: 分区表 (不支持)
-- ============================================================================

-- 测试用例 4.1: 创建分区临时表
-- 预期结果: 报错，临时表不支持分区
create temporary table temp_partition (
    id int,
    sale_date date,
    amount decimal(10,2)
)
partition by range (year(sale_date)) (
    partition p2023 values less than (2024),
    partition p2024 values less than (2025)
);  -- 失败

-- ============================================================================
-- 测试分类 5: alter table 限制 (仅支持索引操作)
-- ============================================================================

-- 测试用例 5.1: alter table add column
-- 预期结果: 失败
create temporary table temp_alter_column (
    id int,
    name varchar(50)
);

alter table temp_alter_column add column age int;  -- 失败

drop table temp_alter_column;

-- 测试用例 5.2: alter table drop column
-- 预期结果: 可能不支持
create temporary table temp_drop_column (
    id int,
    name varchar(50),
    age int
);

alter table temp_drop_column drop column age;  -- 失败

drop table temp_drop_column;

-- 测试用例 5.3: alter table modify column
-- 预期结果: 不支持
create temporary table temp_modify_column (
    id int,
    name varchar(50)
);

alter table temp_modify_column modify column name varchar(100);  -- 失败

drop table temp_modify_column;

-- 测试用例 5.4: alter table rename column
-- 预期结果: 不支持
create temporary table temp_rename_column (
    id int,
    old_name varchar(50)
);

alter table temp_rename_column rename column old_name to new_name;  -- 失败

drop table temp_rename_column;

-- 测试用例 5.5: alter table rename table
-- 预期结果: 不支持
create temporary table temp_old_name (
    id int
);

alter table temp_old_name rename to temp_new_name;  -- 失败

drop table temp_old_name;

-- ============================================================================
-- 测试分类 6: 重复创建
-- ============================================================================

-- 测试用例 6.1: 创建已存在的临时表
-- 预期结果: 报错 "table already exists"
create temporary table temp_duplicate (
    id int
);

create temporary table temp_duplicate (
    id int
);  -- 失败

drop table temp_duplicate;

-- 测试用例 6.2: create table if not exists
-- 预期结果: 第二次创建不报错
create temporary table if not exists temp_if_not_exists (
    id int
);

create temporary table if not exists temp_if_not_exists (
    id int
);  -- 成功，不创建新表

drop table temp_if_not_exists;

-- ============================================================================
-- 测试分类 7: 访问不存在的临时表
-- ============================================================================

-- 测试用例 7.1: 查询不存在的临时表
-- 预期结果: 报错 "table does not exist"
select * from temp_nonexistent;  -- 失败

-- 测试用例 7.2: 删除不存在的临时表
-- 预期结果: 报错 "table does not exist"
drop table temp_nonexistent;  -- 失败

-- 测试用例 7.3: drop table if exists
-- 预期结果: 不报错
drop table if exists temp_nonexistent;  -- 成功，不报错

-- ============================================================================
-- 测试分类 8: 数据库删除对临时表的影响
-- ============================================================================

-- 测试用例 8.1: 删除数据库不会删除临时表
-- 预期结果: 临时表仍然存在（与 mysql 行为一致）
create database test_temp_db;
use test_temp_db;

create temporary table temp_in_db (
    id int,
    data varchar(50)
);

insert into temp_in_db values (1, 'test data');

-- 切换到其他数据库
use mysql;

-- 删除数据库
drop database test_temp_db;

-- 临时表应该仍然可以通过完整名称访问
select * from test_temp_db.temp_in_db;

-- ============================================================================
-- 测试分类 9: 视图相关限制
-- ============================================================================

-- 测试用例 9.1: 基于临时表创建视图
-- 预期结果: 不支持
-- @bvt:issue#23700
drop database if exists temp_view;
create database temp_view;
use temp_view;
create temporary table temp_for_view (
    id int,
    name varchar(50)
);

insert into temp_for_view values (1, 'alice');

create view view_on_temp as
select * from temp_for_view;

-- 清理
drop view if exists view_on_temp;
drop table temp_for_view;
-- @bvt:issue

-- ============================================================================
-- 测试分类 11: 存储过程和函数限制
-- ============================================================================

-- 测试用例 11.1: 在存储过程中创建临时表
-- @bvt:issue#23700
delimiter //

create procedure test_temp_in_proc()
begin
    create table temp_in_proc (
        id int,
        data varchar(50)
    );

    insert into temp_in_proc values (1, 'test');
    select * from temp_in_proc;

    drop table temp_in_proc;
end //

delimiter ;

call test_temp_in_proc();

drop procedure test_temp_in_proc;
-- @bvt:issue


-- ============================================================================
-- 测试分类 12: 权限限制：普通用户没有创建临时表权限
-- ============================================================================
drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
-- @session:id=1&user=acc01:test_account&password=111
drop user if exists user_temp;
create user user_temp identified by '111';
drop role if exists role_temp;
create role role_temp;
grant create database on account * to role_temp;
grant show databases on account * to role_temp;
grant connect on account * to role_temp;
grant select on table * to role_temp;
grant show tables on database * to role_temp;
grant role_temp to user_temp;
-- @session
-- @session:id=1&user=acc01:user_temp&password=111
create database test;
use test;
create temporary table t1(col1 int);
-- @session
-- @session:id=1&user=acc01:test_account&password=111
drop user user_temp;
drop role role_temp;
-- @session
drop account if exists acc01;


-- ============================================================================
-- 测试分类 13: 特殊字符和命名限制
-- ============================================================================

-- 测试用例 13.1: 使用保留字作为表名
-- 预期结果: 需要使用反引号
drop database if exists temp_db;
create database temp_db;
use temp_db;
create temporary table `select` (
    id int
);

insert into `select` values (1);
select * from `select`;

drop table `select`;

-- 测试用例 13.2: 使用特殊字符的表名
-- 预期结果: 可能有限制
create temporary table `temp-table-with-dash` (
    id int
);

drop table if exists `temp-table-with-dash`;

-- ============================================================================
-- 测试分类 14: 大小限制
-- ============================================================================

-- 测试用例 14.1: 超长表名
-- 预期结果: 可能有长度限制
create temporary table temp_very_long_table_name_that_exceeds_normal_limits_and_might_cause_issues (
    id int
);

drop table if exists temp_very_long_table_name_that_exceeds_normal_limits_and_might_cause_issues;

-- 测试用例 14.2: 超长列名
-- 预期结果: 可能有长度限制
create temporary table temp_long_column (
    very_long_column_name_that_exceeds_normal_limits_and_might_cause_issues int
);

drop table if exists temp_long_column;
drop database unnormal_db;
drop database temporary_table_limitation;
drop database temp_db;
show databases;
