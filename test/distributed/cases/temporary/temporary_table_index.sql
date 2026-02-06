-- ============================================================================
-- matrixone 临时表测试用例 - 索引功能
-- 测试文件: test_cases_index.sql
-- ============================================================================

-- ============================================================================
-- 测试分类 1: 普通索引
-- ============================================================================

-- 测试用例 1.1: 创建表时定义索引
-- 预期结果: 创建成功
drop database if exists temp_index;
create database temp_index;
use temp_index;
create temporary table temp_idx1 (
    id int,
    name varchar(50),
    age int,
    index idx_name (name)
);

insert into temp_idx1 values (1, 'alice', 25);
insert into temp_idx1 values (2, 'bob', 30);
insert into temp_idx1 values (3, 'charlie', 25);

-- 测试用例 1.2: 使用索引查询
-- 预期结果: 查询成功
select * from temp_idx1 where name = 'bob';

-- 测试用例 1.3: show index 显示索引信息
-- 预期结果: 显示索引信息
show index from temp_idx1;

drop table temp_idx1;

-- ============================================================================
-- 测试分类 2: create index 语句
-- ============================================================================

-- 测试用例 2.1: 使用 create index 添加索引
-- 预期结果: 索引创建成功
create temporary table temp_idx2 (
    id int,
    name varchar(50),
    age int
);

create index idx_name on temp_idx2(name);
create index idx_age on temp_idx2(age);

insert into temp_idx2 values (1, 'alice', 25);
insert into temp_idx2 values (2, 'bob', 30);
insert into temp_idx2 values (3, 'charlie', 25);

-- 测试用例 2.2: 验证索引可用
select * from temp_idx2 where name = 'alice';
select * from temp_idx2 where age = 25;

show index from temp_idx2;

drop table temp_idx2;

-- ============================================================================
-- 测试分类 3: 唯一索引
-- ============================================================================

-- 测试用例 3.1: 创建唯一索引
-- 预期结果: 创建成功
create temporary table temp_unique (
    id int,
    email varchar(100)
);

create unique index idx_email on temp_unique(email);

-- 测试用例 3.2: 插入唯一值
-- 预期结果: 插入成功
insert into temp_unique values (1, 'alice@example.com');
insert into temp_unique values (2, 'bob@example.com');

-- 测试用例 3.3: 尝试插入重复值
-- 预期结果: 失败，违反唯一约束
-- @regex("Duplicate entry",true)
insert into temp_unique values (3, 'alice@example.com');

select * from temp_unique;
drop table temp_unique;

-- 测试用例 3.4: 表定义时创建唯一索引
-- 预期结果: 创建成功
create temporary table temp_unique2 (
    id int,
    username varchar(50),
    unique index idx_username (username)
);

insert into temp_unique2 values (1, 'user1');
insert into temp_unique2 values (2, 'user2');
-- @regex("Duplicate entry",true)
insert into temp_unique2 values (3, 'user1');  -- 应该失败

select * from temp_unique2;
drop table temp_unique2;

-- ============================================================================
-- 测试分类 4: 复合索引
-- ============================================================================

-- 测试用例 4.1: 创建复合索引
-- 预期结果: 创建成功
create temporary table temp_composite (
    id int,
    first_name varchar(50),
    last_name varchar(50),
    age int
);

create index idx_name on temp_composite(first_name, last_name);
create index idx_name_age on temp_composite(first_name, last_name, age);

insert into temp_composite values (1, 'john', 'doe', 30);
insert into temp_composite values (2, 'jane', 'smith', 25);
insert into temp_composite values (3, 'john', 'smith', 35);

-- 测试用例 4.2: 使用复合索引查询
select * from temp_composite where first_name = 'john';
select * from temp_composite where first_name = 'john' and last_name = 'doe';
select * from temp_composite where first_name = 'john' and last_name = 'smith' and age = 35;

show index from temp_composite;
drop table temp_composite;

-- ============================================================================
-- 测试分类 5: alter table add index
-- ============================================================================

-- 测试用例 5.1: alter table 添加索引
-- 预期结果: 添加成功
create temporary table temp_alter_idx (
    id int,
    name varchar(50),
    email varchar(100),
    age int
);

insert into temp_alter_idx values (1, 'alice', 'alice@example.com', 25);
insert into temp_alter_idx values (2, 'bob', 'bob@example.com', 30);

-- 添加普通索引
alter table temp_alter_idx add index idx_name (name);

-- 添加唯一索引
alter table temp_alter_idx add unique index idx_email (email);

-- 添加复合索引
alter table temp_alter_idx add index idx_name_age (name, age);

show index from temp_alter_idx;

-- 测试用例 5.2: 验证索引可用
select * from temp_alter_idx where name = 'alice';
select * from temp_alter_idx where email = 'bob@example.com';

drop table temp_alter_idx;

-- ============================================================================
-- 测试分类 6: alter table drop index
-- ============================================================================

-- 测试用例 6.1: alter table 删除索引
-- 预期结果: 删除成功
create temporary table temp_drop_idx (
    id int,
    name varchar(50),
    age int,
    index idx_name (name),
    index idx_age (age)
);

insert into temp_drop_idx values (1, 'alice', 25);
insert into temp_drop_idx values (2, 'bob', 30);

-- 显示初始索引
show index from temp_drop_idx;

-- 删除索引
alter table temp_drop_idx drop index idx_name;

-- 验证索引已删除
show index from temp_drop_idx;

-- 删除另一个索引
alter table temp_drop_idx drop index idx_age;

show index from temp_drop_idx;

drop table temp_drop_idx;

-- ============================================================================
-- 测试分类 7: drop index 语句
-- ============================================================================

-- 测试用例 7.1: 使用 drop index 删除索引
-- 预期结果: 删除成功
create temporary table temp_drop_idx2 (
    id int,
    name varchar(50),
    email varchar(100)
);

create index idx_name on temp_drop_idx2(name);
create index idx_email on temp_drop_idx2(email);

show index from temp_drop_idx2;

-- 使用 drop index 删除
drop index idx_name on temp_drop_idx2;

show index from temp_drop_idx2;

drop index idx_email on temp_drop_idx2;

show index from temp_drop_idx2;

drop table temp_drop_idx2;

-- ============================================================================
-- 测试分类 8: 全文索引
-- ============================================================================

-- 测试用例 8.1: 创建全文索引
-- 预期结果: 创建成功
create temporary table temp_fulltext (
    id int primary key,
    title varchar(100),
    content text
);

create fulltext index idx_ft_title on temp_fulltext(title);
create fulltext index idx_ft_content on temp_fulltext(content);

insert into temp_fulltext values (1, 'introduction to matrixone', 'matrixone is a hyperconverged database');
insert into temp_fulltext values (2, 'temporary tables guide', 'learn how to use temporary tables');
insert into temp_fulltext values (3, 'index management', 'creating and managing indexes in matrixone');

show index from temp_fulltext;

drop table temp_fulltext;

-- 测试用例 8.2: 多列全文索引
-- 预期结果: 创建成功
create temporary table temp_fulltext2 (
    id int primary key,
    title varchar(100),
    body text
);

create fulltext index idx_ft_multi on temp_fulltext2(title, body);

insert into temp_fulltext2 values (1, 'database tutorial', 'learn about databases and sql');
insert into temp_fulltext2 values (2, 'matrixone features', 'explore the features of matrixone');

show index from temp_fulltext2;

drop table temp_fulltext2;

-- ============================================================================
-- 测试分类 9: 向量索引 (ivf)
-- ============================================================================

-- 测试用例 9.1: 创建向量索引
-- 预期结果: 创建成功（需要开启 experimental_ivf_index）
set experimental_ivf_index = 1;

create temporary table temp_vector (
    id int primary key,
    embedding vecf32(3)
);

create index idx_ivf using ivfflat on temp_vector(embedding) lists=1 op_type "vector_l2_ops";

insert into temp_vector values (1, '[1.0, 2.0, 3.0]');
insert into temp_vector values (2, '[4.0, 5.0, 6.0]');
insert into temp_vector values (3, '[7.0, 8.0, 9.0]');

show index from temp_vector;

drop table temp_vector;

-- 测试用例 9.2: 不同维度的向量索引
-- 预期结果: 创建成功
create temporary table temp_vector2 (
    id int primary key,
    vec128 vecf32(128)
);

create index idx_vec128 using ivfflat on temp_vector2(vec128) lists=2 op_type "vector_l2_ops";

show index from temp_vector2;

drop table temp_vector2;

set experimental_ivf_index = 0;

-- ============================================================================
-- 测试分类 10: 多索引组合
-- ============================================================================

-- 测试用例 10.1: 在同一表上创建多种类型的索引
-- 预期结果: 所有索引创建成功
create temporary table temp_multi_idx (
    id int primary key,
    username varchar(50),
    email varchar(100),
    age int,
    bio text,
    created_at timestamp
);

-- 普通索引
create index idx_username on temp_multi_idx(username);

-- 唯一索引
create unique index idx_email on temp_multi_idx(email);

-- 复合索引
create index idx_age_created on temp_multi_idx(age, created_at);

-- 全文索引
create fulltext index idx_bio on temp_multi_idx(bio);

-- 显示所有索引
show index from temp_multi_idx;

-- 插入测试数据
insert into temp_multi_idx values
    (1, 'alice', 'alice@example.com', 25, 'software engineer', '2024-01-01 10:00:00'),
    (2, 'bob', 'bob@example.com', 30, 'data scientist', '2024-01-02 11:00:00'),
    (3, 'charlie', 'charlie@example.com', 25, 'product manager', '2024-01-03 12:00:00');

-- 测试各种索引查询
select * from temp_multi_idx where username = 'alice';
select * from temp_multi_idx where email = 'bob@example.com';
select * from temp_multi_idx where age = 25;

drop table temp_multi_idx;

-- ============================================================================
-- 测试分类 11: 索引边界测试
-- ============================================================================

-- 测试用例 11.1: 空表上创建索引
-- 预期结果: 创建成功
create temporary table temp_empty_idx (
    id int,
    name varchar(50)
);

create index idx_name on temp_empty_idx(name);

show index from temp_empty_idx;

-- 后续插入数据
insert into temp_empty_idx values (1, 'test');
select * from temp_empty_idx where name = 'test';

drop table temp_empty_idx;

-- 测试用例 11.2: 在有数据的表上创建索引
-- 预期结果: 创建成功
create temporary table temp_data_idx (
    id int,
    value varchar(50)
);

insert into temp_data_idx values (1, 'value1');
insert into temp_data_idx values (2, 'value2');
insert into temp_data_idx values (3, 'value3');

-- 在已有数据的表上创建索引
create index idx_value on temp_data_idx(value);

show index from temp_data_idx;

select * from temp_data_idx where value = 'value2';

drop table temp_data_idx;

-- 测试用例 11.3: 删除不存在的索引
-- 预期结果: 报错
create temporary table temp_no_idx (
    id int,
    name varchar(50)
);

drop index idx_nonexistent on temp_no_idx;  -- 应该失败

drop table temp_no_idx;
drop database temp_index;