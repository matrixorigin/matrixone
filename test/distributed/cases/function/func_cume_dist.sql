-- ============================================================================
-- CUME_DIST() 窗口函数 完整测试用例
-- CUME_DIST() 返回累积分布值
-- 公式: (当前行的 rank 位置及之前的行数) / (分区总行数)
-- 即: count(rows with value <= current) / total_rows
-- 返回值范围: (0, 1]，最后一行始终为 1
-- 适用数据库: matrixone
-- issue: https://github.com/matrixorigin/matrixone/issues/23011
-- ============================================================================

-- ############################################################################
-- 第一部分: 基础功能测试
-- ############################################################################

-- ============================================================================
-- test 1.1: 基本用法 - 全局 cume_dist
-- ============================================================================
drop database if exists cume_dist_func;
create database cume_dist_func;
use cume_dist_func;
drop table if exists test_cd_basic;
create table test_cd_basic (
    id int primary key,
    department varchar(50),
    employee varchar(50),
    salary decimal(10, 2)
);

insert into test_cd_basic values
(1,  'engineering', 'alice',   80000),
(2,  'engineering', 'bob',     90000),
(3,  'engineering', 'charlie', 90000),
(4,  'engineering', 'david',   100000),
(5,  'engineering', 'eve',     110000),
(6,  'sales',      'frank',   50000),
(7,  'sales',      'grace',   60000),
(8,  'sales',      'hank',    70000),
(9,  'hr',         'ivy',     65000),
(10, 'hr',         'jack',    65000);

select
    employee,
    salary,
    cume_dist() over (order by salary) as cd
from test_cd_basic
order by salary;
-- 预期:
-- frank   50000   0.1    -- 1/10
-- grace   60000   0.2    -- 2/10
-- ivy     65000   0.4    -- 4/10 (两个65000)
-- jack    65000   0.4    -- 4/10
-- hank    70000   0.5    -- 5/10
-- alice   80000   0.6    -- 6/10
-- bob     90000   0.8    -- 8/10 (两个90000)
-- charlie 90000   0.8    -- 8/10
-- david   100000  0.9    -- 9/10
-- eve     110000  1.0    -- 10/10

-- ============================================================================
-- test 1.2: partition by - 按部门分区计算
-- ============================================================================
select
    department,
    employee,
    salary,
    cume_dist() over (partition by department order by salary) as cd
from test_cd_basic
order by department, salary;
-- 预期:
-- engineering  alice    80000   0.2    -- 1/5
-- engineering  bob      90000   0.6    -- 3/5 (两个90000)
-- engineering  charlie  90000   0.6    -- 3/5
-- engineering  david    100000  0.8    -- 4/5
-- engineering  eve      110000  1.0    -- 5/5
-- hr           ivy      65000   1.0    -- 2/2 (两个65000都是最大)
-- hr           jack     65000   1.0    -- 2/2
-- sales        frank    50000   0.3333 -- 1/3
-- sales        grace    60000   0.6667 -- 2/3
-- sales        hank     70000   1.0    -- 3/3

-- ============================================================================
-- test 1.3: desc 降序排列
-- ============================================================================
select
    employee,
    salary,
    cume_dist() over (order by salary desc) as cd_desc
from test_cd_basic
order by salary desc;

-- ============================================================================
-- test 1.4: 与 percent_rank 对比
-- ============================================================================
select
    employee,
    salary,
    cume_dist() over (order by salary) as cd,
    percent_rank() over (order by salary) as pr
from test_cd_basic
order by salary;
-- 注意区别:
-- cume_dist 从 1/n 开始，percent_rank 从 0 开始
-- cume_dist 最后一行=1，percent_rank 最后一行=1

-- ============================================================================
-- test 1.5: 与 rank / dense_rank 对比
-- ============================================================================
select
    employee,
    salary,
    rank() over (order by salary) as rnk,
    dense_rank() over (order by salary) as dense_rnk,
    cume_dist() over (order by salary) as cd
from test_cd_basic
order by salary;

-- ============================================================================
-- test 1.6: 多列 order by
-- ============================================================================
select
    department,
    employee,
    salary,
    cume_dist() over (order by department, salary) as cd
from test_cd_basic
order by department, salary;

-- ============================================================================
-- test 1.7: 多个 partition by 列
-- ============================================================================
drop table if exists test_cd_multi_part;
create table test_cd_multi_part (
    region varchar(20),
    department varchar(20),
    employee varchar(20),
    salary int
);
insert into test_cd_multi_part values
('east', 'it', 'a', 100), ('east', 'it', 'b', 200),
('east', 'hr', 'c', 150), ('east', 'hr', 'd', 250),
('west', 'it', 'e', 120), ('west', 'it', 'f', 180);

select
    region, department, employee, salary,
    cume_dist() over (partition by region, department order by salary) as cd
from test_cd_multi_part
order by region, department, salary;

drop table if exists test_cd_multi_part;

drop table if exists test_cd_basic;

-- ############################################################################
-- 第二部分: 边界条件测试
-- ############################################################################

-- ============================================================================
-- test 2.1: 分区只有一行
-- 预期: 只有一行时 cume_dist = 1/1 = 1.0
-- ============================================================================
drop table if exists test_cd_single;
create table test_cd_single (id int, val int);
insert into test_cd_single values (1, 100);

select val, cume_dist() over (order by val) as cd from test_cd_single;
-- 预期: 100  1.0000

drop table if exists test_cd_single;

-- ============================================================================
-- test 2.2: 所有值相同
-- 预期: 所有行 cume_dist = 1.0 (所有行都 <= 当前值)
-- ============================================================================
drop table if exists test_cd_all_same;
create table test_cd_all_same (id int, val int);
insert into test_cd_all_same values (1, 50), (2, 50), (3, 50), (4, 50);

select
    id, val,
    cume_dist() over (order by val) as cd
from test_cd_all_same order by id;
-- 预期: 全部 cd = 1.0000

drop table if exists test_cd_all_same;

-- ============================================================================
-- test 2.3: 空表
-- ============================================================================
drop table if exists test_cd_empty;
create table test_cd_empty (id int, val int);

select val, cume_dist() over (order by val) as cd from test_cd_empty;
-- 预期: 0 行

drop table if exists test_cd_empty;

-- ============================================================================
-- test 2.4: 两行数据
-- ============================================================================
drop table if exists test_cd_two_rows;
create table test_cd_two_rows (id int, val int);
insert into test_cd_two_rows values (1, 10), (2, 20);

select val, cume_dist() over (order by val) as cd from test_cd_two_rows;
-- 预期: 10→0.5, 20→1.0

drop table if exists test_cd_two_rows;

-- ============================================================================
-- test 2.5: 两行值相同
-- ============================================================================
drop table if exists test_cd_two_same;
create table test_cd_two_same (id int, val int);
insert into test_cd_two_same values (1, 10), (2, 10);

select val, cume_dist() over (order by val) as cd from test_cd_two_same;
-- 预期: 两行都是 1.0

drop table if exists test_cd_two_same;

-- ============================================================================
-- test 2.6: 大量并列值
-- ============================================================================
drop table if exists test_cd_many_ties;
create table test_cd_many_ties (id int, val int);
insert into test_cd_many_ties values
(1, 10), (2, 10), (3, 10),
(4, 20),
(5, 30), (6, 30);

select
    id, val,
    rank() over (order by val) as rnk,
    cume_dist() over (order by val) as cd
from test_cd_many_ties order by val, id;
-- 预期:
-- 1  10  1  0.5     -- 3/6
-- 2  10  1  0.5
-- 3  10  1  0.5
-- 4  20  4  0.6667  -- 4/6
-- 5  30  5  1.0     -- 6/6
-- 6  30  5  1.0

drop table if exists test_cd_many_ties;

-- ============================================================================
-- test 2.7: null 值处理
-- ============================================================================
drop table if exists test_cd_null;
create table test_cd_null (id int, val int);
insert into test_cd_null values (1, null), (2, 10), (3, 20), (4, null), (5, 30);

select
    id, val,
    cume_dist() over (order by val) as cd
from test_cd_null order by val;
-- 预期: null 排在前面

drop table if exists test_cd_null;

-- ============================================================================
-- test 2.8: 全部为 null
-- ============================================================================
drop table if exists test_cd_all_null;
create table test_cd_all_null (id int, val int);
insert into test_cd_all_null values (1, null), (2, null), (3, null);

select
    id, val,
    cume_dist() over (order by val) as cd
from test_cd_all_null order by id;
-- 预期: 全部为 1.0 (所有值相同)

drop table if exists test_cd_all_null;

-- ============================================================================
-- test 2.9: 大数据量（100行）
-- ============================================================================
drop table if exists test_cd_large;
create table test_cd_large (id int, val int);
insert into test_cd_large values
(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10),
(11,11),(12,12),(13,13),(14,14),(15,15),(16,16),(17,17),(18,18),(19,19),(20,20),
(21,21),(22,22),(23,23),(24,24),(25,25),(26,26),(27,27),(28,28),(29,29),(30,30),
(31,31),(32,32),(33,33),(34,34),(35,35),(36,36),(37,37),(38,38),(39,39),(40,40),
(41,41),(42,42),(43,43),(44,44),(45,45),(46,46),(47,47),(48,48),(49,49),(50,50),
(51,51),(52,52),(53,53),(54,54),(55,55),(56,56),(57,57),(58,58),(59,59),(60,60),
(61,61),(62,62),(63,63),(64,64),(65,65),(66,66),(67,67),(68,68),(69,69),(70,70),
(71,71),(72,72),(73,73),(74,74),(75,75),(76,76),(77,77),(78,78),(79,79),(80,80),
(81,81),(82,82),(83,83),(84,84),(85,85),(86,86),(87,87),(88,88),(89,89),(90,90),
(91,91),(92,92),(93,93),(94,94),(95,95),(96,96),(97,97),(98,98),(99,99),(100,100);

-- 验证第1行=0.01, 第50行=0.5, 第100行=1.0
select id, val, cume_dist() over (order by val) as cd
from test_cd_large
where id in (1, 50, 100)
order by val;
-- 预期:
-- 1    1    0.01   -- 1/100
-- 50   50   0.50   -- 50/100
-- 100  100  1.00   -- 100/100

-- 验证所有行的 cume_dist 值在 (0, 1] 范围内
select count(*) as out_of_range_count
from (
    select cume_dist() over (order by val) as cd from test_cd_large
) t
where cd <= 0 or cd > 1;
-- 预期: 0

drop table if exists test_cd_large;

-- ############################################################################
-- 第三部分: 所有 mo 支持的列类型上使用 cume_dist
-- ############################################################################

-- ============================================================================
-- test 3.1: 整数类型 - tinyint
-- ============================================================================
drop table if exists test_cd_tinyint;
create table test_cd_tinyint (id int, val tinyint);
insert into test_cd_tinyint values (1, -128), (2, -1), (3, 0), (4, 1), (5, 127);

select val, cume_dist() over (order by val) as cd from test_cd_tinyint order by val;
-- 预期: -128→0.2, -1→0.4, 0→0.6, 1→0.8, 127→1.0

drop table if exists test_cd_tinyint;

-- ============================================================================
-- test 3.2: 整数类型 - smallint
-- ============================================================================
drop table if exists test_cd_smallint;
create table test_cd_smallint (id int, val smallint);
insert into test_cd_smallint values (1, -32768), (2, 0), (3, 32767);

select val, cume_dist() over (order by val) as cd from test_cd_smallint order by val;

drop table if exists test_cd_smallint;

-- ============================================================================
-- test 3.3: 整数类型 - int
-- ============================================================================
drop table if exists test_cd_int;
create table test_cd_int (id int, val int);
insert into test_cd_int values (1, -2147483648), (2, 0), (3, 2147483647);

select val, cume_dist() over (order by val) as cd from test_cd_int order by val;

drop table if exists test_cd_int;

-- ============================================================================
-- test 3.4: 整数类型 - bigint
-- ============================================================================
drop table if exists test_cd_bigint;
create table test_cd_bigint (id int, val bigint);
insert into test_cd_bigint values (1, -9223372036854775808), (2, 0), (3, 9223372036854775807);

select val, cume_dist() over (order by val) as cd from test_cd_bigint order by val;

drop table if exists test_cd_bigint;

-- ============================================================================
-- test 3.5: 整数类型 - bigint unsigned
-- ============================================================================
drop table if exists test_cd_bigint_u;
create table test_cd_bigint_u (id int, val bigint unsigned);
insert into test_cd_bigint_u values (1, 0), (2, 9223372036854775808), (3, 18446744073709551615);

select val, cume_dist() over (order by val) as cd from test_cd_bigint_u order by val;

drop table if exists test_cd_bigint_u;

-- ============================================================================
-- test 3.6: 浮点类型 - float
-- ============================================================================
drop table if exists test_cd_float;
create table test_cd_float (id int, val float);
insert into test_cd_float values (1, -3.14), (2, 0), (3, 1.5), (4, 3.14), (5, 99.99);

select val, cume_dist() over (order by val) as cd from test_cd_float order by val;

drop table if exists test_cd_float;

-- ============================================================================
-- test 3.7: 浮点类型 - double
-- ============================================================================
drop table if exists test_cd_double;
create table test_cd_double (id int, val double);
insert into test_cd_double values (1, -1.79769e+308), (2, 0), (3, 1.79769e+308);

select val, cume_dist() over (order by val) as cd from test_cd_double order by val;

drop table if exists test_cd_double;

-- ============================================================================
-- test 3.8: decimal 类型
-- ============================================================================
drop table if exists test_cd_decimal;
create table test_cd_decimal (id int, val decimal(18, 6));
insert into test_cd_decimal values
(1, -999999.999999), (2, 0.000001), (3, 123456.789012), (4, 999999.999999);

select val, cume_dist() over (order by val) as cd from test_cd_decimal order by val;

drop table if exists test_cd_decimal;

-- ============================================================================
-- test 3.9: decimal(128) - 38位精度
-- ============================================================================
drop table if exists test_cd_decimal128;
create table test_cd_decimal128 (id int, val decimal(38, 18));
insert into test_cd_decimal128 values
(1, -12345678901234567890.123456789012345678),
(2, 0),
(3, 12345678901234567890.123456789012345678);

select val, cume_dist() over (order by val) as cd from test_cd_decimal128 order by val;

drop table if exists test_cd_decimal128;

-- ============================================================================
-- test 3.10: char 类型
-- ============================================================================
drop table if exists test_cd_char;
create table test_cd_char (id int, val char(10));
insert into test_cd_char values (1, 'apple'), (2, 'banana'), (3, 'cherry'), (4, 'date');

select val, cume_dist() over (order by val) as cd from test_cd_char order by val;

drop table if exists test_cd_char;

-- ============================================================================
-- test 3.11: varchar 类型
-- ============================================================================
drop table if exists test_cd_varchar;
create table test_cd_varchar (id int, val varchar(255));
insert into test_cd_varchar values (1, 'aaa'), (2, 'aab'), (3, 'bbb'), (4, 'zzz');

select val, cume_dist() over (order by val) as cd from test_cd_varchar order by val;

drop table if exists test_cd_varchar;

-- ============================================================================
-- test 3.12: text 类型
-- ============================================================================
drop table if exists test_cd_text;
create table test_cd_text (id int, val text);
insert into test_cd_text values (1, 'short'), (2, 'medium length text'), (3, 'zzz last');

select val, cume_dist() over (order by val) as cd from test_cd_text order by val;

drop table if exists test_cd_text;

-- ============================================================================
-- test 3.13: date 类型
-- ============================================================================
drop table if exists test_cd_date;
create table test_cd_date (id int, val date);
insert into test_cd_date values
(1, '2020-01-01'), (2, '2022-06-15'), (3, '2024-12-31'), (4, '2025-01-01');

select val, cume_dist() over (order by val) as cd from test_cd_date order by val;

drop table if exists test_cd_date;

-- ============================================================================
-- test 3.14: datetime 类型
-- ============================================================================
drop table if exists test_cd_datetime;
create table test_cd_datetime (id int, val datetime);
insert into test_cd_datetime values
(1, '2024-01-01 00:00:00'),
(2, '2024-01-01 00:00:01'),
(3, '2024-06-15 12:30:45'),
(4, '2024-12-31 23:59:59');

select val, cume_dist() over (order by val) as cd from test_cd_datetime order by val;

drop table if exists test_cd_datetime;

-- ============================================================================
-- test 3.15: timestamp 类型
-- ============================================================================
drop table if exists test_cd_timestamp;
create table test_cd_timestamp (id int, val timestamp);
insert into test_cd_timestamp values
(1, '2020-01-01 00:00:00'),
(2, '2022-06-15 12:00:00'),
(3, '2025-12-31 23:59:59');

select val, cume_dist() over (order by val) as cd from test_cd_timestamp order by val;

drop table if exists test_cd_timestamp;

-- ============================================================================
-- test 3.16: time 类型
-- ============================================================================
drop table if exists test_cd_time;
create table test_cd_time (id int, val time);
insert into test_cd_time values
(1, '00:00:00'), (2, '08:30:00'), (3, '12:00:00'), (4, '23:59:59');

select val, cume_dist() over (order by val) as cd from test_cd_time order by val;

drop table if exists test_cd_time;

-- ============================================================================
-- test 3.17: blob 类型
-- ============================================================================
drop table if exists test_cd_blob;
create table test_cd_blob (id int, val blob);
insert into test_cd_blob values (1, 'abc'), (2, 'def'), (3, 'xyz');

select id, cume_dist() over (order by val) as cd from test_cd_blob order by val;

drop table if exists test_cd_blob;
-- ============================================================================
-- test 3.18: enum 类型
-- ============================================================================
drop table if exists test_cd_enum;
create table test_cd_enum (id int, val enum('low', 'medium', 'high', 'critical'));
insert into test_cd_enum values (1, 'low'), (2, 'medium'), (3, 'high'), (4, 'critical');

select val, cume_dist() over (order by val) as cd from test_cd_enum order by val;

drop table if exists test_cd_enum;

-- ============================================================================
-- test 3.19: json 类型 (order by id)
-- ============================================================================
drop table if exists test_cd_json;
create table test_cd_json (id int, val json);
insert into test_cd_json values (1, '{"a":1}'), (2, '{"a":2}'), (3, '{"a":3}');

select id, val, cume_dist() over (order by id) as cd from test_cd_json order by id;

drop table if exists test_cd_json;

-- ============================================================================
-- test 3.20: boolean 类型
-- ============================================================================
drop table if exists test_cd_bool;
create table test_cd_bool (id int, val boolean);
insert into test_cd_bool values (1, false), (2, false), (3, true), (4, true);

select val, cume_dist() over (order by val) as cd from test_cd_bool order by val;
-- 预期: false(0)→0.5, false(0)→0.5, true(1)→1.0, true(1)→1.0

drop table if exists test_cd_bool;

-- ============================================================================
-- test 3.21: bit 类型
-- ============================================================================
drop table if exists test_cd_bit;
create table test_cd_bit (id int, val bit(8));
insert into test_cd_bit values (1, 0), (2, 127), (3, 255);

select id, cume_dist() over (order by val) as cd from test_cd_bit order by val;

drop table if exists test_cd_bit;

-- ############################################################################
-- 第四部分: 列约束与 cume_dist 组合测试
-- ############################################################################

-- ============================================================================
-- test 4.1: primary key 约束
-- ============================================================================
drop table if exists test_cd_pk;
create table test_cd_pk (
    id int primary key,
    val int
);
insert into test_cd_pk values (1, 30), (2, 10), (3, 20), (4, 50), (5, 40);

select id, val, cume_dist() over (order by val) as cd from test_cd_pk order by val;
-- 预期: 10→0.2, 20→0.4, 30→0.6, 40→0.8, 50→1.0

select id, val, cume_dist() over (order by id) as cd from test_cd_pk order by id;

drop table if exists test_cd_pk;

-- ============================================================================
-- test 4.2: not null 约束
-- ============================================================================
drop table if exists test_cd_notnull;
create table test_cd_notnull (
    id int not null,
    name varchar(50) not null,
    score int not null
);
insert into test_cd_notnull values (1, 'alice', 85), (2, 'bob', 90), (3, 'carol', 75);

select name, score, cume_dist() over (order by score) as cd
from test_cd_notnull order by score;

drop table if exists test_cd_notnull;

-- ============================================================================
-- test 4.3: auto_increment 约束
-- ============================================================================
drop table if exists test_cd_autoinc;
create table test_cd_autoinc (
    id bigint primary key auto_increment,
    val varchar(20)
);
insert into test_cd_autoinc (val) values ('first'), ('second'), ('third'), ('fourth'), ('fifth');

select id, val, cume_dist() over (order by id) as cd from test_cd_autoinc order by id;

drop table if exists test_cd_autoinc;

-- ============================================================================
-- test 4.4: unique index 约束
-- ============================================================================
drop table if exists test_cd_unique;
create table test_cd_unique (
    id int primary key,
    email varchar(100) unique,
    score int
);
insert into test_cd_unique values
(1, 'a@test.com', 80), (2, 'b@test.com', 90),
(3, 'c@test.com', 70), (4, 'd@test.com', 85);

select email, score, cume_dist() over (order by score) as cd
from test_cd_unique order by score;

drop table if exists test_cd_unique;

-- ============================================================================
-- test 4.5: default 约束
-- ============================================================================
drop table if exists test_cd_default;
create table test_cd_default (
    id int,
    priority int default 5,
    status varchar(20) default 'active'
);
insert into test_cd_default (id) values (1), (2), (3);
insert into test_cd_default values (4, 10, 'inactive'), (5, 1, 'active');

select id, priority, cume_dist() over (order by priority) as cd
from test_cd_default order by priority;

drop table if exists test_cd_default;

-- ############################################################################
-- 第五部分: 高级用法与组合测试
-- ############################################################################

-- ============================================================================
-- test 5.1: cume_dist 与其他窗口函数组合
-- ============================================================================
drop table if exists test_cd_combo;
create table test_cd_combo (id int, val int);
insert into test_cd_combo values (1,10),(2,20),(3,30),(4,40),(5,50),(6,60);

select
    id, val,
    cume_dist() over (order by val) as cd,
    percent_rank() over (order by val) as pr,
    rank() over (order by val) as rnk,
    dense_rank() over (order by val) as dense_rnk,
    row_number() over (order by val) as rn
from test_cd_combo order by val;

drop table if exists test_cd_combo;

-- ============================================================================
-- test 5.2: cume_dist 在子查询中 - 筛选前50%
-- ============================================================================
drop table if exists test_cd_subq;
create table test_cd_subq (id int, val int);
insert into test_cd_subq values (1,10),(2,20),(3,30),(4,40),(5,50),(6,60);

select * from (
    select id, val, cume_dist() over (order by val) as cd
    from test_cd_subq
) t where cd <= 0.5;
-- 预期: 返回前50%的行 (val=10,20,30)

drop table if exists test_cd_subq;

-- ============================================================================
-- test 5.3: cume_dist 与 where 条件
-- ============================================================================
drop table if exists test_cd_where;
create table test_cd_where (id int, department varchar(20), val int);
insert into test_cd_where values
(1, 'a', 10), (2, 'a', 20), (3, 'a', 30),
(4, 'b', 40), (5, 'b', 50), (6, 'b', 60);

select id, val, cume_dist() over (order by val) as cd
from test_cd_where
where department = 'a'
order by val;
-- 预期: 只对 department='a' 的3行计算

drop table if exists test_cd_where;

-- ============================================================================
-- test 5.4: cume_dist 与 join
-- ============================================================================
drop table if exists test_cd_t1;
drop table if exists test_cd_t2;
create table test_cd_t1 (id int, name varchar(20));
create table test_cd_t2 (id int, score int);
insert into test_cd_t1 values (1, 'alice'), (2, 'bob'), (3, 'carol'), (4, 'dave');
insert into test_cd_t2 values (1, 85), (2, 92), (3, 78), (4, 95);

select t1.name, t2.score,
       cume_dist() over (order by t2.score) as cd
from test_cd_t1 t1 join test_cd_t2 t2 on t1.id = t2.id
order by t2.score;

drop table if exists test_cd_t1;
drop table if exists test_cd_t2;

-- ============================================================================
-- test 5.5: cume_dist 与 group by (在外层查询)
-- ============================================================================
drop table if exists test_cd_group;
create table test_cd_group (department varchar(20), val int);
insert into test_cd_group values
('a', 10), ('a', 20), ('a', 30),
('b', 40), ('b', 50),
('c', 60);

select * from (
    select department, sum(val) as total,
           cume_dist() over (order by sum(val)) as cd
    from test_cd_group
    group by department
) t order by total;

drop table if exists test_cd_group;

-- ============================================================================
-- test 5.6: cume_dist 用于百分位筛选
-- ============================================================================
drop table if exists test_cd_percentile;
create table test_cd_percentile (id int, score int);
insert into test_cd_percentile values
(1,55),(2,60),(3,65),(4,70),(5,75),(6,80),(7,85),(8,90),(9,95),(10,100);

-- 筛选 top 30%
select * from (
    select id, score, cume_dist() over (order by score) as cd
    from test_cd_percentile
) t where cd > 0.7;
-- 预期: score >= 85 的行

-- 筛选 bottom 20%
select * from (
    select id, score, cume_dist() over (order by score) as cd
    from test_cd_percentile
) t where cd <= 0.2;
-- 预期: score <= 60 的行

drop table if exists test_cd_percentile;

-- ============================================================================
-- 清理数据库
-- ============================================================================
drop database if exists cume_dist_func;
