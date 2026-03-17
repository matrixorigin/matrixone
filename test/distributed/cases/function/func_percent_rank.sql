-- ============================================================================
-- percent_rank() 窗口函数 完整测试用例
-- 公式: (rank() - 1) / (total_rows_in_partition - 1)
-- 返回值范围: [0, 1]
-- 适用数据库: matrixone
-- ============================================================================

-- ############################################################################
-- 第一部分: 基础功能测试
-- ############################################################################

-- ============================================================================
-- test 1.1: 基本用法 - 全局 percent_rank
-- ============================================================================
drop database if exists percent_rank_func;
create database percent_rank_func;
use percent_rank_func;
drop table if exists test_pr_basic;
create table test_pr_basic (
    id int primary key,
    department varchar(50),
    employee varchar(50),
    salary decimal(10, 2)
);

insert into test_pr_basic values
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
    rank() over (order by salary) as rnk,
    percent_rank() over (order by salary) as pct_rank
from test_pr_basic
order by salary;
-- 预期结果:
-- frank   50000   1   0.0000    -- (1-1)/(10-1) = 0/9
-- grace   60000   2   0.1111    -- (2-1)/(10-1) = 1/9
-- ivy     65000   3   0.2222    -- (3-1)/(10-1) = 2/9
-- jack    65000   3   0.2222    -- 并列
-- hank    70000   5   0.4444    -- (5-1)/(10-1) = 4/9
-- alice   80000   6   0.5556    -- (6-1)/(10-1) = 5/9
-- bob     90000   7   0.6667    -- (7-1)/(10-1) = 6/9
-- charlie 90000   7   0.6667    -- 并列
-- david   100000  9   0.8889    -- (9-1)/(10-1) = 8/9
-- eve     110000  10  1.0000    -- (10-1)/(10-1) = 1

-- ============================================================================
-- test 1.2: partition by - 按部门分区计算
-- ============================================================================
select
    department,
    employee,
    salary,
    percent_rank() over (partition by department order by salary) as pct_rank
from test_pr_basic
order by department, salary;
-- 预期:
-- engineering  alice    80000   0.0000  -- (1-1)/(5-1) = 0/4
-- engineering  bob      90000   0.2500  -- (2-1)/(5-1) = 1/4
-- engineering  charlie  90000   0.2500  -- 并列
-- engineering  david    100000  0.7500  -- (4-1)/(5-1) = 3/4
-- engineering  eve      110000  1.0000  -- (5-1)/(5-1) = 1
-- hr           ivy      65000   0.0000  -- (1-1)/(2-1) = 0
-- hr           jack     65000   0.0000  -- 并列，都是第一
-- sales        frank    50000   0.0000
-- sales        grace    60000   0.5000
-- sales        hank     70000   1.0000

-- ============================================================================
-- test 1.3: desc 降序排列
-- ============================================================================
select
    employee,
    salary,
    percent_rank() over (order by salary desc) as pct_rank_desc
from test_pr_basic
order by salary desc;
-- 预期: eve=0, david=0.1111, bob/charlie=0.2222, ..., frank=1.0

-- ============================================================================
-- test 1.4: 与 cume_dist 对比
-- ============================================================================
select
    employee,
    salary,
    percent_rank() over (order by salary) as pct_rank from test_pr_basic
order by salary;
-- 注意区别:
-- percent_rank 从 0 开始，cume_dist 从 1/n 开始
-- 最后一行两者都为 1

-- ============================================================================
-- test 1.5: 与 rank / dense_rank 对比
-- ============================================================================
select
    employee,
    salary,
    rank() over (order by salary) as rnk,
    dense_rank() over (order by salary) as dense_rnk,
    percent_rank() over (order by salary) as pct_rank
from test_pr_basic
order by salary;
-- percent_rank 基于 rank()（非 dense_rank）

-- ============================================================================
-- test 1.6: 多列 order by
-- ============================================================================
-- ============================================================================
-- test 1.7: 多个 partition by 列
-- ============================================================================
drop table if exists test_pr_multi_part;
create table test_pr_multi_part (
    region varchar(20),
    department varchar(20),
    employee varchar(20),
    salary int
);
insert into test_pr_multi_part values
('east', 'it', 'a', 100), ('east', 'it', 'b', 200),
('east', 'hr', 'c', 150), ('east', 'hr', 'd', 250),
('west', 'it', 'e', 120), ('west', 'it', 'f', 180);

select
    region, department, employee, salary,
    percent_rank() over (partition by region, department order by salary) as pct_rank
from test_pr_multi_part
order by region, department, salary;
-- 预期: 每个 (region, department) 组合独立计算

drop table if exists test_pr_multi_part;


-- ############################################################################
-- 第二部分: 边界条件测试
-- ############################################################################

-- ============================================================================
-- test 2.1: 分区只有一行
-- 预期: 只有一行时返回 0（分母 n-1=0，定义返回 0）
-- ============================================================================
drop table if exists test_pr_single;
create table test_pr_single (id int, val int);
insert into test_pr_single values (1, 100);

select val, percent_rank() over (order by val) as pct_rank from test_pr_single;
-- 预期: 100  0.0000

drop table if exists test_pr_single;

-- ============================================================================
-- test 2.2: 所有值相同
-- 预期: 所有行 rank=1，percent_rank=0
-- ============================================================================
drop table if exists test_pr_all_same;
create table test_pr_all_same (id int, val int);
insert into test_pr_all_same values (1, 50), (2, 50), (3, 50), (4, 50);

select
    id, val,
    rank() over (order by val) as rnk,
    percent_rank() over (order by val) as pct_rank
from test_pr_all_same order by id;
-- 预期: 全部 rnk=1, pct_rank=0.0000

drop table if exists test_pr_all_same;

-- ============================================================================
-- test 2.3: 空表
-- 预期: 返回空结果集
-- ============================================================================
drop table if exists test_pr_empty;
create table test_pr_empty (id int, val int);

select val, percent_rank() over (order by val) as pct_rank from test_pr_empty;
-- 预期: 0 行

drop table if exists test_pr_empty;

-- ============================================================================
-- test 2.4: 两行数据
-- 预期: 第一行=0, 第二行=1
-- ============================================================================
drop table if exists test_pr_two_rows;
create table test_pr_two_rows (id int, val int);
insert into test_pr_two_rows values (1, 10), (2, 20);

select val, percent_rank() over (order by val) as pct_rank from test_pr_two_rows;
-- 预期: 10→0.0, 20→1.0

drop table if exists test_pr_two_rows;

-- ============================================================================
-- test 2.5: 两行值相同
-- 预期: 两行都是 0
-- ============================================================================
drop table if exists test_pr_two_same;
create table test_pr_two_same (id int, val int);
insert into test_pr_two_same values (1, 10), (2, 10);

select val, percent_rank() over (order by val) as pct_rank from test_pr_two_same;
-- 预期: 两行都是 0.0000

drop table if exists test_pr_two_same;

-- ============================================================================
-- test 2.6: 大量并列值
-- ============================================================================
drop table if exists test_pr_many_ties;
create table test_pr_many_ties (id int, val int);
insert into test_pr_many_ties values
(1, 10), (2, 10), (3, 10),
(4, 20),
(5, 30), (6, 30);

select
    id, val,
    rank() over (order by val) as rnk,
    percent_rank() over (order by val) as pct_rank
from test_pr_many_ties order by val, id;
-- 预期:
-- 1  10  1  0.0000  -- (1-1)/(6-1) = 0/5
-- 2  10  1  0.0000
-- 3  10  1  0.0000
-- 4  20  4  0.6000  -- (4-1)/(6-1) = 3/5
-- 5  30  5  0.8000  -- (5-1)/(6-1) = 4/5
-- 6  30  5  0.8000

drop table if exists test_pr_many_ties;

-- ============================================================================
-- test 2.7: null 值处理
-- ============================================================================
drop table if exists test_pr_null;
create table test_pr_null (id int, val int);
insert into test_pr_null values (1, null), (2, 10), (3, 20), (4, null), (5, 30);

select
    id, val,
    percent_rank() over (order by val) as pct_rank
from test_pr_null order by val;
-- 预期: null 值在 mo 中排序位置取决于实现（通常 nulls first）
-- 两个 null 并列，percent_rank = 0

drop table if exists test_pr_null;

-- ============================================================================
-- test 2.8: 全部为 null
-- ============================================================================
drop table if exists test_pr_all_null;
create table test_pr_all_null (id int, val int);
insert into test_pr_all_null values (1, null), (2, null), (3, null);

select
    id, val,
    percent_rank() over (order by val) as pct_rank
from test_pr_all_null order by id;
-- 预期: 全部为 0（所有值相同/null 并列）

drop table if exists test_pr_all_null;

-- ============================================================================
-- test 2.9: 大数据量（100行）
-- ============================================================================
drop table if exists test_pr_large;
create table test_pr_large (id int, val int);
insert into test_pr_large values
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

-- 验证第1行=0, 第50行≈0.4949, 第100行=1
select id, val, percent_rank() over (order by val) as pct_rank
from test_pr_large
where id in (1, 50, 100)
order by val;
-- 预期:
-- 1    1    0.0000       -- (1-1)/(100-1) = 0
-- 50   50   0.494949...  -- (50-1)/(100-1) = 49/99
-- 100  100  1.0000       -- (100-1)/(100-1) = 1

-- 验证所有行的 percent_rank 值在 [0, 1] 范围内
select count(*) as out_of_range_count
from (
    select percent_rank() over (order by val) as pct_rank from test_pr_large
) t
where pct_rank < 0 or pct_rank > 1;
-- 预期: 0

drop table if exists test_pr_large;

-- ############################################################################
-- 第三部分: 所有 mo 支持的列类型上使用 percent_rank
-- ############################################################################

-- ============================================================================
-- test 3.1: 整数类型 - tinyint
-- ============================================================================
drop table if exists test_pr_tinyint;
create table test_pr_tinyint (id int, val tinyint);
insert into test_pr_tinyint values (1, -128), (2, -1), (3, 0), (4, 1), (5, 127);

select val, percent_rank() over (order by val) as pct_rank from test_pr_tinyint order by val;
-- 预期: -128→0, -1→0.25, 0→0.5, 1→0.75, 127→1.0

drop table if exists test_pr_tinyint;

-- ============================================================================
-- test 3.2: 整数类型 - tinyint unsigned
-- ============================================================================
drop table if exists test_pr_tinyint_u;
create table test_pr_tinyint_u (id int, val tinyint unsigned);
insert into test_pr_tinyint_u values (1, 0), (2, 1), (3, 128), (4, 254), (5, 255);

select val, percent_rank() over (order by val) as pct_rank from test_pr_tinyint_u order by val;
-- 预期: 0→0, 1→0.25, 128→0.5, 254→0.75, 255→1.0

drop table if exists test_pr_tinyint_u;

-- ============================================================================
-- test 3.3: 整数类型 - smallint
-- ============================================================================
drop table if exists test_pr_smallint;
create table test_pr_smallint (id int, val smallint);
insert into test_pr_smallint values (1, -32768), (2, 0), (3, 32767);

select val, percent_rank() over (order by val) as pct_rank from test_pr_smallint order by val;
-- 预期: -32768→0, 0→0.5, 32767→1.0

drop table if exists test_pr_smallint;

-- ============================================================================
-- test 3.4: 整数类型 - smallint unsigned
-- ============================================================================
drop table if exists test_pr_smallint_u;
create table test_pr_smallint_u (id int, val smallint unsigned);
insert into test_pr_smallint_u values (1, 0), (2, 32768), (3, 65535);

select val, percent_rank() over (order by val) as pct_rank from test_pr_smallint_u order by val;
-- 预期: 0→0, 32768→0.5, 65535→1.0

drop table if exists test_pr_smallint_u;

-- ============================================================================
-- test 3.5: 整数类型 - int
-- ============================================================================
drop table if exists test_pr_int;
create table test_pr_int (id int, val int);
insert into test_pr_int values (1, -2147483648), (2, 0), (3, 2147483647);

select val, percent_rank() over (order by val) as pct_rank from test_pr_int order by val;
-- 预期: -2147483648→0, 0→0.5, 2147483647→1.0

drop table if exists test_pr_int;

-- ============================================================================
-- test 3.6: 整数类型 - int unsigned
-- ============================================================================
drop table if exists test_pr_int_u;
create table test_pr_int_u (id int, val int unsigned);
insert into test_pr_int_u values (1, 0), (2, 2147483648), (3, 4294967295);

select val, percent_rank() over (order by val) as pct_rank from test_pr_int_u order by val;
-- 预期: 0→0, 2147483648→0.5, 4294967295→1.0

drop table if exists test_pr_int_u;

-- ============================================================================
-- test 3.7: 整数类型 - bigint
-- ============================================================================
drop table if exists test_pr_bigint;
create table test_pr_bigint (id int, val bigint);
insert into test_pr_bigint values (1, -9223372036854775808), (2, 0), (3, 9223372036854775807);

select val, percent_rank() over (order by val) as pct_rank from test_pr_bigint order by val;
-- 预期: min→0, 0→0.5, max→1.0

drop table if exists test_pr_bigint;

-- ============================================================================
-- test 3.8: 整数类型 - bigint unsigned
-- ============================================================================
drop table if exists test_pr_bigint_u;
create table test_pr_bigint_u (id int, val bigint unsigned);
insert into test_pr_bigint_u values (1, 0), (2, 9223372036854775808), (3, 18446744073709551615);

select val, percent_rank() over (order by val) as pct_rank from test_pr_bigint_u order by val;
-- 预期: 0→0, mid→0.5, max→1.0

drop table if exists test_pr_bigint_u;

-- ============================================================================
-- test 3.9: 浮点类型 - float
-- ============================================================================
drop table if exists test_pr_float;
create table test_pr_float (id int, val float);
insert into test_pr_float values (1, -3.14), (2, 0), (3, 1.5), (4, 3.14), (5, 99.99);

select val, percent_rank() over (order by val) as pct_rank from test_pr_float order by val;
-- 预期: -3.14→0, 0→0.25, 1.5→0.5, 3.14→0.75, 99.99→1.0

drop table if exists test_pr_float;

-- ============================================================================
-- test 3.10: 浮点类型 - double
-- ============================================================================
drop table if exists test_pr_double;
create table test_pr_double (id int, val double);
insert into test_pr_double values (1, -1.79769e+308), (2, 0), (3, 1.79769e+308);

select val, percent_rank() over (order by val) as pct_rank from test_pr_double order by val;
-- 预期: min→0, 0→0.5, max→1.0

drop table if exists test_pr_double;

-- ============================================================================
-- test 3.11: float 带精度
-- ============================================================================
drop table if exists test_pr_float_prec;
create table test_pr_float_prec (id int, val float(10, 2));
insert into test_pr_float_prec values (1, 10.50), (2, 10.51), (3, 10.52), (4, 20.00);

select val, percent_rank() over (order by val) as pct_rank from test_pr_float_prec order by val;
-- 预期: 10.50→0, 10.51→0.3333, 10.52→0.6667, 20.00→1.0

drop table if exists test_pr_float_prec;

-- ============================================================================
-- test 3.12: decimal(64) - 18位精度
-- ============================================================================
drop table if exists test_pr_decimal64;
create table test_pr_decimal64 (id int, val decimal(18, 6));
insert into test_pr_decimal64 values
(1, -999999999999.999999),
(2, 0.000001),
(3, 123456.789012),
(4, 999999999999.999999);

select val, percent_rank() over (order by val) as pct_rank from test_pr_decimal64 order by val;
-- 预期: 0, 0.3333, 0.6667, 1.0

drop table if exists test_pr_decimal64;

-- ============================================================================
-- test 3.13: decimal(128) - 38位精度
-- ============================================================================
drop table if exists test_pr_decimal128;
create table test_pr_decimal128 (id int, val decimal(38, 18));
insert into test_pr_decimal128 values
(1, -12345678901234567890.123456789012345678),
(2, 0),
(3, 12345678901234567890.123456789012345678);

select val, percent_rank() over (order by val) as pct_rank from test_pr_decimal128 order by val;
-- 预期: 0, 0.5, 1.0

drop table if exists test_pr_decimal128;

-- ============================================================================
-- test 3.14: decimal 边界 - 精度为0（纯整数）
-- ============================================================================
drop table if exists test_pr_decimal_int;
create table test_pr_decimal_int (id int, val decimal(10, 0));
insert into test_pr_decimal_int values (1, -100), (2, 0), (3, 100);

select val, percent_rank() over (order by val) as pct_rank from test_pr_decimal_int order by val;
-- 预期: -100→0, 0→0.5, 100→1.0

drop table if exists test_pr_decimal_int;

-- ============================================================================
-- test 3.15: char 类型
-- ============================================================================
drop table if exists test_pr_char;
create table test_pr_char (id int, val char(10));
insert into test_pr_char values (1, 'apple'), (2, 'banana'), (3, 'cherry'), (4, 'date');

-- ============================================================================
-- test 3.16: varchar 类型
-- ============================================================================
drop table if exists test_pr_varchar;
create table test_pr_varchar (id int, val varchar(255));
insert into test_pr_varchar values (1, 'aaa'), (2, 'aab'), (3, 'bbb'), (4, 'zzz');
drop table if exists test_pr_varchar;

-- ============================================================================
-- test 3.17: varchar 中文排序
-- ============================================================================
drop table if exists test_pr_varchar_cn;
create table test_pr_varchar_cn (id int, val varchar(50));
insert into test_pr_varchar_cn values (1, '阿里'), (2, '百度'), (3, '腾讯');

-- ============================================================================
-- test 3.18: text 类型
-- ============================================================================
drop table if exists test_pr_text;
create table test_pr_text (id int, val text);
insert into test_pr_text values (1, 'short'), (2, 'medium length text'), (3, 'zzz last');

-- ============================================================================
-- test 3.19: blob 类型
-- ============================================================================
drop table if exists test_pr_blob;
create table test_pr_blob (id int, val blob);
insert into test_pr_blob values (1, 'abc'), (2, 'def'), (3, 'xyz');

-- ============================================================================
-- test 3.20: binary 类型
-- ============================================================================
drop table if exists test_pr_binary;
create table test_pr_binary (id int, val binary(4));
insert into test_pr_binary values (1, 'ab'), (2, 'cd'), (3, 'ef');

-- ============================================================================
-- test 3.21: varbinary 类型
-- ============================================================================
drop table if exists test_pr_varbinary;
create table test_pr_varbinary (id int, val varbinary(255));
insert into test_pr_varbinary values (1, 'abc'), (2, 'def'), (3, 'ghi');

-- ============================================================================
-- test 3.22: date 类型
-- ============================================================================
drop table if exists test_pr_date;
create table test_pr_date (id int, val date);
insert into test_pr_date values
(1, '2020-01-01'), (2, '2022-06-15'), (3, '2024-12-31'), (4, '2025-01-01');

select val, percent_rank() over (order by val) as pct_rank from test_pr_date order by val;
-- 预期: 2020→0, 2022→0.3333, 2024→0.6667, 2025→1.0

drop table if exists test_pr_date;

-- ============================================================================
-- test 3.23: datetime 类型
-- ============================================================================
drop table if exists test_pr_datetime;
create table test_pr_datetime (id int, val datetime);
insert into test_pr_datetime values
(1, '2024-01-01 00:00:00'),
(2, '2024-01-01 00:00:01'),
(3, '2024-06-15 12:30:45'),
(4, '2024-12-31 23:59:59');

select val, percent_rank() over (order by val) as pct_rank from test_pr_datetime order by val;
-- 预期: 0, 0.3333, 0.6667, 1.0

drop table if exists test_pr_datetime;

-- ============================================================================
-- test 3.24: datetime 带微秒精度
-- ============================================================================
drop table if exists test_pr_datetime_us;
create table test_pr_datetime_us (id int, val datetime(6));
insert into test_pr_datetime_us values
(1, '2024-01-01 00:00:00.000000'),
(2, '2024-01-01 00:00:00.000001'),
(3, '2024-01-01 00:00:00.999999');

select val, percent_rank() over (order by val) as pct_rank from test_pr_datetime_us order by val;
-- 预期: 0, 0.5, 1.0

drop table if exists test_pr_datetime_us;

-- ============================================================================
-- test 3.25: timestamp 类型
-- ============================================================================
drop table if exists test_pr_timestamp;
create table test_pr_timestamp (id int, val timestamp);
insert into test_pr_timestamp values
(1, '2020-01-01 00:00:00'),
(2, '2022-06-15 12:00:00'),
(3, '2025-12-31 23:59:59');

select val, percent_rank() over (order by val) as pct_rank from test_pr_timestamp order by val;
-- 预期: 0, 0.5, 1.0

drop table if exists test_pr_timestamp;

-- ============================================================================
-- test 3.26: time 类型
-- ============================================================================
drop table if exists test_pr_time;
create table test_pr_time (id int, val time);
insert into test_pr_time values
(1, '00:00:00'), (2, '08:30:00'), (3, '12:00:00'), (4, '23:59:59');

select val, percent_rank() over (order by val) as pct_rank from test_pr_time order by val;
-- 预期: 0, 0.3333, 0.6667, 1.0

drop table if exists test_pr_time;

-- ============================================================================
-- test 3.27: boolean 类型
-- ============================================================================
drop table if exists test_pr_bool;
create table test_pr_bool (id int, val boolean);
insert into test_pr_bool values (1, false), (2, false), (3, true), (4, true);

-- ============================================================================
-- test 3.28: enum 类型
-- ============================================================================
drop table if exists test_pr_enum;
create table test_pr_enum (id int, val enum('low', 'medium', 'high', 'critical'));
insert into test_pr_enum values (1, 'low'), (2, 'medium'), (3, 'high'), (4, 'critical');

-- ============================================================================
-- test 3.29: json 类型 (order by 可能不支持，验证行为)
-- ============================================================================
drop table if exists test_pr_json;
create table test_pr_json (id int, val json);
insert into test_pr_json values (1, '{"a":1}'), (2, '{"a":2}'), (3, '{"a":3}');

-- json 类型可能不支持直接 order by，使用 id 排序
select id, val, percent_rank() over (order by id) as pct_rank from test_pr_json order by id;
-- 预期: 0, 0.5, 1.0

drop table if exists test_pr_json;

-- ============================================================================
-- test 3.30: bit 类型
-- ============================================================================
drop table if exists test_pr_bit;
create table test_pr_bit (id int, val bit(8));
insert into test_pr_bit values (1, 0), (2, 127), (3, 255);

select id, percent_rank() over (order by val) as pct_rank from test_pr_bit order by val;
-- 预期: 0, 0.5, 1.0

drop table if exists test_pr_bit;

-- ############################################################################
-- 第四部分: 列约束与 percent_rank 组合测试
-- ############################################################################

-- ============================================================================
-- test 4.1: primary key 约束
-- ============================================================================
drop table if exists test_pr_pk;
create table test_pr_pk (
    id int primary key,
    val int
);
insert into test_pr_pk values (1, 30), (2, 10), (3, 20), (4, 50), (5, 40);

select id, val, percent_rank() over (order by val) as pct_rank from test_pr_pk order by val;
-- 预期: 10→0, 20→0.25, 30→0.5, 40→0.75, 50→1.0

-- 按主键排序
select id, val, percent_rank() over (order by id) as pct_rank from test_pr_pk order by id;
-- 预期: 1→0, 2→0.25, 3→0.5, 4→0.75, 5→1.0

drop table if exists test_pr_pk;

-- ============================================================================
-- test 4.2: not null 约束
-- ============================================================================
drop table if exists test_pr_notnull;
create table test_pr_notnull (
    id int not null,
    name varchar(50) not null,
    score int not null
);
insert into test_pr_notnull values (1, 'alice', 85), (2, 'bob', 90), (3, 'carol', 75);

select name, score, percent_rank() over (order by score) as pct_rank
from test_pr_notnull order by score;
-- 预期: 75→0, 85→0.5, 90→1.0

drop table if exists test_pr_notnull;

-- ============================================================================
-- test 4.3: default 约束
-- ============================================================================
drop table if exists test_pr_default;
create table test_pr_default (
    id int,
    status varchar(20) default 'active',
    priority int default 5,
    created_at timestamp default current_timestamp
);
insert into test_pr_default (id) values (1), (2), (3);
insert into test_pr_default values (4, 'inactive', 10, '2024-01-01 00:00:00');
insert into test_pr_default values (5, 'active', 1, '2025-01-01 00:00:00');

-- 按默认值列排序
select id, priority, percent_rank() over (order by priority) as pct_rank
from test_pr_default order by priority;

-- ============================================================================
-- test 4.4: auto_increment 约束
-- ============================================================================
drop table if exists test_pr_autoinc;
create table test_pr_autoinc (
    id bigint primary key auto_increment,
    val varchar(20)
);
insert into test_pr_autoinc (val) values ('first'), ('second'), ('third'), ('fourth'), ('fifth');

select id, val, percent_rank() over (order by id) as pct_rank from test_pr_autoinc order by id;
-- 预期: 1→0, 2→0.25, 3→0.5, 4→0.75, 5→1.0

-- auto_increment 跳跃后
insert into test_pr_autoinc (id, val) values (100, 'jump');
insert into test_pr_autoinc (val) values ('after_jump');

select id, val, percent_rank() over (order by id) as pct_rank from test_pr_autoinc order by id;
-- 预期: 7行数据，id 不连续但排序正确

drop table if exists test_pr_autoinc;

-- ============================================================================
-- test 4.5: unique index 约束
-- ============================================================================
drop table if exists test_pr_unique;
create table test_pr_unique (
    id int primary key,
    email varchar(100) unique,
    score int
);
insert into test_pr_unique values
(1, 'a@test.com', 80),
(2, 'b@test.com', 90),
(3, 'c@test.com', 70),
(4, 'd@test.com', 85);

-- unique 列上的 percent_rank（无并列）
select email, score, percent_rank() over (order by score) as pct_rank
from test_pr_unique order by score;
-- 预期: 70→0, 80→0.3333, 85→0.6667, 90→1.0

drop table if exists test_pr_unique;

-- ============================================================================
-- test 4.6: 复合主键
-- ============================================================================
drop table if exists test_pr_composite_pk;
create table test_pr_composite_pk (
    dept_id int,
    emp_id int,
    salary decimal(10,2),
    primary key (dept_id, emp_id)
);
insert into test_pr_composite_pk values
(1, 1, 50000), (1, 2, 60000), (1, 3, 55000),
(2, 1, 70000), (2, 2, 65000);

select dept_id, emp_id, salary,
    percent_rank() over (partition by dept_id order by salary) as pct_rank
from test_pr_composite_pk order by dept_id, salary;
-- 预期:
-- dept 1: 50000→0, 55000→0.5, 60000→1.0
-- dept 2: 65000→0, 70000→1.0

drop table if exists test_pr_composite_pk;

-- ============================================================================
-- test 4.7: 多约束组合
-- ============================================================================
drop table if exists test_pr_multi_constraint;
create table test_pr_multi_constraint (
    id bigint primary key auto_increment,
    name varchar(50) not null,
    email varchar(100) unique,
    age int default 0,
    status enum('active', 'inactive') default 'active',
    score decimal(5,2) not null,
    index idx_score (score)
);
insert into test_pr_multi_constraint (name, email, age, status, score) values
('alice', 'alice@test.com', 25, 'active', 88.50),
('bob', 'bob@test.com', 30, 'active', 92.00),
('carol', 'carol@test.com', 28, 'inactive', 75.30),
('david', 'david@test.com', 35, 'active', 88.50),
('eve', 'eve@test.com', 22, 'inactive', 95.00);

-- 按 score 排序（有并列）
select name, score,
    percent_rank() over (order by score) as pct_rank
from test_pr_multi_constraint order by score;
-- 预期: 75.30→0, 88.50→0.25, 88.50→0.25(并列), 92.00→0.75, 95.00→1.0

-- 按 status 分区
select name, status, score,
    percent_rank() over (partition by status order by score) as pct_rank
from test_pr_multi_constraint order by status, score;

-- 按 age 排序
select name, age,
    percent_rank() over (order by age) as pct_rank
from test_pr_multi_constraint order by age;

drop table if exists test_pr_multi_constraint;

-- ############################################################################
-- 第五部分: 高级场景与组合测试
-- ############################################################################

-- ============================================================================
-- test 5.1: cte 中使用 percent_rank 进行过滤
-- ============================================================================
drop table if exists test_pr_cte;
create table test_pr_cte (
    id int primary key,
    department varchar(50),
    salary decimal(10,2)
);
insert into test_pr_cte values
(1, 'it', 50000), (2, 'it', 60000), (3, 'it', 70000), (4, 'it', 80000), (5, 'it', 90000),
(6, 'hr', 45000), (7, 'hr', 55000), (8, 'hr', 65000);

-- 筛选各部门中排名前 50% 的员工
with ranked as (
    select id, department, salary,
        percent_rank() over (partition by department order by salary) as pct_rank
    from test_pr_cte
)
select * from ranked where pct_rank >= 0.5;
-- 预期: it 中 salary>=70000 的, hr 中 salary>=55000 的

drop table if exists test_pr_cte;

-- ============================================================================
-- test 5.2: 子查询中使用 percent_rank
-- ============================================================================
drop table if exists test_pr_subquery;
create table test_pr_subquery (id int, category varchar(20), amount decimal(10,2));
insert into test_pr_subquery values
(1, 'a', 100), (2, 'a', 200), (3, 'a', 300),
(4, 'b', 150), (5, 'b', 250);

select * from (
    select id, category, amount,
        percent_rank() over (partition by category order by amount) as pct_rank
    from test_pr_subquery
) t where pct_rank = 1.0;
-- 预期: 返回每个分区中排名最后的行 (a:300, b:250)

drop table if exists test_pr_subquery;

-- ============================================================================
-- test 5.3: 多个窗口函数同时使用
-- ============================================================================
drop table if exists test_pr_multi_window;
create table test_pr_multi_window (id int, val int);
insert into test_pr_multi_window values (1,10),(2,20),(3,20),(4,30),(5,40);

select
    id, val,
    row_number() over (order by val) as row_num,
    rank() over (order by val) as rnk,
    dense_rank() over (order by val) as dense_rnk,
    percent_rank() over (order by val) as pct_rank
from test_pr_multi_window order by val;

drop table if exists test_pr_multi_window;

-- ============================================================================
-- test 5.4: 不同窗口定义（多个 over 子句）
-- ============================================================================
drop table if exists test_pr_diff_window;
create table test_pr_diff_window (
    id int, dept varchar(20), region varchar(20), salary int
);
insert into test_pr_diff_window values
(1, 'it', 'east', 100), (2, 'it', 'west', 200),
(3, 'hr', 'east', 150), (4, 'hr', 'west', 250),
(5, 'it', 'east', 300);

select id, dept, region, salary,
    percent_rank() over (order by salary) as global_pct,
    percent_rank() over (partition by dept order by salary) as dept_pct,
    percent_rank() over (partition by region order by salary) as region_pct
from test_pr_diff_window order by salary;

drop table if exists test_pr_diff_window;

-- ============================================================================
-- test 5.5: join 后使用 percent_rank
-- ============================================================================
drop table if exists test_pr_join_dept;
drop table if exists test_pr_join_emp;
create table test_pr_join_dept (dept_id int primary key, dept_name varchar(50));
create table test_pr_join_emp (emp_id int primary key, dept_id int, salary int);

insert into test_pr_join_dept values (1, 'engineering'), (2, 'sales');
insert into test_pr_join_emp values (1,1,80000),(2,1,90000),(3,1,100000),(4,2,50000),(5,2,60000);

select
    d.dept_name, e.emp_id, e.salary,
    percent_rank() over (partition by d.dept_name order by e.salary) as pct_rank
from test_pr_join_emp e
join test_pr_join_dept d on e.dept_id = d.dept_id
order by d.dept_name, e.salary;

drop table if exists test_pr_join_emp;
drop table if exists test_pr_join_dept;

-- ============================================================================
-- test 5.6: union all 后使用 percent_rank
-- ============================================================================
drop table if exists test_pr_union_a;
drop table if exists test_pr_union_b;
create table test_pr_union_a (id int, val int);
create table test_pr_union_b (id int, val int);
insert into test_pr_union_a values (1, 10), (2, 30);
insert into test_pr_union_b values (3, 20), (4, 40);

select id, val, percent_rank() over (order by val) as pct_rank
from (
    select * from test_pr_union_a
    union all
    select * from test_pr_union_b
) combined
order by val;
-- 预期: 10→0, 20→0.3333, 30→0.6667, 40→1.0

drop table if exists test_pr_union_a;
drop table if exists test_pr_union_b;

-- ============================================================================
-- test 5.7: group by + 窗口函数
-- ============================================================================
drop table if exists test_pr_group;
create table test_pr_group (id int, category varchar(20), amount int);
insert into test_pr_group values
(1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40),(5,'c',50);

select
    category,
    sum(amount) as total,
    percent_rank() over (order by sum(amount)) as pct_rank
from test_pr_group
group by category
order by total;
-- 预期: a(30)→0, b(70)→0.5, c(50)→... 需要按 sum 排序

drop table if exists test_pr_group;

-- ============================================================================
-- test 5.8: case when 与 percent_rank 组合
-- ============================================================================
drop table if exists test_pr_case;
create table test_pr_case (id int, score int);
insert into test_pr_case values (1,60),(2,70),(3,75),(4,80),(5,90),(6,95),(7,100);

select
    id, score,
    percent_rank() over (order by score) as pct_rank,
    case
        when percent_rank() over (order by score) >= 0.8 then 'top 20%'
        when percent_rank() over (order by score) >= 0.5 then 'top 50%'
        else 'bottom 50%'
    end as tier
from test_pr_case order by score;

drop table if exists test_pr_case;

-- ============================================================================
-- test 5.9: insert select 中使用 percent_rank
-- ============================================================================
drop table if exists test_pr_source;
drop table if exists test_pr_target;
create table test_pr_source (id int, val int);
create table test_pr_target (id int, val int, pct_rank double);

insert into test_pr_source values (1,10),(2,20),(3,30),(4,40),(5,50);

insert into test_pr_target
select id, val, percent_rank() over (order by val) from test_pr_source;

select * from test_pr_target order by val;
-- 预期: 5行，pct_rank 分别为 0, 0.25, 0.5, 0.75, 1.0

drop table if exists test_pr_target;
drop table if exists test_pr_source;

-- ============================================================================
-- test 5.10: create table as select 中使用 percent_rank
-- ============================================================================
drop table if exists test_pr_ctas_src;
create table test_pr_ctas_src (id int, val int);
insert into test_pr_ctas_src values (1,100),(2,200),(3,300);

drop table if exists test_pr_ctas_result;
create table test_pr_ctas_result as
select id, val, percent_rank() over (order by val) as pct_rank
from test_pr_ctas_src;

select * from test_pr_ctas_result order by val;
-- 预期: 0, 0.5, 1.0

drop table if exists test_pr_ctas_result;
drop table if exists test_pr_ctas_src;

-- ############################################################################
-- 第六部分: 特殊值与极端场景
-- ############################################################################

-- ============================================================================
-- test 6.1: 负数值
-- ============================================================================
drop table if exists test_pr_negative;
create table test_pr_negative (id int, val decimal(10,2));
insert into test_pr_negative values
(1, -100.50), (2, -50.25), (3, 0), (4, 50.25), (5, 100.50);

select val, percent_rank() over (order by val) as pct_rank from test_pr_negative order by val;
-- 预期: -100.50→0, -50.25→0.25, 0→0.5, 50.25→0.75, 100.50→1.0

drop table if exists test_pr_negative;

-- ============================================================================
-- test 6.2: 极小差异值（精度测试）
-- ============================================================================
drop table if exists test_pr_precision;
create table test_pr_precision (id int, val decimal(18, 10));
insert into test_pr_precision values
(1, 1.0000000001),
(2, 1.0000000002),
(3, 1.0000000003);

select val, percent_rank() over (order by val) as pct_rank from test_pr_precision order by val;
-- 预期: 0, 0.5, 1.0 — 验证极小差异能正确区分排序

drop table if exists test_pr_precision;

-- ============================================================================
-- test 6.3: 混合 null 和非 null 值的分区
-- ============================================================================
drop table if exists test_pr_null_partition;
create table test_pr_null_partition (grp varchar(10), val int);
insert into test_pr_null_partition values
('a', null), ('a', 10), ('a', 20),
('b', null), ('b', null),
('c', 30);

select grp, val,
    percent_rank() over (partition by grp order by val) as pct_rank
from test_pr_null_partition order by grp, val;
-- 预期:
-- a: null→0, 10→0.5, 20→1.0
-- b: null→0, null→0 (两个 null 并列)
-- c: 30→0 (单行)

drop table if exists test_pr_null_partition;

-- ============================================================================
-- test 6.4: 空字符串排序
-- ============================================================================
drop table if exists test_pr_empty_str;
create table test_pr_empty_str (id int, val varchar(50));
insert into test_pr_empty_str values (1, ''), (2, 'a'), (3, 'b'), (4, '');

-- ============================================================================
-- test 6.5: 特殊字符排序
-- ============================================================================
drop table if exists test_pr_special_char;
create table test_pr_special_char (id int, val varchar(50));
insert into test_pr_special_char values
(1, '!abc'), (2, '#def'), (3, '123'), (4, 'abc'), (5, 'abc');

-- ============================================================================
-- test 6.6: date 边界值
-- ============================================================================
drop table if exists test_pr_date_boundary;
create table test_pr_date_boundary (id int, val date);
insert into test_pr_date_boundary values
(1, '0001-01-01'),
(2, '1970-01-01'),
(3, '2024-02-29'),  -- 闰年
(4, '9999-12-31');

select val, percent_rank() over (order by val) as pct_rank
from test_pr_date_boundary order by val;
-- 预期: 0, 0.3333, 0.6667, 1.0

drop table if exists test_pr_date_boundary;

-- ============================================================================
-- test 6.7: timestamp 边界值
-- ============================================================================
drop table if exists test_pr_ts_boundary;
create table test_pr_ts_boundary (id int, val timestamp);
select val, percent_rank() over (order by val) as pct_rank
from test_pr_ts_boundary order by val;
-- 预期: 0, 1.0

drop table if exists test_pr_ts_boundary;

-- ============================================================================
-- test 6.8: 超长字符串
-- ============================================================================
drop table if exists test_pr_long_str;
create table test_pr_long_str (id int, val text);
insert into test_pr_long_str values
(1, repeat('a', 1000)),
(2, repeat('b', 1000)),
(3, repeat('a', 999));

-- ============================================================================
-- test 6.9: 单分区多行 vs 多分区单行
-- ============================================================================
drop table if exists test_pr_partition_size;
create table test_pr_partition_size (grp int, val int);
insert into test_pr_partition_size values
(1, 10), (1, 20), (1, 30), (1, 40), (1, 50),  -- 一个大分区
(2, 100),  -- 单行分区
(3, 200),  -- 单行分区
(4, 300);  -- 单行分区

select grp, val,
    percent_rank() over (partition by grp order by val) as pct_rank
from test_pr_partition_size order by grp, val;
-- 预期:
-- grp=1: 0, 0.25, 0.5, 0.75, 1.0
-- grp=2: 0 (单行)
-- grp=3: 0 (单行)
-- grp=4: 0 (单行)

drop table if exists test_pr_partition_size;

-- ============================================================================
-- test 6.10: percent_rank 结果的算术运算
-- ============================================================================
drop table if exists test_pr_arithmetic;
create table test_pr_arithmetic (id int, val int);
insert into test_pr_arithmetic values (1,10),(2,20),(3,30),(4,40),(5,50);

select
    id, val,
    percent_rank() over (order by val) as pct_rank,
    percent_rank() over (order by val) * 100 as pct_rank_100,
    round(percent_rank() over (order by val), 2) as pct_rank_rounded,
    1 - percent_rank() over (order by val) as inverse_pct_rank
from test_pr_arithmetic order by val;
-- 预期: pct_rank_100 分别为 0, 25, 50, 75, 100

drop table if exists test_pr_arithmetic;

-- ############################################################################
-- 第七部分: 临时表上的 percent_rank 测试
-- ############################################################################

-- ============================================================================
-- test 7.1: 临时表基本 percent_rank
-- ============================================================================
create temporary table temp_pr_basic (id int, val int);
insert into temp_pr_basic values (1,10),(2,20),(3,30);

select val, percent_rank() over (order by val) as pct_rank from temp_pr_basic order by val;
-- 预期: 0, 0.5, 1.0

drop table temp_pr_basic;

-- ============================================================================
-- test 7.2: 临时表带约束
-- ============================================================================
create temporary table temp_pr_constrained (
    id int primary key auto_increment,
    name varchar(50) not null,
    score decimal(5,2) default 0.00,
    index idx_score (score)
);
insert into temp_pr_constrained (name, score) values
('a', 85.50), ('b', 92.00), ('c', 78.30), ('d', 85.50), ('e', 96.00);

select name, score,
    percent_rank() over (order by score) as pct_rank
from temp_pr_constrained order by score;
-- 预期: 78.30→0, 85.50→0.25, 85.50→0.25(并列), 92.00→0.75, 96.00→1.0

drop table temp_pr_constrained;

-- ============================================================================
-- test 7.3: 临时表与普通表 join 后使用 percent_rank
-- ============================================================================
drop table if exists perm_departments;
create table perm_departments (dept_id int primary key, dept_name varchar(50));
insert into perm_departments values (1, 'it'), (2, 'hr');

create temporary table temp_employees (
    emp_id int primary key,
    dept_id int,
    salary int not null
);
insert into temp_employees values (1,1,80000),(2,1,90000),(3,2,70000),(4,2,85000);

select d.dept_name, e.salary,
    percent_rank() over (partition by d.dept_name order by e.salary) as pct_rank
from temp_employees e
join perm_departments d on e.dept_id = d.dept_id
order by d.dept_name, e.salary;

drop table temp_employees;
drop table if exists perm_departments;

-- ============================================================================
-- test 7.4: 临时表 - 所有 mo 支持的数据类型
-- ============================================================================
create temporary table temp_pr_all_types (
    col_tinyint tinyint,
    col_smallint smallint,
    col_int int,
    col_bigint bigint,
    col_tinyint_u tinyint unsigned,
    col_smallint_u smallint unsigned,
    col_int_u int unsigned,
    col_bigint_u bigint unsigned,
    col_float float,
    col_double double,
    col_decimal64 decimal(15, 6),
    col_decimal128 decimal(30, 10),
    col_char char(20),
    col_varchar varchar(100),
    col_text text,
    col_blob blob,
    col_binary binary(8),
    col_varbinary varbinary(100),
    col_date date,
    col_datetime datetime,
    col_timestamp timestamp,
    col_time time,
    col_bool boolean,
    col_enum enum('a','b','c'),
    col_json json,
    col_bit bit(8)
);

insert into temp_pr_all_types values (
    1, 100, 1000, 100000,
    1, 100, 1000, 100000,
    1.5, 2.5, 123.456789, 1234567890.1234567890,
    'hello', 'world', 'text data', 'blob data',
    'binary', 'varbinary',
    '2024-06-15', '2024-06-15 12:00:00', '2024-06-15 12:00:00', '12:30:00',
    true, 'a', '{"key":"val"}', 255
);
insert into temp_pr_all_types values (
    2, 200, 2000, 200000,
    2, 200, 2000, 200000,
    3.5, 4.5, 456.789012, 9876543210.9876543210,
    'world', 'hello', 'more text', 'more blob',
    'binary2', 'varbinary2',
    '2025-01-01', '2025-01-01 00:00:00', '2025-01-01 00:00:00', '18:00:00',
    false, 'c', '{"key":"val2"}', 128
);

-- 在各种类型列上使用 percent_rank
select col_tinyint, percent_rank() over (order by col_tinyint) as pr from temp_pr_all_types;
select col_float, percent_rank() over (order by col_float) as pr from temp_pr_all_types;
select col_decimal64, percent_rank() over (order by col_decimal64) as pr from temp_pr_all_types;
select col_date, percent_rank() over (order by col_date) as pr from temp_pr_all_types;
select col_datetime, percent_rank() over (order by col_datetime) as pr from temp_pr_all_types;
select col_timestamp, percent_rank() over (order by col_timestamp) as pr from temp_pr_all_types;
select col_time, percent_rank() over (order by col_time) as pr from temp_pr_all_types;

-- ############################################################################
-- 第八部分: 错误场景与语法边界测试
-- ############################################################################

-- ============================================================================
-- test 8.1: percent_rank 不带 order by（验证行为）
-- 预期: 没有 order by 时，所有行排名相同，percent_rank 应为 0
-- ============================================================================
drop table if exists test_pr_no_orderby;
create table test_pr_no_orderby (id int, val int);
insert into test_pr_no_orderby values (1,10),(2,20),(3,30);

select id, val, percent_rank() over () as pct_rank from test_pr_no_orderby;
-- 预期: 所有行 pct_rank = 0（无排序，所有行视为并列）

drop table if exists test_pr_no_orderby;

-- ============================================================================
-- test 8.2: percent_rank 带参数（应报错）
-- 预期: percent_rank 不接受参数，应报语法错误
-- ============================================================================
drop table if exists test_pr_with_arg;
create table test_pr_with_arg (id int, val int);
insert into test_pr_with_arg values (1,10),(2,20);

-- 以下语句应报错
-- select percent_rank(val) over (order by val) from test_pr_with_arg;

drop table if exists test_pr_with_arg;

-- ============================================================================
-- test 8.3: percent_rank 不能用在非窗口上下文
-- 预期: 不能在 where / group by / having 中直接使用
-- ============================================================================
drop table if exists test_pr_invalid_ctx;
create table test_pr_invalid_ctx (id int, val int);
insert into test_pr_invalid_ctx values (1,10),(2,20),(3,30);

-- 以下语句应报错（窗口函数不能在 where 中使用）
-- select * from test_pr_invalid_ctx where percent_rank() over (order by val) > 0.5;

-- 正确做法：用子查询或 cte
select * from (
    select id, val, percent_rank() over (order by val) as pct_rank
    from test_pr_invalid_ctx
) t where pct_rank > 0.5;
-- 预期: 返回 val=30 的行 (pct_rank=1.0)

drop table if exists test_pr_invalid_ctx;

-- ============================================================================
-- test 8.4: percent_rank 与 rows/range 窗口帧（验证是否忽略）
-- percent_rank 是排名函数，窗口帧对其无效
-- ============================================================================
drop table if exists test_pr_frame;
create table test_pr_frame (id int, val int);
insert into test_pr_frame values (1,10),(2,20),(3,30),(4,40),(5,50);

-- 带 rows 帧的 percent_rank（应忽略帧或报错）
-- select val, percent_rank() over (order by val rows between 1 preceding and 1 following) as pct_rank
-- from test_pr_frame order by val;

-- 不带帧的正常查询
select val, percent_rank() over (order by val) as pct_rank
from test_pr_frame order by val;

drop table if exists test_pr_frame;

-- ============================================================================
-- test 8.5: partition by 使用表达式
-- ============================================================================
drop table if exists test_pr_expr_partition;
create table test_pr_expr_partition (id int, val int, category varchar(20));
insert into test_pr_expr_partition values
(1, 10, 'type_a'), (2, 20, 'type_a'), (3, 30, 'type_b'),
(4, 40, 'type_b'), (5, 50, 'type_a');

select id, val, category,
    percent_rank() over (partition by substring(category, 1, 4) order by val) as pct_rank
from test_pr_expr_partition order by category, val;
-- 所有 category 都以 'type' 开头，所以全部在同一分区

drop table if exists test_pr_expr_partition;

-- ============================================================================
-- test 8.6: order by 使用表达式
-- ============================================================================
drop table if exists test_pr_expr_orderby;
create table test_pr_expr_orderby (id int, val int);
insert into test_pr_expr_orderby values (1,5),(2,-3),(3,8),(4,-1),(5,0);

select id, val,
    percent_rank() over (order by abs(val)) as pct_rank_abs,
    percent_rank() over (order by val * val) as pct_rank_sq
from test_pr_expr_orderby order by abs(val);
-- 按绝对值排序: 0→0, -1→0.25, -3→0.5, 5→0.75, 8→1.0

drop table if exists test_pr_expr_orderby;

-- ============================================================================
-- test 8.7: order by 使用 case when 表达式
-- ============================================================================
drop table if exists test_pr_case_orderby;
create table test_pr_case_orderby (id int, status varchar(20), priority int);
insert into test_pr_case_orderby values
(1, 'critical', 1), (2, 'high', 2), (3, 'medium', 3),
(4, 'low', 4), (5, 'critical', 1);

select id, status, priority,
    percent_rank() over (order by
        case status
            when 'critical' then 1
            when 'high' then 2
            when 'medium' then 3
            when 'low' then 4
        end
    ) as pct_rank
from test_pr_case_orderby order by priority;
-- 预期: 两个 critical 并列→0, high→0.5, medium→0.75, low→1.0

drop table if exists test_pr_case_orderby;

-- ############################################################################
-- 第九部分: 事务与并发场景
-- ############################################################################

-- ============================================================================
-- test 9.1: 事务中使用 percent_rank
-- ============================================================================
drop table if exists test_pr_transaction;
create table test_pr_transaction (id int primary key, val int);

start transaction;
insert into test_pr_transaction values (1, 10), (2, 20), (3, 30);

select val, percent_rank() over (order by val) as pct_rank
from test_pr_transaction order by val;
-- 预期: 0, 0.5, 1.0

commit;

-- 事务提交后再查
select val, percent_rank() over (order by val) as pct_rank
from test_pr_transaction order by val;
-- 预期: 同上

drop table if exists test_pr_transaction;

-- ============================================================================
-- test 9.2: 事务回滚后 percent_rank
-- ============================================================================
drop table if exists test_pr_rollback;
create table test_pr_rollback (id int primary key, val int);
insert into test_pr_rollback values (1, 10), (2, 20);

start transaction;
insert into test_pr_rollback values (3, 15);

select val, percent_rank() over (order by val) as pct_rank
from test_pr_rollback order by val;
-- 预期: 10→0, 15→0.5, 20→1.0 (3行)

rollback;

select val, percent_rank() over (order by val) as pct_rank
from test_pr_rollback order by val;
-- 预期: 10→0, 20→1.0 (回滚后只有2行)

drop table if exists test_pr_rollback;

-- ############################################################################
-- 第十部分: 复杂业务场景
-- ############################################################################

-- ============================================================================
-- test 10.1: 学生成绩百分位排名
-- ============================================================================
drop table if exists test_pr_student;
create table test_pr_student (
    student_id int primary key auto_increment,
    name varchar(50) not null,
    subject varchar(30) not null,
    score decimal(5,2) not null,
    index idx_subject_score (subject, score)
);
insert into test_pr_student (name, subject, score) values
('alice', 'math', 95), ('bob', 'math', 87), ('carol', 'math', 92),
('david', 'math', 78), ('eve', 'math', 95),
('alice', 'english', 88), ('bob', 'english', 92), ('carol', 'english', 85),
('david', 'english', 90), ('eve', 'english', 88);

-- 每科的百分位排名
select name, subject, score,
    percent_rank() over (partition by subject order by score) as pct_rank
from test_pr_student order by subject, score;
-- 验证: math 中 alice 和 eve 都是 95 分，应该并列

-- 找出每科排名前 25% 的学生
with ranked as (
    select name, subject, score,
        percent_rank() over (partition by subject order by score) as pct_rank
    from test_pr_student
)
select * from ranked where pct_rank >= 0.75 order by subject, score desc;

drop table if exists test_pr_student;

-- ============================================================================
-- test 10.2: 销售数据分析 - 多维度百分位
-- ============================================================================
drop table if exists test_pr_sales;
create table test_pr_sales (
    sale_id int primary key auto_increment,
    region varchar(20) not null,
    product varchar(30) not null,
    amount decimal(12,2) not null,
    sale_date date not null,
    index idx_region (region),
    index idx_date (sale_date)
);
insert into test_pr_sales (region, product, amount, sale_date) values
('east', 'laptop', 1200.00, '2024-01-15'),
('east', 'phone', 800.00, '2024-01-20'),
('east', 'tablet', 500.00, '2024-02-10'),
('west', 'laptop', 1500.00, '2024-01-18'),
('west', 'phone', 600.00, '2024-02-05'),
('west', 'tablet', 900.00, '2024-02-15'),
('east', 'laptop', 1100.00, '2024-03-01'),
('west', 'phone', 750.00, '2024-03-10');

-- 全局销售额百分位
select sale_id, region, product, amount,
    percent_rank() over (order by amount) as global_pct
from test_pr_sales order by amount;

-- 按区域的销售额百分位
select sale_id, region, product, amount,
    percent_rank() over (partition by region order by amount) as region_pct
from test_pr_sales order by region, amount;

-- 按产品的销售额百分位
select sale_id, region, product, amount,
    percent_rank() over (partition by product order by amount) as product_pct
from test_pr_sales order by product, amount;

-- 多维度同时计算
select sale_id, region, product, amount,
    percent_rank() over (order by amount) as global_pct,
    percent_rank() over (partition by region order by amount) as region_pct,
    percent_rank() over (partition by product order by amount) as product_pct
from test_pr_sales order by amount;

drop table if exists test_pr_sales;

-- ============================================================================
-- test 10.3: 时间序列数据的百分位分析
-- ============================================================================
drop table if exists test_pr_timeseries;
create table test_pr_timeseries (
    ts datetime not null,
    metric_name varchar(30) not null,
    value double not null
);
insert into test_pr_timeseries values
('2024-01-01 00:00:00', 'cpu', 45.2),
('2024-01-01 01:00:00', 'cpu', 62.8),
('2024-01-01 02:00:00', 'cpu', 78.5),
('2024-01-01 03:00:00', 'cpu', 35.1),
('2024-01-01 04:00:00', 'cpu', 91.3),
('2024-01-01 00:00:00', 'mem', 55.0),
('2024-01-01 01:00:00', 'mem', 58.2),
('2024-01-01 02:00:00', 'mem', 72.1),
('2024-01-01 03:00:00', 'mem', 68.9),
('2024-01-01 04:00:00', 'mem', 80.5);

-- 找出各指标中异常高的值（百分位 > 0.8）
with ranked as (
    select ts, metric_name, value,
        percent_rank() over (partition by metric_name order by value) as pct_rank
    from test_pr_timeseries
)
select * from ranked where pct_rank > 0.8;
-- 预期: cpu=91.3 和 mem=80.5

drop table if exists test_pr_timeseries;

-- ============================================================================
-- test 10.4: 薪资分析 - 综合所有约束类型
-- ============================================================================
drop table if exists test_pr_salary_analysis;
create table test_pr_salary_analysis (
    emp_id bigint primary key auto_increment,
    emp_name varchar(50) not null,
    department varchar(30) not null,
    title varchar(50) default 'staff',
    salary decimal(12,2) not null,
    hire_date date not null,
    is_active boolean default true,
    performance_rating tinyint unsigned,
    unique index idx_name_dept (emp_name, department),
    index idx_salary (salary),
    index idx_hire_date (hire_date)
);

insert into test_pr_salary_analysis (emp_name, department, title, salary, hire_date, is_active, performance_rating) values
('alice', 'engineering', 'senior', 120000.00, '2018-03-15', true, 5),
('bob', 'engineering', 'junior', 75000.00, '2022-06-01', true, 3),
('carol', 'engineering', 'staff', 95000.00, '2020-01-10', true, 4),
('david', 'engineering', 'lead', 140000.00, '2016-09-20', true, 5),
('eve', 'sales', 'senior', 90000.00, '2019-04-05', true, 4),
('frank', 'sales', 'junior', 55000.00, '2023-01-15', true, 2),
('grace', 'sales', 'staff', 70000.00, '2021-07-20', false, 3),
('hank', 'hr', 'senior', 85000.00, '2017-11-01', true, 4),
('ivy', 'hr', 'staff', 65000.00, '2022-03-10', true, 3),
('jack', 'hr', 'junior', 50000.00, '2024-01-05', true, null);

-- 全公司薪资百分位
select emp_name, department, salary,
    percent_rank() over (order by salary) as company_pct
from test_pr_salary_analysis
order by salary;

-- 部门内薪资百分位
select emp_name, department, salary,
    percent_rank() over (partition by department order by salary) as dept_pct
from test_pr_salary_analysis
order by department, salary;

-- 按职级分区
select emp_name, title, salary,
    percent_rank() over (partition by title order by salary) as title_pct
from test_pr_salary_analysis
order by title, salary;

-- 按入职年份分区
select emp_name, hire_date, salary,
    percent_rank() over (partition by year(hire_date) order by salary) as year_pct
from test_pr_salary_analysis
order by year(hire_date), salary;

-- 只看在职员工
select emp_name, department, salary,
    percent_rank() over (partition by department order by salary) as dept_pct
from test_pr_salary_analysis
where is_active = true
order by department, salary;

-- 按绩效评分排序（含 null）
select emp_name, performance_rating,
    percent_rank() over (order by performance_rating) as perf_pct
from test_pr_salary_analysis
order by performance_rating;

-- 综合排名：多个 percent_rank 同时计算
select
    emp_name, department, title, salary, hire_date,
    percent_rank() over (order by salary) as salary_pct,
    percent_rank() over (order by hire_date) as seniority_pct,
    percent_rank() over (partition by department order by salary) as dept_salary_pct
from test_pr_salary_analysis
order by salary;

drop table if exists test_pr_salary_analysis;


-- ============================================
-- MO 的 percent_rank 窗口函数不支持非数值/时间类型的 ORDER BY
-- ============================================
select
    department,
    employee,
    salary,
    percent_rank() over (order by department, salary) as pct_rank
from test_pr_basic
order by department, salary;


select val, percent_rank() over (order by val) as pct_rank from test_pr_char order by val;
-- 预期: apple→0, banana→0.3333, cherry→0.6667, date→1.0


select val, percent_rank() over (order by val) as pct_rank from test_pr_varchar order by val;
-- 预期: aaa→0, aab→0.3333, bbb→0.6667, zzz→1.0


select val, percent_rank() over (order by val) as pct_rank from test_pr_varchar_cn order by val;


select val, percent_rank() over (order by val) as pct_rank from test_pr_text order by val;

-- @bvt:issue#23863
select id, percent_rank() over (order by val) as pct_rank from test_pr_blob order by val;

select id, percent_rank() over (order by val) as pct_rank from test_pr_binary order by val;

select id, percent_rank() over (order by val) as pct_rank from test_pr_varbinary order by val;
-- @bvt:issue

select val, percent_rank() over (order by val) as pct_rank from test_pr_bool order by val;
-- 预期: false(0)→0, false(0)→0, true(1)→0.6667, true(1)→0.6667
-- 因为 (rank=3-1)/(4-1) = 2/3


select val, percent_rank() over (order by val) as pct_rank from test_pr_enum order by val;
-- 注意: enum 内部按定义顺序的整数值排序

-- 预期: priority=1→0, 5,5,5→0.25(并列), 10→1.0

select id, status, percent_rank() over (order by status) as pct_rank
from test_pr_default order by status;


select id, val, percent_rank() over (order by val) as pct_rank
from test_pr_empty_str order by val, id;
-- 预期: 两个空字符串并列排第一


select val, percent_rank() over (order by val) as pct_rank
from test_pr_special_char order by val;

insert into test_pr_ts_boundary values
(1, '0001-01-01 00:00:00'),
(2, '9999-12-31 23:59:59');


select id, length(val) as len, percent_rank() over (order by val) as pct_rank
from test_pr_long_str order by val;

select col_varchar, percent_rank() over (order by col_varchar) as pr from temp_pr_all_types;

select col_bool, percent_rank() over (order by col_bool) as pr from temp_pr_all_types;

select col_enum, percent_rank() over (order by col_enum) as pr from temp_pr_all_types;


-- ============================================================================
-- test a.1: uuid 类型
-- ============================================================================
drop table if exists test_pr_uuid;
create table test_pr_uuid (id int, val uuid);
insert into test_pr_uuid values
(1, '00000000-0000-0000-0000-000000000001'),
(2, '550e8400-e29b-41d4-a716-446655440000'),
(3, 'ffffffff-ffff-ffff-ffff-ffffffffffff');

select val, percent_rank() over (order by val) as pct_rank from test_pr_uuid order by val;
-- 预期: 0, 0.5, 1.0

drop table if exists test_pr_uuid;

-- ============================================================================
-- test a.2: uuid 带 null
-- ============================================================================
drop table if exists test_pr_uuid_null;
create table test_pr_uuid_null (id int, val uuid);
insert into test_pr_uuid_null values
(1, null),
(2, '00000000-0000-0000-0000-000000000001'),
(3, 'ffffffff-ffff-ffff-ffff-ffffffffffff'),
(4, null);

select id, val, percent_rank() over (order by val) as pct_rank
from test_pr_uuid_null order by val;
-- 预期: 两个 null 并列→0, 然后 0.6667, 1.0

drop table if exists test_pr_uuid_null;

-- ============================================================================
-- test a.3: uuid 分区
-- ============================================================================
drop table if exists test_pr_uuid_part;
create table test_pr_uuid_part (grp int, val uuid);
insert into test_pr_uuid_part values
(1, '00000000-0000-0000-0000-000000000001'),
(1, '550e8400-e29b-41d4-a716-446655440000'),
(1, 'ffffffff-ffff-ffff-ffff-ffffffffffff'),
(2, '11111111-1111-1111-1111-111111111111'),
(2, '22222222-2222-2222-2222-222222222222');

select grp, val,
    percent_rank() over (partition by grp order by val) as pct_rank
from test_pr_uuid_part order by grp, val;
-- 预期: grp=1: 0, 0.5, 1.0; grp=2: 0, 1.0

drop table if exists test_pr_uuid_part;

-- ============================================================================
-- test a.4: vecf32 向量类型 (order by 不支持向量列，使用 id 排序)
-- ============================================================================
drop table if exists test_pr_vecf32;
create table test_pr_vecf32 (id int, val vecf32(3));
insert into test_pr_vecf32 values (1, '[1,2,3]'), (2, '[4,5,6]'), (3, '[7,8,9]');

select id, val, percent_rank() over (order by id) as pct_rank from test_pr_vecf32 order by id;
-- 预期: 0, 0.5, 1.0

drop table if exists test_pr_vecf32;

-- ============================================================================
-- test a.5: vecf64 向量类型
-- ============================================================================
drop table if exists test_pr_vecf64;
create table test_pr_vecf64 (id int, val vecf64(2));
insert into test_pr_vecf64 values (1, '[1.1,2.2]'), (2, '[3.3,4.4]'), (3, '[5.5,6.6]');

select id, val, percent_rank() over (order by id) as pct_rank from test_pr_vecf64 order by id;
-- 预期: 0, 0.5, 1.0

drop table if exists test_pr_vecf64;

-- ============================================================================
-- test a.6: 向量类型 partition by 其他列
-- ============================================================================
drop table if exists test_pr_vec_partition;
create table test_pr_vec_partition (id int, grp varchar(10), val vecf32(3), score int);
insert into test_pr_vec_partition values
(1, 'a', '[1,0,0]', 10), (2, 'a', '[0,1,0]', 20), (3, 'a', '[0,0,1]', 30),
(4, 'b', '[1,1,0]', 15), (5, 'b', '[0,1,1]', 25);

select id, grp, val, score,
    percent_rank() over (partition by grp order by score) as pct_rank
from test_pr_vec_partition order by grp, score;
-- 预期: a: 0, 0.5, 1.0; b: 0, 1.0

drop table if exists test_pr_vec_partition;

-- ============================================================================
-- test a.7: 临时表 + uuid/向量类型
-- ============================================================================
create temporary table temp_pr_mo_types (
    id int,
    col_uuid uuid,
    col_vec vecf32(3),
    score int
);
insert into temp_pr_mo_types values
(1, '00000000-0000-0000-0000-000000000001', '[1,2,3]', 100),
(2, '550e8400-e29b-41d4-a716-446655440000', '[4,5,6]', 200),
(3, 'ffffffff-ffff-ffff-ffff-ffffffffffff', '[7,8,9]', 300);

-- uuid 列排序
select id, col_uuid, percent_rank() over (order by col_uuid) as pct_rank
from temp_pr_mo_types order by col_uuid;
-- 预期: 0, 0.5, 1.0

-- 向量表中按 score 排序
select id, col_vec, score, percent_rank() over (order by score) as pct_rank
from temp_pr_mo_types order by score;
-- 预期: 0, 0.5, 1.0

drop table temp_pr_mo_types;
drop database if exists percent_rank_func;

