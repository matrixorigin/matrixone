-- ============================================================================
-- NTILE(N) 窗口函数 完整测试用例
-- NTILE(N) 将分区内的行分成 N 个桶，为每行分配桶编号 (1 到 N)
-- 如果行数不能均分，前面的桶多分一行
-- 适用数据库: matrixone
-- issue: https://github.com/matrixorigin/matrixone/issues/23017
-- ============================================================================

-- ############################################################################
-- 第一部分: 基础功能测试
-- ############################################################################

-- ============================================================================
-- test 1.1: 基本用法 - NTILE(2) 分成两组
-- ============================================================================
drop database if exists ntile_func;
create database ntile_func;
use ntile_func;
drop table if exists test_ntile_basic;
create table test_ntile_basic (
    id int primary key,
    department varchar(50),
    employee varchar(50),
    salary decimal(10, 2)
);

insert into test_ntile_basic values
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
    ntile(2) over (order by salary) as bucket_2
from test_ntile_basic
order by salary;
-- 预期: 10行分2组，前5行=1，后5行=2

-- ============================================================================
-- test 1.2: NTILE(3) 分成三组
-- ============================================================================
select
    employee,
    salary,
    ntile(3) over (order by salary) as bucket_3
from test_ntile_basic
order by salary;
-- 预期: 10行分3组 -> 4+3+3，前4行=1，中3行=2，后3行=3

-- ============================================================================
-- test 1.3: NTILE(4) 分成四组
-- ============================================================================
select
    employee,
    salary,
    ntile(4) over (order by salary) as bucket_4
from test_ntile_basic
order by salary;
-- 预期: 10行分4组 -> 3+3+2+2

-- ============================================================================
-- test 1.4: NTILE(5) 均分
-- ============================================================================
select
    employee,
    salary,
    ntile(5) over (order by salary) as bucket_5
from test_ntile_basic
order by salary;
-- 预期: 10行分5组，每组2行

-- ============================================================================
-- test 1.5: NTILE(10) 每行一组
-- ============================================================================
select
    employee,
    salary,
    ntile(10) over (order by salary) as bucket_10
from test_ntile_basic
order by salary;
-- 预期: 每行一个桶，1到10

-- ============================================================================
-- test 1.6: NTILE(1) 所有行同一组
-- ============================================================================
select
    employee,
    salary,
    ntile(1) over (order by salary) as bucket_1
from test_ntile_basic
order by salary;
-- 预期: 所有行都是 1

-- ============================================================================
-- test 1.7: NTILE(N) N > 行数
-- ============================================================================
select
    employee,
    salary,
    ntile(20) over (order by salary) as bucket_20
from test_ntile_basic
order by salary;
-- 预期: 10行分20组，每行一个桶(1-10)，桶11-20为空

-- ============================================================================
-- test 1.8: partition by - 按部门分区
-- ============================================================================
select
    department,
    employee,
    salary,
    ntile(2) over (partition by department order by salary) as bucket
from test_ntile_basic
order by department, salary;
-- 预期: 每个部门内独立分桶

-- ============================================================================
-- test 1.9: desc 降序排列
-- ============================================================================
select
    employee,
    salary,
    ntile(3) over (order by salary desc) as bucket_desc
from test_ntile_basic
order by salary desc;

-- ============================================================================
-- test 1.10: 多列 partition by
-- ============================================================================
drop table if exists test_ntile_multi_part;
create table test_ntile_multi_part (
    region varchar(20),
    department varchar(20),
    employee varchar(20),
    salary int
);
insert into test_ntile_multi_part values
('east', 'it', 'a', 100), ('east', 'it', 'b', 200), ('east', 'it', 'c', 300),
('east', 'hr', 'd', 150), ('east', 'hr', 'e', 250),
('west', 'it', 'f', 120), ('west', 'it', 'g', 180);

select
    region, department, employee, salary,
    ntile(2) over (partition by region, department order by salary) as bucket
from test_ntile_multi_part
order by region, department, salary;

drop table if exists test_ntile_multi_part;

-- ============================================================================
-- test 1.11: 多个 ntile 在同一查询中
-- ============================================================================
select
    employee,
    salary,
    ntile(2) over (order by salary) as bucket_2,
    ntile(3) over (order by salary) as bucket_3,
    ntile(4) over (order by salary) as bucket_4
from test_ntile_basic
order by salary;

drop table if exists test_ntile_basic;

-- ############################################################################
-- 第二部分: 边界条件测试
-- ############################################################################

-- ============================================================================
-- test 2.1: 分区只有一行
-- ============================================================================
drop table if exists test_ntile_single;
create table test_ntile_single (id int, val int);
insert into test_ntile_single values (1, 100);

select val, ntile(1) over (order by val) as b1,
       ntile(3) over (order by val) as b3,
       ntile(100) over (order by val) as b100
from test_ntile_single;
-- 预期: 全部为 1

drop table if exists test_ntile_single;

-- ============================================================================
-- test 2.2: 所有值相同
-- ============================================================================
drop table if exists test_ntile_all_same;
create table test_ntile_all_same (id int, val int);
insert into test_ntile_all_same values (1, 50), (2, 50), (3, 50), (4, 50), (5, 50), (6, 50);

select id, val, ntile(3) over (order by val) as bucket
from test_ntile_all_same order by id;
-- 预期: 6行分3组 -> 2+2+2，id 1,2=1, id 3,4=2, id 5,6=3

drop table if exists test_ntile_all_same;

-- ============================================================================
-- test 2.3: 空表
-- ============================================================================
drop table if exists test_ntile_empty;
create table test_ntile_empty (id int, val int);

select val, ntile(3) over (order by val) as bucket from test_ntile_empty;
-- 预期: 0 行

drop table if exists test_ntile_empty;

-- ============================================================================
-- test 2.4: 两行数据
-- ============================================================================
drop table if exists test_ntile_two;
create table test_ntile_two (id int, val int);
insert into test_ntile_two values (1, 10), (2, 20);

select val, ntile(2) over (order by val) as b2,
       ntile(3) over (order by val) as b3
from test_ntile_two;
-- 预期: ntile(2): 10→1, 20→2; ntile(3): 10→1, 20→2

drop table if exists test_ntile_two;

-- ============================================================================
-- test 2.5: null 值处理
-- ============================================================================
drop table if exists test_ntile_null;
create table test_ntile_null (id int, val int);
insert into test_ntile_null values (1, null), (2, 10), (3, 20), (4, null), (5, 30);

select id, val, ntile(3) over (order by val) as bucket
from test_ntile_null order by val;
-- 预期: null 排在前面，5行分3组 -> 2+2+1

drop table if exists test_ntile_null;

-- ============================================================================
-- test 2.6: 全部为 null
-- ============================================================================
drop table if exists test_ntile_all_null;
create table test_ntile_all_null (id int, val int);
insert into test_ntile_all_null values (1, null), (2, null), (3, null);

select id, val, ntile(2) over (order by val) as bucket
from test_ntile_all_null order by id;
-- 预期: 3行分2组 -> 2+1

drop table if exists test_ntile_all_null;

-- ============================================================================
-- test 2.7: 大数据量（100行）
-- ============================================================================
drop table if exists test_ntile_large;
create table test_ntile_large (id int, val int);
insert into test_ntile_large values
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

-- ntile(4): 100行分4组 -> 25+25+25+25
select id, val, ntile(4) over (order by val) as bucket
from test_ntile_large
where id in (1, 25, 26, 50, 51, 75, 76, 100)
order by val;
-- 预期: 1→1, 25→1, 26→2, 50→2, 51→3, 75→3, 76→4, 100→4

-- ntile(3): 100行分3组 -> 34+33+33
select id, val, ntile(3) over (order by val) as bucket
from test_ntile_large
where id in (1, 34, 35, 67, 68, 100)
order by val;

-- 验证桶编号范围
select min(bucket) as min_b, max(bucket) as max_b
from (
    select ntile(4) over (order by val) as bucket from test_ntile_large
) t;
-- 预期: min=1, max=4

drop table if exists test_ntile_large;

-- ############################################################################
-- 第三部分: 所有 mo 支持的列类型上使用 ntile
-- ############################################################################

-- ============================================================================
-- test 3.1: 整数类型 - tinyint
-- ============================================================================
drop table if exists test_ntile_tinyint;
create table test_ntile_tinyint (id int, val tinyint);
insert into test_ntile_tinyint values (1, -128), (2, -1), (3, 0), (4, 1), (5, 127);

select val, ntile(3) over (order by val) as bucket from test_ntile_tinyint order by val;
-- 预期: 5行分3组 -> 2+2+1

drop table if exists test_ntile_tinyint;

-- ============================================================================
-- test 3.2: 整数类型 - smallint
-- ============================================================================
drop table if exists test_ntile_smallint;
create table test_ntile_smallint (id int, val smallint);
insert into test_ntile_smallint values (1, -32768), (2, 0), (3, 32767);

select val, ntile(2) over (order by val) as bucket from test_ntile_smallint order by val;
-- 预期: 3行分2组 -> 2+1

drop table if exists test_ntile_smallint;

-- ============================================================================
-- test 3.3: 整数类型 - int
-- ============================================================================
drop table if exists test_ntile_int;
create table test_ntile_int (id int, val int);
insert into test_ntile_int values (1, -2147483648), (2, 0), (3, 2147483647);

select val, ntile(2) over (order by val) as bucket from test_ntile_int order by val;

drop table if exists test_ntile_int;

-- ============================================================================
-- test 3.4: 整数类型 - bigint
-- ============================================================================
drop table if exists test_ntile_bigint;
create table test_ntile_bigint (id int, val bigint);
insert into test_ntile_bigint values (1, -9223372036854775808), (2, 0), (3, 9223372036854775807);

select val, ntile(2) over (order by val) as bucket from test_ntile_bigint order by val;

drop table if exists test_ntile_bigint;

-- ============================================================================
-- test 3.5: 整数类型 - bigint unsigned
-- ============================================================================
drop table if exists test_ntile_bigint_u;
create table test_ntile_bigint_u (id int, val bigint unsigned);
insert into test_ntile_bigint_u values (1, 0), (2, 9223372036854775808), (3, 18446744073709551615);

select val, ntile(2) over (order by val) as bucket from test_ntile_bigint_u order by val;

drop table if exists test_ntile_bigint_u;

-- ============================================================================
-- test 3.6: 浮点类型 - float
-- ============================================================================
drop table if exists test_ntile_float;
create table test_ntile_float (id int, val float);
insert into test_ntile_float values (1, -3.14), (2, 0), (3, 1.5), (4, 3.14), (5, 99.99);

select val, ntile(3) over (order by val) as bucket from test_ntile_float order by val;

drop table if exists test_ntile_float;

-- ============================================================================
-- test 3.7: 浮点类型 - double
-- ============================================================================
drop table if exists test_ntile_double;
create table test_ntile_double (id int, val double);
insert into test_ntile_double values (1, -1.79769e+308), (2, 0), (3, 1.79769e+308);

select val, ntile(2) over (order by val) as bucket from test_ntile_double order by val;

drop table if exists test_ntile_double;

-- ============================================================================
-- test 3.8: decimal 类型
-- ============================================================================
drop table if exists test_ntile_decimal;
create table test_ntile_decimal (id int, val decimal(18, 6));
insert into test_ntile_decimal values
(1, -999999.999999), (2, 0.000001), (3, 123456.789012), (4, 999999.999999);

select val, ntile(2) over (order by val) as bucket from test_ntile_decimal order by val;

drop table if exists test_ntile_decimal;

-- ============================================================================
-- test 3.9: char 类型
-- ============================================================================
drop table if exists test_ntile_char;
create table test_ntile_char (id int, val char(10));
insert into test_ntile_char values (1, 'apple'), (2, 'banana'), (3, 'cherry'), (4, 'date');

select val, ntile(2) over (order by val) as bucket from test_ntile_char order by val;

drop table if exists test_ntile_char;

-- ============================================================================
-- test 3.10: varchar 类型
-- ============================================================================
drop table if exists test_ntile_varchar;
create table test_ntile_varchar (id int, val varchar(255));
insert into test_ntile_varchar values (1, 'aaa'), (2, 'aab'), (3, 'bbb'), (4, 'zzz');

select val, ntile(2) over (order by val) as bucket from test_ntile_varchar order by val;

drop table if exists test_ntile_varchar;

-- ============================================================================
-- test 3.11: text 类型
-- ============================================================================
drop table if exists test_ntile_text;
create table test_ntile_text (id int, val text);
insert into test_ntile_text values (1, 'short'), (2, 'medium length text'), (3, 'zzz last');

select val, ntile(2) over (order by val) as bucket from test_ntile_text order by val;

drop table if exists test_ntile_text;

-- ============================================================================
-- test 3.12: date 类型
-- ============================================================================
drop table if exists test_ntile_date;
create table test_ntile_date (id int, val date);
insert into test_ntile_date values
(1, '2020-01-01'), (2, '2022-06-15'), (3, '2024-12-31'), (4, '2025-01-01');

select val, ntile(2) over (order by val) as bucket from test_ntile_date order by val;

drop table if exists test_ntile_date;

-- ============================================================================
-- test 3.13: datetime 类型
-- ============================================================================
drop table if exists test_ntile_datetime;
create table test_ntile_datetime (id int, val datetime);
insert into test_ntile_datetime values
(1, '2024-01-01 00:00:00'),
(2, '2024-01-01 00:00:01'),
(3, '2024-06-15 12:30:45'),
(4, '2024-12-31 23:59:59');

select val, ntile(2) over (order by val) as bucket from test_ntile_datetime order by val;

drop table if exists test_ntile_datetime;

-- ============================================================================
-- test 3.14: timestamp 类型
-- ============================================================================
drop table if exists test_ntile_timestamp;
create table test_ntile_timestamp (id int, val timestamp);
insert into test_ntile_timestamp values
(1, '2020-01-01 00:00:00'),
(2, '2022-06-15 12:00:00'),
(3, '2025-12-31 23:59:59');

select val, ntile(2) over (order by val) as bucket from test_ntile_timestamp order by val;

drop table if exists test_ntile_timestamp;

-- ============================================================================
-- test 3.15: time 类型
-- ============================================================================
drop table if exists test_ntile_time;
create table test_ntile_time (id int, val time);
insert into test_ntile_time values
(1, '00:00:00'), (2, '08:30:00'), (3, '12:00:00'), (4, '23:59:59');

select val, ntile(2) over (order by val) as bucket from test_ntile_time order by val;

drop table if exists test_ntile_time;

-- ============================================================================
-- test 3.16: blob 类型
-- ============================================================================
-- @bvt:issue#23863
drop table if exists test_ntile_blob;
create table test_ntile_blob (id int, val blob);
insert into test_ntile_blob values (1, 'abc'), (2, 'def'), (3, 'xyz');

select id, ntile(2) over (order by val) as bucket from test_ntile_blob order by val;

drop table if exists test_ntile_blob;
-- @bvt:issue
-- ============================================================================
-- test 3.17: enum 类型
-- ============================================================================
-- @bvt:issue#23863
drop table if exists test_ntile_enum;
create table test_ntile_enum (id int, val enum('low', 'medium', 'high', 'critical'));
insert into test_ntile_enum values (1, 'low'), (2, 'medium'), (3, 'high'), (4, 'critical');

select val, ntile(2) over (order by val) as bucket from test_ntile_enum order by val;

drop table if exists test_ntile_enum;
-- @bvt:issue
-- ============================================================================
-- test 3.18: json 类型 (order by id)
-- ============================================================================
drop table if exists test_ntile_json;
create table test_ntile_json (id int, val json);
insert into test_ntile_json values (1, '{"a":1}'), (2, '{"a":2}'), (3, '{"a":3}');

select id, val, ntile(2) over (order by id) as bucket from test_ntile_json order by id;

drop table if exists test_ntile_json;

-- ============================================================================
-- test 3.19: boolean 类型
-- ============================================================================
drop table if exists test_ntile_bool;
create table test_ntile_bool (id int, val boolean);
insert into test_ntile_bool values (1, false), (2, false), (3, true), (4, true);

select val, ntile(2) over (order by val) as bucket from test_ntile_bool order by val;

drop table if exists test_ntile_bool;

-- ############################################################################
-- 第四部分: 列约束与 ntile 组合测试
-- ############################################################################

-- ============================================================================
-- test 4.1: primary key 约束
-- ============================================================================
drop table if exists test_ntile_pk;
create table test_ntile_pk (
    id int primary key,
    val int
);
insert into test_ntile_pk values (1, 30), (2, 10), (3, 20), (4, 50), (5, 40);

select id, val, ntile(3) over (order by val) as bucket from test_ntile_pk order by val;
-- 预期: 5行分3组 -> 2+2+1

select id, val, ntile(3) over (order by id) as bucket from test_ntile_pk order by id;

drop table if exists test_ntile_pk;

-- ============================================================================
-- test 4.2: not null 约束
-- ============================================================================
drop table if exists test_ntile_notnull;
create table test_ntile_notnull (
    id int not null,
    name varchar(50) not null,
    score int not null
);
insert into test_ntile_notnull values (1, 'alice', 85), (2, 'bob', 90), (3, 'carol', 75);

select name, score, ntile(2) over (order by score) as bucket
from test_ntile_notnull order by score;

drop table if exists test_ntile_notnull;

-- ============================================================================
-- test 4.3: auto_increment 约束
-- ============================================================================
drop table if exists test_ntile_autoinc;
create table test_ntile_autoinc (
    id bigint primary key auto_increment,
    val varchar(20)
);
insert into test_ntile_autoinc (val) values ('first'), ('second'), ('third'), ('fourth'), ('fifth');

select id, val, ntile(2) over (order by id) as bucket from test_ntile_autoinc order by id;

drop table if exists test_ntile_autoinc;

-- ============================================================================
-- test 4.4: unique index 约束
-- ============================================================================
drop table if exists test_ntile_unique;
create table test_ntile_unique (
    id int primary key,
    email varchar(100) unique,
    score int
);
insert into test_ntile_unique values
(1, 'a@test.com', 80), (2, 'b@test.com', 90),
(3, 'c@test.com', 70), (4, 'd@test.com', 85);

select email, score, ntile(2) over (order by score) as bucket
from test_ntile_unique order by score;

drop table if exists test_ntile_unique;

-- ============================================================================
-- test 4.5: default 约束
-- ============================================================================
drop table if exists test_ntile_default;
create table test_ntile_default (
    id int,
    priority int default 5,
    status varchar(20) default 'active'
);
insert into test_ntile_default (id) values (1), (2), (3);
insert into test_ntile_default values (4, 10, 'inactive'), (5, 1, 'active');

select id, priority, ntile(2) over (order by priority) as bucket
from test_ntile_default order by priority;

drop table if exists test_ntile_default;

-- ############################################################################
-- 第五部分: 高级用法与组合测试
-- ############################################################################

-- ============================================================================
-- test 5.1: ntile 与其他窗口函数组合
-- ============================================================================
drop table if exists test_ntile_combo;
create table test_ntile_combo (id int, val int);
insert into test_ntile_combo values (1,10),(2,20),(3,30),(4,40),(5,50),(6,60);

select
    id, val,
    ntile(3) over (order by val) as bucket,
    rank() over (order by val) as rnk,
    dense_rank() over (order by val) as dense_rnk,
    row_number() over (order by val) as rn,
    percent_rank() over (order by val) as pct_rank
from test_ntile_combo order by val;

drop table if exists test_ntile_combo;

-- ============================================================================
-- test 5.2: ntile 在子查询中
-- ============================================================================
drop table if exists test_ntile_subq;
create table test_ntile_subq (id int, val int);
insert into test_ntile_subq values (1,10),(2,20),(3,30),(4,40),(5,50),(6,60);

select * from (
    select id, val, ntile(3) over (order by val) as bucket
    from test_ntile_subq
) t where bucket = 1;
-- 预期: 只返回第一组的行

drop table if exists test_ntile_subq;

-- ============================================================================
-- test 5.3: ntile 与 where 条件
-- ============================================================================
drop table if exists test_ntile_where;
create table test_ntile_where (id int, department varchar(20), val int);
insert into test_ntile_where values
(1, 'a', 10), (2, 'a', 20), (3, 'a', 30),
(4, 'b', 40), (5, 'b', 50), (6, 'b', 60);

select id, val, ntile(2) over (order by val) as bucket
from test_ntile_where
where department = 'a'
order by val;
-- 预期: 只对 department='a' 的3行分桶

drop table if exists test_ntile_where;

-- ============================================================================
-- test 5.4: ntile 与 join
-- ============================================================================
drop table if exists test_ntile_t1;
drop table if exists test_ntile_t2;
create table test_ntile_t1 (id int, name varchar(20));
create table test_ntile_t2 (id int, score int);
insert into test_ntile_t1 values (1, 'alice'), (2, 'bob'), (3, 'carol'), (4, 'dave');
insert into test_ntile_t2 values (1, 85), (2, 92), (3, 78), (4, 95);

select t1.name, t2.score,
       ntile(2) over (order by t2.score) as bucket
from test_ntile_t1 t1 join test_ntile_t2 t2 on t1.id = t2.id
order by t2.score;

drop table if exists test_ntile_t1;
drop table if exists test_ntile_t2;

-- ============================================================================
-- test 5.5: ntile 与 group by (在外层查询)
-- ============================================================================
drop table if exists test_ntile_group;
create table test_ntile_group (department varchar(20), val int);
insert into test_ntile_group values
('a', 10), ('a', 20), ('a', 30),
('b', 40), ('b', 50),
('c', 60);

select * from (
    select department, sum(val) as total,
           ntile(2) over (order by sum(val)) as bucket
    from test_ntile_group
    group by department
) t order by total;
-- 预期: 3个部门分2组

drop table if exists test_ntile_group;

-- ============================================================================
-- test 5.6: ntile 与 having (在外层查询)
-- ============================================================================
drop table if exists test_ntile_having;
create table test_ntile_having (category varchar(20), amount int);
insert into test_ntile_having values
('x', 10), ('x', 20), ('y', 30), ('y', 40), ('y', 50), ('z', 5);

select * from (
    select category, count(*) as cnt, sum(amount) as total,
           ntile(2) over (order by sum(amount)) as bucket
    from test_ntile_having
    group by category
    having count(*) >= 2
) t order by total;
-- 预期: 只有 x 和 y 满足 having，2行分2组

drop table if exists test_ntile_having;

-- ============================================================================
-- 清理数据库
-- ============================================================================
drop database if exists ntile_func;
