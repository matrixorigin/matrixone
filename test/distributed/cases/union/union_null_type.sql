-- 裸 NULL 只贡献可空性，不把数值公共类型提升为字符串。
drop database if exists union_null_type;
create database union_null_type;
use union_null_type;

-- 普通 SELECT：用 2 和 10 区分数值排序与字典序，同时检查协议类型。
-- @metacmp(true)
select 2 as c union all select 10 union all select null order by c;
-- @metacmp(true)
select null as c union all select 10 union all select 2 order by c;
select c, c+1 as next_value from (select 2 as c union all select null union all select 10) u order by c;
select 2 as c union select null union select 2 order by c;
select null as c union all select 10 union all select 2 order by c limit 2;

-- 嵌套集合运算的全 NULL 分支、括号及分支内排序不能提前固化文本类型。
select 2 as c union all (select null union all select null) order by c;
select 2 as c union all (select null union select null) order by c;
(select null as c union all select null) union all select 2 order by c;
select 2 as c union all (select null as c order by c limit 1) order by c;
select 2 as c union all (select (null) as c order by c limit 1) order by c;
select 2 as c union all (select (null) union select (null)) order by c;
select 2 as c union all select (null) order by c;
with u as (select null as c union all select 10 union all select 2) select c from u order by c;
with u as (select null as c union all select 2) select c from u union all select c from u order by c;
select c,d from (select null as c,2 as d union all select 10,cast(null as char)) u order by c;

-- 重复分支折叠产生的内部 DISTINCT 投影保留来源，真实派生表仍是类型边界。
-- @metacmp(true)
select 2 as c union all select 10 union all (((select null)) union ((select null))) order by c;
(((select null as c)) union ((select null as c))) union all select 2 union all select 10 order by c;
select c,d from (select 2 as c,'a' as d union all (((select null as c,null as d)) union ((select null as c,null as d)))) u order by c;

-- CTAS：实际落表类型、数值运算、NULL 行和 nullable 约束。
create table numeric_result as select 2 as c union all select 10 union all select null;
create table reverse_result as select null as c union all select 10 union all select 2;
create table count_result as select count(*) as c from numeric_result union all select null;
create table reverse_count as select null as c union all select count(*) from numeric_result;
create table nested_result as select 2 as c union all (select null union all select null);
create table folded_result as select 2 as c union all select 10 union all (((select null)) union ((select null)));
create table folded_count as select count(*) as c from numeric_result union all (((select null)) union ((select null)));
create table folded_derived as select 2 as c union all select 10 union all (((select c from (select null as c) d)) union ((select c from (select null as c) d)));
select table_name, data_type, is_nullable from information_schema.columns
where table_schema='union_null_type' and column_name='c' order by table_name;
select c, c+1 as next_value from numeric_result order by c;
select c from reverse_result order by c;
-- @metacmp(true)
select count(*) as c from numeric_result union all select null order by c;
select c from count_result order by c;
select c from reverse_count order by c;
select c from nested_result order by c;
select c,c+1 as next_value from folded_result order by c;
select c from folded_count order by c;
select c from folded_derived order by c;
insert into numeric_result values(null);
select count(*) as all_rows, count(c) as nonnull_rows from numeric_result;

-- 已声明类型的 NULL 和真实字符串必须继续参与公共类型推断。
create table char_result as select 2 as c union all select 10 union all select cast(null as char);
create table reverse_char as select cast(null as char) as c union all select 2 union all select 10;
create table folded_char as select 2 as c union all select 10 union all (((select cast(null as char))) union ((select cast(null as char))));
create table string_result as select 2 as c union all select '10' union all select null;
create table unsigned_result as select cast(2 as unsigned) as c union all select null;
create table decimal_result as select cast(2 as decimal(10,2)) as c union all select null;
select table_name, data_type, is_nullable from information_schema.columns
where table_schema='union_null_type' and column_name='c'
and table_name in ('char_result','reverse_char','folded_char','string_result','unsigned_result','decimal_result') order by table_name;
select c from char_result order by c;
select c from reverse_char order by c;
select c from folded_char order by c;
select c from string_result order by c;
select c, c+1 as next_value from unsigned_result order by c;
select c, c+1 as next_value from decimal_result order by c;

-- 全 NULL 和非集合 SELECT 仍须能执行；共享集合入口不破坏过滤语义。
select null as c;
select null as c union all select null;
select 2 as c intersect select null;
select 2 as c except select null;

drop database union_null_type;
