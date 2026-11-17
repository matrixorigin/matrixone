drop database if exists tmp_alias_binding;
create database tmp_alias_binding;
use tmp_alias_binding;

-- base temp table projection/filter should resolve via alias
create temporary table t1(a int, b int);
insert into t1 values (1, 2), (3, 4);
select t1.a from t1 where t1.b > 2;

-- derived table keeps alias while underlying uses real name
select x.a from (select * from t1) as x where x.b > 2;

-- join path also uses alias mapping
create table t2(a int, b int);
insert into t2 values (3, 5);
select t1.a, t2.b from t1 join t2 on t1.a = t2.a where t1.b > 2;

-- aggregation and group/order with alias
select count(*), sum(b) from t1;
select a, b from t1 where b > 1 order by b desc;

-- subquery uses alias mapping
select a from t1 where a in (select a from t1 where b > 2);

-- self join through alias
select l.a, r.b from t1 as l join t1 as r on l.a = r.a order by l.a;

-- cte on temp table
with c as (select * from t1 where b > 2)
select a from c;

-- 已保存的 VIEW 正文不能使用调用者的临时表别名。
create view v(c) as select b + 1 from t2;
create view qualified_v as select b from tmp_alias_binding.t2;
create view nested_v as select c from v;
prepare read_view from 'select c from tmp_alias_binding.nested_v order by c';
execute read_view;
create temporary table t2(a int, b bigint);
select count(*) from t2;
select c from v order by c;
select b from qualified_v order by b;
execute read_view;
insert into t2 values (10, 90);
select b from t2;
select v.c, t2.b from v cross join t2;
select t2.b, v.c from t2 cross join v;
select c from nested_v order by c;
execute read_view;
desc qualified_v;
show columns from nested_v;
select table_name, column_name, data_type from information_schema.columns
where table_schema = 'tmp_alias_binding' and table_name in ('v', 'qualified_v', 'nested_v')
order by table_name, ordinal_position;
create table view_copy as select b from qualified_v;
desc view_copy;
select b from view_copy;

-- 创建定义时直接引用临时来源仍然必须拒绝；展开已有 VIEW 不应误拒绝。
-- @regex(".*temporary table.*",true)
create view invalid_v as select b from t2;
-- @regex(".*temporary table.*",true)
alter view qualified_v as select b from t2;
create view through_view as select b from qualified_v;
select b from through_view;
select b from t2;

-- 另一会话始终读取永久来源，源类型变化仍需被当前 VIEW 推导看见。
-- @session:id=1{
select b from tmp_alias_binding.t2;
select c from tmp_alias_binding.nested_v;
alter table tmp_alias_binding.t2 modify column b smallint;
-- @session}
desc qualified_v;
select b from qualified_v;
select b from t2;

-- 永久依赖消失不能退而读取临时来源，失败也不能污染外层名称解析。
-- @session:id=1{
drop table tmp_alias_binding.t2;
-- @session}
-- @regex(".*does not exist.*",true)
select c from v;
select b from t2;
-- @session:id=1{
create table tmp_alias_binding.t2(a int, b smallint);
insert into tmp_alias_binding.t2 values (3, 5);
-- @session}
execute read_view;
drop temporary table t2;
execute read_view;
select b from t2;
deallocate prepare read_view;

-- 同名临时表也不能替换持久 VIEW 的中间 VIEW 依赖。
create temporary table v(c bigint);
insert into v values (99);
select c from v;
select c from nested_v;
drop temporary table v;
select c from nested_v;

drop database if exists tmp_alias_binding;

