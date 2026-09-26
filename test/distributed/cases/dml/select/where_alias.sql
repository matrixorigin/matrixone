-- WHERE 别名扩展仅影响显式开启的会话。
drop database if exists where_alias_test;
create database where_alias_test;
use where_alias_test;
create table t(a int, b int);
insert into t values (0, 10), (2, 20), (null, 30);

select @@session.enable_where_alias;
set global enable_where_alias = 1;
set session enable_where_alias = 2;
select a as alias_a, b from t where alias_a > 1;
set session enable_where_alias = 1;
select a as alias_a, b from t where alias_a > 1;
select abs(a) + 1 as k, b from t where k > 1 order by b;
select a as k, b from t where k is null;
select 2 as k where k > 1;

-- 真实列（包括相关外层列）优先，不能用别名掩盖歧义。
select b as a from t where a > 1;
select x.a as a from t x join t y on x.b = y.b where a > 1;
select a from t o where exists (select b as a from t i where a > 1) order by a;
select a, b from t where exists (select 1 as b where b > 15) order by b;
select a as k, b as k from t where b = 20;
select a as k, b as k from t where k > 1;
select a as k from t where t.k > 1;

-- 展开不能绕过 WHERE 的语义限制或跨查询层传播。
select sum(a) as k from t where k > 1;
select row_number() over () as k from t where k > 1;
select a as k, k + 1 as j from t where j > 1;
select a as k from t where exists (select 1 where k > 1);
select (select max(b) from t) as k from t where k = 30 order by a;

-- 改变开关使普通缓存与准备语句失效，重开后可恢复执行。
prepare p from 'select a as k, b from t where k > ? order by b';
set @v = 1;
execute p using @v;
select a as alias_a, b from t where alias_a > 1;
set session enable_where_alias = 0;
select a as alias_a, b from t where alias_a > 1;
execute p using @v;
set session enable_where_alias = 1;
execute p using @v;
deallocate prepare p;
prepare p from 'select cast(? as signed) as k where k > 1';
set @v = 2;
execute p using @v;
set @v = 0;
execute p using @v;
deallocate prepare p;
set session enable_where_alias = 0;
drop database where_alias_test;
