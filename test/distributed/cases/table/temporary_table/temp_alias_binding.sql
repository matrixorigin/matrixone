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

drop database if exists tmp_alias_binding;

