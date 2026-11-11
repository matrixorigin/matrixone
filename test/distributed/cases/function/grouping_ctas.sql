-- 分组扩展的源查询、CTAS、显式 INSERT 必须保留相同的小计和行重数。
drop database if exists grouping_ctas_test;
create database grouping_ctas_test;
use grouping_ctas_test;
create table src(g int not null,h int not null,v int not null);
insert into src values(1,10,5),(1,20,7),(2,10,9);

-- 独立的普通 GROUP BY / UNION ALL 参考结果：三条明细、两条小计、一条总计。
select g,h,sum(v) s from src group by g,h
union all select g,null,sum(v) from src group by g
union all select null,null,sum(v) from src
order by g,h;
select g,h,sum(v) s from src group by g,h with rollup order by g,h;
create table r as select g,h,sum(v) s from src group by g,h with rollup;
select * from r order by g,h;
create table inserted(g int,h int,s decimal(38,0));
insert into inserted select g,h,sum(v) from src group by g,h with rollup;
select * from inserted order by g,h;
select column_name,is_nullable from information_schema.columns
where table_schema='grouping_ctas_test' and table_name='r' order by ordinal_position;
insert into r(g,h,s) values(null,null,0);
select count(*) from r where s=0 and g is null and h is null;

-- CUBE 还包含 h 小计，总计八行；字面量 fallback 仍保留非空约束。
select g,h,sum(v) s from src group by cube(g,h) order by g,h;
create table c as select coalesce(g,-1) g,coalesce(h,-1) h,sum(v) s
from src group by cube(g,h);
select * from c order by g,h;
select column_name,is_nullable from information_schema.columns
where table_schema='grouping_ctas_test' and table_name='c' order by ordinal_position;
insert into c(g,h,s) values(null,0,0);
insert into c(g,h,s) values(0,null,0);

-- 格式往返不能把显式集合平铺成普通 GROUP BY。
create table sets_result as select g,h,sum(v) s
from src group by grouping sets ((g,h),(g),());
select * from sets_result order by g,h;
create table duplicates as select g,sum(v) s
from src group by grouping sets ((g),(g),());
select * from duplicates order by g,s;
create table empty_set as select count(*) c from src group by grouping sets (());
select * from empty_set;
create table duplicate_empty as select count(*) c from src group by grouping sets ((),());
select * from duplicate_empty order by c;

-- 所有集合共有的键和 COUNT 不得失去非空约束。
create table common_key as select g,h,count(*) c
from src group by grouping sets ((g,h),(g));
select * from common_key order by g,h;
select column_name,is_nullable from information_schema.columns
where table_schema='grouping_ctas_test' and table_name='common_key' order by ordinal_position;
insert into common_key(g,h,c) values(null,0,0);
insert into common_key(g,h,c) values(0,0,null);
insert into common_key(g,h,c) values(3,null,0);
select * from common_key where g=3;
create table ordinary as select g,count(*) c from src group by g;
select column_name,is_nullable from information_schema.columns
where table_schema='grouping_ctas_test' and table_name='ordinary' order by ordinal_position;
insert into ordinary(g,c) values(null,0);

-- 视图和重复引用 CTE 不应重复展开同一 ROLLUP/CUBE。
create view grouped_view as select g,h,count(*) c from src group by g,h with rollup;
select * from grouped_view order by g,h;
select column_name,is_nullable from information_schema.columns
where table_schema='grouping_ctas_test' and table_name='grouped_view' order by ordinal_position;
create table view_result as select * from grouped_view;
select * from view_result order by g,h;
with grouped as (select g,h,count(*) c from src group by cube(g,h))
select (select count(*) from grouped) a,(select count(*) from grouped) b;
prepare grouping_stmt from 'select g,h,count(*) c from src group by g,h with rollup order by g,h';
execute grouping_stmt;
execute grouping_stmt;
execute grouping_stmt;
deallocate prepare grouping_stmt;

-- HAVING、投影剪枝仍保留分组重数。
select g,h,sum(v) s from src group by g,h with rollup having sum(v)>7 order by g,h;
create table pruned as select 1 k from src group by g,h with rollup;
select count(*) from pruned;

-- 保留空输入 ROLLUP/CUBE 的零行结果；显式重复空集合分别执行全局聚合。
create table empty_src(g int not null,h int not null,v int not null);
create table empty_rollup as select g,h,count(*) c from empty_src group by g,h with rollup;
select * from empty_rollup order by g,h;
select count(*) from empty_rollup;
create table empty_cube as select g,h,count(*) c from empty_src group by cube(g,h);
select * from empty_cube order by g,h;
select count(*) from empty_cube;
create table empty_duplicates as select count(*) c from empty_src group by grouping sets ((),());
select * from empty_duplicates order by c;

-- 真实 NULL 与补 NULL 可以显示为相同键，但不能丢失或合并对应行。
create table nullable_src(g int,h int,v int);
insert into nullable_src values(null,10,5),(1,null,7);
select g,h,sum(v) s,grouping(g,h) flag from nullable_src group by g,h with rollup order by g,h,flag;
create table nullable_result as select g,h,sum(v) s,grouping(g,h) flag
from nullable_src group by g,h with rollup;
select * from nullable_result order by g,h,flag;

drop database grouping_ctas_test;
