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

-- DISTINCT 属于合并后的 SELECT；不带 DISTINCT 时必须保留显式重复集合。
select g from src group by grouping sets ((g),(g)) order by g;
select distinct g from src group by grouping sets ((g),(g)) order by g;
create table repeated_keys as select g from src group by grouping sets ((g),(g));
select * from repeated_keys order by g;
create table distinct_keys as select distinct g from src group by grouping sets ((g),(g));
select * from distinct_keys order by g;
select distinct g from src group by grouping sets ((g),(g),()) order by g limit 2;
create table distinct_limit as select distinct g from src group by grouping sets ((g),(g),()) order by g limit 2;
select * from distinct_limit order by g;
select distinct g from src group by g,h with rollup order by g;

-- 完整的表达式才是分组键，递归操作数不能标记其它键在当前集合出现。
select g,h+1 hh,sum(v) s from src group by cube(g,h+1) order by g,hh;
create table expression_cube as select g,h+1 hh,sum(v) s from src group by cube(g,h+1);
select * from expression_cube order by g,hh;
select g,h+1 hh,sum(v) s from src group by grouping sets ((g,h+1),(h+1),()) order by g,hh;
create table expression_sets as select g,h+1 hh,sum(v) s
from src group by grouping sets ((g,h+1),(h+1),());
select * from expression_sets order by g,hh;
select g,abs(h) hh,sum(v) s from src group by cube(g,abs(h)) order by g,hh;
select g,h+1 hh,sum(v) s from src group by cube(1,2) order by g,hh;

-- 输入主键唯一，不代表缺失该键的小计分支可删除聚合。
create table pk_src(id int primary key);
insert into pk_src values(1),(2);
select id from pk_src group by id order by id;
select id from pk_src group by id with rollup order by id;
create table pk_rollup as select id from pk_src group by id with rollup;
select * from pk_rollup order by id;
select column_name,is_nullable from information_schema.columns
where table_schema='grouping_ctas_test' and table_name='pk_rollup' and column_name='id';
insert into pk_rollup values(null);
select count(*) from pk_rollup where id is null;
select id from pk_src group by cube(id) order by id;
create table pk_cube as select id from pk_src group by cube(id);
select * from pk_cube order by id;
create table pk_ordinary as select id from pk_src group by id;
select column_name,is_nullable from information_schema.columns
where table_schema='grouping_ctas_test' and table_name='pk_ordinary' and column_name='id';
insert into pk_ordinary values(null);

-- 空分组集合即使没有聚合函数也要执行一次全局分组。
select 1 k from src group by grouping sets (());
select 1 k from src group by grouping sets ((),());
create table constant_empty_sets as select 1 k from src group by grouping sets ((),());
select * from constant_empty_sets order by k;
select 1 k from empty_src group by grouping sets (());
select 1 k from empty_src group by grouping sets ((),());
create table constant_empty_input as select 1 k from empty_src group by grouping sets ((),());
select * from constant_empty_input order by k;
select distinct 1 k from src group by grouping sets ((),());
create table distinct_empty_sets as select distinct 1 k from empty_src group by grouping sets ((),());
select * from distinct_empty_sets;
select 1 k from src order by k;
select 1 k from empty_src;
select 1 k from src group by grouping sets (()) having 1=0;
select 1 k from empty_src group by grouping sets (()) having 1=0;
select g from src group by grouping sets (());

-- 外层谓词作用于分组输出；输入 WHERE 与补 NULL 后的输出 WHERE 不可互换。
select g from (select g from src group by g with rollup) t where g=1;
create table filtered_rollup as select g from (select g from src group by g with rollup) t where g=1;
select * from filtered_rollup;
select g from (select g from src group by g with rollup) t where g is null;
select g from (select g from src group by g with rollup) t where g is not null order by g;
select g from (select g from src group by g with rollup) t where coalesce(g,-1)=-1;
select g from (select g from src group by g with rollup) t where g in (1,2) order by g;
select g from src where g=1 group by g with rollup order by g;
select g,h from (select g,h from src group by grouping sets ((g,h),(g))) t where g=1 order by h;
select k from (select g+1 k from src group by g+1 with rollup) t where k=2;

-- 无键全局分组仍会在零输入上发出一行，输出过滤不能变成输入过滤。
select * from (select 1 k from src group by grouping sets (())) t where k=0;
create table filtered_empty_set as select * from (select 1 k from src group by grouping sets (())) t where k=0;
select * from filtered_empty_set;
select * from (select 1 k from empty_src group by grouping sets (())) t where k=0;
select * from (select 1 k from src group by grouping sets ((),())) t where k=0;
select * from (select 1 k from src group by grouping sets ((),())) t where k=1;
select count(*) from src having 1=0;
select * from (select count(*) k from src) t where false;

-- 窗口在所有分组集合合并后执行，不在分支内分别执行。
select g,count(*) over() n from src group by g with rollup order by g;
create table global_window as select g,count(*) over() n from src group by g with rollup;
select * from global_window order by g;
select g,row_number() over(order by g) rn from src group by g with rollup order by g;
select g,count(g) over() present_keys,count(*) over() n from src group by g with rollup order by g;
select g,sum(v) s,sum(sum(v)) over() total,count(*) over() n from src group by g with rollup order by g;
select g,sum(v) s,sum(sum(v)) over(order by g rows between unbounded preceding and current row) running
from src group by g with rollup order by g;
select g,count(*) over(partition by g) n from src group by grouping sets ((g),(g),()) order by g,n;
select g,count(*) over() n from src group by g with rollup having sum(v)>10 order by g;
select distinct count(*) over() n from src group by g with rollup;
create table distinct_window as select distinct count(*) over() n from src group by g with rollup;
select * from distinct_window;
select g from src group by g with rollup order by count(*) over(),g;
select g,count(*) over() n from src group by g with rollup order by g limit 2;
select * from (select g,count(*) over() n from src group by g with rollup) t where g=1;
select 1 k,count(*) over() n from empty_src group by grouping sets ((),());
create table empty_window as select 1 k,count(*) over() n from empty_src group by grouping sets ((),());
select * from empty_window;
select g,count(*) over() n from empty_src group by g with rollup;
select g,sum(v) s,grouping(g) flag,row_number() over(order by g,grouping(g)) rn
from nullable_src group by g with rollup order by g,flag;
create view global_window_view as select g,count(*) over() n from src group by g with rollup;
create table window_view_result as select * from global_window_view;
select * from window_view_result order by g;
prepare global_window_stmt from 'select g,count(*) over() n from src group by g with rollup order by g';
execute global_window_stmt;
execute global_window_stmt;
execute global_window_stmt;
deallocate prepare global_window_stmt;

-- 量化子查询的 Child 引用分组输出，包括合成 NULL 和复合左值。
select g,g in (select id from pk_src) in_rhs,g not in (select id from pk_src) not_in_rhs,
g>any(select id from pk_src) any_rhs,g<=all(select id from pk_src) all_rhs
from src group by g with rollup order by g;
create table quantified_group as select g,g in (select id from pk_src) in_rhs,
g not in (select id from pk_src) not_in_rhs from src group by g with rollup;
select * from quantified_group order by g;
select g from quantified_group where in_rhs is null;
select g,g in (select id from pk_src where id<0) in_empty,
g not in (select id from pk_src where id<0) not_in_empty
from src group by g with rollup order by g;
select g,h,(g,h) in (select id,id+9 from pk_src) tuple_rhs
from src group by g,h with rollup order by g,h;

-- DISTINCT 的键投影必须先展开子查询，再去重；窗口仍只看完整的分组结果。
select distinct (select 7) k from src group by g with rollup;
create table distinct_scalar_group as select distinct (select 7) k from src group by g with rollup;
select * from distinct_scalar_group;
select distinct (select id from pk_src where id=1) k from src group by g with rollup;
select distinct (select id from pk_src where id<0) k from src group by g with rollup;
select distinct coalesce((select 7),9) k from src group by grouping sets ((g),(g));
select distinct g in (select id from pk_src) flag from src group by g with rollup order by flag;
select distinct count(*) over() n,(select 7) k from src group by g with rollup;
create table distinct_window_subquery as
select distinct count(*) over() n,(select 7) k from src group by g with rollup;
select * from distinct_window_subquery;

-- 100 percent 或超过输入行数的预算使采样结果确定；保留重复集合的完整采样后投影。
select g,sample(v,100 percent) chosen from src group by grouping sets ((g),(g)) order by g,chosen;
create table grouped_sample as select g,sample(v,100 percent) chosen
from src group by grouping sets ((g),(g));
select * from grouped_sample order by g,chosen;
select g,h,sample(v,100 percent) chosen from src group by grouping sets ((),()) order by g,h,chosen;
select g,sample(h,v,100 percent) from src group by grouping sets ((g),(g)) order by g,h,v;
select distinct g,sample(v,100 percent) chosen from src group by grouping sets ((g),(g)) order by g,chosen;
select g,sample(v,10 rows) chosen from src group by grouping sets ((g),(g)) order by g,chosen;
select sample(v,100 percent) chosen from src group by grouping sets ((),()) order by chosen;
create table empty_sets_sample as select sample(v,100 percent) chosen
from src group by grouping sets ((),());
select * from empty_sets_sample order by chosen;
create table sample_null_union as
select null z,sample(v,100 percent) chosen from src group by grouping sets ((),()) union all select 42,99;
select * from sample_null_union order by z,chosen;
select sample(v,100 percent) chosen from empty_src group by grouping sets ((),());
select g,sample(v,0 percent) chosen from src group by grouping sets ((g),(g));

-- 内部传输保留二进制键的 grouping bitmap：实际 NULL 与总计 NULL 的标记不同。
create table binary_key(k binary(4),v int not null);
insert into binary_key values('a',1),('b',2),(null,4);
create table varbinary_key(k varbinary(4),v int not null);
insert into varbinary_key values('a',1),('b',2),(null,4);
select k is null null_key,grouping(k) flag,sum(v) s from binary_key group by k with rollup order by flag,s;
create table binary_markers as
select k is null null_key,grouping(k) flag,sum(v) s from binary_key group by k with rollup;
select * from binary_markers order by flag,s;
create table varbinary_markers as
select hex(k) hk,grouping(k) flag,sum(v) s from varbinary_key group by k with rollup;
select * from varbinary_markers order by flag,s;

-- SAMPLE 必须使用当前集合，补 NULL 的键不能参与原明细分组或继续声明非空。
select g,grouping(g) flag,sample(v,100 percent) chosen from src group by g with rollup order by flag,g,chosen;
create table sampled_rollup as
select g,grouping(g) flag,sample(v,100 percent) chosen from src group by g with rollup;
select * from sampled_rollup order by flag,g,chosen;
select column_name,is_nullable from information_schema.columns
where table_schema='grouping_ctas_test' and table_name='sampled_rollup' order by ordinal_position;
select g,h,grouping(g,h) flag,sample(v,100 percent) chosen from src group by cube(g,h) order by flag,g,h,chosen;
create table sampled_cube as
select g,h,grouping(g,h) flag,sample(v,100 percent) chosen from src group by cube(g,h);
select * from sampled_cube order by flag,g,h,chosen;
select h,grouping(g,h) flag,sample(v,100 percent) chosen from src group by cube(g,h) order by flag,h,chosen;
select g,grouping(g) flag,sample(v,100 percent) chosen
from src group by grouping sets ((g),(g),()) order by flag,g,chosen;
select g,grouping(g) flag,sample(v,100 percent) chosen
from nullable_src group by g with rollup order by flag,g,chosen;
select g,grouping(g) flag,sample(v,100 percent) chosen from empty_src group by g with rollup;

-- 窗口读取合成 NULL 后必须刷新值类型；partition-only 键属于上游坐标。
select first_value(g) over(order by g) f from src group by g with rollup;
create view first_group_value as select first_value(g) over(order by g) f from src group by g with rollup;
select * from first_group_value;
select column_name,is_nullable from information_schema.columns
where table_schema='grouping_ctas_test' and table_name='first_group_value';
create table first_group_ctas as select first_value(g) over(order by g) f from src group by g with rollup;
select * from first_group_ctas;
create table partition_group_ctas as
select first_value(g) over(partition by h order by g) f from src group by g,h with rollup;
select * from partition_group_ctas order by f;
create table first_group_fallback as
select coalesce(first_value(g) over(order by g),7) f,count(*) over() n from src group by g with rollup;
select * from first_group_fallback;
select column_name,is_nullable from information_schema.columns
where table_schema='grouping_ctas_test' and table_name='first_group_fallback' order by ordinal_position;

-- DISTINCT 必须拒绝 tuple 的未选列，而不是把它当没有列依赖的常量。
select distinct g from src group by g,h order by (g,h);
select distinct g from src group by g,h with rollup order by (g,h);
create table rejected_tuple_order as select distinct g from src group by g,h with rollup order by (g,h);
select count(*) from information_schema.tables
where table_schema='grouping_ctas_test' and table_name='rejected_tuple_order';

-- 只消费 COUNT 时不需要传输分组键；SAMPLE 的合法排序表达式在 DISTINCT 后计算。
select sum(c) from (select count(*) c from src group by g,h with rollup) t;
select distinct g,sample(v,100 percent) chosen from src group by grouping sets ((g),(g)) order by chosen+1;
create table ordered_sample as
select distinct g,sample(v,100 percent) chosen from src group by grouping sets ((g),(g)) order by chosen+1;
select * from ordered_sample order by chosen;
select distinct g,sample(v,100 percent) chosen from src group by grouping sets ((g),(g)) order by h;

-- 隐藏 GROUPING 排序必须保留分支来源，不能把明细分支的 0 提升为全局常量。
select sample(v,100 percent) chosen from src group by g with rollup
order by grouping(g) desc,chosen limit 3;
create table hidden_grouping_order as select sample(v,100 percent) chosen
from src group by g with rollup order by grouping(g) desc,chosen limit 3;
select * from hidden_grouping_order order by chosen;
select g,sample(v,100 percent) chosen from src group by g with rollup
order by grouping(g) desc,chosen limit 3;
create table visible_grouping_key_order as select g,sample(v,100 percent) chosen
from src group by g with rollup order by grouping(g) desc,chosen limit 3;
select * from visible_grouping_key_order order by chosen;
select g,grouping(g) flag,sample(v,100 percent) chosen from src group by g with rollup
order by grouping(g) desc,chosen limit 3;
select sample(v,100 percent) chosen from src group by g with rollup
order by grouping(g)+0 desc,chosen limit 3;
select sample(v,100 percent) chosen from src group by grouping sets ((g),(g),())
order by grouping(g) desc,chosen limit 3;
select sample(v,100 percent) chosen from nullable_src group by g with rollup
order by grouping(g) desc,chosen limit 2;

-- SQL 字面量零不能与折叠后的 GROUPING 零混淆；flag 的并列排序使 LIMIT 选行确定。
create table sort_big_sample(g int not null,v bigint not null);
insert into sort_big_sample select g,v from src;
select grouping(g) flag,sample(v,100 percent) chosen from sort_big_sample group by g with rollup
order by chosen+0,flag desc limit 3;
create table literal_zero_sample_order as
select grouping(g) flag,sample(v,100 percent) chosen from sort_big_sample group by g with rollup
order by chosen+0,flag desc limit 3;
select * from literal_zero_sample_order order by chosen,flag desc;
select grouping(g) flag,0 z,sample(v,100 percent) chosen from sort_big_sample group by g with rollup
order by chosen+0,flag desc limit 3;
select distinct grouping(g) flag,sample(v,100 percent) chosen from sort_big_sample group by g with rollup
order by chosen+0,flag desc limit 3;

-- 复合子查询左值必须继承 subtotal 的 nullable，不能把 UNKNOWN 当作 ANTI 的不匹配。
select g from (select g,g+0 in(select id from pk_src) flag from src group by g with rollup) t where not flag;
create table compound_operand_filtered as
select g from (select g,g+0 in(select id from pk_src) flag from src group by g with rollup) t where not flag;
select * from compound_operand_filtered;
select count(*) from compound_operand_filtered;
select g from (select g,g+0 not in(select id from pk_src) flag from src group by g with rollup) t where flag;
select g from (select g,g+0 in(select id from pk_src) flag from src group by g with rollup) t where flag is null;
select g from (select g,coalesce(g,0) in(select id from pk_src) flag from src group by g with rollup) t where not flag;
select g from (select g,g+0 in(select id from pk_src where id<0) flag from src group by g with rollup) t where not flag order by g;

-- DISTINCT 排序依赖选出的子查询别名，不重复绑定/展开同一个 alias；CTAS 保留原别名。
select distinct (select max(id) from pk_src) k from src group by g with rollup order by k+1;
create table scalar_alias_order as
select distinct (select max(id) from pk_src) k from src group by g with rollup order by k+1;
select * from scalar_alias_order;
select distinct coalesce((select max(id) from pk_src),0) k from src group by g with rollup order by k+1;
select distinct (select count(*) from pk_src) k from src group by g with rollup order by abs(k) desc limit 1;
select distinct (select id from pk_src where id<0) k from src group by g with rollup order by k+1;
select distinct (select max(id) from pk_src) a,(select min(id) from pk_src) b
from src group by g with rollup order by a+1,b+1;
create table rejected_scalar_order as
select distinct (select max(id) from pk_src) k from src group by g with rollup order by g+1;
select count(*) from information_schema.tables
where table_schema='grouping_ctas_test' and table_name='rejected_scalar_order';

-- 独立可变输出 a/b 结构相同，但排序必须使用 b 的确切已选结果；随机文本不作 golden。
create table volatile_alias_order as
select row_number() over() seq,t.* from (
select distinct hex(random_bytes(8)) a,hex(random_bytes(8)) b
from src group by cube(g,h) order by concat(b,'') limit 1000000) t;
select count(*) from (select b,lag(b) over(order by seq) previous_b from volatile_alias_order) s
where b<previous_b;
select count(*) from volatile_alias_order;

-- SAMPLE 的分支物化同样保留别名身份，嵌套包装不能回退到 a。
create table volatile_sample_alias_order as
select row_number() over() seq,t.* from (
select distinct hex(random_bytes(8)) a,hex(random_bytes(8)) b,sample(v,100 percent) chosen
from src group by cube(g,h) order by concat(reverse(b),'') limit 1000000) t;
select count(*) from (select b,lag(b) over(order by seq) previous_b from volatile_sample_alias_order) s
where reverse(b)<reverse(previous_b);
select count(*) from volatile_sample_alias_order;

-- 非 DISTINCT SAMPLE 与 LIMIT 也必须消费同一个 b，而不是重新求值。
create table volatile_sample_limited as
select row_number() over() seq,t.* from (
select hex(random_bytes(8)) a,hex(random_bytes(8)) b,sample(v,100 percent) chosen
from src group by cube(g,h) order by concat(b,'') desc limit 3) t;
select count(*) from (select b,lag(b) over(order by seq) previous_b from volatile_sample_limited) s
where b>previous_b;
select count(*) from volatile_sample_limited;

-- SAMPLE 混合排序把 selected chosen 与分支 GROUPING 分别传输，不能回放进同一 PROJECT。
select sample(v,100 percent) chosen from src group by g with rollup
order by chosen-100*grouping(g) limit 3;
create table sample_mixed_order as
select sample(v,100 percent) chosen from src group by g with rollup
order by chosen-100*grouping(g) limit 3;
select * from sample_mixed_order order by chosen;
select sample(v,100 percent) chosen from src group by g with rollup
order by case when grouping(g)=1 then chosen-100 else chosen end limit 3;
select sample(v,100 percent) chosen from src group by cube(g,h)
order by chosen-100*grouping(g)-10*grouping(h) limit 3;
select sample(v,100 percent) chosen from src group by g with rollup
order by case when chosen in(select v from src) then chosen else 0 end-100*grouping(g) limit 3;
select sample(v,100 percent) chosen from src group by g with rollup
order by coalesce((select max(v) from src),0)+chosen-100*grouping(g) limit 3;

-- 拆分依赖不放宽 DISTINCT 的来源要求；选出 flag 才能跨 DISTINCT 使用。
select distinct grouping(g) flag,sample(v,100 percent) chosen from src group by g with rollup
order by chosen-100*grouping(g) limit 3;
create table rejected_mixed_order as
select distinct sample(v,100 percent) chosen from src group by g with rollup
order by chosen-100*grouping(g);
select count(*) from information_schema.tables
where table_schema='grouping_ctas_test' and table_name='rejected_mixed_order';

-- 隐藏 branch marker 与可变 alias 混合时仍必须使用真正选出的 b，不能重新求值或偷换到 a。
create table volatile_mixed_order as
select row_number() over() seq,t.* from (
select g,hex(random_bytes(8)) a,hex(random_bytes(8)) b,sample(v,100 percent) chosen
from src group by g with rollup order by concat(grouping(g),b) limit 1000000) t;
select count(*) from (
select concat(if(g is null,1,0),b) k,
lag(concat(if(g is null,1,0),b)) over(order by seq) previous_k from volatile_mixed_order) s
where k<previous_k;
select count(*) from volatile_mixed_order;

-- GROUPING 的 alias 实参描述键来源，不是读取同一 PROJECT 尚未产生的 selected 值。
select g k,sample(v,100 percent) chosen from src group by g with rollup
order by grouping(k) desc,chosen;
create table grouping_alias_order as
select g k,sample(v,100 percent) chosen from src group by g with rollup
order by grouping(k) desc,chosen;
select * from grouping_alias_order order by k,chosen;
select g v,sample(v,100 percent) chosen from src group by g with rollup
order by grouping(v) desc,chosen;
select g `KeyAlias`,sample(v,100 percent) chosen from src group by g with rollup
order by chosen-100*grouping(`KeyAlias`) limit 3;
select g k,h label,sample(v,100 percent) chosen from src group by cube(g,h)
order by grouping(k,label) desc,chosen;
select distinct g k,grouping(g) flag,sample(v,100 percent) chosen from src group by g with rollup
order by grouping(k) desc,chosen;
select distinct g k,grouping(g) flag,count(*) total from src group by g with rollup
order by grouping(k) desc,k;

-- 真实 NULL 不代表 subtotal；仍由 GROUPING 的原始键位决定 lineage。
select g k,sample(v,100 percent) chosen from nullable_src group by g with rollup
order by grouping(k) desc,chosen;
create table rejected_grouping_alias as
select distinct g k,sample(v,100 percent) chosen from src group by g with rollup
order by grouping(k) desc,chosen;
select count(*) from information_schema.tables
where table_schema='grouping_ctas_test' and table_name='rejected_grouping_alias';

drop database grouping_ctas_test;
