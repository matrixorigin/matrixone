-- @suite
-- @setup
drop database if exists dependent_set_window;
create database dependent_set_window;
use dependent_set_window;
create table outer_keys (id int);
insert into outer_keys values (1), (2);

-- 规范化等值键必须在关联 UNION 中保留，尾部空格不产生第二个值。
select o.id, (select count(*) from (
    select cast('a' as char(2)) k where (select o.id)>0
    union select cast('a ' as varchar(2))
) u) as cnt from outer_keys o order by o.id;
-- 普通路径对照。
select count(*) as cnt from (
    select cast('a' as char(2)) k union select cast('a ' as varchar(2))
) u;
-- UNION ALL 保留两行。
select o.id, (select count(*) from (
    select cast('a' as char(2)) k where (select o.id)>0
    union all select cast('a ' as varchar(2))
) u) as cnt from outer_keys o order by o.id;

-- PARTITION 与 WINDOW 的关联表达式身份不能相互覆盖。
select o.id, (select sum(i.id) over (partition by (select o.id))
    from outer_keys i limit 1) as total from outer_keys o order by o.id;
-- 非关联对照。
select sum(id) over (partition by (select 1)) as total from outer_keys order by id;

-- 无关联需求的等值 JOIN 保留原生 hash 路径和重复、NULL、未匹配行语义。
create table join_keys (id int);
insert into join_keys values (1), (1), (null), (3);
select a.id, b.id, (select (select a.id)) as captured
    from outer_keys a join join_keys b on a.id=b.id order by a.id, b.id;
select a.id, b.id, (select (select a.id)) as captured
    from outer_keys a left join join_keys b on a.id=b.id order by a.id, b.id;
select a.id, b.id, (select (select a.id)) as captured
    from outer_keys a full join join_keys b on a.id=b.id order by a.id, b.id;
-- 独立 residual 在匹配等值键后执行；非等值仍走原有逐对路径。
select a.id, b.id, (select (select a.id)) as captured
    from outer_keys a join join_keys b on a.id=b.id and b.id>1 order by a.id, b.id;
select a.id, b.id, (select (select a.id)) as captured
    from outer_keys a join join_keys b on a.id<b.id order by a.id, b.id;
select a.id, (select (select a.id)) as captured from outer_keys a
    where a.id in (select b.id from join_keys b) order by a.id;
select a.id, (select (select a.id)) as captured from outer_keys a
    where a.id not in (select b.id from join_keys b where b.id is not null) order by a.id;
drop database dependent_set_window;
