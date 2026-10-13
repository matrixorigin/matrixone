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
drop database dependent_set_window;
