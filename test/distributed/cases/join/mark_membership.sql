drop database if exists mark_membership;
create database mark_membership;
use mark_membership;
create table probes(id int);
create table members(id int);
create table extra(id int);
insert into probes values (NULL), (1), (2), (3);
insert into members values (1), (1);
insert into extra values (4);

-- 匹配、不匹配、NULL probe；重复 RHS 不增加输出行。
select id, id in (select id from members) as member,
       id not in (select id from members) as nonmember from probes order by id;
select count(*) from probes where id not in (select id from members union all select id from extra);
select count(*) from probes where id in (select id from members union all select 4);

-- RHS NULL 不掩盖已命中的 TRUE，但使未命中为 UNKNOWN。
insert into extra values (NULL);
select id, id in (select id from members union all select id from extra) as member,
       id not in (select id from members union all select id from extra) as nonmember from probes order by id;
select count(*) from probes where id not in (select id from members union all select id from extra);

-- prepared 执行跨代：先有 NULL，再无 NULL，最后空 RHS。
prepare membership_stmt from 'select id, id not in (select id from extra) as nonmember from probes order by id';
execute membership_stmt;
delete from extra where id is NULL;
execute membership_stmt;
delete from extra;
execute membership_stmt;
deallocate prepare membership_stmt;

-- 全 NULL 和空 RHS 是不同状态，NULL probe 对空集也返回 FALSE。
delete from members;
insert into members values (NULL);
select id, id in (select id from members) as member from probes order by id;
delete from members;
select id, id in (select id from members) as member,
       id not in (select id from members) as nonmember from probes order by id;

-- 字符串借用键：空串、长字符串、嵌入 NUL 和不相等的后缀。
create table string_probes(id int, k varchar(8192));
create table string_members(k varchar(8192));
insert into string_probes values (1, ''), (2, repeat('x', 4096)), (3, concat('a', char(0), 'b')), (4, 'a'), (5, NULL);
insert into string_members values (''), (repeat('x', 4096)), (concat('a', char(0), 'b')), (NULL);
select id, k in (select k from string_members) as member,
       k not in (select k from string_members) as nonmember from string_probes order by id;

-- 已物化的表达式投影，以及需要 evaluator 的复合/相关谓词对照。
insert into members values (-1), (2);
select id, id in (select abs(id) from members) as member from probes order by id;
select id, (id, id) in (select id, id from members) as member from probes order by id;
select id, id not in (select id from members where members.id < probes.id) as nonmember from probes order by id;

-- 小 RHS 和足够的非 NULL miss，验证高工作量输入及 prepared 跨代的三值结果。
-- 数据满足渐进选择条件；实际切换仍由算子测试和 EXPLAIN ANALYZE 证明。
delete from probes;
delete from members;
insert into probes values
(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),
(18),(19),(20),(21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),
(34),(35),(36),(37),(38),(39),(40),(41),(42),(43),(44),(45),(46),(47),(48),(49),
(50),(51),(52),(53),(54),(55),(56),(57),(58),(59),(60),(61),(62),(63),(64),(65),(66),
(1),(NULL);
insert into members values (1),(NULL);
prepare membership_threshold from 'select count(*) as total, count(distinct id) as distinct_ids, count(case when member then 1 end) as in_true, count(case when not member then 1 end) as in_false, count(case when member is null then 1 end) as in_null, count(case when nonmember then 1 end) as not_in_true, count(case when not nonmember then 1 end) as not_in_false, count(case when nonmember is null then 1 end) as not_in_null from (select id, id in (select id from members) as member, id not in (select id from members) as nonmember from probes) marks';
execute membership_threshold;
delete from members where id is NULL;
execute membership_threshold;
delete from members;
execute membership_threshold;
deallocate prepare membership_threshold;

drop database mark_membership;
