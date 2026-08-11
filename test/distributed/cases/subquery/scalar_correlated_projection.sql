-- @suite
-- @setup
drop database if exists test_subq_corr_project;
create database test_subq_corr_project;
use test_subq_corr_project;
create table t1 (a int, b int, c int);
create table t2 (d int);
insert into t1 values (1, 2, 3), (11, 22, 33);
create table parent_agg (id int primary key, corr_key int);
create table child_agg (corr_key int, v int);
insert into parent_agg values (1, 10), (2, 20), (3, 30), (4, 30);
insert into child_agg values (10, 5), (10, 7), (20, null);
create table `Author` (`id` int primary key, `name` varchar(32));
create table `Post` (`id` int primary key, `title` varchar(32), `authorId` int);
insert into `Author` values (1, 'Zero'), (2, 'One'), (3, 'Many');
insert into `Post` values (20, 'Only', 2), (30, 'First', 3), (31, null, 3);

-- @case
-- @desc:direct outer column projected by a correlated scalar subquery
-- @label:bvt
select t1.*, (select t1.a from t2 where t2.d > t1.a) as x from t1 order by t1.a;
select t1.*, (select distinct t1.a from t2) as x from t1 order by t1.a;
select t1.*, (select t1.a from t2 limit 1) as x from t1 order by t1.a;
insert into t2 values (99);
select t1.*, (select t1.a from t2 where t2.d > t1.a) as x from t1 order by t1.a;
select t1.*, (select distinct t1.a from t2) as x from t1 order by t1.a;
select t1.*, (select t1.a from t2 limit 1) as x from t1 order by t1.a;
insert into t2 values (100);
select t1.*, (select t1.a from t2 where t2.d > t1.a) from t1;
select t1.*, (select t1.a from t2 where t2.d > t1.a) as x from t1;
select t1.*, (select distinct t1.a from t2 where t2.d > t1.a) as x from t1 order by t1.a;

delete from t2;
insert into t2 values (5), (100);
select t1.*, (select t1.a from t2 where t2.d > t1.a order by t2.d limit 1) as x from t1 order by t1.a;
select (select t1.a from t2 where t2.d > t1.a limit 2) as x from t1 where t1.a = 11;
select (select t1.a from t2 where t2.d > t1.a limit 2) as x from t1 where t1.a = 1;

-- @case
-- @desc:issue #25959 - evaluate the final scalar aggregate projection after LEFT JOIN null extension
-- @label:bvt
select p.id, (select coalesce(sum(c.v), 0) from child_agg c where c.corr_key = p.corr_key) as sum_value from parent_agg p order by p.id;
select p.id, (select ifnull(avg(c.v), 7) from child_agg c where c.corr_key = p.corr_key) as avg_value, (select ifnull(min(c.v), 8) from child_agg c where c.corr_key = p.corr_key) as min_value, (select ifnull(max(c.v), 9) from child_agg c where c.corr_key = p.corr_key) as max_value from parent_agg p order by p.id;
select p.id, (select sum(c.v) from child_agg c where c.corr_key = p.corr_key) as raw_sum from parent_agg p order by p.id;
select p.id, (select count(*) from child_agg c where c.corr_key = p.corr_key) as row_count, (select count(c.v) from child_agg c where c.corr_key = p.corr_key) as value_count from parent_agg p order by p.id;
select p.id, (select count(*) + 1 from child_agg c where c.corr_key = p.corr_key) as count_plus_one, (select coalesce(count(*), 5) from child_agg c where c.corr_key = p.corr_key) as count_fallback from parent_agg p order by p.id;
select p.id, (select coalesce(sum(c.v), 100) + count(*) from child_agg c where c.corr_key = p.corr_key) as mixed_value from parent_agg p order by p.id;
select p.id, (select case when count(*) = 0 then 42 else coalesce(sum(c.v), 0) end from child_agg c where c.corr_key = p.corr_key) as case_value from parent_agg p order by p.id;
select p.id, (select bit_and(c.v) from child_agg c where c.corr_key = p.corr_key) <=> (select bit_and(c.v) from child_agg c where false) as bit_and_matches, (select bit_or(c.v) from child_agg c where c.corr_key = p.corr_key) <=> (select bit_or(c.v) from child_agg c where false) as bit_or_matches, (select bit_xor(c.v) from child_agg c where c.corr_key = p.corr_key) <=> (select bit_xor(c.v) from child_agg c where false) as bit_xor_matches, (select approx_count_distinct(c.v) from child_agg c where c.corr_key = p.corr_key) <=> (select approx_count_distinct(c.v) from child_agg c where false) as approx_count_matches, (select sum(c.v) from child_agg c where c.corr_key = p.corr_key) <=> (select sum(c.v) from child_agg c where false) as sum_matches from parent_agg p where p.id = 3;
select p.id, (select coalesce(json_arrayagg(c.v), convert('[]', json)) from child_agg c where c.corr_key = p.corr_key) as json_value from parent_agg p order by p.id;
with correlated_input as (select corr_key, v from child_agg) select p.id, (select coalesce(sum(c.v), 0) from correlated_input c where c.corr_key = p.corr_key) as cte_sum from parent_agg p order by p.id;
select p.id, (with correlated_input as (select c.v from child_agg c where c.corr_key = p.corr_key) select coalesce(sum(v), 0) from correlated_input) as cte_correlated_sum from parent_agg p order by p.id;
select p.id, (select sum(c.v) from child_agg c where c.corr_key = p.corr_key group by c.corr_key) as grouped_sum from parent_agg p order by p.id;
select p.id, (select sum(c.v) from child_agg c where c.corr_key = p.corr_key having sum(c.v) > 100) as having_sum from parent_agg p order by p.id;

-- @case
-- @desc:issue #24737 - Prisma 7.9.1 to-many relation join through transparent derived tables
-- @label:bvt
-- @ignore:2
SELECT `t0`.`id`, `t0`.`name`, (SELECT COALESCE(JSON_ARRAYAGG(`__prisma_data__`), CONVERT('[]', JSON)) AS `__prisma_data__` FROM (SELECT `t3`.`__prisma_data__` FROM (SELECT JSON_OBJECT('id', `t2`.`id`, 'title', `t2`.`title`, 'authorId', `t2`.`authorId`) AS `__prisma_data__` FROM (SELECT `t1`.* FROM `Post` AS `t1` WHERE `t0`.`id` = `t1`.`authorId` /* root select */) AS `t2` /* inner select */) AS `t3` /* middle select */) AS `t4` /* outer select */) AS `posts` FROM `Author` AS `t0` ORDER BY `t0`.`id` ASC;
SELECT `q`.`id`, JSON_LENGTH(`q`.`posts`) AS `post_count` FROM (SELECT `t0`.`id`, (SELECT COALESCE(JSON_ARRAYAGG(`__prisma_data__`), CONVERT('[]', JSON)) FROM (SELECT `t3`.`__prisma_data__` FROM (SELECT JSON_OBJECT('id', `t2`.`id`, 'title', `t2`.`title`, 'authorId', `t2`.`authorId`) AS `__prisma_data__` FROM (SELECT `t1`.* FROM `Post` AS `t1` WHERE `t0`.`id` = `t1`.`authorId`) AS `t2`) AS `t3`) AS `t4`) AS `posts` FROM `Author` AS `t0`) AS `q` ORDER BY `q`.`id`;
-- @ignore:1
SELECT `t0`.`id`, (SELECT COALESCE(JSON_ARRAYAGG(`__prisma_data__`), CONVERT('[]', JSON)) FROM (SELECT JSON_OBJECT('id', `t2`.`id`, 'title', `t2`.`title`) AS `__prisma_data__` FROM (SELECT `t1`.* FROM `Post` AS `t1` WHERE `t0`.`id` = `t1`.`authorId` AND `t1`.`id` >= 30) AS `t2`) AS `t3`) AS `filtered_posts` FROM `Author` AS `t0` ORDER BY `t0`.`id`;
SELECT `q`.`id`, JSON_LENGTH(`q`.`filtered_posts`) AS `filtered_post_count` FROM (SELECT `t0`.`id`, (SELECT COALESCE(JSON_ARRAYAGG(`__prisma_data__`), CONVERT('[]', JSON)) FROM (SELECT JSON_OBJECT('id', `t2`.`id`, 'title', `t2`.`title`) AS `__prisma_data__` FROM (SELECT `t1`.* FROM `Post` AS `t1` WHERE `t0`.`id` = `t1`.`authorId` AND `t1`.`id` >= 30) AS `t2`) AS `t3`) AS `filtered_posts` FROM `Author` AS `t0`) AS `q` ORDER BY `q`.`id`;
SELECT `t0`.`id`, (SELECT COUNT(*) FROM (SELECT `t1`.`id` FROM `Post` AS `t1` WHERE `t0`.`id` = `t1`.`authorId`) AS `t2`) AS `post_count` FROM `Author` AS `t0` ORDER BY `t0`.`id`;
SELECT `t1`.`id`, (SELECT `t0`.`name` FROM `Author` AS `t0` WHERE `t0`.`id` = `t1`.`authorId`) AS `author_name` FROM `Post` AS `t1` ORDER BY `t1`.`id`;
SELECT JSON_LENGTH(COALESCE(JSON_ARRAYAGG(`__prisma_data__`), CONVERT('[]', JSON))) AS `post_count` FROM (SELECT JSON_OBJECT('id', `t2`.`id`, 'title', `t2`.`title`, 'authorId', `t2`.`authorId`) AS `__prisma_data__` FROM (SELECT `t1`.* FROM `Post` AS `t1` WHERE `t1`.`authorId` = 3) AS `t2`) AS `t3`;

-- Empty query blocks are real correlation levels even without FROM bindings.
-- @regex("correlated subquery in FROM clause is not yet implemented",true)
SELECT `a`.`id` FROM `Author` AS `a` WHERE EXISTS (SELECT 1 WHERE EXISTS (SELECT 1 FROM (SELECT `p`.`id` FROM `Post` AS `p` WHERE `p`.`authorId` = `a`.`id`) AS `d`));
-- Mixed empty/non-empty ancestor orderings must both stay fail-closed.
-- @regex("correlated subquery in FROM clause is not yet implemented",true)
SELECT `a`.`id` FROM `Author` AS `a` WHERE EXISTS (SELECT 1 WHERE EXISTS (SELECT 1 FROM `Post` AS `p` WHERE EXISTS (SELECT 1 FROM (SELECT `a2`.`id` FROM `Author` AS `a2` WHERE `a2`.`id` = `a`.`id`) AS `d`)));
-- @regex("correlated subquery in FROM clause is not yet implemented",true)
SELECT `a`.`id` FROM `Author` AS `a` WHERE EXISTS (SELECT 1 FROM `Post` AS `p` WHERE EXISTS (SELECT 1 WHERE EXISTS (SELECT 1 FROM (SELECT `a2`.`id` FROM `Author` AS `a2` WHERE `a2`.`id` = `p`.`authorId`) AS `d`)));

-- @teardown
drop database test_subq_corr_project;
