set global enable_privilege_cache = off;
-- cleanup residual resources
drop user if exists test_rule_user;
drop user if exists test_rule_user_multi;
drop user if exists test_rule_user_multi_diff;
drop user if exists test_rule_user_inherit;
drop user if exists test_rule_user_unmergeable;
drop user if exists test_rule_user_alias;
drop user if exists test_rule_user_where;
drop user if exists test_rule_user_dup_projection;
drop user if exists test_rule_user_expr_projection;
drop role if exists test_rule_role;
drop role if exists test_rule_role_multi_a;
drop role if exists test_rule_role_multi_b;
drop role if exists test_rule_role_multi_c;
drop role if exists test_rule_role_multi_d;
drop role if exists test_rule_role_inherit_a;
drop role if exists test_rule_role_inherit_b;
drop role if exists test_rule_role_validate;
drop role if exists test_rule_role_unmergeable_a;
drop role if exists test_rule_role_unmergeable_b;
drop role if exists test_rule_role_alias_a;
drop role if exists test_rule_role_alias_b;
drop role if exists test_rule_role_where_a;
drop role if exists test_rule_role_where_b;
drop role if exists test_rule_role_dup_projection_a;
drop role if exists test_rule_role_dup_projection_b;
drop role if exists test_rule_role_expr_projection_a;
drop role if exists test_rule_role_expr_projection_b;
drop database if exists db1;
drop database if exists db2;
create database db1;
create table db1.t1(a int, age int);
insert into db1.t1 values (1,1),(2,2),(100,30);
create table db1.t_dup(a int, marker int);
insert into db1.t_dup values (1,1),(1,2),(2,3);

-- 1. ADD RULE normal case + SHOW RULES verification
create role test_rule_role;
alter role test_rule_role add rule "select * from db1.t1 where age > 28" on table db1.t1;
show rules on role test_rule_role;

-- 2. ADD RULE error case: non-existent role
alter role non_existent_role add rule "select * from db1.t1" on table db1.t1;

-- 3. Rule update case: same table overwrites existing rule
alter role test_rule_role add rule "select * from db1.t1 where age > 50" on table db1.t1;
show rules on role test_rule_role;

-- 4. Multiple rules merge verification
alter role test_rule_role add rule "select id from db2.t2_new" on table db2.t2;
show rules on role test_rule_role;

-- 5. DROP RULE normal case + SHOW RULES verification
alter role test_rule_role drop rule on table db1.t1;
show rules on role test_rule_role;

-- 6. DROP RULE error case: non-existent role
alter role non_existent_role drop rule on table db1.t1;

-- 7. DROP RULE error case: non-existent rule
alter role test_rule_role drop rule on table no_such.rule;

-- 8. SHOW RULES empty result set
alter role test_rule_role drop rule on table db2.t2;
show rules on role test_rule_role;

-- 9. SHOW RULES error case: non-existent role
show rules on role non_existent_role;

-- 10. @session tag: verify user with role can execute queries normally with hint injection
alter role test_rule_role add rule "select * from db1.t1 where age > 28" on table db1.t1;
create user test_rule_user identified by '123456' default role test_rule_role;
grant connect on account * to test_rule_role;
grant select on table *.* to test_rule_role;
-- @session:id=1&user=sys:test_rule_user:test_rule_role&password=123456
set enable_remap_hint = 1;
select * from db1.t1;
-- @session

-- 11. SET SECONDARY ROLE ALL merges select * rewrite rules from all active roles
create database db2;
create table db2.t2(a int, age int);
insert into db2.t2 values (10,10),(20,35),(200,60);
create role test_rule_role_multi_a;
create role test_rule_role_multi_b;
alter role test_rule_role_multi_a add rule "select * from db1.t1 where age > 1" on table db1.t1;
alter role test_rule_role_multi_b add rule "select * from db1.t1 where age < 3" on table db1.t1;
alter role test_rule_role_multi_b add rule "select * from db2.t2 where age > 30" on table db2.t2;
create user test_rule_user_multi identified by '123456' default role test_rule_role_multi_a;
grant connect on account * to test_rule_role_multi_a;
grant select on table db1.t1 to test_rule_role_multi_a;
grant select on table db1.t1 to test_rule_role_multi_b;
grant select on table db2.t2 to test_rule_role_multi_b;
grant test_rule_role_multi_b to test_rule_user_multi;
-- @session:id=2&user=sys:test_rule_user_multi:test_rule_role_multi_a&password=123456
set enable_remap_hint = 1;
select * from db1.t1 order by a;
set secondary role all;
select * from db1.t1 order by a;
select * from db2.t2 order by a;
set secondary role none;
select * from db1.t1 order by a;
-- @session

-- 12. Same-table rules with different output columns use the last encountered rule
create role test_rule_role_multi_c;
create role test_rule_role_multi_d;
alter role test_rule_role_multi_c add rule "select * from db1.t1 where age > 28" on table db1.t1;
alter role test_rule_role_multi_d add rule "select a from db1.t1 where a = 2" on table db1.t1;
create user test_rule_user_multi_diff identified by '123456' default role test_rule_role_multi_c;
grant connect on account * to test_rule_role_multi_c;
grant select on table db1.t1 to test_rule_role_multi_c;
grant select on table db1.t1 to test_rule_role_multi_d;
grant test_rule_role_multi_d to test_rule_user_multi_diff;
-- @session:id=3&user=sys:test_rule_user_multi_diff:test_rule_role_multi_c&password=123456
set enable_remap_hint = 1;
set secondary role all;
select * from db1.t1 order by a;
-- @session

-- 13. Inherited roles contribute rewrite rules
create role test_rule_role_inherit_a;
create role test_rule_role_inherit_b;
alter role test_rule_role_inherit_b add rule "select a, age from db2.t2 where age > 30" on table db2.t2;
create user test_rule_user_inherit identified by '123456' default role test_rule_role_inherit_a;
grant connect on account * to test_rule_role_inherit_a;
grant select on table db2.t2 to test_rule_role_inherit_b;
grant test_rule_role_inherit_b to test_rule_role_inherit_a;
-- @session:id=4&user=sys:test_rule_user_inherit:test_rule_role_inherit_a&password=123456
set enable_remap_hint = 1;
select * from db2.t2 order by a;
-- @session

-- 14. Invalid rewrite SQL is rejected before overwriting existing rule
create role test_rule_role_validate;
alter role test_rule_role_validate add rule "select a, age from db1.t1 where age > 28" on table db1.t1;
alter role test_rule_role_validate add rule "delete from db1.t1 where a = 1" on table db1.t1;
show rules on role test_rule_role_validate;

-- 15. ORDER BY/LIMIT rules are not merged and the later compatible rule wins
create role test_rule_role_unmergeable_a;
create role test_rule_role_unmergeable_b;
alter role test_rule_role_unmergeable_a add rule "select a, age from db1.t1 where age > 28 order by a limit 1" on table db1.t1;
alter role test_rule_role_unmergeable_b add rule "select a, age from db1.t1 where age < 3" on table db1.t1;
create user test_rule_user_unmergeable identified by '123456' default role test_rule_role_unmergeable_a;
grant connect on account * to test_rule_role_unmergeable_a;
grant select on table db1.t1 to test_rule_role_unmergeable_a;
grant select on table db1.t1 to test_rule_role_unmergeable_b;
grant test_rule_role_unmergeable_b to test_rule_user_unmergeable;
-- @session:id=5&user=sys:test_rule_user_unmergeable:test_rule_role_unmergeable_a&password=123456
set enable_remap_hint = 1;
set secondary role all;
select * from db1.t1 order by a;
-- @session

-- 16. Same output names with swapped expressions are not merged
create role test_rule_role_alias_a;
create role test_rule_role_alias_b;
alter role test_rule_role_alias_a add rule "select a, age from db1.t1 where age >= 1" on table db1.t1;
alter role test_rule_role_alias_b add rule "select age as a, a as age from db1.t1 where a = 100" on table db1.t1;
create user test_rule_user_alias identified by '123456' default role test_rule_role_alias_a;
grant connect on account * to test_rule_role_alias_a;
grant select on table db1.t1 to test_rule_role_alias_a;
grant select on table db1.t1 to test_rule_role_alias_b;
grant test_rule_role_alias_b to test_rule_user_alias;
-- @session:id=6&user=sys:test_rule_user_alias:test_rule_role_alias_a&password=123456
set enable_remap_hint = 1;
set secondary role all;
select * from db1.t1 order by a;
-- @session

-- 17. Rules with and without WHERE are merged correctly
create role test_rule_role_where_a;
create role test_rule_role_where_b;
alter role test_rule_role_where_a add rule "select * from db1.t1 where age > 28" on table db1.t1;
alter role test_rule_role_where_b add rule "select * from db1.t1" on table db1.t1;
create user test_rule_user_where identified by '123456' default role test_rule_role_where_a;
grant connect on account * to test_rule_role_where_a;
grant select on table db1.t1 to test_rule_role_where_a;
grant select on table db1.t1 to test_rule_role_where_b;
grant test_rule_role_where_b to test_rule_user_where;
-- @session:id=7&user=sys:test_rule_user_where:test_rule_role_where_a&password=123456
set enable_remap_hint = 1;
set secondary role all;
select * from db1.t1 order by a;
-- @session

-- 18. Partial projections keep duplicate projected values from distinct visible rows
create role test_rule_role_dup_projection_a;
create role test_rule_role_dup_projection_b;
alter role test_rule_role_dup_projection_a add rule "select a from db1.t_dup where marker = 1" on table db1.t_dup;
alter role test_rule_role_dup_projection_b add rule "select a from db1.t_dup where marker = 2" on table db1.t_dup;
create user test_rule_user_dup_projection identified by '123456' default role test_rule_role_dup_projection_a;
grant connect on account * to test_rule_role_dup_projection_a;
grant select on table db1.t_dup to test_rule_role_dup_projection_a;
grant select on table db1.t_dup to test_rule_role_dup_projection_b;
grant test_rule_role_dup_projection_b to test_rule_user_dup_projection;
-- @session:id=8&user=sys:test_rule_user_dup_projection:test_rule_role_dup_projection_a&password=123456
set enable_remap_hint = 1;
select count(*) from db1.t_dup;
set secondary role all;
select a, count(*) from db1.t_dup group by a order by a;
select count(*) from db1.t_dup;
-- @session

-- 19. Row-wise expression projections also merge without dropping role visibility
create role test_rule_role_expr_projection_a;
create role test_rule_role_expr_projection_b;
alter role test_rule_role_expr_projection_a add rule "select a + 1 as b from db1.t_dup where marker = 1" on table db1.t_dup;
alter role test_rule_role_expr_projection_b add rule "select a + 1 as b from db1.t_dup where marker = 2" on table db1.t_dup;
create user test_rule_user_expr_projection identified by '123456' default role test_rule_role_expr_projection_a;
grant connect on account * to test_rule_role_expr_projection_a;
grant select on table db1.t_dup to test_rule_role_expr_projection_a;
grant select on table db1.t_dup to test_rule_role_expr_projection_b;
grant test_rule_role_expr_projection_b to test_rule_user_expr_projection;
-- @session:id=9&user=sys:test_rule_user_expr_projection:test_rule_role_expr_projection_a&password=123456
set enable_remap_hint = 1;
set secondary role all;
select b, count(*) from db1.t_dup group by b order by b;
-- @session

-- cleanup all test resources
drop user if exists test_rule_user;
drop user if exists test_rule_user_multi;
drop user if exists test_rule_user_multi_diff;
drop user if exists test_rule_user_inherit;
drop user if exists test_rule_user_unmergeable;
drop user if exists test_rule_user_alias;
drop user if exists test_rule_user_where;
drop user if exists test_rule_user_dup_projection;
drop user if exists test_rule_user_expr_projection;
drop role if exists test_rule_role;
drop role if exists test_rule_role_multi_a;
drop role if exists test_rule_role_multi_b;
drop role if exists test_rule_role_multi_c;
drop role if exists test_rule_role_multi_d;
drop role if exists test_rule_role_inherit_a;
drop role if exists test_rule_role_inherit_b;
drop role if exists test_rule_role_validate;
drop role if exists test_rule_role_unmergeable_a;
drop role if exists test_rule_role_unmergeable_b;
drop role if exists test_rule_role_alias_a;
drop role if exists test_rule_role_alias_b;
drop role if exists test_rule_role_where_a;
drop role if exists test_rule_role_where_b;
drop role if exists test_rule_role_dup_projection_a;
drop role if exists test_rule_role_dup_projection_b;
drop role if exists test_rule_role_expr_projection_a;
drop role if exists test_rule_role_expr_projection_b;
drop database if exists db1;
drop database if exists db2;
set global enable_privilege_cache = on;
