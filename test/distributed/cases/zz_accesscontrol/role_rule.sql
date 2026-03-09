set global enable_privilege_cache = off;
-- cleanup residual resources
drop user if exists test_rule_user;
drop role if exists test_rule_role;
drop database if exists db1;
create database db1;
create table db1.t1(a int, age int);
insert into db1.t1 values (1,1),(2,2),(100,30);

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
alter role test_rule_role drop rule "db1.t1";
show rules on role test_rule_role;

-- 6. DROP RULE error case: non-existent role
alter role non_existent_role drop rule "db1.t1";

-- 7. DROP RULE error case: non-existent rule
alter role test_rule_role drop rule "no_such_rule";

-- 8. SHOW RULES empty result set
alter role test_rule_role drop rule "db2.t2";
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

-- cleanup all test resources
drop user if exists test_rule_user;
drop role if exists test_rule_role;
drop database if exists db1;
set global enable_privilege_cache = on;
