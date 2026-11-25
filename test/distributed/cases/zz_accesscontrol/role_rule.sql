set global enable_privilege_cache = off;
-- cleanup residual resources
drop user if exists test_rule_user;
drop user if exists test_rule_user_show;
drop user if exists test_rule_user_multi;
drop user if exists test_rule_user_multi_diff;
drop user if exists test_rule_user_inherit;
drop user if exists test_rule_user_unmergeable;
drop user if exists test_rule_user_alias;
drop user if exists test_rule_user_where;
drop user if exists test_rule_user_dup_projection;
drop user if exists test_rule_user_expr_projection;
drop user if exists test_rule_user_columns;
drop user if exists test_rule_user_legacy;
drop user if exists test_rule_user_dependency;
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
drop role if exists test_rule_role_columns;
drop role if exists test_rule_role_legacy;
drop role if exists test_rule_role_dependency;
drop database if exists db1;
drop database if exists db2;
create database db1;
create table db1.t1(a int, age int);
insert into db1.t1 values (1,1),(2,2),(100,30);
create table db1.t_dup(a int, marker int);
insert into db1.t_dup values (1,1),(1,2),(2,3);

-- 0. Rename admission keeps tables without role rules working and closes the
-- shared cluster-table path conservatively.
create table db1.t_rename_no_rule(a int);
alter table db1.t_rename_no_rule rename to db1.t_rename_no_rule_after;
rename table db1.t_rename_no_rule_after to db1.t_rename_no_rule;
create table db1.`t.with.dot`(a int);
alter table db1.`t.with.dot` rename to db1.`t.with.dot.after`;
rename table db1.`t.with.dot.after` to db1.`t.with.dot`;
create table db1.t_rename_chain_ok_a(a int);
create table db1.t_rename_chain_ok_b(a int);
rename table db1.t_rename_chain_ok_a to db1.t_rename_chain_ok_a_after, db1.t_rename_chain_ok_b to db1.t_rename_chain_ok_b_after;
rename table db1.t_rename_chain_ok_a_after to db1.t_rename_chain_ok_a, db1.t_rename_chain_ok_b_after to db1.t_rename_chain_ok_b;

-- 1. ADD RULE normal case + SHOW RULES verification
create role test_rule_role;
alter role test_rule_role add rule "select * from db1.t1 where age > 28" on table db1.t1;
show rules on role test_rule_role;

-- 2. ADD RULE error case: non-existent role
alter role non_existent_role add rule "select * from db1.t1" on table db1.t1;

-- 3. Rule update case: same table overwrites existing rule
alter role test_rule_role add rule "select * from db1.t1 where age > 50" on table db1.t1;
show rules on role test_rule_role;

create database db2;
create table db2.t2(a int, age int);
insert into db2.t2 values (10,10),(20,35),(200,60);

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
grant create table,drop table on database db1 to test_rule_role;
-- @session:id=1&user=sys:test_rule_user:test_rule_role&password=123456
set enable_remap_hint = 1;
select * from db1.t1;
-- CTAS must apply the same row rule to its generated INSERT ... SELECT.
create table db1.t1_ctas as select a from db1.t1 order by a;
select * from db1.t1_ctas order by a;
drop table db1.t1_ctas;
-- #29142: disabling the optional remap hint must not bypass mandatory role rules
set enable_remap_hint = 0;
select * from db1.t1;
-- @session

-- Ordinary views must preserve the base-table row rule.
create view db1.v_t1 as select * from db1.t1;
grant select on view db1.v_t1 to test_rule_role;
-- @session:id=1&user=sys:test_rule_user:test_rule_role&password=123456
set enable_remap_hint = 1;
select * from db1.v_t1;
set enable_remap_hint = 0;
select * from db1.v_t1;
-- @session

-- #31695: metadata must respect the same column rule as SELECT.
create table db1.t_columns(id int, tenant int, amount int, secret varchar(20));
create table db1.t_hidden(id int, secret varchar(20));
create table db1.t_columns_unruled(id int, public_note varchar(20));
insert into db1.t_columns values (1, 1, 100, 's1'), (2, 2, 200, 's2');
create role test_rule_role_columns;
alter role test_rule_role_columns add rule "select id, amount from db1.t_columns where tenant = 1" on table db1.t_columns;
create user test_rule_user_columns identified by '123456' default role test_rule_role_columns;
grant connect on account * to test_rule_role_columns;
grant select on table db1.t_columns to test_rule_role_columns;
grant select on table db1.t_columns_unruled to test_rule_role_columns;
-- @session:id=21&user=sys:test_rule_user_columns:test_rule_role_columns&password=123456
set enable_remap_hint = 1;
select * from db1.t_columns order by id;
select column_name from information_schema.columns where table_schema = 'db1' and table_name = 't_columns' order by ordinal_position;
desc db1.t_columns;
show create table db1.t_columns;
use db1;
desc t_columns;
show create table t_columns;
desc db1.t_hidden;
show create table db1.t_hidden;
select count(*) from information_schema.columns where table_schema = 'db1' and table_name = 't_hidden';
desc db1.t_columns_unruled;
show create table db1.t_columns_unruled;
set enable_remap_hint = 0;
select column_name from information_schema.columns where table_schema = 'db1' and table_name = 't_columns' order by ordinal_position;
-- @session

-- Extra database visibility still leaves the effective column rule in place.
grant show databases on account * to test_rule_role_columns;
-- @session:id=28&user=sys:test_rule_user_columns:test_rule_role_columns&password=123456
desc db1.t_columns;
show create table db1.t_columns;
-- @session
revoke show databases on account * from test_rule_role_columns;

-- The administrator can see the complete structure of the same table.
desc db1.t_columns;
show create table db1.t_columns;

-- #31674: accepted rule text must survive hint injection on new connections.
show rules on role test_rule_role_columns;
-- @session:id=23&user=sys:test_rule_user_columns:test_rule_role_columns&password=123456
set names utf8mb4;
select * from db1.t_columns order by id;
-- @session

alter role test_rule_role_columns add rule "select id, amount from db1.t_columns where tenant = 1 /* harmless */" on table db1.t_columns;
show rules on role test_rule_role_columns;
-- @session:id=24&user=sys:test_rule_user_columns:test_rule_role_columns&password=123456
set names utf8mb4;
select * from db1.t_columns order by id;
prepare rule_stmt from 'select * from db1.t_columns order by id';
execute rule_stmt;
deallocate prepare rule_stmt;
-- @session

alter role test_rule_role_columns add rule "select id, amount from db1.t_columns where tenant = 1 and '*/' = '*/'" on table db1.t_columns;
show rules on role test_rule_role_columns;
-- @session:id=25&user=sys:test_rule_user_columns:test_rule_role_columns&password=123456
set names utf8mb4;
select * from db1.t_columns order by id;
-- @session

-- Rejected multi-statement replacement must leave the installed rule intact.
alter role test_rule_role_columns add rule "select id, amount from db1.t_columns; select * from db1.t_columns" on table db1.t_columns;
show rules on role test_rule_role_columns;
-- @session:id=26&user=sys:test_rule_user_columns:test_rule_role_columns&password=123456
set names utf8mb4;
select * from db1.t_columns order by id;
-- @session
alter role test_rule_role_columns add rule "select id, amount from db1.t_columns where tenant = 1" on table db1.t_columns;
show rules on role test_rule_role_columns;
-- @session:id=27&user=sys:test_rule_user_columns:test_rule_role_columns&password=123456
set names utf8mb4;
select * from db1.t_columns order by id;
-- @session

-- 11. Table renames are rejected while role rewrite metadata exists
alter table db1.t1 rename to db1.t1_renamed;
rename table db1.t1 to db1.t1_renamed;
-- @session:id=10&user=sys:test_rule_user:test_rule_role&password=123456
set enable_remap_hint = 1;
select * from db1.t1;
-- @session

create table db1.t_rename_chain_guard(a int);
insert into db1.t_rename_chain_guard values (7);
rename table db1.t_rename_chain_guard to db1.t_rename_chain_guard_after, db1.t1 to db1.t1_renamed;
select * from db1.t_rename_chain_guard;

-- 11a. Legacy noncanonical rule keys still guard both rename entry points
create table db1.t_rename_legacy(a int, age int);
insert into db1.t_rename_legacy values (1,1),(100,30);
create role test_rule_role_legacy;
alter role test_rule_role_legacy add rule "select * from db1.t_rename_legacy where age > 28" on table db1.` t_rename_legacy`;
create user test_rule_user_legacy identified by '123456' default role test_rule_role_legacy;
grant connect on account * to test_rule_role_legacy;
grant select on table db1.t_rename_legacy to test_rule_role_legacy;
-- @session:id=11&user=sys:test_rule_user_legacy:test_rule_role_legacy&password=123456
set enable_remap_hint = 1;
select * from db1.t_rename_legacy order by a;
-- @session

alter table db1.t_rename_legacy rename to db1.t_rename_legacy_after;
rename table db1.t_rename_legacy to db1.t_rename_legacy_after;
-- @session:id=11&user=sys:test_rule_user_legacy:test_rule_role_legacy&password=123456
set enable_remap_hint = 1;
select * from db1.t_rename_legacy order by a;
-- @session

-- 11b. A rule's joined allowlist relation cannot be renamed out from under it
create table db1.t_rename_rule_dependency(id int primary key, tenant_id int);
insert into db1.t_rename_rule_dependency values (1,1),(2,2);
create table db1.t_rename_rule_allowlist(tenant_id int);
insert into db1.t_rename_rule_allowlist values (1);
create role test_rule_role_dependency;
alter role test_rule_role_dependency add rule "select target.id, target.tenant_id from db1.t_rename_rule_dependency as target join db1.t_rename_rule_allowlist as access_list on target.tenant_id = access_list.tenant_id" on table db1.t_rename_rule_dependency;
create user test_rule_user_dependency identified by '123456' default role test_rule_role_dependency;
grant connect on account * to test_rule_role_dependency;
grant select on table db1.t_rename_rule_dependency to test_rule_role_dependency;
grant select on table db1.t_rename_rule_allowlist to test_rule_role_dependency;
-- @session:id=12&user=sys:test_rule_user_dependency:test_rule_role_dependency&password=123456
set enable_remap_hint = 1;
select id, tenant_id from db1.t_rename_rule_dependency order by id;
-- @session
alter table db1.t_rename_rule_allowlist rename to db1.t_rename_rule_allowlist_old;
rename table db1.t_rename_rule_allowlist to db1.t_rename_rule_allowlist_old;
-- @session:id=12&user=sys:test_rule_user_dependency:test_rule_role_dependency&password=123456
set enable_remap_hint = 1;
select id, tenant_id from db1.t_rename_rule_dependency order by id;
-- @session

-- 12. SET SECONDARY ROLE ALL merges select * rewrite rules from all active roles
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

-- 相同 COLLATE 投影必须合并主角色和继承角色允许的行。
alter role test_rule_role_inherit_a add rule "select a, cast(age as char) collate utf8mb4_bin as age from db2.t2 where a = 10" on table db2.t2;
alter role test_rule_role_inherit_b add rule "select a, cast(age as char) collate utf8mb4_bin as age from db2.t2 where a = 20" on table db2.t2;
-- @session:id=22&user=sys:test_rule_user_inherit:test_rule_role_inherit_a&password=123456
set enable_remap_hint = 1;
select * from db2.t2 order by a;
-- @session

-- A leading-zero code must remain distinct from its unpadded spelling.
create table db1.t_rule_text(id int, code varchar(20));
insert into db1.t_rule_text values (1, '0001'), (2, '1'), (3, '销售订单'), (4, 'O''Brien');
grant select on table db1.t_rule_text to test_rule_role_inherit_a;
grant select on table db1.t_rule_text to test_rule_role_inherit_b;
alter role test_rule_role_inherit_a add rule "select * from db1.t_rule_text where code = '0001'" on table db1.t_rule_text;
-- @session:id=4&user=sys:test_rule_user_inherit:test_rule_role_inherit_a&password=123456
select id from db1.t_rule_text order by id;
-- @session
alter role test_rule_role_inherit_b add rule "select * from db1.t_rule_text where trim(code) = '销售订单'" on table db1.t_rule_text;
-- @session:id=4&user=sys:test_rule_user_inherit:test_rule_role_inherit_a&password=123456
select id from db1.t_rule_text order by id;
-- @session
alter role test_rule_role_inherit_b add rule "select * from db1.t_rule_text where code = 'O''Brien'" on table db1.t_rule_text;
-- @session:id=4&user=sys:test_rule_user_inherit:test_rule_role_inherit_a&password=123456
select id from db1.t_rule_text order by id;
-- @session

-- Double quotes denote a string or an identifier depending on the reader.
alter role test_rule_role_inherit_a add rule 'select "code" as x from db1.t_rule_text where (select true)' on table db1.t_rule_text;
alter role test_rule_role_inherit_b add rule "select 'code' as x from db1.t_rule_text where id = 3" on table db1.t_rule_text;
-- @session:id=4&user=sys:test_rule_user_inherit:test_rule_role_inherit_a&password=123456
set session sql_mode = '';
select x, count(*) from db1.t_rule_text group by x;
set session sql_mode = 'ANSI_QUOTES';
select x, count(*) from db1.t_rule_text group by x;
set session sql_mode = '';
select x, count(*) from db1.t_rule_text group by x;
-- @session

-- Restore SQL mode even when the active role rule cannot parse in that mode.
alter role test_rule_role_inherit_b drop rule on table db1.t_rule_text;
alter role test_rule_role_inherit_a add rule "select * from db1.t_rule_text where code = 'O\\'Brien'" on table db1.t_rule_text;
-- @session:id=4&user=sys:test_rule_user_inherit:test_rule_role_inherit_a&password=123456
select id from db1.t_rule_text;
set session sql_mode = 'NO_BACKSLASH_ESCAPES';
select id from db1.t_rule_text;
commit;
rollback;
set session sql_mode = '';
select id from db1.t_rule_text;
-- @session

-- Quoted names referring to the same charset and collation keep both roles' rows.
alter role test_rule_role_inherit_a add rule "select id, cast(code as char character set 'UTF8MB4') collate 'UTF8MB4_BIN' as code from db1.t_rule_text where id = 1" on table db1.t_rule_text;
alter role test_rule_role_inherit_b add rule "select id, cast(code as char character set 'utf8mb4') collate 'utf8mb4_bin' as code from db1.t_rule_text where id = 3" on table db1.t_rule_text;
-- @session:id=4&user=sys:test_rule_user_inherit:test_rule_role_inherit_a&password=123456
select id, code from db1.t_rule_text order by id;
-- @session

-- Quoted and unquoted references to the same column keep both roles' rows.
alter role test_rule_role_inherit_a add rule "select trim(`Code`) as x from db1.t_rule_text where id = 1" on table db1.t_rule_text;
alter role test_rule_role_inherit_b add rule "select trim(code) as x from db1.t_rule_text where id = 3" on table db1.t_rule_text;
-- @session:id=4&user=sys:test_rule_user_inherit:test_rule_role_inherit_a&password=123456
select x from db1.t_rule_text order by x;
-- @session

-- Role-rule output names must stay bindable through the derived result.
alter role test_rule_role_inherit_a add rule "select _binary 'A' from db1.t_rule_text where id = 1" on table db1.t_rule_text;
alter role test_rule_role_inherit_b add rule "select _binary 0x41 from db1.t_rule_text where id = 3" on table db1.t_rule_text;
-- @session:id=4&user=sys:test_rule_user_inherit:test_rule_role_inherit_a&password=123456
select count(`0x41`), min(hex(`0x41`)) from db1.t_rule_text;
-- @session
alter role test_rule_role_inherit_a add rule "select _binary 'A' as code from db1.t_rule_text where id = 1" on table db1.t_rule_text;
alter role test_rule_role_inherit_b add rule "select _binary 0x41 as code from db1.t_rule_text where id = 3" on table db1.t_rule_text;
-- @session:id=4&user=sys:test_rule_user_inherit:test_rule_role_inherit_a&password=123456
select count(code), min(hex(code)) from db1.t_rule_text;
-- @session

-- The table alias must bind in the reader's table-name mode.
alter role test_rule_role_inherit_a add rule "select id from db1.t_rule_text as `X` where id = 1" on table db1.t_rule_text;
alter role test_rule_role_inherit_b add rule "select id from db1.t_rule_text as x where x.id = 3" on table db1.t_rule_text;
-- @session:id=4&user=sys:test_rule_user_inherit:test_rule_role_inherit_a&password=123456
select id from db1.t_rule_text order by id;
-- @session
set global lower_case_table_names = 0;
-- @session:id=28&user=sys:test_rule_user_inherit:test_rule_role_inherit_a&password=123456
select @@lower_case_table_names;
select id from db1.t_rule_text order by id;
-- @session
set global lower_case_table_names = 1;

-- System catalog source names compare without case in every table-name mode.
grant select on table information_schema.character_sets to test_rule_role_inherit_a;
alter role test_rule_role_inherit_a add rule "select character_set_name, maxlen from information_schema.character_sets where maxlen = 1" on table information_schema.character_sets;
alter role test_rule_role_inherit_b add rule "select character_set_name, maxlen from INFORMATION_SCHEMA.CHARACTER_SETS where INFORMATION_SCHEMA.CHARACTER_SETS.maxlen = 4" on table information_schema.character_sets;
-- @session:id=4&user=sys:test_rule_user_inherit:test_rule_role_inherit_a&password=123456
select character_set_name from information_schema.character_sets where character_set_name in ('binary', 'utf8mb4') order by character_set_name;
-- @session
set global lower_case_table_names = 0;
-- @session:id=29&user=sys:test_rule_user_inherit:test_rule_role_inherit_a&password=123456
select @@lower_case_table_names;
select character_set_name from information_schema.character_sets where character_set_name in ('binary', 'utf8mb4') order by character_set_name;
-- @session
-- Only a missing inner qualifier may resolve in the outer scope.
-- @session:id=31&user=sys:dump&password=111
select (select ChArAcTeR_SeTs.maxlen from (select 1 as marker) as n) as maxlen from (select 73 as maxlen) as ChArAcTeR_SeTs;
select (select ChArAcTeR_SeTs.maxlen from information_schema.character_sets where character_set_name = 'binary') as maxlen from (select 73 as maxlen) as ChArAcTeR_SeTs;
select (select ChArAcTeR_SeTs.maxlen from information_schema.character_sets cross join INFORMATION_SCHEMA.CHARACTER_SETS where character_sets.character_set_name = 'binary' and CHARACTER_SETS.character_set_name = 'binary') as maxlen from (select 73 as maxlen) as ChArAcTeR_SeTs;
-- @session
set global lower_case_table_names = 2;
-- @session:id=30&user=sys:test_rule_user_inherit:test_rule_role_inherit_a&password=123456
select @@lower_case_table_names;
select character_set_name from information_schema.character_sets where character_set_name in ('binary', 'utf8mb4') order by character_set_name;
-- @session
set global lower_case_table_names = 1;

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

-- 17. Merging with a no-WHERE rule produces the broader rule
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

-- 20. SHOW RULES privilege: ordinary user cannot disclose another role's rules (#29159)
create user test_rule_user_show identified by '123456';
-- @session:id=20&user=sys:test_rule_user_show&password=123456
show rules on role test_rule_role_validate;
-- @session

-- cleanup all test resources
drop user if exists test_rule_user;
drop user if exists test_rule_user_show;
drop user if exists test_rule_user_multi;
drop user if exists test_rule_user_multi_diff;
drop user if exists test_rule_user_inherit;
drop user if exists test_rule_user_unmergeable;
drop user if exists test_rule_user_alias;
drop user if exists test_rule_user_where;
drop user if exists test_rule_user_dup_projection;
drop user if exists test_rule_user_expr_projection;
drop user if exists test_rule_user_columns;
drop user if exists test_rule_user_legacy;
drop user if exists test_rule_user_dependency;
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
drop role if exists test_rule_role_columns;
drop role if exists test_rule_role_legacy;
drop role if exists test_rule_role_dependency;
drop database if exists db1;
drop database if exists db2;
select count(*) from mo_catalog.mo_user where user_name in ('test_rule_user','test_rule_user_show','test_rule_user_multi','test_rule_user_multi_diff','test_rule_user_inherit','test_rule_user_unmergeable','test_rule_user_alias','test_rule_user_where','test_rule_user_dup_projection','test_rule_user_expr_projection','test_rule_user_columns','test_rule_user_legacy','test_rule_user_dependency');
select count(*) from mo_catalog.mo_role where role_name in ('test_rule_role','test_rule_role_multi_a','test_rule_role_multi_b','test_rule_role_multi_c','test_rule_role_multi_d','test_rule_role_inherit_a','test_rule_role_inherit_b','test_rule_role_validate','test_rule_role_unmergeable_a','test_rule_role_unmergeable_b','test_rule_role_alias_a','test_rule_role_alias_b','test_rule_role_where_a','test_rule_role_where_b','test_rule_role_dup_projection_a','test_rule_role_dup_projection_b','test_rule_role_expr_projection_a','test_rule_role_expr_projection_b','test_rule_role_columns','test_rule_role_legacy','test_rule_role_dependency');
select count(*) from mo_catalog.mo_role_rule where rule_name in ('db1.t1','db2.t2','db1.t_rename_legacy','db1. t_rename_legacy','db1.t_dup','db1.t_columns','db1.t_rename_rule_dependency');
select count(*) from mo_catalog.mo_tables where reldatabase in ('db1','db2') and relname in ('t_rename_no_rule','t_rename_no_rule_after','t.with.dot','t.with.dot.after','t_rename_chain_ok_a','t_rename_chain_ok_a_after','t_rename_chain_ok_b','t_rename_chain_ok_b_after','t_rename_chain_guard','t_rename_chain_guard_after','t_rename_legacy','t_rename_legacy_after','t_rename_rule_dependency','t_rename_rule_allowlist','t_rename_rule_allowlist_old','t_columns','t_hidden','t_columns_unruled');
select count(*) from mo_catalog.mo_database where datname in ('db1','db2');
set global enable_privilege_cache = on;
